import pytest
import json
import os
import csv
import vcr
import copy
from pathlib import Path
from freezegun import freeze_time
from component import Component
from output_validator import SnapshotManager

# Constants
TEST_DIR = Path("tests/fixtures")
CASSETTES_DIR = TEST_DIR / "cassettes"
SECRETS_FILE = TEST_DIR / "config.secrets.json"
QUERIES_SANITIZED_FILE = TEST_DIR / "queries_sanitized.csv"
SNAPSHOTS_FILE = TEST_DIR / "output_snapshots.json"
FIXED_DATETIME = "2025-01-01 12:00:00"

# Load snapshot manager once
snapshot_manager = SnapshotManager(SNAPSHOTS_FILE)


def load_configs():
    """
    Load test configurations from sanitized CSV queries.

    Creates test cases by:
    1. Loading queries from queries_sanitized.csv
    2. Grouping by component (Facebook Ads, Facebook Pages, Instagram)
    3. Creating separate test cases for each API version (v22.0, v23.0)
    4. Assigning sequential IDs to queries (matching cassette generation)

    Returns:
        List of test case dicts with name, description, action, and params
    """
    cases = []

    # Load generated cases from sanitized CSV
    # CI fallback: use config.secrets.json.ci if main secrets file doesn't exist
    effective_secrets_file = SECRETS_FILE
    if not SECRETS_FILE.exists() and (TEST_DIR / "config.secrets.json.ci").exists():
        effective_secrets_file = TEST_DIR / "config.secrets.json.ci"

    if QUERIES_SANITIZED_FILE.exists() and effective_secrets_file.exists():
        try:
            with open(effective_secrets_file) as f:
                secrets = json.load(f)

            # Use placeholders for secrets during re-run
            # The cassettes already have 'token' replaced
            secrets_placeholder = copy.deepcopy(secrets)
            if "authorization" in secrets_placeholder:
                # We want the test runner to use 'token' as the value so it matches the recorded VCR filter
                creds = (
                    secrets_placeholder["authorization"]
                    .get("oauth_api", {})
                    .get("credentials", {})
                )
                if "token" in creds:
                    creds["token"] = "token"
                if "access_token" in creds:
                    creds["access_token"] = "token"
                if "#data" in creds:
                    try:
                        data = json.loads(creds["#data"])
                        data["access_token"] = "token"
                        creds["#data"] = json.dumps(data)
                    except:
                        pass

            # Grouping logic identical to generate_tests.py
            component_queries = {}
            with open(QUERIES_SANITIZED_FILE, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    comp_id = row.get("kbc_component_id", "Facebook Ads V2")
                    q_type = row.get("query_type", "nested-query")
                    json_str = row.get("query_json", "")

                    if not json_str:
                        continue

                    try:
                        q = json.loads(json_str)

                        # Normalize query object (matching generate_tests.py logic)
                        real_query_params = (
                            q.get("query", q) if isinstance(q.get("query"), dict) else q
                        )
                        if "limit" in real_query_params and real_query_params["limit"]:
                            real_query_params["limit"] = str(real_query_params["limit"])

                        q_obj = {
                            "id": q.get("id"),  # Will be None for CSV queries
                            "type": q_type,
                            "name": q.get("name", "query"),
                            "query": real_query_params,
                            "run-by-id": q.get("run-by-id", False),
                        }

                        if comp_id not in component_queries:
                            component_queries[comp_id] = []
                        component_queries[comp_id].append(q_obj)
                    except json.JSONDecodeError:
                        continue

            for version in ["v22.0", "v23.0"]:
                for comp_id, queries_raw in component_queries.items():
                    comp_clean = comp_id.lower().replace(" ", "_")
                    version_clean = version.replace(".", "_")
                    case_name = f"gen_{comp_clean}_{version_clean}"

                    # Normalize and add technical IDs if missing (matching generate_tests.py)
                    final_queries = []
                    for i, q in enumerate(queries_raw):
                        # Check if q is already a full object or just parameters
                        if "query" in q and isinstance(q["query"], dict) and "id" in q:
                            # Likely already full object, just ensure id is int
                            try:
                                q["id"] = int(q["id"])
                            except (TypeError, ValueError):
                                q["id"] = i + 1
                            final_queries.append(q)
                        else:
                            # q is parameters or incomplete object
                            # Reconstruct full object
                            final_queries.append(
                                {
                                    "id": i + 1,
                                    "name": q.get("name", f"query_{i + 1}"),
                                    "type": q.get("type", "nested-query"),
                                    "query": q if "query" not in q else q["query"],
                                    "run-by-id": q.get("run-by-id", False),
                                }
                            )

                    config = copy.deepcopy(secrets_placeholder)
                    if "parameters" not in config:
                        config["parameters"] = {}
                    config["parameters"]["queries"] = final_queries
                    config["parameters"]["api-version"] = version

                    cases.append(
                        {
                            "name": case_name,
                            "description": f"Sanitized queries for {comp_id} (API {version})",
                            "action": "run",
                            "params": config,
                        }
                    )

        except Exception as e:
            print(f"Warning: Failed to load sanitized CSV cases: {e}")

    return cases


@pytest.mark.parametrize("config_data", load_configs())
@freeze_time(FIXED_DATETIME)
def test_functional_component(config_data, tmpdir, monkeypatch):
    """
    Functional test: Run component with VCR cassettes and validate outputs.

    Flow:
    1. Load test configuration with queries from queries_sanitized.csv
    2. Setup temp directory and write config.json
    3. Run component with VCR in replay-only mode (uses cassette responses)
    4. Verify output CSV tables were generated
    5. Validate output matches saved snapshot (row counts, columns, hashes)

    This ensures the component produces consistent outputs from recorded API responses.
    """
    # Setup Environment
    monkeypatch.setenv("KBC_DATADIR", str(tmpdir))
    os.makedirs(os.path.join(tmpdir, "out", "tables"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "out", "files"), exist_ok=True)

    params = copy.deepcopy(config_data.get("params", {}))
    params["action"] = config_data.get("action", "run")

    # Ensure authorization exists for the Component to be happy
    if "authorization" not in params:
        params["authorization"] = {
            "oauth_api": {"credentials": {"token": "token", "access_token": "token"}}
        }

    with open(tmpdir.join("config.json"), "w") as f:
        json.dump(params, f)

    # 3. Setup VCR
    cassette_name = f"{config_data['name']}.json"
    cassette_path = CASSETTES_DIR / cassette_name

    if not cassette_path.exists():
        pytest.fail(
            f"Cassette {cassette_name} not found. Please run 'python scripts/generate_tests.py' to generate it."
        )

    my_vcr = vcr.VCR(
        cassette_library_dir=str(CASSETTES_DIR),
        record_mode="none",  # REPLAY ONLY - ensures we don't hit live API
        match_on=["method", "scheme", "host", "port", "path", "query", "body"],
        filter_headers=[("Authorization", "Bearer token")],
        filter_query_parameters=[("access_token", "token")],
        decode_compressed_response=True,
        serializer="json",
    )

    with my_vcr.use_cassette(cassette_name):
        comp = Component()
        if config_data.get("action") == "run":
            comp.run()
        else:
            comp.execute_action()

    # Verification: Ensure some data was written to tables
    out_tables_dir = Path(tmpdir) / "out" / "tables"
    found_tables = list(out_tables_dir.glob("*.csv"))
    assert len(found_tables) > 0 or config_data.get("action") != "run", (
        "Component produced no output tables"
    )

    # Validate outputs against snapshot
    test_name = config_data["name"]
    if snapshot_manager.has_snapshot(test_name):
        validation_errors = snapshot_manager.validate_snapshot(test_name, tmpdir)
        if validation_errors:
            error_msg = f"Output validation failed for {test_name}:\n"
            error_msg += "\n".join(f"  - {error}" for error in validation_errors)
            pytest.fail(error_msg)
        print(f"✓ Output validation passed for {test_name}")
    else:
        print(f"⚠ No snapshot found for {test_name} - skipping output validation")


# Snapshot Infrastructure Tests


def test_snapshot_capture_and_validation(tmpdir):
    """Test capturing and validating output snapshots."""
    from output_validator import OutputSnapshot
    from tempfile import TemporaryDirectory

    # Create a temporary directory with mock component outputs
    with TemporaryDirectory() as temp:
        tmpdir = Path(temp)

        # Create output structure
        tables_dir = tmpdir / "out" / "tables"
        tables_dir.mkdir(parents=True)

        # Create a sample CSV
        sample_csv = tables_dir / "campaigns.csv"
        with open(sample_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "impressions"])
            writer.writeheader()
            writer.writerow(
                {"id": "123", "name": "Test Campaign", "impressions": "1000"}
            )
            writer.writerow(
                {"id": "456", "name": "Another Campaign", "impressions": "2000"}
            )

        # Create a sample manifest
        manifest_file = tables_dir / "campaigns.csv.manifest"
        manifest_data = {
            "incremental": True,
            "primary_key": ["id"],
            "columns": ["id", "name", "impressions"],
        }
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f)

        # Test 1: Capture snapshot
        snapshot = OutputSnapshot("test_case_1", tmpdir)
        captured = snapshot.capture()

        # Verify capture
        assert "tables" in captured
        assert "campaigns.csv" in captured["tables"]
        assert captured["tables"]["campaigns.csv"]["row_count"] == 2
        assert captured["tables"]["campaigns.csv"]["column_count"] == 3
        assert set(captured["tables"]["campaigns.csv"]["columns"]) == {
            "id",
            "name",
            "impressions",
        }
        assert "hash" in captured["tables"]["campaigns.csv"]
        assert len(captured["tables"]["campaigns.csv"]["sample_rows"]) == 2

        # Verify manifest capture
        assert "campaigns.csv.manifest" in captured["tables"]
        assert captured["tables"]["campaigns.csv.manifest"]["incremental"] == True
        assert captured["tables"]["campaigns.csv.manifest"]["primary_key"] == ["id"]

        # Test 2: Validation passes with same data
        errors = snapshot.validate_against(captured)
        assert errors == [], (
            f"Validation should pass with same data, but got errors: {errors}"
        )

        # Test 3: Validation detects changes
        modified_snapshot = copy.deepcopy(captured)
        modified_snapshot["tables"]["campaigns.csv"]["row_count"] = 5  # Wrong count

        errors = snapshot.validate_against(modified_snapshot)
        assert len(errors) > 0, "Validation should fail with different row count"
        assert any("Row count mismatch" in e for e in errors)

        # Test 4: Validation detects column changes
        modified_snapshot = copy.deepcopy(captured)
        modified_snapshot["tables"]["campaigns.csv"]["columns"] = [
            "id",
            "name",
            "clicks",
        ]  # Wrong columns

        errors = snapshot.validate_against(modified_snapshot)
        assert len(errors) > 0, "Validation should fail with different columns"
        assert any("Column mismatch" in e for e in errors)


def test_snapshot_manager(tmpdir):
    """Test the SnapshotManager for saving and loading snapshots."""
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as temp:
        tmpdir = Path(temp)
        snapshots_file = tmpdir / "snapshots.json"

        # Create manager
        manager = SnapshotManager(snapshots_file)
        assert manager.list_snapshots() == []

        # Create mock output directory
        output_dir = tmpdir / "output"
        tables_dir = output_dir / "out" / "tables"
        tables_dir.mkdir(parents=True)

        # Create sample CSV
        sample_csv = tables_dir / "test.csv"
        with open(sample_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            writer.writerow(["1", "100"])

        # Capture snapshot
        manager.capture_snapshot("test_1", output_dir)
        assert manager.has_snapshot("test_1")
        assert "test_1" in manager.list_snapshots()

        # Save to file
        manager.save()
        assert snapshots_file.exists()

        # Load from file
        manager2 = SnapshotManager(snapshots_file)
        assert manager2.has_snapshot("test_1")
        snapshot = manager2.get_snapshot("test_1")
        assert "tables" in snapshot
        assert "test.csv" in snapshot["tables"]

        # Validate
        errors = manager2.validate_snapshot("test_1", output_dir)
        assert errors == [], f"Validation should pass, but got: {errors}"
