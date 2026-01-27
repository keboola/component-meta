import sys
import os
import json
import logging
import csv
import re
import argparse
from pathlib import Path
from tempfile import TemporaryDirectory
import vcr
from freezegun import freeze_time

# Add src and tests to path (relative to this file)
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "tests"))

# Import after path modification - flake8: noqa
from component import Component  # noqa: E402
from output_validator import SnapshotManager  # noqa: E402

# Constants
TEST_DIR = Path("tests/fixtures")
CONFIGS_FILE = TEST_DIR / "configs/test_cases.json"
CASSETTES_DIR = TEST_DIR / "cassettes"
SECRETS_FILE = TEST_DIR / "config.secrets.json"
QUERIES_FILE = TEST_DIR / "queries.csv"
SNAPSHOTS_FILE = TEST_DIR / "output_snapshots.json"
FIXED_DATETIME = "2025-01-01 12:00:00"

# Global snapshot manager
snapshot_manager = None


def scrub_string(string, replacements):
    if not string:
        return string
    for target, replacement in replacements.items():
        if target and target in string:
            string = string.replace(target, replacement)
    return string


def sanitize_url(url):
    """
    Sanitize URLs by removing dynamic Facebook/Meta parameters.

    Facebook CDN URLs contain session-specific parameters that change
    between requests but don't affect the actual resource.

    Args:
        url: String that may contain a URL

    Returns:
        Sanitized string with dynamic parameters removed
    """
    if not url or not isinstance(url, str):
        return url

    # Only sanitize if it looks like a Facebook/Meta CDN URL
    if not any(domain in url for domain in ["fbcdn.net", "facebook.com"]):
        return url

    # Dynamic parameters to remove
    dynamic_params = [
        "_nc_gid",  # Session/group ID
        "_nc_tpa",  # Tracking parameter
        "_nc_oc",  # Cache parameter
        "oh",  # Hash/signature
        "oe",  # Expiry timestamp
    ]

    # Remove each parameter
    for param in dynamic_params:
        url = re.sub(f"[&?]{param}=[^&]*", "", url)

    # Clean up trailing ? or & characters
    url = re.sub(r"[?&]+$", "", url)
    # Fix double && or &? patterns
    url = re.sub(r"&{2,}", "&", url)
    url = re.sub(r"\?&", "?", url)

    return url


def scrub_headers(headers, is_response=False):
    """
    Remove sensitive and dynamic headers from requests/responses.

    Args:
        headers: Header dictionary
        is_response: If True, also removes Content-Length from responses

    Returns:
        Filtered headers dictionary
    """
    # Only keep essential headers to ensure deterministic cassettes and no environment leaks
    whitelist = ["content-type", "content-length", "facebook-api-version"]
    new_headers = {}

    # Use items() if available (dict-like), otherwise attempt to iterate directly
    try:
        source = headers.items() if hasattr(headers, "items") else headers
        for k, v in source:
            if k.lower() in whitelist:
                # VCR expects header values to be lists
                new_headers[k] = v if isinstance(v, list) else [v]
    except Exception:
        return headers

    # For responses, remove Content-Length as it changes with body sanitization
    if is_response and "Content-Length" in new_headers:
        del new_headers["Content-Length"]

    return new_headers


def recursive_scrub(obj, replacements):
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            if k in ["access_token", "token"]:
                new_obj[k] = "token"
            else:
                new_obj[k] = recursive_scrub(v, replacements)
        return new_obj
    elif isinstance(obj, list):
        return [recursive_scrub(i, replacements) for i in obj]
    elif isinstance(obj, str):
        # First apply token scrubbing
        scrubbed = scrub_string(obj, replacements)
        # Then sanitize URLs
        scrubbed = sanitize_url(scrubbed)
        return scrubbed
    return obj


def before_record_response(response, replacements):
    # Scrub headers (response headers - remove Content-Length)
    response["headers"] = scrub_headers(response.get("headers", {}), is_response=True)

    # Scrub body
    if "body" in response and "string" in response["body"]:
        try:
            body_bytes = response["body"]["string"]
            if not body_bytes:
                return response

            body_str = body_bytes.decode("utf-8")

            # First try parsing as JSON
            try:
                body_json = json.loads(body_str)
                scrubbed_json = recursive_scrub(body_json, replacements)
                # Sort keys for deterministic git diffs
                response["body"]["string"] = json.dumps(scrubbed_json, sort_keys=True).encode("utf-8")
            except json.JSONDecodeError:
                # Fallback to string replacement
                scrubbed_str = scrub_string(body_str, replacements)
                response["body"]["string"] = scrubbed_str.encode("utf-8")

        except Exception as e:
            logging.warning(f"Failed to scrub response body: {e}")
            pass
    return response


def before_record_request(request, replacements):
    # Apply general string replacements (tokens, etc.)
    request.uri = scrub_string(request.uri, replacements)

    # Scrub request headers (keep Content-Length for requests)
    request.headers = scrub_headers(request.headers, is_response=False)

    if request.body:
        try:
            body_str = request.body.decode("utf-8")
            # Try to handle as JSON for sorting
            try:
                body_json = json.loads(body_str)
                scrubbed_json = recursive_scrub(body_json, replacements)
                request.body = json.dumps(scrubbed_json, sort_keys=True).encode("utf-8")
            except json.JSONDecodeError:
                request.body = scrub_string(body_str, replacements).encode("utf-8")
        except Exception:
            pass
    return request


def inject_secrets(config, token):
    if isinstance(config, dict):
        new_config = {}
        for k, v in config.items():
            if k == "#access_token" and v == "token":
                new_config[k] = token
            elif k == "access_token" and v == "token":
                new_config[k] = token
            else:
                new_config[k] = inject_secrets(v, token)
        return new_config
    elif isinstance(config, list):
        return [inject_secrets(i, token) for i in config]
    return config


def run_from_csv(csv_path, secrets_path, full_output=False):
    # Try sanitized file first if it exists
    sanitized_path = csv_path.parent / "queries_sanitized.csv"
    if sanitized_path.exists():
        print(f"Using sanitized queries from: {sanitized_path}")
        csv_path = sanitized_path

    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}")
        return
    if not secrets_path.exists():
        print(f"Secrets file not found: {secrets_path}")
        return

    with open(secrets_path) as f:
        secrets = json.load(f)

    # Group queries by normalized kbc_component_id
    component_queries = {}
    total_found = 0

    # Mapping to consolidate into V2 components
    ID_MAPPING = {
        "Facebook Ads": "Facebook Ads V2",
        "Facebook Pages": "Facebook Pages V2",
        "Instagram": "Instagram V2",
    }

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            original_comp_id = row.get("kbc_component_id", "Facebook Ads")
            comp_id = ID_MAPPING.get(original_comp_id, original_comp_id)

            q_type = row.get("query_type", "nested-query")
            json_str = row.get("query_json", "")

            if not json_str:
                continue

            try:
                q = json.loads(json_str)

                # Normalize query object
                real_query_params = q.get("query", q) if isinstance(q.get("query"), dict) else q
                if "limit" in real_query_params and real_query_params["limit"]:
                    real_query_params["limit"] = str(real_query_params["limit"])

                q_obj = {
                    "id": q.get("id"),
                    "type": q_type,
                    "name": q.get("name", "query"),
                    "query": real_query_params,
                    "run-by-id": q.get("run-by-id", False),
                }

                if comp_id not in component_queries:
                    component_queries[comp_id] = []

                component_queries[comp_id].append(q_obj)
                total_found += 1

            except json.JSONDecodeError:
                continue

    print(f"Total unique queries found across {len(component_queries)} components: {total_found}")

    # Generate cassettes for both v22.0 and v23.0
    for version in ["v22.0", "v23.0"]:
        for comp_id, queries_raw in component_queries.items():
            print(f"Generating test for component: {comp_id} ({len(queries_raw)} queries) - API {version}")

            # Normalize and add technical IDs if missing
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

            # Merge into secrets config
            base_config = secrets.copy()
            if "parameters" not in base_config:
                base_config["parameters"] = {}

            comp_clean = comp_id.lower().replace(" ", "_")
            version_clean = version.replace(".", "_")
            case_name = f"gen_{comp_clean}_{version_clean}"

            config = base_config.copy()  # Deeper copy for safety
            config["parameters"] = base_config["parameters"].copy()
            config["parameters"]["queries"] = final_queries
            config["parameters"]["api-version"] = version

            case = {
                "name": case_name,
                "description": f"Generated for {comp_id} using {version}",
                "action": "run",
                "params": config,
            }

            # Extract token for scrubbing
            token = "token"
            try:
                auth = config.get("authorization", {})
                creds = auth.get("oauth_api", {}).get("credentials", {})
                data_str = creds.get("#data")
                if data_str:
                    data_json = json.loads(data_str)
                    token = data_json.get("access_token", "token")
                else:
                    token = creds.get("token") or creds.get("access_token") or "token"
            except Exception:
                pass

            run_test_case(case, token, full_output=full_output)


def sanitize_output_csvs(output_dir, replacements):
    """
    Sanitize CSV files in the output directory by applying replacements.

    This ensures that output snapshots are deterministic and don't contain
    sensitive data like access tokens or dynamic URLs.

    Parses CSV properly to avoid corrupting the file structure.

    Args:
        output_dir: Path to the KBC_DATADIR
        replacements: Dictionary of string replacements to apply
    """
    import csv as csv_module

    tables_dir = Path(output_dir) / "out" / "tables"
    if not tables_dir.exists():
        return

    for csv_file in tables_dir.glob("*.csv"):
        try:
            # Read the CSV properly
            with open(csv_file, "r", encoding="utf-8", newline="") as f:
                reader = csv_module.DictReader(f)
                rows = list(reader)
                fieldnames = reader.fieldnames

            # Sanitize each cell value
            sanitized_rows = []
            for row in rows:
                sanitized_row = {}
                for key, value in row.items():
                    if value is not None and isinstance(value, str):
                        # Apply string replacements
                        sanitized_value = scrub_string(value, replacements)
                        # Apply URL sanitization
                        sanitized_value = sanitize_url(sanitized_value)
                        sanitized_row[key] = sanitized_value
                    else:
                        sanitized_row[key] = value
                sanitized_rows.append(sanitized_row)

            # Write back as valid CSV
            with open(csv_file, "w", encoding="utf-8", newline="") as f:
                if fieldnames:
                    writer = csv_module.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(sanitized_rows)

        except Exception as e:
            logging.error(f"Failed to sanitize {csv_file}: {e}")
            # Don't silently continue - re-raise to make the error visible
            raise


def run_test_case(case, token, full_output=False):
    # Replacements map: Real -> Dummy
    replacements = {token: "token"}

    # Ensure cassettes dir exists
    CASSETTES_DIR.mkdir(parents=True, exist_ok=True)

    my_vcr = vcr.VCR(
        cassette_library_dir=str(CASSETTES_DIR),
        record_mode="new_episodes",
        match_on=["method", "scheme", "host", "port", "path", "query", "body"],
        filter_headers=[("Authorization", "Bearer token")],
        filter_query_parameters=[("access_token", "token")],
        before_record_response=lambda r: before_record_response(r, replacements),
        before_record_request=lambda r: before_record_request(r, replacements),
        decode_compressed_response=True,
        serializer="json",
    )

    print(f"Recording case: {case['name']}")

    # Inject secrets into parameters for execution
    runtime_params = inject_secrets(case["params"], token)

    # FIX: Promote token to authorization if needed
    params = runtime_params.get("parameters", {})
    param_token = params.get("access_token") or params.get("#access_token")
    if param_token and "authorization" not in runtime_params:
        runtime_params["authorization"] = {
            "oauth_api": {"credentials": {"token": param_token, "access_token": param_token}}
        }

    runtime_params["action"] = case.get("action", "run")

    # Setup temp env
    with TemporaryDirectory() as tmpdir:
        os.environ["KBC_DATADIR"] = tmpdir
        os.makedirs(os.path.join(tmpdir, "out", "tables"), exist_ok=True)
        os.makedirs(os.path.join(tmpdir, "out", "files"), exist_ok=True)

        with open(os.path.join(tmpdir, "config.json"), "w") as f:
            json.dump(runtime_params, f)

        with freeze_time(FIXED_DATETIME):
            cassette_name = f"{case['name']}.json"
            with my_vcr.use_cassette(cassette_name):
                try:
                    comp = Component()
                    if case.get("action") == "run":
                        comp.run()
                    else:
                        comp.execute_action()
                    print(f"Success: {case['name']}")

                    # Sanitize output CSVs before capturing snapshot
                    sanitize_output_csvs(tmpdir, replacements)

                    # Capture output snapshot if snapshot manager is enabled
                    if snapshot_manager:
                        snapshot_manager.capture_snapshot(case["name"], tmpdir, full_output=full_output)
                        mode = "full output" if full_output else "samples only"
                        print(f"  → Captured output snapshot for {case['name']} ({mode})")

                except BaseException as e:
                    import traceback

                    traceback.print_exc()
                    print(f"Error executing {case['name']}: {e}")

            cassette_path = CASSETTES_DIR / cassette_name
            if not cassette_path.exists():
                with open(cassette_path, "w") as f:
                    f.write('{"interactions": [], "version": 1}\n')
                print(f"Created empty cassette for {case['name']}")


def run_gen():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Run from queries.csv")
    parser.add_argument(
        "--capture-outputs",
        action="store_true",
        help="Capture output snapshots for validation",
    )
    parser.add_argument(
        "--full-output",
        action="store_true",
        help="Capture all rows in snapshots instead of just samples (for debugging hash differences)",
    )
    args = parser.parse_args()

    # Initialize snapshot manager if requested
    global snapshot_manager
    if args.capture_outputs:
        snapshot_manager = SnapshotManager(SNAPSHOTS_FILE)
        print(f"Output snapshot capture enabled → {SNAPSHOTS_FILE}")

    if args.csv:
        run_from_csv(QUERIES_FILE, SECRETS_FILE, full_output=args.full_output)
        # Save snapshots after all tests
        if snapshot_manager:
            snapshot_manager.save()
        return

    # Legacy/Default mode: read from test_cases.json
    token = os.environ.get("KBC_SECRET_TOKEN")
    if not token and SECRETS_FILE.exists():
        try:
            with open(SECRETS_FILE) as f:
                secrets = json.load(f)
            # Try to extract token using same logic as in run_from_csv
            auth = secrets.get("authorization", {})
            creds = auth.get("oauth_api", {}).get("credentials", {})
            data_str = creds.get("#data")
            if data_str:
                data_json = json.loads(data_str)
                token = data_json.get("access_token")

            if not token:
                token = creds.get("token") or creds.get("access_token")

            if token:
                print("Using token from config.secrets.json for legacy tests.")
        except Exception as e:
            print(f"Failed to load token from secrets: {e}")

    if not token:
        print(
            "WARNING: KBC_SECRET_TOKEN env var is missing and could not load "
            "from secrets. Using 'test_token'. Live requests will likely fail."
        )
        token = "test_token"

    if not CONFIGS_FILE.exists():
        print(f"Config file not found: {CONFIGS_FILE}")
        return

    with open(CONFIGS_FILE) as f:
        cases = json.load(f)

    for case in cases:
        run_test_case(case, token)


if __name__ == "__main__":
    run_gen()
