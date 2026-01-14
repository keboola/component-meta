
import pytest
import json
import os
import csv
import vcr
import copy
from pathlib import Path
from freezegun import freeze_time
from component import Component

# Constants
TEST_DIR = Path("tests/fixtures")
CONFIGS_FILE = TEST_DIR / "configs/test_cases.json"
CASSETTES_DIR = TEST_DIR / "cassettes"
SECRETS_FILE = TEST_DIR / "config.secrets.json"
QUERIES_SANITIZED_FILE = TEST_DIR / "queries_sanitized.csv"
FIXED_DATETIME = "2025-01-01 12:00:00"

def load_configs():
    cases = []
    
    # 1. Load legacy test cases from test_cases.json
    if CONFIGS_FILE.exists():
        with open(CONFIGS_FILE) as f:
            cases.extend(json.load(f))
            
    # 2. Load generated cases from sanitized CSV
    # Replay-only CI fallback
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
                creds = secrets_placeholder["authorization"].get("oauth_api", {}).get("credentials", {})
                if "token" in creds: creds["token"] = "token"
                if "access_token" in creds: creds["access_token"] = "token"
                if "#data" in creds: 
                    try:
                        data = json.loads(creds["#data"])
                        data["access_token"] = "token"
                        creds["#data"] = json.dumps(data)
                    except: pass

            # Grouping logic identical to generate_tests.py
            component_queries = {}
            with open(QUERIES_SANITIZED_FILE, encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    comp_id = row.get('kbc_component_id', 'Facebook Ads V2')
                    q_type = row.get('query_type', 'nested-query')
                    json_str = row.get('query_json', '')
                    
                    if not json_str: continue
                    
                    try:
                        q = json.loads(json_str)
                        # Reconstruct full object for Component
                        q_obj = {
                            "id": q.get("id", 1),
                            "type": q_type,
                            "name": q.get("name", "query"),
                            "query": q.get("query", q) if isinstance(q.get("query"), dict) else q,
                            "run-by-id": q.get("run-by-id", False)
                        }
                        if comp_id not in component_queries:
                            component_queries[comp_id] = []
                        component_queries[comp_id].append(q_obj)
                    except json.JSONDecodeError:
                        continue

            for version in ["v22.0", "v23.0"]:
                for comp_id, queries in component_queries.items():
                    comp_clean = comp_id.lower().replace(" ", "_")
                    version_clean = version.replace(".", "_")
                    case_name = f"gen_{comp_clean}_{version_clean}"
                    
                    config = copy.deepcopy(secrets_placeholder)
                    if "parameters" not in config: config["parameters"] = {}
                    config["parameters"]["queries"] = queries
                    config["parameters"]["api-version"] = version
                    
                    cases.append({
                        "name": case_name,
                        "description": f"Sanitized queries for {comp_id} (API {version})",
                        "action": "run",
                        "params": config
                    })

        except Exception as e:
            print(f"Warning: Failed to load sanitized CSV cases: {e}")
            
    return cases

@pytest.mark.parametrize("config_data", load_configs())
@freeze_time(FIXED_DATETIME)
def test_functional_component(config_data, tmpdir, monkeypatch):
    """
    Runs the component with the given config, finding the corresponding cassette.
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
            "oauth_api": {
                "credentials": {
                    "token": "token",
                    "access_token": "token"
                }
            }
        }
    
    with open(tmpdir.join("config.json"), "w") as f:
        json.dump(params, f)
    
    # 3. Setup VCR
    cassette_name = f"{config_data['name']}.json"
    cassette_path = CASSETTES_DIR / cassette_name
    
    if not cassette_path.exists():
        pytest.fail(f"Cassette {cassette_name} not found. Please run 'python scripts/generate_tests.py' to generate it.")

    my_vcr = vcr.VCR(
        cassette_library_dir=str(CASSETTES_DIR),
        record_mode='none', # REPLAY ONLY - ensures we don't hit live API
        match_on=['method', 'scheme', 'host', 'port', 'path', 'query', 'body'],
        filter_headers=[('Authorization', 'Bearer token')],
        filter_query_parameters=[('access_token', 'token')],
        decode_compressed_response=True,
        serializer='json'
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
    assert len(found_tables) > 0 or config_data.get("action") != "run", "Component produced no output tables"
