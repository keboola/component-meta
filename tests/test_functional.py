
import pytest
import json
import os
import vcr
from pathlib import Path
from freezegun import freeze_time
from component import Component

# Constants
TEST_DIR = Path("tests/fixtures")
CONFIGS_FILE = TEST_DIR / "configs/test_cases.json"
CASSETTES_DIR = TEST_DIR / "cassettes"
FIXED_DATETIME = "2025-01-01 12:00:00"

SECRETS_FILE = TEST_DIR / "config.secrets.json"
QUERIES_FILE = TEST_DIR / "queries.csv"

def load_configs():
    cases = []
    if CONFIGS_FILE.exists():
        with open(CONFIGS_FILE) as f:
            cases = json.load(f)
            
    # Optionally load generated case from CSV if files exist
    if QUERIES_FILE.exists() and SECRETS_FILE.exists():
        import csv
        try:
            with open(SECRETS_FILE) as f:
                secrets = json.load(f)
            
            queries = []
            seen_queries = set()
            with open(QUERIES_FILE) as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    
                    # The CSV format is a single column containing a JSON string of a list of queries
                    try:
                        json_str = row[0]
                        # Skip if it looks like a header
                        if not json_str.strip().startswith("["):
                            continue

                        queries_list = json.loads(json_str)
                        
                        if isinstance(queries_list, list):
                            for q in queries_list:
                                real_query_params = q.get("query", {})
                                if "limit" in real_query_params:
                                    real_query_params["limit"] = str(real_query_params["limit"])
                                
                                q_obj = {
                                    "id": q.get("id"),
                                    "type": q.get("type", "nested-query"),
                                    "name": q.get("name", "query"),
                                    "query": real_query_params,
                                    "run-by-id": q.get("run-by-id", False)
                                }

                                # Deduplication
                                query_fingerprint = json.dumps(q_obj['query'], sort_keys=True)
                                
                                if query_fingerprint in seen_queries:
                                    continue
                                
                                seen_queries.add(query_fingerprint)
                                queries.append(q_obj)
                    except (json.JSONDecodeError, IndexError):
                        pass

            # Construct config
            config = secrets.copy()
            if "parameters" not in config:
                config["parameters"] = {}
                
            config["parameters"]["queries"] = queries
            
            # Determine version for test name
            current_version = config["parameters"].get("api-version", "v16.0")
            config["parameters"]["api-version"] = current_version
            version_clean = current_version.replace(".", "_")
            
            cases.append({
                "name": f"from_csv_generated_{version_clean}",
                "description": f"Generated from queries.csv for {current_version}",
                "action": "run",
                "params": config
            })
        except Exception as e:
            print(f"Failed to load CSV test case: {e}")
            
    return cases

@pytest.mark.parametrize("config_data", load_configs())
@freeze_time(FIXED_DATETIME)
def test_functional_component(config_data, tmpdir, monkeypatch):
    """
    Runs the component with the given config, finding the corresponding cassette.
    """
    # 1. Setup Environment
    monkeypatch.setenv("KBC_DATADIR", str(tmpdir))
    
    # 2. Write Config
    # Ensure nested directories exist
    os.makedirs(os.path.join(tmpdir, "out", "tables"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "out", "files"), exist_ok=True) # Good practice
    
    # FIX: Promote token...
    params = config_data.get("params", {})
    
    # Create copy
    import copy
    params = copy.deepcopy(params)
    
    # Inject action
    params["action"] = config_data.get("action", "run")

    inner_params = params.get("parameters", {})
    param_token = inner_params.get("access_token") or inner_params.get("#access_token")
    if param_token and "authorization" not in params:
        params["authorization"] = {
            "oauth_api": {
                "credentials": {
                    "token": param_token,
                    "access_token": param_token
                }
            }
        }
    
    with open(tmpdir.join("config.json"), "w") as f:
        json.dump(params, f)
    
    # 3. Setup VCR
    # Ensure cassettes dir exists (even if empty)
    CASSETTES_DIR.mkdir(parents=True, exist_ok=True)
    
    cassette_name_json = f"{config_data['name']}.json"
    cassette_name_yaml = f"{config_data['name']}.yaml"
    
    if (CASSETTES_DIR / cassette_name_json).exists():
        cassette_name = cassette_name_json
        serializer = 'json'
    elif (CASSETTES_DIR / cassette_name_yaml).exists():
        cassette_name = cassette_name_yaml
        serializer = 'yaml'
    else:
        # Default fallback (or fail)
        cassette_name = cassette_name_yaml
        serializer = 'yaml'
    
    cassette_path = CASSETTES_DIR / cassette_name
    
    if not cassette_path.exists():
        pytest.fail(f"Cassette {cassette_name} not found. Please run 'python scripts/generate_tests.py' to generate it.")

    my_vcr = vcr.VCR(
        cassette_library_dir=str(CASSETTES_DIR),
        record_mode='none', # Replay only
        match_on=['method', 'scheme', 'host', 'port', 'path', 'query', 'body'],
        filter_headers=[('Authorization', 'Bearer token')],
        filter_query_parameters=[('access_token', 'token')],
        decode_compressed_response=True,
        serializer=serializer
    )

    with my_vcr.use_cassette(cassette_name):
        comp = Component()
        if config_data.get("action") == "run":
            comp.run()
        else:
            # For actions like 'accounts', we need to capture the return value if possible,
            # but usually they output to stdout/tables.
            # Component.execute_action prints JSON to stdout for sync actions.
            # We might want to capture stdout to verify?
            # For now, just ensure it runs without error.
            comp.execute_action()
            
    # 4. Assert Output Tables
    expected_tables = config_data.get("expected_output_tables", [])
    if expected_tables:
        out_tables_dir = Path(tmpdir) / "out" / "tables"
        for table in expected_tables:
             assert (out_tables_dir / f"{table}.csv").exists(), f"Table {table} was not created."
