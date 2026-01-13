
# Functional Test Generation Workflow

This component uses a semi-automated workflow to create functional tests using real data from the Meta API.

## Prerequisites

1.  **Secrets File**: You need a `tests/fixtures/config.secrets.json` file. This file matches the structure of a component configuration `config.json` but must contain valid credentials.
    *   **Get it**: Ask a teammate or extract it from a working Keboola Storage configuration.
    *   **Format**: JSON. Must contain `authorization` and `parameters`.
    *   **Git**: This file is ignored by git. **DO NOT COMMIT IT.**

2.  **Queries File**: `tests/fixtures/queries.csv`.
    *   This file defines the queries used to generate the test case.
    *   It should contain columns like `id, type, name, query` (fields, limit, etc).

## Generating Tests

To regenerate the functional test cassettes (e.g., after changing queries or updating the API version):

```bash
python scripts/generate_tests.py --csv
```

This script will:
1.  Read `config.secrets.json` for credentials.
2.  Read unique queries from `queries.csv`.
3.  Execute the component code against the live Meta API.
4.  Record the interactions into `tests/fixtures/cassettes/from_csv_generated.yaml`.
5.  **Sanitize** sensitive tokens from the recording.

## Running Tests

To run the tests (which replay the cassettes):

```bash
pytest tests/test_functional.py
```

## API Versioning Strategy

Currently, the API version is defined in your `config.secrets.json` under `parameters.api-version` (e.g., `v16.0`) or defaults to a fallback in the script.

To test multiple versions:
1.  Update `api-version` in `config.secrets.json`.
2.  Run `python scripts/generate_tests.py --csv`.
3.  This will update the `from_csv_generated` cassette with interactions for that version.
    *   *Note: Currently, this overwrites the single cassette. To support multiple versions simultaneously, we would need to parameterize the output filename based on the version.*
