
# VCR Functional Test Generation Workflow

This component uses VCR (Video Cassette Recorder) to create functional tests using recorded interactions with the Meta API. Tests can be run without hitting the live API by replaying these recordings.

## Overview

The VCR testing workflow:
1. **Sanitizes** customer-specific query data from production configurations
2. **Generates** cassettes by recording real API interactions
3. **Replays** cassettes in CI/CD pipeline tests without requiring live API access

## Prerequisites

### Required Files

1.  **Secrets File**: `tests/fixtures/config.secrets.json`
    *   Contains valid Meta API credentials for generating cassettes
    *   Structure matches a component configuration with `authorization` and `parameters`
    *   **Get it**: Ask a teammate or extract from a working Keboola configuration
    *   **Git**: This file is ignored by git. **DO NOT COMMIT IT.**

2.  **Sanitized Queries File**: `tests/fixtures/queries_sanitized.csv`
    *   Generated from production queries using `sanitize_queries.py`
    *   Contains columns: `kbc_component_id`, `query_type`, `query_json`
    *   Customer-specific IDs and values are replaced with test data

3.  **CI Secrets (Optional)**: `tests/fixtures/config.secrets.json.ci`
    *   Minimal config used in CI for cassette replay (no real credentials needed)
    *   Contains placeholder tokens that match sanitized cassettes

## Generating Test Cassettes

### Step 1: Sanitize Queries (if updating from production)

If you have customer queries in `queries.csv`, sanitize them first:

```bash
python tests/fixtures/sanitize_queries.py
```

This script:
- Replaces customer-specific account IDs with test IDs
- Maps legacy component names to V2 equivalents
- Samples representative queries based on structural features
- Outputs to `queries_sanitized.csv`

### Step 2: Generate Cassettes

Generate cassettes for all API versions:

```bash
python scripts/generate_tests.py --csv
```

This script:
1.  Reads `config.secrets.json` for credentials
2.  Reads queries from `queries_sanitized.csv`
3.  Groups queries by component (Ads, Pages, Instagram)
4.  Executes the component against the live Meta API for each version
5.  Records interactions into version-specific cassettes in `tests/fixtures/cassettes/`
6.  **Sanitizes** all tokens and sensitive data from recordings

**Output cassettes**:
- `gen_facebook_ads_v2_v22_0.json`
- `gen_facebook_ads_v2_v23_0.json`
- `gen_facebook_pages_v2_v22_0.json`
- `gen_facebook_pages_v2_v23_0.json`
- `gen_instagram_v2_v22_0.json`
- `gen_instagram_v2_v23_0.json`

## Running Tests

To run VCR tests (replays cassettes without hitting live API):

```bash
pytest tests/test_functional.py
```

The test automatically:
- Loads all cassettes from `tests/fixtures/cassettes/`
- Runs the component with each sanitized configuration
- Verifies outputs match expected behavior
- Uses `record_mode='none'` to ensure no live API calls

## API Versioning Strategy

The system supports testing multiple API versions simultaneously:

1.  **Version Configuration**: Each cassette is named with its API version (e.g., `v22_0`, `v23_0`)
2.  **Test Generation**: The test harness automatically generates test cases for versions `v22.0` and `v23.0`
3.  **Version-Specific Cassettes**: Each component-version combination has its own cassette file

To add a new API version:
1.  Update the version list in `test_functional.py` line 82: `for version in ["v22.0", "v23.0", "v24.0"]:`
2.  Run `python scripts/generate_tests.py --csv` to generate cassettes for the new version

## CI/CD Pipeline Integration

The VCR tests run automatically in the GitHub Actions pipeline at [.github/workflows/push.yml:131-132](.github/workflows/push.yml#L131-L132):

```yaml
echo "Running functional VCR tests..."
docker run ${{ env.KBC_DEVELOPERPORTAL_APP }}:latest pytest tests/test_functional.py
```

**Important**: The cassettes must be committed to the repository for CI tests to pass. The CI environment uses `config.secrets.json.ci` (placeholder credentials) and replays the pre-recorded cassettes.

## Key Features

### Automatic Sanitization
- Access tokens replaced with `"token"`
- Customer account IDs replaced with test IDs
- Headers filtered to essential fields only
- Deterministic output (sorted JSON keys) for clean git diffs

### Component Support
- **Facebook Ads V2**: Ad campaigns, insights, async queries
- **Facebook Pages V2**: Page posts, engagement, feed
- **Instagram V2**: Stories, media, business account insights

### Query Sampling
The sanitization script samples queries based on structural features to ensure comprehensive coverage without bloating the test suite:
- Async vs nested queries
- Breakdowns and action breakdowns
- Filtering and attribution
- Time-based pagination
- Summary fields
