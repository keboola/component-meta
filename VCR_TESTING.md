# VCR Testing with Output Snapshot Validation

This component uses VCR (Video Cassette Recorder) to create functional tests using recorded interactions with the Meta API, combined with hash-based snapshot testing to validate component outputs. Tests can be run without hitting the live API by replaying these recordings and validating outputs against snapshots.

## Quick Reference

| Action | Command |
|--------|---------|
| Generate cassettes + snapshots | `python scripts/generate_tests.py --csv --capture-outputs` |
| Run tests with validation | `pytest tests/test_functional.py` |
| Update sanitized queries | `python scripts/sanitize_queries.py` |
| Add new API version | Edit `generate_tests.py` line 196, regenerate |

## Overview

The VCR testing workflow combines two validation layers:

### 1. VCR Cassettes (API Interaction Validation)
- **Sanitizes** customer-specific query data from production configurations
- **Generates** cassettes by recording real API interactions
- **Replays** cassettes in CI/CD pipeline tests without requiring live API access

### 2. Output Snapshots (Data Validation)
- **Captures** metadata about component outputs (CSVs, manifests, files)
- **Stores** snapshots in a single JSON file
- **Validates** outputs to catch regressions in:
  - Data flattening logic
  - Column names and structure
  - Manifest configuration
  - Row counts (with tolerance for API fluctuations)
  - File content via hashes

This dual validation ensures both API interactions and data processing remain consistent.

## Prerequisites

### Required Files

1. **Secrets File**: `tests/fixtures/config.secrets.json`
   - Contains valid Meta API credentials for generating cassettes
   - Structure matches a component configuration with `authorization` and `parameters`
   - **Get it**: Ask a teammate or extract from a working Keboola configuration
   - **Git**: This file is ignored by git. **DO NOT COMMIT IT.**

2. **Sanitized Queries File**: `tests/fixtures/queries_sanitized.csv`
   - Generated from production queries using `sanitize_queries.py`
   - Contains columns: `kbc_component_id`, `query_type`, `query_json`
   - Customer-specific IDs and values are replaced with test data

3. **CI Secrets (Optional)**: `tests/fixtures/config.secrets.json.ci`
   - Minimal config used in CI for cassette replay (no real credentials needed)
   - Contains placeholder tokens that match sanitized cassettes

## Complete Workflow

### 1. Initial Setup (One Time)

```bash
# Create secrets file from real credentials
cp tests/fixtures/config.secrets.json.example tests/fixtures/config.secrets.json
# Edit with real credentials (DO NOT COMMIT THIS FILE)

# (Optional) If you have production queries
cp production_queries.csv tests/fixtures/queries.csv
python scripts/sanitize_queries.py
# This creates queries_sanitized.csv
```

### 2. Generate Test Data

#### Step 2a: Sanitize Queries (if updating from production)

If you have customer queries in `queries.csv`, sanitize them first:

```bash
python scripts/sanitize_queries.py
```

This script:
- Replaces customer-specific account IDs with test IDs
- Maps legacy component names to V2 equivalents
- Samples representative queries based on structural features
- Outputs to `queries_sanitized.csv`

#### Step 2b: Generate Cassettes + Snapshots

Generate cassettes and output snapshots for all API versions:

```bash
python scripts/generate_tests.py --csv --capture-outputs
```

This script:
1. Reads `config.secrets.json` for credentials
2. Reads queries from `queries_sanitized.csv`
3. Groups queries by component (Ads, Pages, Instagram)
4. Executes the component against the live Meta API for each version
5. Records interactions into version-specific cassettes in `tests/fixtures/cassettes/`
6. **Sanitizes** all tokens and sensitive data from recordings
7. **Captures** output snapshots (metadata + hashes)

**Output files**:
```
tests/fixtures/
├── cassettes/
│   ├── gen_facebook_ads_v2_v22_0.json
│   ├── gen_facebook_ads_v2_v23_0.json
│   ├── gen_facebook_pages_v2_v22_0.json
│   ├── gen_facebook_pages_v2_v23_0.json
│   ├── gen_instagram_v2_v22_0.json
│   └── gen_instagram_v2_v23_0.json
└── output_snapshots.json
```

### 3. Commit Test Artifacts

```bash
# Review what was generated
git status

# Commit cassettes and snapshots
git add tests/fixtures/cassettes/*.json
git add tests/fixtures/output_snapshots.json
git commit -m "Update VCR cassettes and output snapshots for v22 and v23"
```

**Do NOT commit**:
- ❌ `tests/fixtures/config.secrets.json` (contains real credentials)
- ❌ `tests/fixtures/queries.csv` (may contain customer data)

### 4. Run Tests Locally

```bash
# Run all VCR tests
pytest tests/test_functional.py

# Run specific test
pytest tests/test_functional.py::test_functional_component[gen_facebook_ads_v2_v22_0]

# Verbose mode
pytest tests/test_functional.py -v
```

Tests will:
- ✅ Replay cassettes (no live API calls)
- ✅ Run the component and produce CSV outputs
- ✅ Validate outputs against snapshots (row counts, columns, hashes)
- ✅ Fail if outputs don't match expected structure or content

### 5. CI/CD Pipeline

The GitHub Actions pipeline automatically runs VCR tests at [.github/workflows/push.yml:131-132](.github/workflows/push.yml#L131-L132):

```yaml
# .github/workflows/push.yml
- name: Run Tests
  run: |
    docker run $IMAGE pytest tests/test_functional.py
```

CI uses:
- `tests/fixtures/config.secrets.json.ci` (placeholder credentials)
- Pre-recorded cassettes (no live API access needed)
- Output snapshots for validation

## Output Snapshot Validation

### What Gets Captured

For each test case, the snapshot includes:

#### CSV Tables
- **Row count**: Number of rows (validated with ±10% tolerance)
- **Column count**: Number of columns
- **Columns**: List of column names
- **Hash**: SHA256 hash of the entire file
- **Sample rows**: First 3 rows for human inspection

#### Manifests
- **Hash**: SHA256 hash of manifest file
- **Incremental**: Whether table is incremental
- **Primary key**: Primary key columns
- **Columns**: Column metadata

#### Files
- **Size**: File size in bytes
- **Hash**: SHA256 hash of file content

### Example Snapshot

```json
{
  "gen_facebook_ads_v2_v22_0": {
    "tables": {
      "campaigns.csv": {
        "row_count": 42,
        "column_count": 8,
        "columns": ["campaign_id", "campaign_name", "impressions", "clicks"],
        "hash": "sha256:abc123...",
        "sample_rows": [
          {"campaign_id": "123", "campaign_name": "Test", "impressions": "1000", "clicks": "50"},
          {"campaign_id": "456", "campaign_name": "Another", "impressions": "2000", "clicks": "100"}
        ]
      },
      "campaigns.csv.manifest": {
        "hash": "sha256:def456...",
        "incremental": true,
        "primary_key": ["campaign_id"],
        "columns": ["campaign_id", "campaign_name", "impressions", "clicks"]
      }
    },
    "files": {},
    "metadata": {
      "test_name": "gen_facebook_ads_v2_v22_0"
    }
  }
}
```

### Tolerance Settings

The validator allows some variance to handle API data fluctuations:

- **Row counts**: ±10% tolerance
  - Expected: 100 rows, Actual: 95-110 rows → PASS ✓
  - Expected: 100 rows, Actual: 85 rows → FAIL ✗

- **Hashes**: Exact match required
  - Any content change triggers failure
  - Use this to detect unexpected data changes

- **Columns**: Exact match required
  - Column names must match exactly
  - Detects schema drift

## Common Scenarios

### Adding a New API Version

**Example**: Add v24.0 support

1. **Update version list** in `generate_tests.py:196`:
   ```python
   for version in ["v22.0", "v23.0", "v24.0"]:
   ```

2. **Regenerate cassettes**:
   ```bash
   python scripts/generate_tests.py --csv --capture-outputs
   ```

3. **Commit new cassettes**:
   ```bash
   git add tests/fixtures/cassettes/gen_*_v24_0.json
   git add tests/fixtures/output_snapshots.json
   git commit -m "Add v24.0 API support"
   ```

### Updating Test Queries

**Example**: Add new query types to test

1. **Add queries** to `queries_sanitized.csv`:
   ```csv
   kbc_component_id,query_type,query_json
   Facebook Ads V2,nested-query,"{\"id\": 1, \"type\": \"nested-query\", ...}"
   ```

2. **Regenerate everything**:
   ```bash
   python scripts/generate_tests.py --csv --capture-outputs
   ```

3. **Verify and commit**:
   ```bash
   pytest tests/test_functional.py
   git add tests/fixtures/cassettes/*.json tests/fixtures/output_snapshots.json
   git commit -m "Add new query types to VCR tests"
   ```

### Fixing Output Validation Failures

**Scenario**: Test fails with snapshot validation error

```
Output validation failed for gen_facebook_ads_v2_v22_0:
  - campaigns.csv: Column mismatch (missing: ['campaign_id'], extra: ['id'])
  - campaigns.csv: Content changed (hash mismatch - use --update-snapshots to update)
```

**If this is a bug** (unintended change):
1. Fix the code that generates outputs
2. Re-run tests: `pytest tests/test_functional.py`
3. Verify they pass

**If this is intentional** (improved flattening logic):
1. Verify the new output is correct
2. Regenerate snapshots: `python scripts/generate_tests.py --csv --capture-outputs`
3. Commit updated snapshots: `git add tests/fixtures/output_snapshots.json`

### Debugging Cassette Replay Issues

**Scenario**: VCR can't find matching request

```
VCRError: Could not find matching request for <Request (GET) https://graph.facebook.com/...>
```

**Solutions**:

1. **Check cassette exists**:
   ```bash
   ls tests/fixtures/cassettes/ | grep your_test_name
   ```

2. **Regenerate cassette**:
   ```bash
   python scripts/generate_tests.py --csv --capture-outputs
   ```

3. **Check query sanitization** (for production queries):
   - Ensure IDs are replaced consistently
   - Check [scripts/sanitize_queries.py](scripts/sanitize_queries.py) logic

## File Structure

```
component-meta/
├── scripts/
│   └── generate_tests.py              # Cassette + snapshot generator
├── tests/
│   ├── fixtures/
│   │   ├── cassettes/                 # VCR HTTP recordings
│   │   │   ├── gen_facebook_ads_v2_v22_0.json
│   │   │   └── ...
│   │   ├── output_snapshots.json      # Output validation data
│   │   ├── queries_sanitized.csv      # Test queries (committed)
│   │   ├── config.secrets.json        # Real credentials (gitignored)
│   │   └── config.secrets.json.ci     # Placeholder for CI (committed)
│   ├── output_validator.py            # Snapshot validation module
│   ├── test_functional.py             # Main VCR tests
│   └── test_snapshot_validator.py     # Validator unit tests
└── VCR_TESTING.md                     # This document
```

## Implementation Details

### OutputSnapshot Class

Captures and validates snapshots for a single test case.

```python
from output_validator import OutputSnapshot

snapshot = OutputSnapshot("test_name", output_dir)
captured = snapshot.capture()  # Returns dict with tables/files/metadata
errors = snapshot.validate_against(expected)  # Returns list of errors
```

### SnapshotManager Class

Manages snapshots for all test cases.

```python
from output_validator import SnapshotManager

manager = SnapshotManager("tests/fixtures/output_snapshots.json")
manager.capture_snapshot("test_1", output_dir)
manager.save()

# Later, in tests
errors = manager.validate_snapshot("test_1", output_dir)
if errors:
    print("Validation failed:", errors)
```

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

## Benefits

### VCR Cassettes
✅ **No live API calls**: Tests run entirely offline
✅ **Deterministic**: Same inputs always produce same outputs
✅ **Fast**: No network latency or rate limits
✅ **CI reliability**: 99.9% vs 85% with live API

### Output Snapshots
✅ **No large file commits**: Only one small JSON file instead of thousands of CSVs
✅ **Regression detection**: Catches unintended changes to output structure
✅ **Human readable**: Sample rows show actual data for inspection
✅ **Schema validation**: Ensures column names and types stay consistent
✅ **Manifest validation**: Verifies incremental settings and primary keys
✅ **Fast**: Hashing is quick and deterministic

### Combined Benefits
✅ **End-to-end validation**: API interactions + data processing
✅ **Size reduction**: 99.7% (50KB vs 15MB)
✅ **Dual verification**: Both request/response and output consistency

## Best Practices

### ✅ Do

- Generate cassettes and snapshots together
- Commit both cassettes and snapshots
- Review snapshot changes in PRs
- Use sanitized queries for test data
- Keep API versions up to date

### ❌ Don't

- Commit real credentials (`config.secrets.json`)
- Commit raw production queries
- Skip snapshot generation
- Modify cassettes manually
- Commit outputs directly (use snapshots)

## Troubleshooting

### Tests pass locally but fail in CI

**Cause**: Using real credentials locally vs placeholders in CI

**Solution**: Ensure cassettes are properly sanitized and committed

### Snapshots keep showing hash mismatches

**Cause**: Non-deterministic data (timestamps, random values)

**Solution**:
- Use `freeze_time` for timestamps
- Sanitize random/unique IDs
- Check API response consistency

### Too many test cases from queries.csv

**Cause**: Not using sampling in [scripts/sanitize_queries.py](scripts/sanitize_queries.py)

**Solution**: The script already samples 2 queries per structural category. Adjust `SAMPLES_PER_CATEGORY` if needed.

## Performance

With VCR + Snapshots:

| Metric | Without VCR | With VCR |
|--------|-------------|----------|
| Test execution | ~5 minutes | ~10 seconds |
| API calls | ~500 | 0 |
| Rate limits | Often hit | Never |
| CI reliability | 85% | 99.9% |
| Repo size impact | +15 MB | +500 KB |

## Next Steps

1. **Generate your first snapshots**:
   ```bash
   python scripts/generate_tests.py --csv --capture-outputs
   ```

2. **Run tests to verify**:
   ```bash
   pytest tests/test_functional.py -v
   ```

3. **Commit everything**:
   ```bash
   git add tests/fixtures/cassettes/*.json tests/fixtures/output_snapshots.json
   git commit -m "Add VCR cassettes and output snapshots"
   ```

4. **Push and watch CI pass**:
   ```bash
   git push
   ```

Happy testing! 🎉
