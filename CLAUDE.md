# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A unified Keboola extractor component for Facebook Pages, Facebook Ads, and Instagram — all three share the same codebase and Docker image, deployed to three separate Keboola component IDs (`keboola.ex-facebook-pages`, `keboola.ex-facebook-ads-v2`, `keboola.ex-instagram-v2`).

## Commands

```bash
# Install dependencies (uses uv, NOT pip)
uv sync --all-groups

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_functional.py
uv run pytest tests/test_page_loader_dsl.py

# Run a specific test by name
uv run pytest tests/test_functional.py -k "test_name"

# Lint
uv run flake8 . --config=flake8.cfg

# Format
uv run ruff format

# Docker build & test (CI equivalent)
docker-compose run --rm test

# Generate VCR cassettes from real API (requires secrets)
uv run python scripts/generate_tests.py --csv --capture-outputs
```

## Architecture

### Source Modules (`src/`)

- **`component.py`** — Entry point. Extends `ComponentBase` from `keboola-component`. Runs queries against the Facebook Graph API and writes output CSVs with manifests. Contains sync actions (`accounts`, `adaccounts`, `igaccounts`) for the Keboola UI.
- **`client.py`** — `FacebookClient` orchestrates API interactions. Handles token resolution (user tokens vs page tokens), sync/async query dispatch, batch requests, and fallback logic. `PageTokenResolver` fetches page-level tokens for endpoints that require them.
- **`configuration.py`** — Pydantic models: `Configuration`, `Account`, `QueryRow`, `QueryConfig`. Uses field aliases with hyphens (`api-version`, `bucket-id`, `run-by-id`).
- **`output_parser.py`** — `OutputParser` transforms Graph API responses into flat CSV rows. Handles pagination following, nested data extraction, Facebook Ads action stats (separate tables with `_insights` suffix), value arrays, and field flattening.
- **`page_loader.py`** — `PageLoader` builds API requests from query configs, including DSL parameter parsing for insights queries (e.g., `insights.level(ad).date_preset(last_3d){fields}`). Also handles async insights job lifecycle (start → poll → fetch results) and Facebook error categorization.

### Query Types

Two query types flow through the system differently:
- **`nested-query`** — Sync Graph API calls with pagination. Supports DSL syntax in `fields` for insights endpoints (when `path` is empty and fields start with `insights`).
- **`async-insights-query`** — Starts Marketing API async jobs, polls for completion, then fetches results.

### Testing (`tests/`)

- **`test_functional.py`** — VCR-based functional tests. Uses `VCRDataDirTester` from `keboola.datadirtest.vcr`. Each test case is a directory under `tests/functional/` with `source/data/` (input config + cassettes) and `expected/data/` (expected output CSVs/manifests). Tests replay recorded HTTP interactions via vcrpy.
- **`test_component.py`** — Basic unit test (component fails without config).
- **`test_page_loader_dsl.py`** — Unit tests for DSL parameter parsing in `PageLoader._build_params()`.
- **`conftest.py`** — Adds `src/` and `tests/` to `sys.path`, suppresses VCR debug logging.

### VCR Testing Details

VCR cassettes live at `tests/functional/*/source/data/cassettes/requests.json`. The `VCRDataDirTester` from `keboola.datadirtest.vcr` handles cassette replay and output comparison.

`VCR_SANITIZERS` is defined in both `src/component.py` and `tests/test_functional.py`:
- `DefaultSanitizer(additional_sensitive_fields=["page_token"])` — sanitizes access tokens and other sensitive fields from cassettes.
- `ResponseUrlSanitizer(dynamic_params=[...], url_domains=[...])` — normalizes Facebook CDN URLs (`fbcdn.net`, `facebook.com`, `cdninstagram.com`) by stripping dynamic query params.

### Scripts (`scripts/`)

- **`generate_tests.py`** — Records VCR cassettes from real API calls using configs from `tests/fixtures/`. Requires `tests/fixtures/config.secrets.json` (gitignored).
- **`sanitize_queries.py`** — Sanitizes production query configs for use in test generation.

## Key Dependencies

- `keboola-component` — Base class, config loading, table definitions, OAuth credentials
- `keboola-http-client` — HTTP client with retry logic
- `keboola.datadirtest` — Functional test framework with VCR support (`VCRDataDirTester`)
- `keboola-vcr` — VCR sanitizers (`DefaultSanitizer`, `ResponseUrlSanitizer`)
- `pydantic` — Configuration models
- `freezegun` — Time freezing for deterministic VCR replay
- `vcrpy` — HTTP interaction recording/replay

## CI Pipeline

GitHub Actions (`.github/workflows/push.yml`): builds Docker image → runs flake8 + unit tests + VCR functional tests → pushes to ECR for all three component IDs → deploys on semantic version tags.

## Style

- Line length: 120 (both ruff and flake8)
- Python 3.13
- flake8 excludes `tests/` directory from linting
