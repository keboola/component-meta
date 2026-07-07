# FB Ads V2 — opt-in V1-compatibility flag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-introduce the CFTL-630 FB Ads V2 output fix gated behind a single opt-in `v1_compatibility` flag, so default-off is byte-identical to 0.0.17 and on gives full V1-parity output.

**Architecture:** A boolean `v1_compatibility` on the root `Configuration` is threaded `component.py → FacebookClient → OutputParser`. When true, `OutputParser` (1) copies all scalar fields from the originating insights row onto per-action-breakdown rows, and (2) backfills declared-but-omitted query fields with `""`. When false, both behaviors are skipped and output is unchanged.

**Tech Stack:** Python 3.13, `uv`, pytest, pydantic v2, `keboola.datadirtest` VCR functional tests.

## Global Constraints

- Default behavior MUST be byte-identical to current `0.0.17` — the entire CFTL-630 delta is gated; nothing changes unless `v1_compatibility` is true. (This is the whole point: prior unconditional widening caused two rollbacks.)
- New config key is exactly `v1_compatibility` (snake_case, no alias) under `parameters` — the future `keboola/ui` checkbox writes this same key.
- Package manager is `uv`; run tests with `uv run pytest`.
- No customer/company names in code, tests, commits, or docs — reference `CFTL-630` / `SUPPORT-16160` only.
- "Scalar field" = a value that is not a `list` or `dict` **and** whose key is not in `OutputParser.ADS_ACTION_STATS_ROW`.
- Follow existing patterns: unit tests live directly in `tests/test_*.py` and build `OutputParser` with a `MagicMock` row_config (see `tests/test_output_parser_streaming.py`).

---

### Task 1: Add `v1_compatibility` to `Configuration`

**Files:**
- Modify: `src/configuration.py:37-41` (the `Configuration` model)
- Test: `tests/test_configuration.py` (create)

**Interfaces:**
- Produces: `Configuration.v1_compatibility: bool` (default `False`), populated from `parameters["v1_compatibility"]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_configuration.py`:

```python
"""Unit tests for the Configuration model (CFTL-630 v1_compatibility flag)."""

from configuration import Configuration


def test_v1_compatibility_defaults_to_false():
    cfg = Configuration(**{"accounts": {}, "queries": []})
    assert cfg.v1_compatibility is False


def test_v1_compatibility_reads_true_from_parameters():
    cfg = Configuration(**{"accounts": {}, "queries": [], "v1_compatibility": True})
    assert cfg.v1_compatibility is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_configuration.py -v`
Expected: `test_v1_compatibility_defaults_to_false` FAILS with `AttributeError: 'Configuration' object has no attribute 'v1_compatibility'`.

- [ ] **Step 3: Add the field**

In `src/configuration.py`, change the `Configuration` class body from:

```python
class Configuration(BaseModel):
    accounts: dict[str, Account] = Field(default_factory=dict)
    queries: list[QueryRow] = Field(default_factory=list)
    api_version: str = Field(alias="api-version", default="v23.0")
    bucket_id: str | None = Field(alias="bucket-id", default=None)
```

to:

```python
class Configuration(BaseModel):
    accounts: dict[str, Account] = Field(default_factory=dict)
    queries: list[QueryRow] = Field(default_factory=list)
    api_version: str = Field(alias="api-version", default="v23.0")
    bucket_id: str | None = Field(alias="bucket-id", default=None)
    # CFTL-630 / SUPPORT-16160: opt-in V1-parity output. When false (default) the
    # OutputParser behaves exactly like 0.0.17. When true it copies all scalar fields
    # onto per-action-breakdown rows and backfills declared-but-omitted fields.
    v1_compatibility: bool = Field(default=False)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_configuration.py -v`
Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/configuration.py tests/test_configuration.py
git commit -m "feat(CFTL-630): add v1_compatibility flag to Configuration"
```

---

### Task 2: `_copy_common_fields` copies all scalar fields when flag is on

**Files:**
- Modify: `src/output_parser.py:51-54` (`__init__`), `src/output_parser.py:431-446` (`_copy_common_fields`)
- Test: `tests/test_output_parser_v1_compat.py` (create)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `OutputParser.__init__(self, page_loader, page_id, row_config, v1_compatibility: bool = False)` storing `self.v1_compatibility`. `_copy_common_fields(base_row, original_row, extended)` copies all scalar fields when `extended and self.v1_compatibility`, else the existing narrow list.

- [ ] **Step 1: Write the failing test**

Create `tests/test_output_parser_v1_compat.py`:

```python
"""Unit tests for the opt-in v1_compatibility behavior of OutputParser (CFTL-630)."""

from unittest.mock import MagicMock

from output_parser import OutputParser


def _make_row_config(fields: str = "insights", path: str = "", parameters=None, name: str = "my_query"):
    row_config = MagicMock()
    row_config.name = name
    row_config.type = "regular"
    row_config.query.path = path
    row_config.query.fields = fields
    row_config.query.parameters = parameters
    return row_config


def _parser(v1_compatibility: bool, **rc_kwargs) -> OutputParser:
    return OutputParser(
        page_loader=None,
        page_id="act_1",
        row_config=_make_row_config(**rc_kwargs),
        v1_compatibility=v1_compatibility,
    )


# --- _copy_common_fields -------------------------------------------------

ORIGINAL_ROW = {
    "account_id": "act_1",
    "ad_id": "ad_9",
    "ad_name": "Spring Sale",
    "impressions": "1234",
    "clicks": "56",
    "spend": "7.89",
    "reach": "1000",
    "actions": [{"action_type": "link_click", "value": "10"}],  # list -> skipped
}


def test_copy_common_fields_off_keeps_narrow_list():
    parser = _parser(v1_compatibility=False)
    base: dict = {}
    parser._copy_common_fields(base, ORIGINAL_ROW, extended=True)
    assert base["account_id"] == "act_1"
    assert base["ad_id"] == "ad_9"
    assert "ad_name" not in base
    assert "impressions" not in base


def test_copy_common_fields_on_copies_all_scalar_fields():
    parser = _parser(v1_compatibility=True)
    base: dict = {}
    parser._copy_common_fields(base, ORIGINAL_ROW, extended=True)
    assert base["ad_name"] == "Spring Sale"
    assert base["impressions"] == "1234"
    assert base["clicks"] == "56"
    assert base["spend"] == "7.89"
    assert base["reach"] == "1000"
    # nested action-stat arrays are never copied as a scalar column
    assert "actions" not in base
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_output_parser_v1_compat.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'v1_compatibility'`.

- [ ] **Step 3: Add the constructor param**

In `src/output_parser.py`, change `__init__` from:

```python
    def __init__(self, page_loader, page_id: str, row_config):
        self.page_loader = page_loader
        self.page_id = page_id
        self.row_config = row_config
```

to:

```python
    def __init__(self, page_loader, page_id: str, row_config, v1_compatibility: bool = False):
        self.page_loader = page_loader
        self.page_id = page_id
        self.row_config = row_config
        # CFTL-630: opt-in V1-parity output. Default False = identical to 0.0.17.
        self.v1_compatibility = v1_compatibility
```

- [ ] **Step 4: Rewrite `_copy_common_fields`**

In `src/output_parser.py`, change `_copy_common_fields` from:

```python
    def _copy_common_fields(self, base_row: dict, original_row: dict, extended: bool) -> None:
        fields = [
            "account_id",
            "ad_id",
            "adset_id",
            "campaign_id",
            "date_start",
            "date_stop",
            "publisher_platform",
        ]
        if extended:
            fields += ["account_name", "campaign_name"]

        for field in fields:
            if field in original_row:
                base_row[field] = original_row[field]
```

to:

```python
    def _copy_common_fields(self, base_row: dict, original_row: dict, extended: bool) -> None:
        """Copy fields from the originating insights row onto a per-action row.

        With ``v1_compatibility`` on, action-breakdown rows (``extended=True``) receive
        every scalar field from the originating row — true V1 parity, so metric columns
        the user requested (``ad_name``, ``impressions``, ``clicks``, ``spend``,
        ``reach``, …) flow through instead of being silently dropped (CFTL-630). Nested
        action-stat arrays/dicts are skipped — they are unpacked by ``_populate_action_row``.
        Off (default) keeps the narrow 0.0.17 list so existing output is unchanged.
        """
        if extended and self.v1_compatibility:
            for key, value in original_row.items():
                if key in self.ADS_ACTION_STATS_ROW or isinstance(value, dict | list):
                    continue
                base_row[key] = value
            return

        fields = [
            "account_id",
            "ad_id",
            "adset_id",
            "campaign_id",
            "date_start",
            "date_stop",
            "publisher_platform",
        ]
        if extended:
            fields += ["account_name", "campaign_name"]

        for field in fields:
            if field in original_row:
                base_row[field] = original_row[field]
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_output_parser_v1_compat.py -v`
Expected: both tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/output_parser.py tests/test_output_parser_v1_compat.py
git commit -m "feat(CFTL-630): copy all scalar fields onto per-action rows when v1_compatibility on"
```

---

### Task 3: Backfill declared-but-omitted fields when flag is on

**Files:**
- Modify: `src/output_parser.py` — `__init__` (parse declared fields once), `_process_row:154-163` (gated backfill call), add 4 helper methods after `_process_row`.
- Test: `tests/test_output_parser_v1_compat.py` (append)

**Interfaces:**
- Consumes: `self.v1_compatibility` from Task 2.
- Produces: `_parse_declared_fields(query) -> list[str]`, `_split_field_dsl(s) -> list[str]`, `_base_field_name(token) -> str`, `_backfill_declared_fields(row) -> dict`. `self._declared_fields: list[str]` set in `__init__`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_output_parser_v1_compat.py`:

```python
# --- declared-field parsing ---------------------------------------------

def test_parse_declared_fields_from_dsl():
    parser = _parser(v1_compatibility=True, fields="insights.date_preset(last_30d){impressions,ad_name,actions}")
    assert parser._declared_fields == ["impressions", "ad_name", "actions"]


def test_parse_declared_fields_strips_expansion_and_modifiers():
    parser = _parser(
        v1_compatibility=True,
        fields="insights{impressions,comments{message,from{name}},comments.limit(0).summary(true)}",
    )
    assert parser._declared_fields == ["impressions", "comments", "comments"]


def test_parse_declared_fields_from_plain_csv():
    parser = _parser(v1_compatibility=True, fields="id,name,impressions")
    assert parser._declared_fields == ["id", "name", "impressions"]


def test_parse_declared_fields_from_parameters_string():
    parser = _parser(v1_compatibility=True, fields="", parameters="level=ad&fields=impressions,ad_name")
    assert parser._declared_fields == ["impressions", "ad_name"]


def test_parse_declared_fields_from_parameters_dict():
    parser = _parser(v1_compatibility=True, fields="", parameters={"fields": "impressions,spend"})
    assert parser._declared_fields == ["impressions", "spend"]


# --- _backfill_declared_fields ------------------------------------------

def test_backfill_fills_missing_declared_field_with_empty_string():
    parser = _parser(v1_compatibility=True, fields="id,impressions,spend")
    filled = parser._backfill_declared_fields({"id": "1", "spend": "5.00"})
    assert filled["impressions"] == ""
    assert filled["spend"] == "5.00"  # present values are never overwritten


def test_backfill_noop_when_nothing_missing():
    parser = _parser(v1_compatibility=True, fields="id,impressions")
    row = {"id": "1", "impressions": "10"}
    assert parser._backfill_declared_fields(row) == row


def test_off_path_does_not_backfill_end_to_end():
    """With the flag off, an omitted declared field must NOT appear in output."""
    off = _parser(v1_compatibility=False, fields="id,spend,impressions", name="q")
    on = _parser(v1_compatibility=True, fields="id,spend,impressions", name="q")
    # spend is a meaningful (non-identifier) field so the row is emitted; impressions
    # is declared but omitted by FB. _has_meaningful_data drops rows that carry only
    # basic identifiers (id/parent_id/ex_account_id/fb_graph_node), so spend is required.
    response = {"data": [{"id": "1", "spend": "5.00"}]}
    off_rows = off.parse_data(response, fb_node="page", parent_id="act_1")["q"]
    on_rows = on.parse_data(response, fb_node="page", parent_id="act_1")["q"]
    assert "impressions" not in off_rows[0]
    assert off_rows[0]["spend"] == "5.00"
    assert on_rows[0]["impressions"] == ""
    assert on_rows[0]["spend"] == "5.00"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_output_parser_v1_compat.py -v -k "parse_declared or backfill or off_path"`
Expected: FAIL — `AttributeError: 'OutputParser' object has no attribute '_declared_fields'` / `_backfill_declared_fields`.

- [ ] **Step 3: Parse declared fields in `__init__`**

In `src/output_parser.py`, extend `__init__` (now from Task 2) to:

```python
    def __init__(self, page_loader, page_id: str, row_config, v1_compatibility: bool = False):
        self.page_loader = page_loader
        self.page_id = page_id
        self.row_config = row_config
        # CFTL-630: opt-in V1-parity output. Default False = identical to 0.0.17.
        self.v1_compatibility = v1_compatibility
        # Parsed once; only consulted by _backfill_declared_fields when the flag is on.
        self._declared_fields = self._parse_declared_fields(getattr(row_config, "query", None))
```

- [ ] **Step 4: Gate the backfill call in `_process_row`**

In `src/output_parser.py`, change the top of `_process_row` from:

```python
        """Process a single row from the API response."""
        table_name = self._get_table_name(table_name or getattr(self.row_config.query, "path", ""))
```

to:

```python
        """Process a single row from the API response."""
        table_name = self._get_table_name(table_name or getattr(self.row_config.query, "path", ""))

        # CFTL-630: backfill fields the user requested but FB omitted for this period,
        # so Storage loads don't fail with "Missing columns: ...". Opt-in only.
        if self.v1_compatibility:
            row = self._backfill_declared_fields(row)
```

- [ ] **Step 5: Add the helper methods**

In `src/output_parser.py`, insert these methods immediately after `_process_row` (before `_create_base_row`):

```python
    def _backfill_declared_fields(self, row: dict[str, Any]) -> dict[str, Any]:
        """Return a row containing every field the user declared in the query.

        Facebook omits a field entirely (not null) when there is no data for it in the
        queried period. Downstream Storage loads then fail with ``Missing columns: ...``
        because the destination table already has the column from a prior run. Filling
        absent declared fields with ``""`` keeps the output CSV schema stable (CFTL-630).
        """
        if not self._declared_fields:
            return row
        missing = [field for field in self._declared_fields if field not in row]
        if not missing:
            return row
        filled = dict(row)
        for field in missing:
            filled[field] = ""
        return filled

    @staticmethod
    def _parse_declared_fields(query) -> list[str]:
        """Return the explicit field list the user declared in the query config.

        Recognises the DSL ``insights...{a,b,c}`` form, a plain CSV ``fields = "a,b,c"``,
        and a ``fields=...`` entry inside ``parameters`` (string or dict).
        """
        if query is None:
            return []

        fields_attr = str(getattr(query, "fields", "") or "")
        if fields_attr.startswith("insights"):
            if "{" in fields_attr and "}" in fields_attr:
                inner = fields_attr.split("{", 1)[1].rsplit("}", 1)[0]
                return OutputParser._split_field_dsl(inner)
        elif fields_attr:
            return OutputParser._split_field_dsl(fields_attr)

        parameters = getattr(query, "parameters", None)
        if isinstance(parameters, str):
            for pair in parameters.split("&"):
                if pair.startswith("fields="):
                    return OutputParser._split_field_dsl(pair[len("fields=") :])
        elif isinstance(parameters, dict):
            fields_val = parameters.get("fields")
            if isinstance(fields_val, str):
                return OutputParser._split_field_dsl(fields_val)
            if isinstance(fields_val, list):
                return [OutputParser._base_field_name(str(f)) for f in fields_val if str(f).strip()]

        return []

    @staticmethod
    def _split_field_dsl(fields_str: str) -> list[str]:
        """Split a FB Graph field DSL list into base field names.

        Splits on commas at depth 0 of both ``{}`` and ``()`` so field expansion
        (``comments{message,from{name}}``) and modifier calls
        (``comments.limit(0).summary(true)``) don't split on their inner commas.
        """
        tokens: list[str] = []
        depth_brace = 0
        depth_paren = 0
        current: list[str] = []
        for ch in fields_str:
            if ch == "{":
                depth_brace += 1
                current.append(ch)
            elif ch == "}":
                depth_brace -= 1
                current.append(ch)
            elif ch == "(":
                depth_paren += 1
                current.append(ch)
            elif ch == ")":
                depth_paren -= 1
                current.append(ch)
            elif ch == "," and depth_brace == 0 and depth_paren == 0:
                tokens.append("".join(current))
                current = []
            else:
                current.append(ch)
        if current:
            tokens.append("".join(current))
        return [name for name in (OutputParser._base_field_name(t) for t in tokens) if name]

    @staticmethod
    def _base_field_name(field_dsl: str) -> str:
        """Strip ``{...}`` expansion and ``.modifier(args)`` suffix from a DSL token."""
        token = field_dsl.replace("\n", "").strip()
        cuts = [i for i in (token.find(c) for c in "{.(") if i >= 0]
        return token[: min(cuts)].strip() if cuts else token
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_output_parser_v1_compat.py -v`
Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/output_parser.py tests/test_output_parser_v1_compat.py
git commit -m "feat(CFTL-630): backfill declared-but-omitted fields when v1_compatibility on"
```

---

### Task 4: Thread the flag through FacebookClient and the component

**Files:**
- Modify: `src/client.py:124-126` (`FacebookClient.__init__`) and the 5 `OutputParser(...)` call sites (`223, 302, 334, 379, 394`)
- Modify: `src/component.py:147` (`FacebookClient(...)` construction)
- Test: `tests/test_output_parser_v1_compat.py` (append a propagation test)

**Interfaces:**
- Consumes: `Configuration.v1_compatibility` (Task 1), `OutputParser(..., v1_compatibility=...)` (Task 2).
- Produces: `FacebookClient.__init__(self, oauth, api_version, v1_compatibility: bool = False)` storing `self.v1_compatibility` and forwarding it to every `OutputParser` it builds.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_output_parser_v1_compat.py`:

```python
def test_facebook_client_stores_v1_compatibility():
    from client import FacebookClient

    oauth = MagicMock()
    oauth.data = {"access_token": "tok"}
    client = FacebookClient(oauth, "v23.0", True)
    assert client.v1_compatibility is True

    default_client = FacebookClient(oauth, "v23.0")
    assert default_client.v1_compatibility is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_output_parser_v1_compat.py::test_facebook_client_stores_v1_compatibility -v`
Expected: FAIL — `AttributeError: 'FacebookClient' object has no attribute 'v1_compatibility'`.

- [ ] **Step 3: Add the param to `FacebookClient.__init__`**

In `src/client.py`, change:

```python
    def __init__(self, oauth: OauthCredentials, api_version: str):
        self.oauth = oauth
        self.api_version = api_version
```

to:

```python
    def __init__(self, oauth: OauthCredentials, api_version: str, v1_compatibility: bool = False):
        self.oauth = oauth
        self.api_version = api_version
        # CFTL-630: forwarded to every OutputParser this client builds.
        self.v1_compatibility = v1_compatibility
```

- [ ] **Step 4: Forward the flag to every OutputParser (2 edits, replace-all)**

In `src/client.py`, replace **all occurrences** of:

```python
OutputParser(page_loader, page_id, row_config)
```

with:

```python
OutputParser(page_loader, page_id, row_config, self.v1_compatibility)
```

(updates lines 223, 379, 394). Then replace **all occurrences** of:

```python
OutputParser(page_loader=None, page_id=item_id, row_config=row_config)
```

with:

```python
OutputParser(page_loader=None, page_id=item_id, row_config=row_config, v1_compatibility=self.v1_compatibility)
```

(updates lines 302, 334).

- [ ] **Step 5: Pass the flag from the component**

In `src/component.py`, change line 147 from:

```python
        self.client: FacebookClient = FacebookClient(self.configuration.oauth_credentials, self.config.api_version)
```

to:

```python
        self.client: FacebookClient = FacebookClient(
            self.configuration.oauth_credentials, self.config.api_version, self.config.v1_compatibility
        )
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_output_parser_v1_compat.py -v`
Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/client.py src/component.py tests/test_output_parser_v1_compat.py
git commit -m "feat(CFTL-630): thread v1_compatibility from config through client to parser"
```

---

### Task 5: Regression proof — full suite unchanged with flag off

**Files:**
- No source changes. Verification task. The existing functional cassettes and unit tests have no `v1_compatibility` in their configs, so they exercise the default-off path and must pass unchanged.

**Interfaces:**
- Consumes: all prior tasks.
- Produces: evidence that default-off == 0.0.17.

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -q`
Expected: PASS, with **no diffs** in any `tests/functional/*/expected/` output. If any functional test fails on a column/row diff, the gating is wrong (something widened output with the flag off) — STOP and fix before proceeding; do not edit the expected fixtures to make them pass.

- [ ] **Step 2: Lint**

Run: `uv run ruff check src/ tests/` then `uv run ruff format --check src/ tests/`
Expected: clean. Fix any findings (e.g. `uv run ruff format src/ tests/`) and re-run.

- [ ] **Step 3: Commit (only if lint produced formatting changes)**

```bash
git add -A
git commit -m "style(CFTL-630): ruff format"
```

---

### Task 6: Document the parameter

**Files:**
- Modify: `README.md` (configuration/parameters section)

**Interfaces:** none.

- [ ] **Step 1: Add documentation**

In `README.md`, in the configuration/parameters section, add an entry documenting the flag. Use this content:

```markdown
### `v1_compatibility` (optional, boolean, default `false`)

Opt-in flag for customers migrating from the V1 (Clojure) Facebook Ads extractor who
need V2 output to match V1. When `true`:

- **Action-breakdown queries** (`action_breakdowns=action_type` / `action_reaction`)
  copy **all** scalar fields from the originating insights row onto each per-action row,
  so metrics such as `impressions`, `ad_name`, `clicks`, `spend`, and `reach` are present
  on every row instead of only on the rows where `action_type` is empty.
- Any field requested in the query but **omitted by the Facebook API** for a period with
  no data is written as an empty value, keeping the output table schema stable across runs
  (avoids `Some columns are missing in the csv file` Storage load failures).

When `false` (default) the extractor output is unchanged. Leave it off unless you are
matching V1 output — enabling it makes affected tables wider.

```json
{
  "parameters": {
    "v1_compatibility": true
  }
}
```
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs(CFTL-630): document v1_compatibility parameter"
```

---

### Task 7 (optional, needs live credentials): record ON-path VCR functional test

**Files:**
- Create: `tests/functional/facebook_ads_v1_compatibility/` (config.json + cassette + expected) via the recorder.

**Interfaces:** none — adds end-to-end coverage of the on-path.

> This task records a real FB API interaction and therefore needs live OAuth credentials. The deterministic unit tests in Tasks 2–3 already prove the behavior; this adds full pipeline coverage. Skip if credentials are unavailable and track separately.

- [ ] **Step 1:** Invoke the `component-developer:generate-vcr-tests` skill to record a new functional case whose `config.json` sets `"parameters": {"v1_compatibility": true, ...}` and uses an `action_breakdowns=action_type` insights query. Verify the recorded `expected/` per-action rows contain `impressions`/`ad_name`.
- [ ] **Step 2:** Run `uv run pytest tests/test_functional.py -k facebook_ads_v1_compatibility -v`; expect PASS.
- [ ] **Step 3:** Commit the recorded fixtures.

---

## Follow-up (out of this plan's scope)

- **`keboola/ui` checkbox:** add a checkbox to the ex-facebook custom UI module that writes `parameters.v1_compatibility`. Separate repo, separate PR/release. Backend (this plan) ships first; the customer can set raw config meanwhile.
- **Release:** new minor tag once merged. Default-off means no per-customer coordination is required.
