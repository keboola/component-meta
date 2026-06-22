# FB Ads V2 — opt-in V1-compatibility flag (CFTL-630 / SUPPORT-16160)

**Status:** Approved design — ready for implementation plan
**Date:** 2026-06-18
**Component:** `keboola.ex-facebook-ads-v2` (repo: `component-meta`)
**Ticket:** [CFTL-630](https://linear.app/keboola/issue/CFTL-630) · SUPPORT-16160

## Problem

A customer migrating from the V1 (Clojure) Facebook Ads extractor to V2 cannot
migrate because V2 output does not match V1 in two ways:

- **Bug 1 — missing metric columns on per-action rows.** For queries using
  `action_breakdowns=action_type` (or `action_reaction`), the parser routes rows
  through `_process_action_stats → _copy_common_fields`, which copies only a
  hardcoded subset of fields. `impressions`, `ad_name`, `clicks`, `spend`, `reach`
  are present in the raw FB response but silently dropped from every per-action row.
  V1 copied **every** scalar field, so V2 output is narrower than V1.

- **Bug 2 — Storage load crashes when FB omits a declared field.** When the FB
  Marketing API returns no data for a requested field (e.g. `impressions` for a
  zero-impression period), the field is **absent** from the response (not null).
  The output CSV then lacks that column, and Storage load fails:
  `Some columns are missing in the csv file. Missing columns: impressions.`

## Why this was rolled back twice

Every prior attempt changed output **width unconditionally**:

- `0.0.18` (commit `a77fe60`): copied *all* scalar fields onto per-action rows **and**
  backfilled omitted declared fields. This widened tables for V2-native customers
  whose Storage table schemas were built on the narrower `0.0.17` output → loads
  failed for other customers (e.g. GRPN, SUPPORT-16397).
- `0.0.19` (commit `00cfec6`): narrowed the copy to an explicit list but **still**
  widened those tables unconditionally → reverted again (PR #56 → back to `0.0.17`).

**Root cause of the regressions:** any change to output width, applied to all
configs, breaks existing customers whose destination Storage schemas were built on
the previous (narrow) output. The fix that cannot regress is one that is **opt-in**.

## Goal

Re-introduce the entire CFTL-630 output delta **gated behind a single opt-in flag**:

- **Default OFF → byte-identical to current `0.0.17`** (the stable, released
  behavior). Proven by the existing VCR functional cassettes passing unchanged.
- **ON → full V1-parity output** for the migrating customer.

Because the default is a no-op, this release needs **no per-customer coordination**
and cannot cause the regressions that triggered the previous rollbacks.

## Config contract

- New **root-level** parameter `v1_compatibility` (boolean, default `false`),
  sourced from `parameters`.
- Added to the `Configuration` pydantic model in `src/configuration.py`.
- The **backend honors it immediately**. The customer can set it via raw config /
  debug mode today (the customer already uses debug mode and custom tags).
- A **`keboola/ui` checkbox is a fast-follow** PR in the ex-facebook custom UI
  module that writes the **same JSON key**. (This component's `configSchema.json`
  is empty — these Meta extractors render through a custom UI, not a schema-driven
  form, so the checkbox is a separate repo + release. Backend ships first.)

Example config:

```json
{
  "parameters": {
    "v1_compatibility": true,
    "...": "..."
  }
}
```

## Behavior

### When ON — both behaviors gated by the single flag

1. **Wide per-action rows (Bug 1).** For action-breakdown queries
   (`action_breakdowns=action_type` / `action_reaction`), copy **all scalar fields**
   from the originating insights row onto each per-action row. "Scalar" = values
   that are not `list`/`dict` and whose key is not in `ADS_ACTION_STATS_ROW` (those
   nested action-stat arrays are unpacked separately by `_populate_action_row`).
   This is true V1 parity and restores `impressions`, `ad_name`, `clicks`, `spend`,
   `reach`, and anything else the query requested.

2. **Backfill declared-but-omitted fields (Bug 2).** Any field the user declared in
   the query but that FB omitted for the period is added to the row with value `""`,
   keeping the output CSV schema stable across runs regardless of API content.

### When OFF (default)

- No backfill is performed.
- `_copy_common_fields` uses today's narrow hardcoded list.
- Output is identical to `0.0.17`.

## Wiring

```
Configuration.v1_compatibility   (src/configuration.py)
    └─> FacebookClient.__init__   (src/client.py)  — store as self.v1_compatibility
            └─> OutputParser(...)  (5 call sites in client.py) — pass the flag
                    └─> self.v1_compatibility gates both behaviors (src/output_parser.py)
```

- `Configuration(**params)` is built in `component.py:146`; `FacebookClient` is
  constructed at `component.py:147` — extend its constructor with the flag.
- `OutputParser` is instantiated at 5 sites in `client.py`
  (`223, 302, 334, 379, 394`); each must receive the flag (default `False` keeps the
  constructor backward-compatible).

## Reuse vs re-implement

- **Re-implement fresh on current `main`** — do **not** merge the old
  `cftl-630-fb-ads-v2-missing-fields` branch. It is based on a stale pre-v24/v25
  merge-base (`2222247`); merging would drag in unrelated divergence.
- The helper functions from that branch are reused **nearly verbatim** (they are
  self-contained and correct):
  - `_parse_declared_fields(query)` — reads the declared field list from the DSL
    `insights...{a,b,c}` form, plain CSV `fields="a,b,c"`, or a `fields=` entry in
    `parameters` (string or dict).
  - `_split_field_dsl(s)` — brace/paren-aware split so field-expansion
    (`comments{message,from{name}}`) and modifier calls
    (`comments.limit(0).summary(true)`) don't split on inner commas.
  - `_base_field_name(token)` — strips `{...}` expansion and `.modifier(args)`.
  - `_backfill_declared_fields(row)` — fills missing declared fields with `""`.
- **The one deviation from the branch:** the ON path of `_copy_common_fields`
  becomes **copy-all-scalar** (true V1 parity), not the branch's explicit list.

## Tests

- **Regression proof (the core guarantee):** existing VCR functional cassettes pass
  **unchanged** with the flag absent/false. This demonstrates default-OFF is
  identical to `0.0.17`.
- **New functional test** with `v1_compatibility: true` on an
  `action_breakdowns=action_type` query: per-action rows carry `impressions` /
  `ad_name`; a field omitted by FB on an empty period is backfilled to `""`.
- **Unit tests:**
  - `_copy_common_fields` — OFF (narrow list) vs ON (all scalar fields, skipping
    lists/dicts and `ADS_ACTION_STATS_ROW`).
  - `_backfill_declared_fields` — OFF is a no-op; ON fills only missing declared
    fields and never overwrites present values.
  - `_parse_declared_fields` / `_split_field_dsl` / `_base_field_name` — DSL,
    plain CSV, and `parameters` (string + dict) parsing; brace/paren nesting.

## Out of scope (noted)

- The **`keboola/ui` checkbox** — separate PR in the ex-facebook custom UI module,
  writing the same `v1_compatibility` JSON key. Tracked as a fast-follow.
- The pre-existing **"first batch defines the CSV header"** behavior in
  `_create_cached_writer` — unchanged. Backfill (when ON) actually *stabilizes* the
  column set across batches; OFF behavior is untouched.

## Release

- New minor tag. Default-OFF means it is safe to release without per-customer
  coordination.
- The customer enables `v1_compatibility: true` (raw config now; checkbox later) to
  obtain V1-parity output and stable schemas on empty periods.
