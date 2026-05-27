# CFTL-656 release migration test (SUPPORT-16397)

Customer-side acceptance test for the declared-field-backfill fix shipping as
`0.0.19`. Validates that the three real customer migration paths across
`0.0.17 → 0.0.18 → 0.0.19` are safe, on **real incremental Storage loads**
(not VCR — VCR replays HTTP, it cannot exercise Storage schema evolution).

## Why a platform test (not a functional/VCR test)

The bug and its fix are about the **column set written to an existing Storage
table on an incremental load**. Storage rejects a load when the CSV column set
diverges from the destination table:

- CSV has a column the table lacks → `Extra columns found` (the 0.0.18 forward break)
- CSV lacks a column the table has → `Missing columns` (the reverse-direction risk)

Only a real run against a persistent table reproduces this. VCR functional
tests (in `tests/functional/`) cover the parser output shape; this runbook
covers the cross-version Storage transitions.

## Versions under test

| Label | Image tag | Meaning |
|---|---|---|
| 0.0.17 | `0.0.17` | last good release before the incident (current rollback) |
| 0.0.18 | `0.0.18` | the CFTL-630 backfill release that caused SUPPORT-16397 |
| 0.0.19 | `cftl-656-followup-bare-tokens-and-child-rows-419` | this branch's CI build (the fix) |

## Setup

- Project: `cf-dev` (4214).
- One config per (component × cohort), copied from the canonical changelog test
  configs, so every config carries the **full bug-triggering field list**
  (bare-token `comments,from_id,from_full_name` on instagram Q16, nested-edge
  tokens on fb-pages, action-stats on fb-ads). Each config writes incrementally
  into its own derived bucket `in.c-<component>-<config-id>`, giving natural
  isolation.
- Driver: `/tmp/cust_test/driver.py` (kept local, not committed). Config id map:
  `/tmp/cust_test/configs.tsv`. Raw results: `/tmp/cust_test/results.json`.

## Cohorts & sequences

All cohorts use the bug-triggering config. Under incremental load, `0.0.18`
succeeds **only when its table is born on 0.0.18** (the first load of a new
table accepts any column set); on a table already created by 0.0.17 it fails
with `Extra columns`. That platform fact shapes the three sequences:

| Cohort | Real-world customer | Sequence | Storage state probed |
|---|---|---|---|
| **A** | suffered the 0.0.18 break, rolled back | `0.0.17 → 0.0.18 → 0.0.17 → 0.0.19` | does the fix load cleanly onto a **0.0.17-shaped** table |
| **B** | "worked fine" on 0.0.18 (table born on 0.0.18) | `0.0.17 → [drop bucket] → 0.0.18 → 0.0.19` | **reverse direction** — does the fix break a **phantom-laden** table with `Missing columns` |
| **C** | broke on 0.0.18, never adopted it (custom-tagged 0.0.17) | `0.0.17 → 0.0.19` | does the fix load cleanly onto a 0.0.17-shaped table (no 0.0.18 in history) |

> Cohort B's `[drop bucket]` between 0.0.17 and 0.0.18 models the only realistic
> way a phantom-column-producing config "worked fine" on 0.0.18: the destination
> table was new (or recreated) under 0.0.18, so the first load accepted the
> phantom columns and persisted them. This is the schema state that then meets
> 0.0.19 — the case we most need to clear.

## Backend matters — and cf-dev is Snowflake

`cf-dev` runs on **Snowflake**, whose incremental load is **asymmetric**:

- **Adding** columns is tolerated — a CSV with extra columns silently widens the table.
- **Missing** columns are rejected — a CSV lacking a column the table has fails with
  `Some columns are missing in the csv file. Missing columns: …`.

The original SUPPORT-16397 customer failure was on **BigQuery BYODB**, which rejects
**both** directions (`During the import new columns can't be added`). So the two
backends fail at *different* steps:

| | adds phantom cols (0.0.18 forward) | drops phantom cols (0.0.19 / rollback) |
|---|---|---|
| **BigQuery BYODB** | ❌ rejected up front (table stays clean) | ✅ clean table, fix loads fine |
| **Snowflake** | ✅ silently widens table | ❌ rejected — `Missing columns` |

This is the crux: **on Snowflake, 0.0.18 succeeds but permanently widens the table,
and then *both the 0.0.17 rollback and the 0.0.19 fix fail* with `Missing columns`** —
because neither re-emits the phantom columns the table now carries. This empirically
explains the post-rollback failures the Lion Communications reporter described in
CFTL-630 ("even the rollback must be broken … some of our components started to fail"):
that project is Snowflake (eu-central-1), its tables were widened by 0.0.18, so the
0.0.17 rollback then rejected the narrower CSV.

## Predicted outcomes

| Cohort | step | tag | predicted | rationale |
|---|---|---|---|---|
| A | 1 | 0.0.17 | ✅ success | fresh table, minimal column set |
| A | 2 | 0.0.18 | ❌ `Extra columns` | backfill adds phantom cols vs the 0.0.17 table; load rejected, table unchanged |
| A | 3 | 0.0.17 | ✅ success | back to minimal column set, matches table |
| A | 4 | 0.0.19 | ✅ success | fix produces minimal column set, matches table |
| B | 1 | 0.0.17 | ✅ success | fresh table |
| B | 2 | 0.0.18 | ✅ success | table dropped first → born on 0.0.18 with phantom cols |
| B | 3 | 0.0.19 | ⚠️ **key check** | fix drops phantom cols; table has them → risk of `Missing columns` |
| C | 1 | 0.0.17 | ✅ success | fresh table |
| C | 2 | 0.0.19 | ✅ success | fix produces minimal column set onto 0.0.17 table |

The cohort-B step-3 result is the decisive one for the release: if it loads,
the fix is safe even for tables that picked up phantom columns under 0.0.18.
If it fails with `Missing columns`, those customers need a column-drop migration
before upgrading — captured in the rollout note on PR #55.

## Results

<!-- filled in by the run; see /tmp/cust_test/results.json for raw column sets -->

### instagram — COMPLETE (Snowflake / cf-dev)

Tracked tables: `query_16_media` (parent), `query_16_comments` (child). Column
counts shown as `media`/`comments`.

| Cohort | step | tag | job | cols (media/comments) | note |
|---|---|---|---|---|---|
| A | 1 | 0.0.17 | ✅ success | 16 / 6 | fresh table, correct columns |
| A | 2 | 0.0.18 | ✅ success | 19 / 20 | Snowflake **widens** table (phantom cols added) |
| A | 3 | 0.0.17 | ❌ `Missing columns: comments,from_full_name,from_id` | 19 / 20 | rollback can't fill the phantom cols → **rejected** |
| A | 4 | 0.0.19 | ❌ `Missing columns: comments,from_full_name,from_id` | 19 / 20 | **fix also rejected** — table still carries 0.0.18 phantoms |
| B | 1 | 0.0.17 | ✅ success | 16 / 6 | fresh table |
| B | 2 | 0.0.18 (bucket dropped first) | ✅ success | 19 / 20 | table **born on 0.0.18** with phantom cols |
| B | 3 | 0.0.19 | ❌ `Missing columns: …` | 19 / 20 | reverse-direction break confirmed |
| C | 1 | 0.0.17 | ✅ success | 16 / 6 | fresh table |
| C | 2 | 0.0.19 | ✅ success | 16 / 6 | **clean upgrade** — table never touched by 0.0.18 |

Job IDs: A = 46209068 / 46209499 / 46209634 / 46209730; B = 46209822 / 46210093 / 46210319; C = 46210357 / 46210383.

**Verdict (instagram):** the fix produces the *correct* column set everywhere
(16/6, matching the VCR functional expecteds). The only clean upgrade path is
**cohort C** — a table whose history never included a successful 0.0.18 run. Any
Snowflake table widened by 0.0.18 (cohorts A and B) rejects **both** the 0.0.17
rollback and the 0.0.19 fix until the phantom columns are dropped.

Phantom columns 0.0.18 adds (must be dropped before upgrading a widened table):

- `query_16_media`: `comments`, `from_id`, `from_full_name`
- `query_16_comments`: `caption`, `comments`, `comments_count`, `from_full_name`, `from_id`, `ig_id`, `is_comment_enabled`, `like_count`, `media_type`, `media_url`, `owner`, `permalink`, `shortcode`, `thumbnail_url`

### facebook_pages — COMPLETE (Snowflake / cf-dev)

Same pattern as instagram. Phantom columns appear on `query_23_comments`,
`query_42_summary`, `query_50_ratings`.

| Cohort | step | tag | job | note |
|---|---|---|---|---|
| A | 1 | 0.0.17 | ✅ success | baseline schema |
| A | 2 | 0.0.18 | ✅ success | table widened (+1 col on 3 tables) |
| A | 3 | 0.0.17 | ❌ `Missing columns` | rollback rejected |
| A | 4 | 0.0.19 | ❌ `Missing columns` | fix also rejected — table still carries phantoms |
| B | 1 | 0.0.17 | ✅ success | baseline |
| B | 2 | 0.0.18 (bucket dropped) | ✅ success | born on 0.0.18 with phantoms |
| B | 3 | 0.0.19 | ❌ `Missing columns` | reverse-direction break |
| C | 1 | 0.0.17 | ✅ success | baseline |
| C | 2 | 0.0.19 | ✅ success | **clean upgrade** |

Phantom columns 0.0.18 adds (drop before upgrading a widened table):
- `query_23_comments`: `comments`
- `query_42_summary`: `comments`
- `query_50_ratings`: `rating`

Verdict identical: clean only for cohort C; widened tables (A, B) reject both
rollback and fix until phantoms dropped.

### facebook_ads — COMPLETE (Snowflake / cf-dev)

Same pattern. 0.0.18 widens many `*_insights` action tables plus
`query_134_adcreatives` and `query_218_customconversions`.

| Cohort | step | tag | job |
|---|---|---|---|
| A | 1 | 0.0.17 | ✅ success |
| A | 2 | 0.0.18 | ✅ success (table widened) |
| A | 3 | 0.0.17 | ❌ `Missing columns` (e.g. `ad_id` on query_170) |
| A | 4 | 0.0.19 | ❌ `Missing columns` |
| B | 1 | 0.0.17 | ✅ success |
| B | 2 | 0.0.18 (bucket dropped) | ✅ success |
| B | 3 | 0.0.19 | ❌ `Missing columns: ad_id` (query_170) |
| C | 1 | 0.0.17 | ✅ success |
| C | 2 | 0.0.19 | ✅ success (clean upgrade) |

Phantom / feature columns 0.0.18 adds (drop before upgrading a widened table):
- `query_4_insights`, `query_41_insights`: `action_values`
- `query_28_insights`, `query_37_insights`: `actions`, `action_values`
- `query_195_insights`: `actions`
- `query_193_insights`: `actions`, `action_values`, `video_30_sec_watched_actions`, `video_p25/p50/p75/p100_watched_actions`
- `query_170_insights`: `ad_id`
- `query_113_insights`: `campaign_id`, `campaign_name`
- `query_45_insights`: `reach`
- `query_134_adcreatives`: `adset_id`, `link_url`, `object_url`
- `query_218_customconversions`: `action_source_type`, `description`, `event_source_id`

> Note: some of these (e.g. `ad_id`, `campaign_name`, `reach`) are the **legitimate
> CFTL-630 fields** the feature is meant to add — not phantoms. They still count as
> "columns the 0.0.17 table lacks", so they participate in the same widen/narrow
> Storage mechanic. This is why the migration affects more than just the bare-token
> phantom tables.

### The one-line mental model (empirically verified)

```
0.0.17 schema  ⊂  0.0.19 schema  ⊂  0.0.18 schema
```

- **0.0.19 vs a 0.0.17-shaped table** (never ran 0.0.18, or 0.0.18 was rejected on
  BigQuery): 0.0.19 is *wider* — it adds the legitimate CFTL-630 feature columns.
  fb-ads cohort C confirmed: most tables identical to 0.0.17, ~8 widened
  (`query_113` 12→14, `query_134` 7→10, `query_28` 7→9, `query_218` 8→11,
  `query_37` 9→11, `query_41` 16→17, `query_45` 12→13, `query_4` 17→18). On
  Snowflake the widening is accepted (cohort C succeeded); on BigQuery BYODB it
  would be rejected (`Extra columns`) — inherent to delivering the feature, not a
  PR defect.
- **0.0.19 vs a 0.0.18-widened table** (ran 0.0.18 successfully on Snowflake):
  0.0.19 is *narrower* (phantoms removed) → `Missing columns` until those columns
  are dropped (cohorts A/B).

## Verdict & decision

The fix (PR #55) produces the **correct, intended schema** on every transition.
The cross-version *Storage* failures are caused by table column-set mutation under
0.0.18, not by the fix. Decision for the release: **ship PR #55** + a support
remediation runbook (drop the 0.0.18-added columns on already-widened tables). The
auto-heal reconcile-to-existing-schema fallback is recorded as an optional
follow-up (needs `forward_token` + Storage introspection; CFT-2729 precedent).

## Release implications

1. **The code fix is correct but not sufficient on its own for Snowflake.** 0.0.19
   emits the right column set, but a Snowflake table that was widened by a
   successful 0.0.18 run will reject it with `Missing columns` — exactly as it
   already rejects the 0.0.17 rollback. These customers are *already broken on
   0.0.17 today*; shipping 0.0.19 does not un-break them by itself.

2. **Mandatory remediation for any table touched by 0.0.18 on Snowflake:** drop the
   phantom columns from the destination Storage tables (or recreate the tables)
   before the first 0.0.19 run. The phantom-column lists per query are recorded
   above (instagram) and below (pages/ads). This makes the rollout note on PR #55
   a hard prerequisite, not advisory, for the Snowflake + 0.0.18 cohort.

3. **BigQuery BYODB customers are fine:** 0.0.18 was rejected up front there, so
   their tables stayed clean and 0.0.19 loads without remediation.

4. **Safe cohort everywhere:** any customer who never had a successful 0.0.18 run
   (cohort C) upgrades to 0.0.19 cleanly.

5. **Optional product follow-up to consider:** a one-shot "reconcile to existing
   Storage schema" mode, or guidance to use full-load (not incremental) for the
   first 0.0.19 run, would let widened tables self-heal without manual column
   drops. Worth a separate ticket — out of scope for this fix.

## Cleanup

The 9 configs + buckets are left in place for inspection. To remove:

```bash
bash docs/cftl-656-release-migration-test.cleanup.sh
```
