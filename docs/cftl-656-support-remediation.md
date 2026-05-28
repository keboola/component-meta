# Support remediation — FB/Instagram extractors after the 0.0.18 incident (CFTL-656 / SUPPORT-16397)

For customers whose extractor **failed after the 0.0.17 rollback** or **fails on
upgrade to 0.0.19**, with an error like:

```
Failed to load table "...": Some columns are missing in the csv file.
Missing columns: <cols>. Expected columns: ...
```

## What happened

`0.0.18` of `keboola.ex-facebook-ads-v2` / `keboola.ex-facebook-pages` /
`keboola.ex-instagram-v2` added extra columns to output tables. On
**column-add-tolerant backends (Snowflake / most Azure & GCP stacks)** the
0.0.18 job *succeeded and physically widened the Storage table*. After the
rollback to 0.0.17 (and likewise under the 0.0.19 fix) the extractor stops
emitting those columns, so an incremental load now fails with **`Missing
columns`** — the table has columns the new CSV doesn't.

(On **BigQuery BYODB** the 0.0.18 job was rejected up front, so those tables
stayed clean and need no remediation.)

The 0.0.18-added columns are **empty** (no data), so dropping them is safe.

## Fix: drop the 0.0.18-added columns from the affected tables

1. Identify the added columns. The reliable, customer-specific way (works for any
   config): open the failing table's **Events**, find the import event from the
   **0.0.18 run** vs the prior **0.0.17 run**, and note the columns that appeared.
   The error message's `Missing columns: …` list is also exactly the set to drop.
2. (Optional, to reassure the customer) create a **table snapshot** first.
3. In the table **Schema** tab, select those columns and **Delete**.
4. Re-run the configuration — it loads cleanly afterward.

### Columns 0.0.18 typically adds, by component (from the cf-dev reproduction)

The exact set depends on the customer's query field list; use these as a guide
for what to look for (the `Missing columns` error is authoritative per table).

**keboola.ex-instagram-v2**
- media tables: `comments`, `from_id`, `from_full_name`
- comment tables: parent-row fields leaked into the child table — `caption`,
  `comments`, `comments_count`, `from_full_name`, `from_id`, `ig_id`,
  `is_comment_enabled`, `like_count`, `media_type`, `media_url`, `owner`,
  `permalink`, `shortcode`, `thumbnail_url`

**keboola.ex-facebook-pages**
- comments tables: `comments`
- post-summary tables: `comments`
- ratings tables: `rating`
- feed child tables (`*_attachments`, `*_comments`, `*_likes`): parent-row
  scalars leaked in (`message`, `created_time`, `shares`, `permalink_url`,
  `is_published`, …)

**keboola.ex-facebook-ads-v2**
- action-stats / insights tables: `actions`, `action_values`,
  `video_30_sec_watched_actions`, `video_p25/p50/p75/p100_watched_actions`
- some insights tables also gained legitimate CFTL-630 fields that 0.0.17
  lacked (`ad_id`, `campaign_id`, `campaign_name`, `reach`) — these are also
  empty on the affected rows and safe to drop for the migration
- adcreatives tables: `adset_id`, `link_url`, `object_url`
- customconversions tables: `action_source_type`, `description`, `event_source_id`

## Alternative (no column drop): pin the 0.0.18 custom tag temporarily

If the customer is uncomfortable dropping columns, unblock them by pinning the
0.0.18 tag via the config `runtime` field until 0.0.19 ships and they can do the
column drop at leisure:

```json
{ "parameters": { }, "runtime": { "tag": "0.0.18" } }
```

This keeps producing the wide schema that matches their current table.

## Customer messaging notes

- Apologise; the 0.0.18 release contained a bug and was rolled back.
- The extra columns are **empty** — dropping them loses no data (offer a snapshot
  if they're unsure; this addresses the concern raised in SUPPORT-16425).
- After 0.0.19 ships, no custom tag is needed; the column drop is the durable fix.
- Do **not** name other customers; reference SUPPORT/Linear IDs only.
