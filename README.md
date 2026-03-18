Facebook Pages, Facebook Ads and Instagram Extractors
============================

This extractor extracts data from [Facebook Graph API](https://developers.facebook.com/docs/graph-api/). Use Facebook Pages, Facebook Ads, and Instagram extractors to download data from Facebook Graph API.

**Table of Contents:**

- [Supported Endpoints](#supported-endpoints)
- [Configuration](#configuration)
  - [Basic Structure](#basic-structure)
  - [Nested Query](#nested-query)
    - [The Insights DSL](#the-insights-dsl)
    - [DSL Keywords Reference](#dsl-keywords-reference)
    - [Time Expressions](#time-expressions)
    - [Common Paths](#common-paths)
  - [Async Insights Query](#async-insights-query)
- [Output](#output)
  - [Output Bucket](#output-bucket)
- [Development](#development)
  - [Dependency Management](#dependency-management)
  - [Project Structure](#project-structure)
- [Integration](#integration)

Supported Endpoints
===================

The component supports all Facebook Graph API endpoints available through nested queries:
- **feed** - page posts including comments and likes
- **posts** / **published_posts** - published posts
- **insights** - page-level metrics
- **video** / **video_reels** / **videos** - video content and metrics
- **ratings** - page ratings and reviews
- **conversations** - page inbox conversations
- And any other Graph API edge you can access with your token


Configuration
=============

Basic Structure
---------------

The configuration consists of three main parts:

### api-version
Facebook Graph API version (default: `"v23.0"`). It is recommended to keep this up to date as Meta deprecates older versions.

### accounts
Dictionary of accounts from which data will be extracted. Each account contains:
- **id**: Unique account identifier
- **name**: Account name
- **account_id**: Advertising account ID (optional, Facebook Ads only)
- **business_name**: Business name (optional)
- **currency**: Currency (optional)
- **category**: Page category (optional)
- **fb_page_id**: Facebook page ID linked to an Instagram account (optional, Instagram only)

### queries
List of queries for data extraction. Supports two query types: [Nested Query](#nested-query) and [Async Insights Query](#async-insights-query).

---

Nested Query
------------

Used for Facebook Pages and Instagram. Fetches data from any Graph API edge.

```json
{
  "id": 1,
  "name": "query_name",
  "type": "nested-query",
  "disabled": false,
  "query": {
    "path": "feed",
    "fields": "message,created_time,likes,comments{message,from}",
    "ids": "",
    "limit": "25",
    "since": "30 days ago",
    "until": "today"
  }
}
```

**Parameters:**

| Parameter | Description |
|---|---|
| `path` | Graph API edge to query (e.g., `feed`, `posts`, `insights`). Leave empty to query the account root node. |
| `fields` | Comma-separated list of fields or a nested [Insights DSL](#the-insights-dsl) expression. |
| `ids` | Comma-separated account/page IDs to query. If empty, all configured accounts are used. |
| `limit` | Page size for API pagination (default `25`, max `100`). |
| `since` | Start of time range. Accepts [time expressions](#time-expressions) or ISO dates. |
| `until` | End of time range. Accepts [time expressions](#time-expressions) or ISO dates. |
| `time-based-pagination` | Set to `true` to use time-based pagination instead of cursor-based (useful for large date ranges). |
| `stop-on-empty-response` | Set to `true` to stop pagination when an empty response is received. |
| `run-by-id` | Set to `true` to query each account ID individually instead of in batch. |

---

### The Insights DSL

For metrics data, Facebook uses a special dot-notation DSL inside the `fields` parameter. This is supported for both Facebook Pages (`insights`) and Instagram (`insights`), as well as post-level metrics (`insights` on a `feed` or `posts` edge).

The general syntax is:

```
insights.<modifier1>(<value>).<modifier2>(<value>)...
```

You can optionally append `{name, period, values}` at the end to control which sub-fields are returned.

**Page-level example (Facebook Pages):**
```
insights.since(30 days ago).time_increment(1).metric(page_impressions, page_views_total, page_fans)
```

**Post-level example (on `feed` path):**
```
insights.metric(post_impressions, post_clicks).period(lifetime){name, values}
```

**With breakdown (new Meta API format):**
```
insights.metric(post_media_view).breakdown(is_from_ads).period(lifetime)
```

**Instagram example:**
```
insights.since(30 days ago).period(day).metric_type(total_value).breakdown(media_product_type).metric(reach, total_interactions)
```

**Mixed fields and insights (on `feed` path):**
```
message, created_time, permalink_url, insights.metric(post_impressions).period(lifetime){name, values}
```

---

### DSL Keywords Reference

#### `metric(...)` — required
Specifies which metrics to retrieve. Multiple metrics are comma-separated.

- **Where to find valid metric names:**
  - Facebook Pages: [Page Insights metrics reference](https://developers.facebook.com/docs/graph-api/reference/insights)
  - Deprecated Page metrics (replaced Nov 2025): [Deprecated metrics](https://developers.facebook.com/docs/platforminsights/page/deprecated-metrics/)
  - Instagram: [Instagram Insights metrics](https://developers.facebook.com/docs/instagram-platform/reference/ig-user/insights)
  - Post-level metrics: [Post Insights](https://developers.facebook.com/docs/graph-api/reference/post/insights/)

```
insights.metric(page_impressions, page_views_total, page_fans)
insights.metric(post_clicks, post_impressions_unique)
```

> **Note on deprecated metrics:** As of November 15, 2025, Meta replaced metrics like `post_impressions_fan`, `page_impressions_paid` with new metrics that require explicit `breakdown` parameters. For example, `post_impressions_fan` → `post_media_view` with `breakdown(is_from_followers)`. See [breakdown](#breakdown) below.

---

#### `period(value)` — optional
Aggregation period for the metric values.

| Value | Description |
|---|---|
| `day` | Daily aggregation |
| `week` | Weekly aggregation |
| `month` / `days_28` | Monthly aggregation |
| `lifetime` | Entire lifetime of the object (common for post-level insights) |

```
insights.metric(page_impressions).period(day)
insights.metric(post_impressions).period(lifetime)
```

---

#### `breakdown(value)` — optional
Splits metric values by a dimension. The breakdown value is returned in the `key1`/`key2` output columns.

Common breakdown values for **Facebook Pages**:

| Breakdown | Description |
|---|---|
| `is_from_ads` | Whether the impression came from an ad (`0` or `1`) |
| `is_from_followers` | Whether the viewer is a follower (`0` or `1`) |
| `action_type` | Type of action performed |

Common breakdown values for **Instagram**:

| Breakdown | Description |
|---|---|
| `country` | Country of the audience |
| `city` | City of the audience |
| `age` | Age group |
| `gender` | Gender |
| `media_product_type` | Type of media (POST, REEL, STORY, etc.) |

Multiple breakdowns (up to 2 are captured in `key1`/`key2`, sorted alphabetically):
```
insights.metric(post_media_view).breakdown(is_from_ads, is_from_followers).period(lifetime)
```

> **Important:** `breakdown` here is the Pages/Instagram DSL keyword. It is different from the `breakdowns=` URL parameter used in [Async Insights Queries](#async-insights-query) for Facebook Ads. Do not confuse them.

---

#### `since(expr)` / `until(expr)` — optional
Time range filter within the DSL. Accepts the same [time expressions](#time-expressions) as the top-level `since`/`until` query parameters.

```
insights.since(30 days ago).until(today).metric(page_impressions)
insights.since(now).metric(post_impressions_paid)
```

> **Tip:** `since(now)` is a common pattern meaning "fetch from the most recent data point". The actual date range is controlled by the top-level `since`/`until` query parameters.

---

#### `time_increment(value)` — optional
Controls the time granularity of the returned data points. Value is in days or a period keyword.

```
insights.metric(page_impressions).time_increment(1)       # daily data points
insights.metric(page_impressions).time_increment(monthly)
```

---

#### `as(name)` — optional
Renames the output metric. Useful when using the same metric with different breakdowns in separate queries.

```
insights.metric(post_media_view).breakdown(is_from_ads).as(post_media_view_from_ads).period(lifetime)
```

---

#### `timeframe(value)` — optional (Instagram only)
Used for Instagram demographic insights instead of `since`/`until`.

| Value | Description |
|---|---|
| `this_month` | Current month |
| `last_month` | Previous month |
| `this_year` | Current year |

```
insights.metric(engaged_audience_demographics).period(lifetime).timeframe(this_month).metric_type(total_value).breakdown(country)
```

---

#### `metric_type(value)` — optional (Instagram only)
Controls how metric values are aggregated.

| Value | Description |
|---|---|
| `total_value` | Sum of all values |
| `time_series` | Values over time |

```
insights.period(day).metric_type(total_value).breakdown(media_product_type).metric(reach, total_interactions)
```

---

### Time Expressions

Both the top-level `since`/`until` parameters and the DSL `since()`/`until()` modifiers accept flexible time expressions:

| Expression | Meaning |
|---|---|
| `today` | Today's date |
| `yesterday` | Yesterday |
| `now` | Current timestamp (often used in DSL to mean "most recent") |
| `30 days ago` | 30 days before today |
| `3 day ago` | 3 days before today (singular also accepted) |
| `this month` | Start of the current month |
| `2025-01-01` | Absolute ISO date |
| `last_7d` | Last 7 days — Ads API `date_preset` format only |
| `last_90d` | Last 90 days — Ads API `date_preset` format only |

---

### Common Paths

The `path` parameter specifies which Graph API edge to traverse for each account. Common values:

| Path | Description | Component |
|---|---|---|
| *(empty)* | Account root node | Pages, Instagram, Ads |
| `feed` | Published and scheduled posts | Pages |
| `posts` | Published posts | Pages |
| `published_posts` | All published posts (alternative to `posts`) | Pages |
| `insights` | Page-level insights directly | Pages |
| `video` | Videos | Pages |
| `video_reels` | Reels | Pages |
| `videos` | All video objects | Pages |
| `ratings` | Page ratings and reviews | Pages |
| `conversations` | Inbox conversations | Pages |
| `me/posts` | Posts by the authenticated user | Pages |
| `ads-action-stats` | Ad action statistics | Ads |

You can also use absolute Graph API URLs as the path:
```json
"path": "https://graph.facebook.com/v3.3/SomePage/feed"
```

---

Async Insights Query
--------------------

Used exclusively for **Facebook Ads**. Submits an asynchronous job to the Marketing API and waits for results. Required for large ad account reports where synchronous requests would time out.

```json
{
  "id": 1,
  "name": "campaign_performance",
  "type": "async-insights-query",
  "disabled": false,
  "query": {
    "ids": "act_123456789",
    "parameters": "level=campaign&fields=campaign_id,campaign_name,impressions,clicks,spend&date_preset=last_7d&time_increment=1&action_breakdowns=action_type&breakdowns=impression_device",
    "limit": "100"
  }
}
```

**Parameters:**

| Parameter | Description |
|---|---|
| `ids` | Comma-separated ad account IDs (must start with `act_`). |
| `parameters` | URL query string formatted according to the [Marketing API Insights spec](https://developers.facebook.com/docs/marketing-api/insights). |
| `limit` | Page size for result pagination. |
| `since` / `until` | Optional date range override. |
| `split-query-time-range-by-day` | Set to `true` to split the query into daily requests (useful for large accounts hitting API limits). |

**Key `parameters` options:**

| Option | Description |
|---|---|
| `level` | Aggregation level: `account`, `campaign`, `adset`, `ad` |
| `fields` | Comma-separated metric/dimension fields |
| `date_preset` | Shorthand date range: `last_7d`, `last_30d`, `last_90d`, `last_month`, `this_month`, etc. |
| `time_increment` | Granularity in days, or `monthly` |
| `action_breakdowns` | Break down action metrics by: `action_type`, `action_device`, `action_reaction`, etc. |
| `breakdowns` | Break down impressions/clicks by: `impression_device`, `publisher_platform`, `product_id`, etc. |
| `action_attribution_windows` | Attribution windows: `1d_click`, `7d_click`, `1d_view`, `28d_click`, etc. |
| `filtering` | JSON array of filters, e.g. `[{"field":"action_type","operator":"IN","value":["purchase"]}]` |
| `use_account_attribution_setting` | Use the account's default attribution setting (`true`/`false`) |
| `use_unified_attribution_setting` | Use unified attribution setting (`true`/`false`) |

The `parameters` field also supports the DSL format:
```
insights.level(campaign).action_attribution_windows(7d_click).action_breakdowns(action_type).date_preset(last_7d).time_increment(1){campaign_name,campaign_id,actions,action_values}
```

> **Where to find valid fields and breakdowns:** Use the [Marketing API Insights reference](https://developers.facebook.com/docs/marketing-api/insights) and the [Graph API Explorer](https://developers.facebook.com/tools/explorer/) to discover available fields for your account.

---

Output
======

The component generates the following tables:

### accounts.csv
Contains information about configured accounts with columns:
- `id`, `name`, `account_id`, `business_name`, `currency`, `category`

### Query-based tables
For each query, one or more tables are created based on the data structure:
- **`{query_name}`** — top-level fields
- **`{query_name}_{nested_edge}`** — nested objects (e.g., `feed_comments`, `feed_likes`)

### Insights tables
Insights data is flattened into rows with the following columns:

| Column | Description |
|---|---|
| `id` | Object ID |
| `parent_id` | Parent object ID |
| `ex_account_id` | Account ID the data was fetched for |
| `fb_graph_node` | Describes position in the hierarchy (e.g., `page_feed_insights`) |
| `key1` | First breakdown dimension value |
| `key2` | Second breakdown dimension value |
| `value` | Metric value |
| `end_time` | Timestamp of the data point |
| `name` | Metric name |
| `title` | Metric title |
| `description` | Metric description |
| `period` | Aggregation period |

> **Note on `key1`/`key2`:** For newer Meta metrics using `breakdown(...)`, the breakdown dimension **value** (e.g., `"0"`, `"1"`) is placed in `key1`/`key2`. If two breakdowns are used, they are sorted alphabetically and mapped to `key1` and `key2` respectively. For older nested metrics like `post_reactions_by_type_total`, the reaction type name (e.g., `"like"`, `"love"`) is placed in `key1`.

## Output Bucket

By default, data is stored in a bucket with the format `in.c-{component-id}-{configuration-id}`.

If the configuration contains a `bucket-id` parameter, data will be stored in the specified bucket instead. This is useful for:
- **Migration from legacy components**: Redirect data to the original bucket used by previous extractors
- **Custom bucket organization**: Create your own bucket structure

```json
{
  "parameters": {
    "bucket-id": "in.c-my-custom-facebook-bucket",
    "api-version": "v23.0",
    "accounts": {},
    "queries": []
  }
}
```

---

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository and run the component using the following commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-meta component-meta
cd component-meta
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Dependency Management

The project uses [uv](https://docs.astral.sh/uv/) for Python dependency management. Dependencies are defined in `pyproject.toml`.

### Project Structure

- `src/component.py` - Main component logic
- `src/client.py` - Facebook Graph API client
- `src/configuration.py` - Configuration model definitions
- `src/output_parser.py` - Data parsing and transformation
- `src/page_loader.py` - API page loading
