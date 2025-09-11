Facebook Pages, Facebook Ads and Instagram Extractors
============================

This extractor extracts data from [Facebook Graph API](https://developers.facebook.com/docs/graph-api/). Use Facebook Pages, Facebook Ads, and Instagram extractors to download data from Facebook Graph API.

**Table of Contents:**

- [Supported Endpoints](#supported-endpoints)
- [Configuration](#configuration)
  - [Basic Structure](#basic-structure)
  - [Nested Query](#nested-query)
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
- **insights** - page and post metrics
- **accounts** - account information
- **adaccounts** - advertising accounts
- **posts** - specific posts
- And others based on your configuration needs


Configuration
=============

Basic Structure
---------------

The configuration consists of three main parts:

### api-version
Facebook Graph API version (default: "v23.0")

### accounts
Dictionary of accounts from which data will be extracted. Each account contains:
- **id**: Unique account identifier
- **name**: Account name
- **account_id**: Advertising account ID (optional)
- **business_name**: Business name (optional)
- **currency**: Currency (optional)
- **category**: Page category (optional)
- **fb_page_id**: Facebook page ID (optional)

### queries
List of queries for data extraction. Each query contains:

#### Nested Query
```json
{
  "id": 1,
  "name": "query_name",
  "type": "nested-query",
  "disabled": false,
  "query": {
    "path": "feed",
    "fields": "message,created_time,likes,comments{message,from}",
    "ids": "page_id1,page_id2",
    "limit": "25",
    "since": "2024-01-01",
    "until": "2024-12-31"
  }
}
```

**Nested query parameters:**
- **path**: Endpoint URL (e.g., "feed", "posts")
- **fields**: Fields to extract - supports nested queries
- **ids**: Comma-separated list of page IDs (if empty, all from accounts will be used)
- **limit**: Page size (default 25, maximum 100)
- **since**: "From" date for filtering by created_time
- **until**: "To" date for filtering by created_time

#### Async Insights Query
```json
{
  "id": 2,
  "name": "ads_insights",
  "type": "async-insights-query",
  "disabled": false,
  "query": {
    "parameters": "fields=ad_id,impressions,clicks&level=ad&date_preset=last_month",
    "ids": "ad_account_id"
  }
}
```

**Async insights query parameters:**
- **parameters**: URL query string according to Facebook Marketing API specification
- **ids**: List of advertising account IDs

Output
======

The component generates the following tables:

### accounts.csv
Contains information about configured accounts with columns:
- id, name, account_id, business_name, currency, category

### Query-based tables
For each query, tables are created based on data structure:
- **{query_name}_{endpoint}** (e.g., "feed_posts", "feed_comments")
- **{query_name}_insights** for insights data

### Common columns across all tables
- **id**: Unique row identifier
- **parent_id**: Parent object ID
- **ex_account_id**: Account ID from which the data originates
- **fb_graph_node**: Describes position in hierarchy (e.g., "page_feed_comments")

Insights data is structured into columns:
- **key1**, **key2**: Dimensions
- **value**: Metric value
- **end_time**: Timestamp
- **metric name**, **title**, **description**: Metric metadata

## Output Bucket

By default, data is stored in a bucket with the format `in.c-{component-id}-{configuration-id}`. 

If the configuration contains a `bucket-id` parameter, data will be stored in the specified bucket instead. This allows for:
- **Migration from legacy components**: Redirect data to the original bucket used by previous extractors
- **Custom bucket organization**: Create your own bucket structure for better data management

Example configuration with custom bucket:
```json
{
  "parameters": {
    "bucket-id": "in.c-my-custom-facebook-bucket",
    "api-version": "v23.0",
    "accounts": {...},
    "queries": [...]
  }
}
```

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

Integration
===========

For details about deployment and integration with Keboola, refer to the [deployment section of the developer documentation](https://developers.keboola.com/extend/component/deployment/).
