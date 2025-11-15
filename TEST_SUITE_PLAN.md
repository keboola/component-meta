# Comprehensive Test Suite Plan for component-meta

## Overview

This document outlines a comprehensive test suite for the component-meta Facebook Graph API extractor, based on analysis of the legacy Clojure component's 33 test files.

## Test Organization

### Test Structure
```
tests/
├── __init__.py
├── test_component.py          # Basic component tests
├── test_sync_actions.py       # Sync action tests (NEW)
├── test_client.py             # Client and token management tests (NEW)
├── test_page_loader.py        # Page loading and pagination tests (NEW)
├── test_output_parser.py      # Output parsing and transformation tests (NEW)
├── test_integration.py        # End-to-end integration tests (NEW)
└── fixtures/                  # Test fixtures and mock data (NEW)
    ├── __init__.py
    ├── mock_responses.py
    └── sample_configs.py
```

## Test Categories

### 1. Sync Actions Tests (test_sync_actions.py)

#### Test: accounts sync action
- **Purpose**: Verify accounts endpoint returns correct data
- **Mock**: GET /v23.0/me/accounts
- **Assertions**:
  - Returns list of account objects
  - No access_token in response
  - Contains expected fields: id, name, category, business_name
  - Handles pagination correctly

#### Test: adaccounts sync action
- **Purpose**: Verify adaccounts endpoint returns correct data
- **Mock**: GET /v23.0/me/adaccounts
- **Assertions**:
  - Returns list of ad account objects
  - Contains fields: account_id, id, business_name, name, currency
  - Handles pagination correctly

#### Test: igaccounts sync action
- **Purpose**: Verify Instagram accounts endpoint returns correct data
- **Mock**: GET /v23.0/me/accounts with instagram_business_account
- **Assertions**:
  - Returns list of Instagram account objects
  - Contains instagram_business_account field
  - **GAP**: Should transform to {id: ig_account.id, fb_page_id: page.id, name, category}

#### Test: debugtoken sync action (MISSING)
- **Purpose**: Verify debug token endpoint
- **Status**: ❌ Not implemented - should be added
- **Expected**: Return token info without app_id

### 2. Client Tests (test_client.py)

#### Test: Page token retrieval
- **Purpose**: Verify page token fetching from me/accounts
- **Mock**: GET /v23.0/me/accounts with access_token field
- **Assertions**:
  - Caches page tokens correctly
  - Maps account.id to page token
  - Falls back to user token if page token not found
  - Uses fb_page_id when available

#### Test: Page token fallback to user token
- **Purpose**: Verify fallback when page token fails
- **Mock**: me/accounts returns error or no token
- **Assertions**:
  - Uses user token as fallback
  - Logs warning message
  - Continues execution

#### Test: Token masking in logs
- **Purpose**: Verify access tokens are masked in logs
- **Assertions**:
  - access_token=XXX replaced with access_token=---ACCESS-TOKEN---
  - Applies to all log levels
  - Applies to exception messages

#### Test: Batch request execution
- **Purpose**: Verify batch ID fetching
- **Mock**: GET /v23.0/?ids=id1,id2,id3&fields=...
- **Assertions**:
  - Batches multiple IDs in single request
  - Handles individual errors in batch response
  - Falls back to individual requests on "Page Access Token" error
  - **GAP**: Should work for nested-query with empty path

#### Test: Request requires page token logic
- **Purpose**: Verify _request_require_page_token logic
- **Test Cases**:
  - path="insights" → requires page token
  - path="feed" → requires page token
  - path="posts" → requires page token
  - path="ratings" → requires page token
  - path="likes" → requires page token
  - path="stories" → requires page token
  - fields contains "insights" → requires page token
  - fields contains "likes" → requires page token
  - fields contains "from" → requires page token
  - fields contains "username" → requires page token
  - type="async-insights-query" → does NOT require page token

### 3. Page Loader Tests (test_page_loader.py)

#### Test: Build params for regular query
- **Purpose**: Verify parameter construction
- **Assertions**:
  - limit parameter set correctly
  - since/until converted to YYYY-MM-DD format
  - Relative dates parsed (e.g., "90 days ago")
  - fields parameter passed through

#### Test: Build params for insights DSL
- **Purpose**: Verify insights DSL parsing
- **Input**: `fields="insights.metric(page_fans,page_views).period(day).since(90 days ago)"`
- **Assertions**:
  - Extracts metric parameter
  - Extracts period parameter
  - Extracts and converts since parameter
  - Extracts and converts until parameter

#### Test: Build endpoint path
- **Purpose**: Verify endpoint path construction
- **Test Cases**:
  - path="feed", page_id="123" → /v23.0/123/feed
  - path="", fields="insights..." → /v23.0/123/insights
  - path="posts", page_id="456" → /v23.0/456/posts

#### Test: Async insights job start
- **Purpose**: Verify async job initiation
- **Mock**: POST /v23.0/act_123/insights
- **Assertions**:
  - Adds "act_" prefix if missing
  - Parses parameters string correctly
  - Returns report_run_id
  - Handles errors gracefully

#### Test: Async insights job polling
- **Purpose**: Verify polling logic
- **Mock**: GET /v23.0/{report_id} with progressive completion
- **Assertions**:
  - Polls until async_percent_completion == 100
  - Checks async_status == "Job Completed"
  - Sleeps 5 seconds between attempts
  - Max 60 attempts (5 minutes)
  - Raises exception on "Job Failed" or "Job Skipped"
  - Fetches results from /{report_id}/insights

#### Test: Pagination with load_page_from_url
- **Purpose**: Verify pagination URL parsing
- **Mock**: Full Facebook URL with query params
- **Assertions**:
  - Parses URL correctly
  - Extracts path and params
  - Handles version prefix in path
  - Follows paging.next links

### 4. Output Parser Tests (test_output_parser.py)

#### Test: Parse simple data
- **Purpose**: Verify basic row parsing
- **Input**: `{"id": "123", "name": "Test", "value": 100}`
- **Assertions**:
  - Creates row with ex_account_id, fb_graph_node, parent_id
  - Includes all scalar fields
  - Returns correct table name

#### Test: Parse nested data
- **Purpose**: Verify nested object handling
- **Input**: `{"id": "123", "comments": {"data": [{"id": "c1", "message": "Hi"}]}}`
- **Assertions**:
  - Creates separate table for comments
  - Sets correct fb_graph_node (page_feed_comments)
  - Sets parent_id to post id
  - Processes recursively

#### Test: Parse insights values array
- **Purpose**: Verify insights value extraction
- **Input**: `{"values": [{"value": 100, "end_time": "2024-01-01"}, {"value": 200, "end_time": "2024-01-02"}]}`
- **Assertions**:
  - Creates row for each value
  - Includes key1, key2, value columns
  - Includes end_time
  - Skips null/empty values

#### Test: Parse action stats
- **Purpose**: Verify Facebook Ads action stats handling
- **Input**: `{"actions": [{"action_type": "like", "value": "10"}, {"action_type": "comment", "value": "5"}]}`
- **Assertions**:
  - Creates rows with ads_action_name, action_type, value
  - Handles action_breakdowns=action_reaction
  - Handles action_breakdowns=action_type
  - Creates _insights suffix tables when appropriate
  - Transforms "post_save" to "post_reaction"

#### Test: Parse summary data
- **Purpose**: Verify summary field handling
- **Input**: `{"likes": {"data": [], "summary": {"total_count": 42}}}`
- **Assertions**:
  - Creates fake nested structure for summary
  - Processes summary as separate table

#### Test: Flatten arrays
- **Purpose**: Verify array flattening
- **Input**: `{"targeting": [{"age_min": 18, "age_max": 65}]}`
- **Assertions**:
  - Flattens to targeting_0_age_min, targeting_0_age_max
  - Handles nested objects in arrays
  - Handles multiple array items

#### Test: Table naming
- **Purpose**: Verify table name generation
- **Test Cases**:
  - query.name="feed", path="feed" → "feed"
  - query.name="page", path="", fields="insights..." → "page_insights"
  - query.name="ads", type="async-insights-query" → "ads_insights"
  - query.name="feed", nested="comments" → "feed_comments"

#### Test: Serialized lists
- **Purpose**: Verify special list serialization
- **Input**: `{"issues_info": [{"error": "test"}], "frequency_control_specs": [{"event": "impression"}]}`
- **Assertions**:
  - JSON encodes issues_info
  - JSON encodes frequency_control_specs
  - Does not flatten these fields

### 5. Integration Tests (test_integration.py)

These tests mock the full Facebook API and verify end-to-end behavior matching the old component's snapshot tests.

#### Test: Feed with nested comments and likes
- **Based on**: test/keboola/snapshots/feed
- **Config**: Feed query with nested comments{likes}, comments{comments{likes}}
- **Assertions**:
  - Creates feed table
  - Creates feed_comments table
  - Creates feed_comments_comments table (subcomments)
  - Creates feed_likes table
  - Creates feed_comments_likes table
  - Correct fb_graph_node paths
  - Correct parent_id relationships

#### Test: Ads queries (ads, campaigns, adsets)
- **Based on**: test/keboola/snapshots/ads
- **Config**: Multiple nested queries for ads, campaigns, adsets
- **Assertions**:
  - Creates ads_ads table
  - Creates ads_campaigns table
  - Creates ads_adsets table
  - Correct primary keys
  - Correct incremental flags

#### Test: Run by ID
- **Based on**: test/keboola/snapshots/runbyid
- **Config**: run-by-id=true with insights query
- **Assertions**:
  - **GAP**: Should process each ID separately
  - Should use appropriate token for each ID
  - Should merge results correctly

#### Test: Async insights with action breakdowns
- **Based on**: test/keboola/snapshots/asyncinisghtscampaigns
- **Config**: async-insights-query with action_breakdowns=action_reaction
- **Assertions**:
  - Starts async job
  - Polls for completion
  - Creates main insights table
  - Handles action_reaction breakdown correctly

#### Test: Page insights
- **Based on**: test/keboola/snapshots/pageinsights
- **Config**: Empty path with insights.metric(...).period(...)
- **Assertions**:
  - Parses insights DSL correctly
  - Creates page_insights table
  - Includes metric columns
  - Handles values array

#### Test: Posts insights
- **Based on**: test/keboola/snapshots/postsinsights
- **Config**: path="feed" with insights.metric(...)
- **Assertions**:
  - Creates posts_insights table
  - Processes each post's insights
  - Handles since/until parameters

#### Test: Feed with summary
- **Based on**: test/keboola/snapshots/feedsummary
- **Config**: posts{likes.summary(true), reactions.summary(total_count)}
- **Assertions**:
  - Creates summary table
  - Extracts total_count from summary

#### Test: Serialize lists
- **Based on**: test/keboola/snapshots/serializelists
- **Config**: Query with issues_info or frequency_control_specs
- **Assertions**:
  - JSON encodes special list fields
  - Does not flatten them

### 6. Component Tests (test_component.py - expand existing)

#### Test: Write accounts from config
- **Purpose**: Verify accounts.csv generation
- **Assertions**:
  - Creates accounts table
  - Includes all configured account fields
  - Filters None values
  - No access_token in output
  - Correct primary key: ["id"]

#### Test: Primary key selection
- **Purpose**: Verify _get_primary_key logic
- **Test Cases**:
  - Row with id → ["id"]
  - Row with id, parent_id → ["id", "parent_id"]
  - Row with date_start, date_stop → ["id", "parent_id", "date_start", "date_stop"]
  - Insights row with key1, key2, end_time → ["id", "parent_id", "key1", "key2", "end_time"]
  - **GAP**: Verify parent_id always included when present

#### Test: Bucket ID resolution
- **Purpose**: Verify _retrieve_bucket_id logic
- **Test Cases**:
  - Custom bucket-id in config → uses custom
  - No bucket-id → uses default format
  - Default format: in.c-{component-id}-{config-id}

#### Test: Column ordering
- **Purpose**: Verify PREFERRED_COLUMNS_ORDER applied
- **Assertions**:
  - Preferred columns appear first
  - Remaining columns sorted alphabetically

#### Test: Incremental flag handling
- **Purpose**: Verify incremental writes
- **Assertions**:
  - Manifest includes incremental: true
  - Manifest includes primary_key
  - Accounts table is not incremental

## Mock Data Requirements

### fixtures/mock_responses.py

```python
# Sample Facebook API responses for mocking

MOCK_ACCOUNTS_RESPONSE = {
    "data": [
        {"id": "123", "name": "Page 1", "category": "Software", "access_token": "page_token_123"},
        {"id": "456", "name": "Page 2", "category": "Entertainment", "access_token": "page_token_456"}
    ],
    "paging": {"next": "https://graph.facebook.com/v23.0/me/accounts?after=cursor"}
}

MOCK_ADACCOUNTS_RESPONSE = {
    "data": [
        {"account_id": "111", "id": "act_111", "name": "Ad Account 1", "currency": "USD", "business_name": "Business 1"}
    ]
}

MOCK_IGACCOUNTS_RESPONSE = {
    "data": [
        {
            "id": "123",
            "name": "Page 1",
            "category": "Software",
            "instagram_business_account": {"id": "ig_789"}
        }
    ]
}

MOCK_FEED_RESPONSE = {
    "data": [
        {
            "id": "post_1",
            "message": "Test post",
            "created_time": "2024-01-01T00:00:00+0000",
            "comments": {
                "data": [
                    {"id": "comment_1", "message": "Nice!", "from": {"id": "user_1", "name": "User 1"}}
                ]
            },
            "likes": {
                "data": [
                    {"id": "user_2", "name": "User 2"}
                ]
            }
        }
    ],
    "paging": {"next": None}
}

MOCK_INSIGHTS_RESPONSE = {
    "data": [
        {
            "id": "page_123/insights/page_fans/lifetime",
            "name": "page_fans",
            "period": "lifetime",
            "values": [
                {"value": 1000, "end_time": "2024-01-01T08:00:00+0000"}
            ]
        }
    ]
}

MOCK_ASYNC_JOB_START = {
    "report_run_id": "12345",
    "async_status": "Job Running",
    "async_percent_completion": 0
}

MOCK_ASYNC_JOB_COMPLETE = {
    "id": "12345",
    "async_status": "Job Completed",
    "async_percent_completion": 100
}

MOCK_ASYNC_INSIGHTS_RESULT = {
    "data": [
        {
            "account_id": "111",
            "campaign_id": "222",
            "ad_id": "333",
            "impressions": "1000",
            "clicks": "50",
            "date_start": "2024-01-01",
            "date_stop": "2024-01-01",
            "actions": [
                {"action_type": "like", "value": "10"},
                {"action_type": "comment", "value": "5"}
            ]
        }
    ]
}
```

### fixtures/sample_configs.py

```python
# Sample configurations matching old component test configs

FEED_CONFIG = {
    "parameters": {
        "accounts": {
            "177057932317550": {
                "id": "177057932317550",
                "name": "keboola",
                "category": "software"
            }
        },
        "api-version": "v23.0",
        "queries": [
            {
                "id": 1,
                "name": "feed",
                "type": "nested-query",
                "disabled": False,
                "query": {
                    "path": "feed",
                    "fields": "caption,message,created_time,type,description,likes{name,username},comments{message,created_time,from,likes{name,username}}",
                    "ids": "177057932317550",
                    "since": "3 years ago",
                    "until": "now"
                }
            }
        ]
    }
}

# ... more sample configs
```

## Test Execution Plan

### Phase 1: Unit Tests (Priority 1)
1. test_sync_actions.py - Sync action tests
2. test_client.py - Client and token management
3. test_page_loader.py - Page loading and pagination
4. test_output_parser.py - Output parsing

### Phase 2: Integration Tests (Priority 2)
5. test_integration.py - End-to-end scenarios

### Phase 3: Component Tests (Priority 3)
6. Expand test_component.py - Component-level tests

## Success Criteria

- All tests pass with `PYTHONPATH=src uv run python -m unittest discover tests/`
- Test coverage > 80% for src/ files
- All critical gaps documented with test cases
- Mock data matches Facebook API response format
- Tests are maintainable and well-documented

## Known Gaps to Document in Tests

1. **run-by-id**: Config exists but not used - test should document expected behavior
2. **igaccounts transformation**: Test should show gap vs old component
3. **debugtoken**: Test should be skipped with note about missing feature
4. **split-query-time-range-by-day**: Test should document missing feature
5. **time-based-pagination**: Test should document missing feature
6. **stop-on-empty-response**: Test should document missing feature
7. **Batch for nested-query**: Test should show current limitation
8. **parent_id in PK**: Test should verify current behavior

## Next Steps

1. Create fixtures/mock_responses.py
2. Create fixtures/sample_configs.py
3. Implement test_sync_actions.py
4. Implement test_client.py
5. Implement test_page_loader.py
6. Implement test_output_parser.py
7. Implement test_integration.py
8. Expand test_component.py
9. Run full test suite
10. Document results and gaps
