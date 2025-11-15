# Component Parity Analysis: ex-facebook-graph-api (Clojure) vs component-meta (Python)

## Executive Summary

This document provides a comprehensive analysis comparing the legacy Clojure Facebook Graph API extractor (`keboola/ex-facebook-graph-api`) with the new Python replacement (`keboola/component-meta`). The analysis identifies feature gaps, behavioral differences, and provides recommendations for achieving full parity.

## Analysis Date
November 15, 2025

## Component Overview

### Old Component (ex-facebook-graph-api)
- **Language**: Clojure
- **Default API Version**: v20.0
- **Test Coverage**: 33 test files with snapshot-based testing
- **Key Features**: nested-query, async-insights-query, sync actions, page token management, incremental loading

### New Component (component-meta)
- **Language**: Python 3.13
- **Default API Version**: v23.0
- **Test Coverage**: 1 basic test file
- **Key Features**: nested-query, async-insights-query, sync actions, page token management, incremental loading

---

## Feature Parity Matrix

### ✅ Fully Implemented Features

| Feature | Old Component | New Component | Status |
|---------|---------------|---------------|--------|
| nested-query type | ✅ | ✅ | ✅ COMPLETE |
| async-insights-query type | ✅ | ✅ | ✅ COMPLETE |
| Page token retrieval | ✅ | ✅ | ✅ COMPLETE |
| User token fallback | ✅ | ✅ | ✅ COMPLETE |
| Incremental loading | ✅ | ✅ | ✅ COMPLETE |
| Pagination support | ✅ | ✅ | ✅ COMPLETE |
| Nested field extraction | ✅ | ✅ | ✅ COMPLETE |
| Action stats handling | ✅ | ✅ | ✅ COMPLETE |
| Insights DSL parsing | ✅ | ✅ | ✅ COMPLETE |
| Batch ID requests | ✅ | ✅ | ✅ COMPLETE |
| Custom bucket-id | ✅ | ✅ | ✅ COMPLETE |
| Async job polling | ✅ | ✅ | ✅ COMPLETE |
| Access token masking | ✅ | ✅ | ✅ COMPLETE |

### ⚠️ Partially Implemented or Different Behavior

| Feature | Old Component | New Component | Gap Description |
|---------|---------------|---------------|-----------------|
| **run-by-id** | ✅ Implemented (query.clj:42-59) | ⚠️ Config only | Configuration field exists but not used in execution logic (client.py) |
| **Batch requests for nested-query** | ✅ All empty path queries | ⚠️ Limited | Only batches when `type != "nested-query"` (client.py:188) |
| **igaccounts sync action** | ✅ Transforms response | ⚠️ Raw response | Old returns `{id: ig_account.id, fb_page_id: page.id}`, new returns raw me/accounts (component.py:192-194) |
| **accounts sync action fields** | ✅ All fields | ⚠️ Filtered | Old returns all fields, new filters to specific fields (component.py:186) |
| **Primary key selection** | ✅ Always includes parent_id | ⚠️ Conditional | Old always includes parent_id in PK (output.clj:45-58), new uses candidates list (component.py:159-167) |

### ❌ Missing Features

| Feature | Old Component Reference | New Component | Impact |
|---------|------------------------|---------------|--------|
| **debugtoken sync action** | ✅ core.clj:76, sync_actions.clj:40-48 | ❌ Missing | No sync action exposed, though client.debug_token exists |
| **split-query-time-range-by-day** | ✅ query.clj:106-159 | ❌ Missing | Cannot split large insights queries into daily chunks |
| **time-based-pagination** | ✅ request.clj:120-132 | ❌ Missing | Cannot stop pagination when since/until params appear in next URL |
| **stop-on-empty-response** | ✅ request.clj:120-132 | ❌ Missing | Cannot stop pagination early on empty data response |
| **Limit backoff on errors** | ✅ request.clj:74-91 | ❌ Missing | Cannot reduce limit on "Please reduce the amount of data" errors |
| **Page token via page details** | ✅ request.clj:296-301 | ❌ Missing | Only tries me/accounts, not GET /{page-id}?fields=access_token |

---

## Detailed Comparison by Component

### 1. Sync Actions

#### accounts
**Old Behavior** (sync_actions.clj:11-18):
```clojure
(request/get-accounts token :version version)
;; Returns all fields from Facebook API response
```

**New Behavior** (component.py:185-186):
```python
return self.client.get_accounts("me/accounts", "id,business_name,name,category")
```

**Gap**: New component filters fields explicitly. Should return all fields like old component for backward compatibility.

#### adaccounts
**Status**: ✅ PARITY ACHIEVED
Both components request same fields: `account_id,id,business_name,name,currency`

#### igaccounts
**Old Behavior** (sync_actions.clj:29-36):
```clojure
(let [ig-accounts (filter #(contains? % :instagram_business_account) accounts)
      result (map #(assoc (select-keys % [:name :category]) 
                          :id (-> % :instagram_business_account :id) 
                          :fb_page_id (:id %)) 
                  ig-accounts)]
  (log (generate-string result)))
```

**New Behavior** (component.py:192-194):
```python
return self.client.get_accounts("me/accounts", "instagram_business_account,name,category")
```

**Gap**: New component returns raw response. Should transform to match old format:
- Extract `instagram_business_account.id` as `id`
- Keep original page `id` as `fb_page_id`
- Include `name` and `category`

#### debugtoken
**Old Behavior** (sync_actions.clj:40-48):
```clojure
(defn log-debug-token [app-token credentials prepend-message]
  (let [input-token (docker-config/get-fb-token credentials)
        response-data (:data (request/debug-token app-token input-token))
        result (dissoc response-data :app_id)]
    (log (str prepend-message (generate-string result)))))
```

**New Behavior**: ❌ No sync action exposed

**Gap**: client.debug_token method exists (client.py:341-350) but no @sync_action decorator. Should add sync action.

### 2. Query Execution

#### run-by-id
**Old Behavior** (query.clj:42-59):
```clojure
(defn- run-by-id-merge-and-write [token out-dir prefix query version choose-token-fn]
  (let [ids-str (:ids query)
        ids-seq (s/split ids-str #",")
        run-query (fn [id] (request/nested-request (choose-token-fn id token version) 
                                                    (prepare-query id) :version version))
        all-merged-queries-rows (mapcat #(run-query %) ids-seq)]
    ;; Process and write merged results
    ))
```

**New Behavior**: Configuration field exists (configuration.py:20) but not used in client._process_single_sync_query

**Gap**: Implement run-by-id logic to process each ID separately with appropriate token selection.

#### Batch Requests
**Old Behavior** (request.clj:179-215):
- Batch requests used for all queries with multiple IDs when path is specified
- GET `/?ids=id1,id2,id3&fields=...`

**New Behavior** (client.py:187-233):
```python
is_batchable_query = not row_config.query.path and getattr(row_config, "type", "") != "nested-query"
```

**Gap**: Nested queries with empty path are not batched. Should enable batching for nested-query type when path is empty.

#### Async Insights Splitting
**Old Behavior** (query.clj:106-159):
- Supports `split-query-time-range-by-day` flag
- Automatically splits `last_3d`, `last_7d`, `last_30d` date presets into daily queries
- Parses `time_ranges` parameter and expands into daily ranges

**New Behavior**: ❌ Not implemented

**Gap**: Large insights queries cannot be split into smaller time ranges to avoid "Please reduce the amount of data" errors.

### 3. Pagination Behavior

#### time-based-pagination
**Old Behavior** (request.clj:120-132):
```clojure
(defn get-next-page-url [response time-base-pagination? stop-on-empty-response?]
  (let [next-url (get-in response [:paging :next])
        time-base-pagination-valid (or (not time-base-pagination?)
                                       (and next-url
                                            (not (clojure.string/includes? next-url "since="))
                                            (not (clojure.string/includes? next-url "until="))))]
    ;; Only follow next if time-based pagination is valid
    ))
```

**New Behavior**: Always follows paging.next (output_parser.py:380-399)

**Gap**: Cannot prevent following pagination links that change time ranges.

#### stop-on-empty-response
**Old Behavior**: Stops pagination when data array is empty

**New Behavior**: Always continues pagination until no next link

**Gap**: Cannot optimize by stopping early on empty responses.

### 4. Error Handling

#### Limit Reduction on Errors
**Old Behavior** (request.clj:74-91):
```clojure
(defn call-and-adapt [api-fn url min-limit-count]
  (try+
   (api-fn url)
   (catch retry-exception? e
     (Thread/sleep 60000)
     (let [current-limit (or (parse-limit-from-url url) DEFAULT_LIMIT)
           new-limit (get-next-limit current-limit)  ;; Halves the limit
           new-url (update-url-with-limit url new-limit)]
       (call-and-adapt api-fn new-url new-min-limit-count)))))
```

**New Behavior**: No automatic limit reduction

**Gap**: May fail on large requests instead of adapting limit size.

#### Recoverable Errors
**Old Behavior** (request.clj:49-68):
- Skips "media posted before business account conversion" errors
- Retries on various server errors
- Special handling for "User request limit reached"

**New Behavior**: Standard HTTP error handling via keboola-http-client

**Gap**: May not handle all edge cases gracefully.

### 5. Output Schema

#### Table Naming
**Status**: ✅ Generally compatible
Both components use similar logic for table naming with fb_graph_node paths.

**Test Needed**: Verify naming for:
- Nested comments/subcomments
- Insights tables
- Action stats tables with _insights suffix
- Summary tables

#### Primary Keys
**Old Behavior** (output.clj:45-58):
```clojure
(defn get-primary-key [table-columns table-name context async-insights?]
  (let [basic-pk ["parent_id"]  ;; Always includes parent_id
        all-tables-pk ["id" "key1" "key2" "end_time" ...]
        extended-pk (concat all-tables-pk table-only-pk endpoint-only-pk)]
    (dedupe (concat basic-pk (filter #(table-has-column? table-columns %) extended-pk)))))
```

**New Behavior** (component.py:159-167):
```python
PRIMARY_KEY_CANDIDATES = ["id", "parent_id", "key1", "key2", ...]
primary_key = [col for col in PRIMARY_KEY_CANDIDATES if col in available_columns]
```

**Gap**: Old always includes parent_id first, new treats it as a candidate. This may affect incremental deduplication.

#### Column Ordering
**Status**: ✅ Compatible
Both use preferred column ordering with similar lists.

---

## Test Coverage Analysis

### Old Component Test Scenarios (33 tests)

1. **feed** - Basic feed extraction with nested comments and likes
2. **ads** - Ad account queries (ads, campaigns, adsets)
3. **runbyid** - Run-by-id with insights queries
4. **asyncinisghtscampaigns** - Async insights with action_breakdowns
5. **pageinsights** - Page-level insights with metrics
6. **postsinsights** - Post-level insights
7. **adsinsights** - Ad-level insights
8. **campaignsinsights** - Campaign-level insights
9. **feedsummary** - Feed with summary data
10. **serializelists** - Special list serialization

### New Component Test Coverage

**Current**: 1 basic test (test_run_no_cfg_fails)

**Needed**: Comprehensive test suite covering all scenarios above

---

## Recommendations

### Priority 1: Critical Gaps (Breaking Changes)

1. **Fix igaccounts sync action** to match old transformation
   - Extract instagram_business_account.id as id
   - Include fb_page_id from original page id

2. **Add debugtoken sync action** for UI compatibility

3. **Implement run-by-id execution logic** or remove from configuration schema

### Priority 2: Important Features

4. **Enable batch requests for nested-query with empty path**
   - Reduces API call count significantly

5. **Add split-query-time-range-by-day support**
   - Critical for large insights queries

6. **Implement time-based-pagination and stop-on-empty-response**
   - Optimizes pagination behavior

### Priority 3: Nice to Have

7. **Add limit backoff on errors**
   - Improves resilience for large queries

8. **Add page token via page details fallback**
   - Improves token retrieval reliability

9. **Ensure parent_id always in primary keys**
   - Maintains incremental deduplication behavior

### Priority 4: Testing

10. **Create comprehensive test suite** covering:
    - All sync actions
    - Nested queries (feed, comments, likes)
    - Insights queries (page, posts, ads)
    - Async insights queries
    - Action breakdowns
    - Batch vs per-ID execution
    - Token selection logic
    - Output schema validation
    - Primary key selection
    - Table naming

---

## Configuration Compatibility

### Supported in Both
- `api-version`
- `accounts` (with all fields)
- `queries` array
- `query.path`
- `query.fields`
- `query.ids`
- `query.limit`
- `query.since`
- `query.until`
- `query.parameters` (for async insights)
- `query.disabled`
- `bucket-id` (custom bucket)

### Only in Old Component
- `query.run-by-id` (exists in new config but not used)
- `query.split-query-time-range-by-day`
- `query.time-based-pagination`
- `query.stop-on-empty-response`
- `query.incremental` (explicit flag)

### Only in New Component
- None (new component is subset of old)

---

## Migration Path

For users migrating from old to new component:

1. **No changes needed** for basic nested queries and async insights queries
2. **Review igaccounts usage** - output format changes
3. **Remove debugtoken action calls** - not available in new component
4. **Remove advanced pagination flags** - not supported
5. **Test run-by-id queries** - behavior may differ
6. **Verify primary keys** - may differ for some tables

---

## Conclusion

The new Python component (`component-meta`) implements the core functionality of the old Clojure component with good fidelity. However, several features are missing or behave differently:

**Critical Issues** (must fix):
- igaccounts sync action transformation
- debugtoken sync action missing
- run-by-id not implemented

**Important Missing Features**:
- split-query-time-range-by-day
- time-based-pagination
- stop-on-empty-response
- Batch requests for nested-query

**Recommendation**: Address critical issues before considering this a full replacement. Document missing features clearly for users. Implement comprehensive test suite to prevent regressions.
