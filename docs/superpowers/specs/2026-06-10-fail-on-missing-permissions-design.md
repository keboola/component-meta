# Fail on missing permissions — Meta extractors (CFTL-489 / SUPPORT-15700)

## Problem

The Meta extractors (`keboola.ex-facebook-pages`, `keboola.ex-facebook-ads-v2`,
`keboola.ex-instagram-v2`, all built from this one codebase) can finish a job with
**exit 0 / Success** while extracting nothing beyond the `accounts` table.

The `/me/accounts` endpoint does not require `ads_read` / `ads_management`, so it always
succeeds. When the token has lost per-account permissions (or expired), every other query
fails per-account with a Facebook OAuth error (codes 190 / 200 / 10, or 100/subcode 33
"missing permissions"). Those errors are currently **swallowed** in two layers:

1. `page_loader.FacebookErrorHandler.is_recoverable_error()` treats the 100/33
   "missing permissions" case as recoverable and returns `{"data": []}`.
2. The per-account `except Exception: ... continue` loops in `client.py` log-and-skip
   non-recoverable errors (e.g. code 200).

Result: the job reports Success and the missing data goes unnoticed.

## Goal

Add an opt-in option so the job **fails** (exit 1, `UserException`) when the token is
missing permissions, instead of silently succeeding. Default OFF preserves today's
behavior (most existing configs rely on it).

## Decisions (confirmed with requester)

- **Detection:** per-account authorization errors raised during extraction. No upfront
  `debug_token()` pre-flight — the reported case is a valid token missing per-account
  grants, which a token-validity check would not catch.
- **Failure timing:** **collect all, fail at the end.** Authorization errors are accumulated
  per account during extraction; after all queries run, the job fails once with a
  `UserException` listing every affected account. Safe because a non-zero exit makes the
  platform discard all output — no partial data is committed regardless of when we raise — so
  this gives a complete error message at no data-integrity cost. (Initial design was fail-fast;
  switched to collect-all on review.)
- **UI scope:** show the checkbox for all three Meta extractors (shared codebase + shared
  `ex-facebook` UI module).
- **Config key:** `parameters.fail-on-missing-permissions` (boolean, default `false`).
- **UI label:** "Fail the job on authorization errors".

## Implementation

### Python (`component-meta`)

**`configuration.py`** — add
`fail_on_missing_permissions: bool = Field(alias="fail-on-missing-permissions", default=False)`.

**`page_loader.py`**
- `_FB_AUTHORIZATION_ERROR_CODES = frozenset({10, 190, 200})`.
- `class AuthorizationError(Exception)` — internal sentinel carrying `account_id`, `code`,
  `message`. Deliberately **not** a `UserException` so it doesn't abort on the first hit.
- `FacebookErrorHandler.is_authorization_error(http_error) -> bool` — True for the codes
  above, or the existing `OBJECT_NOT_FOUND_ERROR` (100/33 "missing permissions") match.
- `FacebookErrorHandler.authorization_error_details(http_error) -> (code, message)`.
- `PageLoader.__init__` gains `fail_on_missing_permissions: bool = False`.
- In each HTTP error boundary — `_load_regular_page`, `load_page_from_url`,
  `start_async_insights_job`, and the async final-results fetch in `poll_async_job` — when
  the flag is on and the error is an authorization error, raise `AuthorizationError`
  **before** the recoverable/return-empty logic.

**`client.py`**
- `FacebookClient.__init__` gains the flag and a `permission_errors: list[dict]` collector;
  passes the flag to all three `PageLoader(...)` constructions.
- `_record_permission_error(error, query_name)` appends a record; `raise_for_permission_errors()`
  raises one `UserException` summarizing every affected account (deduped by account id).
- Per-account loops (`_start_async_jobs_for_query`, `_poll_and_process_async_jobs`,
  `_process_single_sync_query` + its page-token/user-token fallback) catch `AuthorizationError`
  and record-and-continue; `except UserException: raise` is kept for genuine immediate failures.
- Batch path records when the error is an authorization error.

**`component.py`** — pass `self.config.fail_on_missing_permissions` into `FacebookClient(...)`,
and call `self.client.raise_for_permission_errors()` after `_process_queries` finishes the loop.

### UI (`ui/apps/kbc-ui/src/scripts/modules/ex-facebook`)

Mirror the existing config-level API-version pattern:
- `constants.ts` — help/tooltip text constant.
- `storeProvisioning.js` — read `parameters.get('fail-on-missing-permissions', false)`.
- `Index.jsx` — render the checkbox in the config-level settings area.
- `actionsProvisioning.js` — `saveFailOnMissingPermissions` writing
  `parameters.fail-on-missing-permissions`.

Shared module → the checkbox shows for facebook-pages, facebook-ads-v2, instagram-v2.

## Tests

- Unit tests for `is_authorization_error` / `authorization_error_details`
  (codes 10/190/200 + 100/33 → True; unrelated codes → False).
- PageLoader boundary: flag ON → code-200 / 100-33 raise `AuthorizationError`; flag OFF →
  100/33 returns empty and code-200 re-raises `HTTPError` (current behavior).
- Client: collects per-account `AuthorizationError`s without aborting mid-iteration, then
  `raise_for_permission_errors()` raises one `UserException` listing every account (deduped).
  Existing datadir/VCR tests unaffected (default off).

## Deliverables

- PR in `component-meta` (Python + tests).
- PR in `ui` (checkbox).
- Cross-link both PRs and attach to Linear CFTL-489.
