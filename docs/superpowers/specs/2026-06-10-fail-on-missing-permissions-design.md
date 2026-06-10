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
- **Failure timing:** fail-fast on the first authorization error.
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
- `FacebookErrorHandler.is_authorization_error(http_error) -> bool` — True for the codes
  above, or the existing `OBJECT_NOT_FOUND_ERROR` (100/33 "missing permissions") match.
- `FacebookErrorHandler.raise_if_authorization_error(http_error, context="")` — raises a
  clear `UserException` (FB message + code + remediation) when it is an authorization error.
- `PageLoader.__init__` gains `fail_on_missing_permissions: bool = False`.
- In each HTTP error boundary — `_load_regular_page`, `load_page_from_url`,
  `start_async_insights_job`, and the async final-results fetch in `poll_async_job` — when
  the flag is on, call `raise_if_authorization_error(...)` **before** the
  recoverable/return-empty logic.

**`client.py`**
- `FacebookClient.__init__` gains the flag; passes it to all three `PageLoader(...)`
  constructions.
- Add `except UserException: raise` to the per-account loops in
  `_start_async_jobs_for_query`, `_poll_and_process_async_jobs`, and
  `_process_single_sync_query` (outer loop + inner user-token fallback) so the deliberate
  failure propagates instead of being logged-and-continued.
- Batch path (`_process_single_sync_query` `except HTTPError`) escalates when the flag is on.

**`component.py`** — pass `self.config.fail_on_missing_permissions` into `FacebookClient(...)`.

### UI (`ui/apps/kbc-ui/src/scripts/modules/ex-facebook`)

Mirror the existing config-level API-version pattern:
- `constants.ts` — help/tooltip text constant.
- `storeProvisioning.js` — read `parameters.get('fail-on-missing-permissions', false)`.
- `Index.jsx` — render the checkbox in the config-level settings area.
- `actionsProvisioning.js` — `saveFailOnMissingPermissions` writing
  `parameters.fail-on-missing-permissions`.

Shared module → the checkbox shows for facebook-pages, facebook-ads-v2, instagram-v2.

## Tests

- Unit tests for `is_authorization_error` / `raise_if_authorization_error`
  (codes 10/190/200 + 100/33 → True; unrelated codes → False).
- Flag ON → a per-account code-200 raises `UserException`; flag OFF → swallowed
  (current behavior). Existing datadir/VCR tests unaffected (default off).

## Deliverables

- PR in `component-meta` (Python + tests).
- PR in `ui` (checkbox).
- Cross-link both PRs and attach to Linear CFTL-489.
