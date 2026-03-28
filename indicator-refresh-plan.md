# Indicator Refresh Optimization Plan

## Goal

Reduce unnecessary indicator work on populated-DB restarts by refreshing indicators only for symbols whose stored OHLCV data actually changed, while preserving correctness for:

- initial full loads
- normal scheduled incremental syncs
- startup incremental syncs
- stock split handling
- optional backfills

## Current Behavior

- On a populated-DB startup, the fetcher always runs an incremental OHLCV sync.
- After that sync, it schedules or runs post-sync tasks using `startup_result.successful_symbols`.
- `successful_symbols` currently means symbols that returned rows during fetch, not symbols whose persisted OHLCV state actually changed.
- As a result, indicator refresh runs for many symbols even when no new closing values were added and no stored rows materially changed.

## Desired Behavior

- Only refresh indicators for symbols whose upsert introduced or changed relevant OHLCV rows.
- Skip indicator refresh entirely when the upsert produced no effective data changes.
- Keep full rebuild behavior for:
  - initial historical load
  - explicit rebuild paths
  - split-affected symbols
  - backfill paths where older data was added

## Proposed Design

### 1. Track changed symbols during price upsert

Update the price persistence path so the sync result can distinguish between:

- symbols fetched successfully
- symbols actually inserted or updated in `stock_prices`

Implementation direction:

- Extend `upsert_stock_prices(...)` to return both:
  - total affected row count
  - set of changed symbols
- Or add a new helper dedicated to upserting prices and returning changed-symbol metadata.

Preferred behavior:

- Detect changes at DB-write time, not by re-querying the entire universe afterward.
- Treat both inserted rows and materially updated existing rows as changed.

### 2. Return changed symbols from sync

Extend the sync result so post-sync work is keyed off changed symbols instead of fetched symbols.

Implementation direction:

- Extend `FetchResult` or introduce a new sync-result structure with:
  - `successful_symbols`
  - `changed_symbols`
  - `upserted_row_count`

Then:

- use `changed_symbols` for indicator refresh
- keep `successful_symbols` for fetch logging and observability

### 3. Skip post-sync indicator refresh when nothing changed

Update startup and scheduled flows so:

- if `changed_symbols` is empty:
  - skip `refresh_indicators(...)`
  - optionally skip analytics/report refresh too, unless there is another explicit reason to run them

This should apply to:

- `run_scheduled_sync()`
- populated-DB startup path

### 4. Preserve correctness for stock splits

The current indicator path already forces a full rebuild when a split is detected inside the synced window.

Keep that behavior, but only for symbols in the changed set.

Rule:

- if a symbol had no effective price change, it should not enter the indicator path
- if a symbol changed and the synced window contains a split, force full rebuild for that symbol

### 5. Handle backfills explicitly

Backfills should continue to force indicator rebuild behavior for affected symbols because older history can change rolling calculations.

Rule:

- startup backfill path remains force-rebuild for the symbols touched by the backfill result

## Suggested Implementation Steps

1. Add changed-symbol tracking to the stock price upsert layer.
2. Extend the sync result model to carry changed-symbol metadata.
3. Update `run_sync(...)` to populate and return that metadata.
4. Change startup and scheduled post-sync callers to use `changed_symbols`.
5. Add early log messages for:
   - fetched symbols
   - changed symbols
   - skipped indicator refresh due to no data changes
6. Verify backfill and split-triggered rebuild behavior still works.

## Logging Expectations

Add explicit logs such as:

- `Fetched 503 symbols; 12 symbols had persisted OHLCV changes`
- `Skipping indicator refresh because no persisted OHLCV changes were detected`
- `Refreshing indicators for 12 changed symbols`

These logs will make restart behavior understandable without reading code.

## Validation Plan

### Scenario 1: No-op restart

- Start against a populated DB where no new market data is available.
- Expected:
  - incremental fetch may still return recent rows
  - changed-symbol count is zero or near zero
  - indicator refresh is skipped

### Scenario 2: Normal market-day incremental update

- Run after new daily bars are available.
- Expected:
  - changed-symbol set contains symbols with new closes
  - indicator refresh runs only for that touched set

### Scenario 3: Split-affected symbol

- Simulate or identify a symbol with a split in the synced window.
- Expected:
  - changed symbol enters indicator path
  - full rebuild is forced for that symbol only

### Scenario 4: Startup backfill

- Run with `BACKFILL_START` against a populated DB.
- Expected:
  - backfill result produces changed symbols
  - force rebuild still happens for those symbols

## Risks

- If change detection is too narrow, indicators could become stale.
- If change detection treats every upsert as a change, the optimization provides no benefit.
- If analytics/report generation also depends on unchanged symbols, skipping too aggressively could hide needed downstream refreshes.

## Recommended Scope

Implement this as a focused backend change only:

- `fetcher/src/database.py`
- `fetcher/src/fetcher.py`
- `fetcher/src/main.py`

Do not change env vars or Docker behavior unless measurement later shows a separate need.
