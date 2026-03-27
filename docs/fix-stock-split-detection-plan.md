# Fix False Stock Split Detection From `NaN` Values

## Summary
Change split handling so only real split events trigger a full indicator rebuild. Normalize `NaN` split values to `0` at ingest time, harden database-side split detection to ignore non-finite values, and optionally clean existing bad `stock_splits` rows already stored in `stock_prices`.

## Key Changes
- Fix split normalization in the fetch pipeline:
  - in `fetcher/src/fetcher.py`, treat missing or `NaN` `Stock Splits` values as `0` before creating records
  - ensure this uses an explicit finite-value check instead of `or 0`, because `NaN` is truthy enough to survive today
- Harden price-row construction before DB upsert:
  - in `fetcher/src/main.py`, normalize `Stock Splits` again when building `rows` for `upsert_stock_prices`
  - keep this as a second safety layer so future fetch-shape changes do not reintroduce `NaN`
- Fix split detection query:
  - in `fetcher/src/database.py`, update `has_stock_split_in_window()` so it only returns true for real non-zero numeric split values
  - do not rely on `COALESCE(stock_splits, 0) <> 0` alone, because stored `NaN` values currently satisfy the effective predicate
  - prefer a predicate that excludes `NULL` and `NaN` explicitly, then checks `<> 0`
- Clean existing bad data:
  - add a small DB cleanup helper or one-time startup-safe SQL path to rewrite existing `stock_prices.stock_splits` `NaN` values to `0`
  - run this before incremental split detection is relied on, otherwise old bad rows keep forcing false rebuilds
- Keep incremental indicator refresh behavior unchanged otherwise:
  - full rebuild still happens for initial load, startup backfill, `INDICATOR_REBUILD_ON_STARTUP=true`, and real split events
  - normal daily refresh stays incremental per touched ticker

## Implementation Details
- `fetcher/src/fetcher.py`
  - introduce a helper for numeric corporate-action fields such as dividends and stock splits
  - for splits: if missing, non-numeric, or `NaN`, persist `0.0`
- `fetcher/src/main.py`
  - when converting fetched rows for DB upsert, sanitize `Stock Splits` with the same finite-value logic
- `fetcher/src/database.py`
  - update `has_stock_split_in_window()` to ignore `NaN`
  - add a cleanup helper like `normalize_invalid_stock_splits()` that updates existing `NaN` rows to `0`
- `fetcher/src/main.py` startup flow
  - call the cleanup helper after schema init and before incremental sync / indicator refresh logic
  - log how many rows were normalized if any
- `README.md`
  - briefly note that split-triggered rebuilds rely on real non-zero split values and that invalid split values are normalized to zero

## Test Plan
- Static validation
  - `python -m compileall fetcher/src scripts`
- Data-shape checks
  - verify fetch normalization turns `NaN` `Stock Splits` into `0`
  - verify DB upsert path does not persist `NaN` split values
- Query behavior
  - confirm `has_stock_split_in_window()` returns:
    - false for `0`
    - false for `NULL`
    - false for `NaN`
    - true for real split ratios such as `2`, `3`, `1.5`
- Existing-data remediation
  - run the cleanup path against the current DB and verify recent `stock_prices` rows no longer show `NaN` in `stock_splits`
- Live behavior
  - rebuild `stock-fetcher`
  - confirm logs no longer report split-triggered full rebuilds for every ticker
  - confirm only true split tickers escalate to full rebuild

## Assumptions
- `yfinance` split values should be treated as “no split” when missing or `NaN`
- Existing `NaN` values in `stock_prices.stock_splits` are invalid historical artifacts and should be normalized to `0`
- The fix should be backward-safe and not require schema changes
