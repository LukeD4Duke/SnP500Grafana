# Code Review Findings

Review date: 2026-03-28

Scope: repository-level review for likely unused functions/files, obvious flaws, inconsistencies, and best-practice improvements.

Validation performed:

- `python -m compileall fetcher/src scripts tests`
- `python -m unittest discover -s tests -v`
- `python scripts/generate_dashboards.py`
- `docker compose config`

Notes:

- The compile and unit-test checks passed.
- `docker compose config` passed.
- `python scripts/generate_dashboards.py` completed without error, but the script currently has no executable generation entrypoint and does not write dashboard JSON.
- "Unused" below means "no in-repo references found" and should be treated as likely-unused until confirmed against any external/local-only workflows.

## High Priority

### 1. Dashboard generator workflow is broken and inconsistent with the repo contract

Files:

- `scripts/generate_dashboards.py`
- `grafana/dashboards/*.json`
- `README.md`

Findings:

- `scripts/generate_dashboards.py` defines only four dashboard builder functions: `ticker_detail_dashboard`, `leaderboard_dashboard`, `trend_regime_dashboard`, and `momentum_dashboard` (`scripts/generate_dashboards.py:529`, `:657`, `:746`, `:830`).
- The repo contains ten committed dashboard JSON files under `grafana/dashboards/`.
- The script has no `main` block and no JSON write path, so `python scripts/generate_dashboards.py` does not regenerate the committed dashboards.
- The script also contains partially-refactored helper code that references undefined constants such as `ALL_SENTINEL` and `TIMEFRAME_VALUES` (`scripts/generate_dashboards.py:164`, `:181`, `:251`, `:269`, `:308`, `:327`).
- Helper functions are duplicated inside the same file, for example `query_variable`, `custom_variable`, and `latest_report_cte` are each defined twice (`scripts/generate_dashboards.py:38` and `:145`, `:58` and `:184`, `:289` and `:520`).

Why this matters:

- The documented source-of-truth workflow for dashboards is not reproducible.
- Generated JSON can drift from the Python generator with no reliable way to regenerate or review changes.
- Future edits can easily target dead helper paths and never affect the actual emitted dashboards.

Recommended change:

- Make `scripts/generate_dashboards.py` the single working source of truth again.
- Add a real entrypoint that emits every committed dashboard JSON file.
- Remove duplicate helper definitions and undefined-constant branches.
- Add a smoke test that asserts the expected dashboard count and output filenames.

Best-practice option:

- Treat generated dashboards as build artifacts from one canonical generator and verify them in CI with a deterministic output check.

### 2. `fetcher/src/database.py` contains shadowed duplicate function definitions

File:

- `fetcher/src/database.py`

Findings:

- The file defines `upsert_signal_snapshots` twice (`:726`, `:1117`).
- The file defines `upsert_rank_snapshots` twice (`:868`, `:1225`).
- The file defines `upsert_market_breadth_snapshots` twice (`:907`, `:1279`).
- The file defines `upsert_report_snapshots` twice (`:953`, `:1343`).
- The file defines `get_ticker_metadata` twice (`:1064`, `:1630`).
- In Python, the later definition silently replaces the earlier one at import time, so the earlier versions are dead code.

Why this matters:

- This is silent behavioral shadowing, not harmless duplication.
- A future maintainer can edit the wrong copy and see no runtime effect.
- The file is already very large and mixes schema, price storage, indicator storage, analytics storage, report storage, and read APIs in one module.

Recommended change:

- Delete the shadowed copies and keep one canonical implementation per function.
- Split the module by responsibility, for example: schema, price data, indicators, analytics snapshots, reports.
- Add targeted tests around the surviving analytics/report persistence helpers.

Best-practice option:

- Keep DB access modules small and domain-specific so duplicate names cannot hide in a 1900+ line file.

## Medium Priority

### 3. Several helpers look unused inside the repository

Files:

- `fetcher/src/database.py`
- `fetcher/src/indicators.py`

Likely-unused functions:

- `fetcher/src/database.py:446` `get_indicator_catalog`
- `fetcher/src/database.py:593` `get_recent_price_history_for_all_symbols`
- `fetcher/src/database.py:1593` `get_last_date`
- `fetcher/src/database.py:1645` `get_analytics_price_history`
- `fetcher/src/database.py:1690` `get_latest_signal_snapshot_date`
- `fetcher/src/database.py:1705` `get_signal_snapshots`
- `fetcher/src/database.py:1772` `get_signal_snapshot_scores_on_or_before`
- `fetcher/src/database.py:1794` `get_latest_report_snapshot_rows`
- `fetcher/src/database.py:1842` `get_rank_snapshots`
- `fetcher/src/database.py:1884` `get_market_breadth_snapshot`
- `fetcher/src/indicators.py:196` `using_talib`
- `fetcher/src/indicators.py:299` `compute_indicators_for_symbols`
- `fetcher/src/indicators.py:323` `get_max_warmup_period`

Why this matters:

- Some of these may be leftovers from earlier iterations or incomplete planned work.
- Unused helpers increase maintenance cost and make it harder to see the true public API surface.

Recommended change:

- Confirm whether any of these are needed for near-term roadmap work.
- Remove or move unused helpers behind a dedicated internal API module.
- If they are intentionally kept for upcoming work, add a comment or test that makes that explicit.

Best-practice option:

- Keep only exercised API surface in production modules; put future-facing experiments behind feature branches or separate draft modules.

### 4. Runtime-generated report artifacts are committed and will create repo churn

Files:

- `reports/weekly/weekly-report-2026-03-26.md`
- `reports/weekly/weekly-report-2026-03-26.html`
- `reports/monthly/monthly-report-2026-03-26.md`
- `reports/monthly/monthly-report-2026-03-26.html`
- `.gitignore`
- `docker-compose.yml`

Findings:

- The running stack bind-mounts `./reports` to `/app/reports`.
- Generated report outputs are already committed to the repo.
- `.gitignore` does not ignore report output paths.

Why this matters:

- Normal runtime use will create noisy git diffs.
- Generated operational artifacts are mixed with source code.

Recommended change:

- Decide whether reports are source-controlled deliverables or runtime artifacts.
- If they are runtime artifacts, ignore `reports/weekly/*.md`, `reports/weekly/*.html`, `reports/monthly/*.md`, and `reports/monthly/*.html`.
- If sample reports are useful, keep curated examples under `docs/` or `examples/` instead of the live output mount path.

Best-practice option:

- Separate immutable source assets from mutable runtime output directories.

### 5. Test coverage is too thin for the newest code paths

Files:

- `tests/`
- `fetcher/src/analytics/engine.py`
- `fetcher/src/reporting/reports.py`
- `scripts/generate_dashboards.py`

Findings:

- Current tests cover config parsing, fetch retries, and startup scheduling.
- There are no tests for analytics snapshot generation, report generation, dashboard generation, or duplicate/unused helper drift.

Why this matters:

- The biggest inconsistencies in this repo are exactly in untested areas.
- Current passing tests create a false sense of coverage.

Recommended change:

- Add smoke tests for:
  - analytics snapshot row generation shape
  - report artifact generation against fixture inputs
  - dashboard generation output filenames/count
  - database helper import sanity, especially duplicate/shadow-prone functions

Best-practice option:

- Add one fast repository smoke suite that validates the documented workflows, not just isolated helper behavior.

### 6. `.env.example` is not aligned with the expanded configuration surface

Files:

- `.env.example`
- `README.md`
- `docker-compose.yml`
- `fetcher/src/config.py`

Findings:

- The repo now documents and consumes many more fetcher/reporting/analytics environment variables than are represented in `.env.example`.
- Example omissions include retry/recovery/reporting/analytics toggles such as `YFINANCE_SYMBOL_RETRIES`, `YFINANCE_RECOVERY_CHUNK_SIZE`, `YFINANCE_FAILED_SYMBOL_LOG_LIMIT`, `INDICATORS_ENABLED`, `INDICATOR_BATCH_SIZE`, `ANALYTICS_ENABLED`, `ANALYTICS_TIMEFRAMES`, `REPORTS_ENABLED`, `REPORT_OUTPUT_DIR`, `REPORT_WEEKLY_CRON`, and `REPORT_MONTHLY_CRON`.

Why this matters:

- The example env file is part of the operator contract.
- Drift between compose, docs, and config makes deployment tuning harder and easier to misconfigure.

Recommended change:

- Align `.env.example` with the currently supported environment variables or intentionally trim the public surface and document only supported overrides.

Best-practice option:

- Treat `.env.example` as the canonical operator-facing config index and keep it updated with every config addition.

## Low Priority

### 7. `fetcher/run.sh` appears unused

File:

- `fetcher/run.sh`

Findings:

- The fetcher image uses `ENTRYPOINT ["python", "-m", "src.main"]` directly in `fetcher/Dockerfile`.
- No in-repo references to `fetcher/run.sh` were found.

Why this matters:

- Small issue, but it adds one more misleading entrypoint path to the repo.

Recommended change:

- Remove it if it is no longer part of any manual or external deployment flow.
- Otherwise document where it is still used.

### 8. Best-practice structural recommendation: split large mixed-responsibility modules

Files:

- `fetcher/src/database.py`
- `scripts/generate_dashboards.py`

Finding:

- The largest inconsistencies in this repo are concentrated in the two modules that carry too many responsibilities at once.

Recommended change:

- Split `database.py` into domain-specific persistence modules.
- Split `generate_dashboards.py` into shared dashboard helpers plus one function per dashboard family and one explicit emission entrypoint.

Best-practice option:

- Small modules with one clear ownership boundary are easier to test, review, and keep consistent with generated artifacts.

## Suggested Implementation Order

1. Fix the dashboard generator so the documented workflow is real and reproducible again.
2. Remove duplicate/shadowed functions from `fetcher/src/database.py`.
3. Add smoke tests for dashboard generation, analytics generation, and report generation.
4. Clean up likely-unused helpers after confirming they are not part of external workflows.
5. Decide whether generated reports belong in git, then update `.gitignore` and repo structure accordingly.
6. Align `.env.example` with the current config surface.
