# Codex Execution Plan for This Repository

This document rewrites the earlier execution plan so it matches the current project structure and delivery model in this repository.

The long-term product ambition remains the same:

1. 10 market dashboards in Grafana
2. Per-ticker machine-readable signal interpretation
3. Bullish and bearish ranking across the S&P 500
4. Weekly and monthly auto-generated reports
5. A recommendation layer with explicit risk disclaimers

The difference is architectural discipline: this plan now starts from the real repo baseline and extends it incrementally instead of assuming a separate analytics platform already exists.

## Current Project Constraints

- `docker compose up -d` remains the standard deployment path from the repo root.
- Dashboard JSON is generated code and must remain committed under `grafana/dashboards/`.
- `scripts/init-db.sql` is the schema source of truth. If schema changes are needed, `fetcher/src/database.py` fallback SQL must stay aligned.
- Environment variable names should not change unless `docker-compose.yml`, `fetcher/src/config.py`, and `README.md` are updated together.
- Current startup behavior must remain intact:
  - empty database -> full historical sync
  - populated database -> startup incremental sync
  - scheduled job -> incremental sync

## Current Repo Baseline

The repo today contains three deployed services:

- `timescaledb`
- `stock-fetcher`
- `grafana`

Current source-of-truth locations:

- Runtime Python: `fetcher/src/`
- Schema: `scripts/init-db.sql`
- Schema fallback loader: `fetcher/src/database.py`
- Scheduler and startup flow: `fetcher/src/main.py`
- Indicator calculation: `fetcher/src/indicators.py`
- Dashboard generator: `scripts/generate_dashboards.py`
- Generated dashboard JSON: `grafana/dashboards/`
- Grafana provisioning: `grafana/provisioning/`

Current persisted tables already in scope:

- `tickers`
- `stock_prices`
- `indicator_catalog`
- `stock_indicators`

Current dashboard state:

- Only two dashboards are emitted by `scripts/generate_dashboards.py` today:
  - `sp500-ticker-detail.json`
  - `sp500-leaderboards.json`
- Additional generator functions already exist in the script for indicator and market-overview dashboards, but `main()` does not currently write them out.

## Delivery Strategy

Use one orchestrator agent and many narrow subagents with explicit ownership. The orchestrator owns sequencing, acceptance criteria, merge order, and conflict prevention. All subagents should work against the existing repo structure unless Track B is explicitly entered later.

### Core Rule

Do not build a second project inside this project.

Track A extends the current stack:

- Python code stays under `fetcher/src/`
- schema changes stay in `scripts/init-db.sql`
- dashboard logic stays in `scripts/generate_dashboards.py`
- generated JSON stays in `grafana/dashboards/`
- compose remains the standard deployment entry point

Track B is optional and only starts if Track A proves too heavy or too coupled for the current `stock-fetcher` service.

## Target Architecture

### Track A: Repo-Native Near-Term Architecture

```text
Wikipedia + Yahoo Finance
    ->
stock-fetcher startup + scheduled sync
    ->
tickers + stock_prices
    ->
indicator refresh in fetcher/src
    ->
indicator_catalog + stock_indicators
    ->
analytics jobs inside fetcher/src
    ->
analytics snapshot tables/views in TimescaleDB
    ->
Grafana dashboards generated from scripts/generate_dashboards.py
    ->
report generation under fetcher/src and/or scripts/
```

### Track B: Optional Later Extraction

Only if the repo-native implementation becomes too large or operationally awkward:

- move analytics logic into a dedicated service or container
- move reporting into its own runtime only if operationally necessary
- keep TimescaleDB, Grafana provisioning, and committed dashboard JSON conventions intact
- preserve the same schema and dashboard contracts established in Track A

Track B is not the starting assumption. It is a later refactor path, not a prerequisite.

## Repo-Native Target Structure

This is the intended shape for Track A work:

```text
.
|-- docker-compose.yml
|-- README.md
|-- fetcher/
|   |-- Dockerfile
|   `-- src/
|       |-- main.py
|       |-- database.py
|       |-- config.py
|       |-- indicators.py
|       |-- analytics/          # planned addition if needed
|       |-- reporting/          # planned addition if needed
|       `-- ...
|-- scripts/
|   |-- init-db.sql
|   `-- generate_dashboards.py
|-- grafana/
|   |-- dashboards/
|   `-- provisioning/
`-- docs/
    `-- codex_execution_plan.md
```

Rules for this structure:

- New runtime analytics modules belong under `fetcher/src/`, not `services/analytics/`.
- New reporting modules belong under `fetcher/src/` and/or `scripts/`, not `reporter/`.
- New persistent schema objects belong in `scripts/init-db.sql`, not a new `sql/schema/` tree.
- Dashboard expansion happens in `scripts/generate_dashboards.py`, followed by regenerated JSON in `grafana/dashboards/`.

## Public Interfaces to Lock

These are the current public runtime and deployment interfaces that the plan must respect.

### Service Names

- `timescaledb`
- `stock-fetcher`
- `grafana`

### Existing Environment Surface

Database and Grafana configuration currently live across `docker-compose.yml`, `fetcher/src/config.py`, and `README.md`.

Current environment variables already in play:

- `DB_PASSWORD`
- `DB_NAME`
- `DB_USER`
- `DB_HOST`
- `DB_PORT`
- `GRAFANA_ADMIN_USER`
- `GRAFANA_ADMIN_PASSWORD`
- `GRAFANA_BIND_IP`
- `GRAFANA_PORT`
- `GRAFANA_HOST`
- `GRAFANA_ROOT_URL`
- `UPDATE_CRON`
- `YFINANCE_CHUNK_SIZE`
- `YFINANCE_SYMBOL_RETRIES`
- `YFINANCE_RECOVERY_CHUNK_SIZE`
- `YFINANCE_FAILED_SYMBOL_LOG_LIMIT`
- `YFINANCE_DELAY_SEC`
- `YFINANCE_MAX_RETRIES`
- `YFINANCE_RETRY_DELAY`
- `HISTORICAL_START`
- `BACKFILL_START`
- `INDICATORS_ENABLED`
- `INDICATOR_INCREMENTAL_LOOKBACK_ROWS`
- `INDICATOR_REBUILD_ON_STARTUP`
- `INDICATOR_BATCH_SIZE`

If Track A adds new schedules, report output paths, scoring weights, benchmark symbols, or liquidity filters, those become new planned interfaces and must be documented in all three places only when actually introduced.

### Grafana Provisioning Contract

- Datasource provisioning stays under `grafana/provisioning/datasources/`
- Dashboard provisioning stays under `grafana/provisioning/dashboards/`
- File-based dashboard loading continues to point at `/opt/grafana/dashboards`

## Planned Additive Analytics Interfaces

The existing tables stay unchanged as foundational sources. New analytics outputs should be additive and pluralized to match repo naming style.

Recommended Track A additions:

- `signal_snapshots`
- `rank_snapshots`
- `market_breadth_snapshots`
- `report_snapshots` (optional)

These are schema additions to `scripts/init-db.sql`, not a separate analytics schema tree.

### `signal_snapshots`

One row per ticker, per snapshot date, per timeframe.

Suggested columns:

- `snapshot_date`
- `symbol`
- `timeframe`
- `close`
- `volume`
- `trend_score`
- `momentum_score`
- `volume_score`
- `relative_strength_score`
- `structure_score`
- `mean_reversion_score`
- `volatility_risk_score`
- `risk_penalty`
- `final_score`
- `trend_state`
- `momentum_state`
- `volume_state`
- `relative_strength_state`
- `structure_state`
- `volatility_state`
- `regime_label`
- `recommendation_label`
- `breakout_flag`
- `breakdown_flag`
- `overbought_flag`
- `oversold_flag`
- `trend_alignment_flag`
- `data_quality_flag`
- `created_at`
- `updated_at`

### `rank_snapshots`

Universe-level ranking output for each snapshot date and timeframe.

Suggested columns:

- `snapshot_date`
- `timeframe`
- `symbol`
- `final_score`
- `bull_rank`
- `bear_rank`
- `regime_label`
- `recommendation_label`
- `score_change_1w`
- `score_change_1m`
- `in_top20_bull`
- `in_top20_bear`

### `market_breadth_snapshots`

Market-wide summary output.

Suggested columns:

- `snapshot_date`
- `timeframe`
- `universe_size`
- `bullish_count`
- `neutral_count`
- `bearish_count`
- `pct_above_ema20`
- `pct_above_ema50`
- `pct_above_ema200`
- `pct_new_20d_high`
- `pct_new_20d_low`
- `pct_near_52w_high`
- `pct_near_52w_low`
- `avg_final_score`
- `median_final_score`

### `report_snapshots` (Optional)

Precomputed narrative or machine-readable report blocks.

Suggested columns:

- `snapshot_date`
- `symbol`
- `timeframe`
- `final_score`
- `regime_label`
- `recommendation_label`
- `summary_text`
- `risk_text`
- `key_drivers_json`

## Execution Model: Subagent-Heavy by Design

The prior draft used five broad agents. That is too coarse for this repo. Use the following narrower map instead.

### Orchestrator Agent

Owns:

- sequencing
- dependency management
- acceptance criteria
- branch and merge order
- preventing duplicated or conflicting work

### Baseline Explorer Agent

Owns read-only repo grounding:

- `README.md`
- `docker-compose.yml`
- Grafana provisioning
- current generated dashboard outputs
- current dashboard generator coverage

Output:

- repo-truth summary
- confirmed current constraints
- list of already-existing vs still-missing capabilities

### Schema Contract Agent

Owns:

- `scripts/init-db.sql`
- fallback schema in `fetcher/src/database.py`
- definition of additive analytics tables or views

Output:

- schema extension spec
- required indexes
- upsert and query contract for analytics outputs

### Indicator Inventory Agent

Owns:

- `fetcher/src/indicators.py`
- `indicator_catalog`
- `stock_indicators`
- indicator join strategy for downstream analytics

Output:

- available indicator inventory
- indicator categories mapped to scoring blocks
- gaps that require derived metrics rather than stored indicators

### Analytics Engine Agent

Owns Track A runtime implementation under `fetcher/src/`, for example:

- `fetcher/src/analytics/`
- signal calculators
- ranking logic
- breadth metrics
- recommendation mapping

This agent does not introduce a separate container in Track A.

### Scheduler Integration Agent

Owns:

- `fetcher/src/main.py`
- `fetcher/src/config.py`
- job sequencing around startup, daily sync, analytics refresh, and report generation

This agent extends the current APScheduler model instead of replacing it.

### Dashboard Generator Agent

Owns:

- `scripts/generate_dashboards.py`
- regenerated JSON under `grafana/dashboards/`

Rules:

- no hand-editing generated JSON
- dashboard queries should prefer analytics snapshot tables/views once available
- dashboard rollout must respect current provisioning conventions

### Reporting Agent

Owns repo-native reporting implementation under:

- `fetcher/src/reporting/` if created
- `scripts/` if command-style report generation is preferred

Output:

- weekly and monthly report generation
- Markdown and HTML output
- systematic disclaimer handling

### Docs and Config Agent

Owns:

- `README.md`
- config documentation
- new env var documentation if new runtime controls are introduced

### Validation Agent

Owns:

- `python -m compileall fetcher/src scripts`
- `python scripts/generate_dashboards.py`
- `docker compose config`
- optional targeted container log validation

This agent is responsible for repo-convention validation, not strategy.

## Parallelization Plan

These workstreams can and should run in parallel:

### Wave 1

- Baseline Explorer Agent
- Schema Contract Agent
- Indicator Inventory Agent

These agents unblock almost all later implementation and should begin immediately.

### Wave 2

- Analytics Engine Agent

Starts after schema direction and indicator inventory are stable enough to lock interfaces.

### Wave 3

- Dashboard Generator Agent
- Reporting Agent

These can proceed in parallel once analytics tables, views, and query contracts are fixed.

### Wave 4

- Scheduler Integration Agent
- Docs and Config Agent

These follow after runtime behavior, configuration needs, and output paths are known.

### Validation Cadence

- Validation Agent runs after each milestone
- Validation Agent runs again after end-to-end integration

## Track A: Repo-Native Near-Term Roadmap

Track A is the default implementation path.

### Phase 1: Baseline Audit and Data Contract

Goal:

- Confirm the actual source tables, indicator coverage, dashboard generation flow, and runtime constraints.

Tasks:

- inspect `tickers`, `stock_prices`, `indicator_catalog`, and `stock_indicators`
- confirm join keys, time granularity, and latest available indicators
- document current generated dashboards vs dormant generator functions
- confirm existing startup and scheduler behavior in `fetcher/src/main.py`

Deliverables:

- data contract summary
- list of required additive analytics outputs
- confirmed interface assumptions for later phases

### Phase 2: Additive Analytics Schema

Goal:

- Add snapshot-oriented analytics tables and any supporting views without disturbing the current fetch and indicator pipeline.

Tasks:

- extend `scripts/init-db.sql`
- mirror changes in `fetcher/src/database.py`
- define indexes and upsert patterns
- keep names plural and aligned with existing repo style

Acceptance criteria:

- new tables are additive
- existing tables remain primary raw-data inputs
- schema source-of-truth discipline remains intact

### Phase 3: Signal and Scoring Engine in `fetcher/src/`

Goal:

- Convert raw OHLCV plus stored indicators into explainable signal blocks and a composite ranking score.

Planned block families:

- trend
- momentum
- volume confirmation
- structure
- relative strength
- mean reversion
- volatility and risk
- multi-timeframe alignment

Implementation rule:

- Track A logic lives inside `fetcher/src/`, ideally in new `analytics/` modules, not in a separate service.

Acceptance criteria:

- scores are deterministic
- block contributions are inspectable
- ranking inputs are materialized into analytics outputs rather than interpreted ad hoc in Grafana

### Phase 4: Ranking and Breadth Snapshots

Goal:

- Move from per-ticker scoring to universe-level outputs usable by Grafana and reports.

Tasks:

- generate bullish and bearish ranks
- calculate score deltas over time
- compute breadth metrics from existing price and analytics outputs
- persist these into additive snapshot tables

Acceptance criteria:

- top 20 bullish and bearish queries become direct DB reads
- breadth can be consumed without complex dashboard-side logic

### Phase 5: Scheduler Integration

Goal:

- Fold analytics and reporting into the existing fetcher lifecycle.

Tasks:

- extend startup flow in `fetcher/src/main.py`
- add config support in `fetcher/src/config.py` only if required
- keep the current sync semantics unchanged
- make analytics and reporting idempotent

Acceptance criteria:

- empty DB behavior remains full historical load first
- populated DB behavior remains startup incremental sync first
- scheduled jobs remain incremental by default

### Phase 6: Dashboard Expansion Through the Existing Generator

Goal:

- Build out the dashboard set through `scripts/generate_dashboards.py` instead of hand-authoring JSON or introducing a different dashboard toolchain.

Current state:

- two dashboards emitted
- several additional generator functions already exist but are not written by `main()`

Track A dashboard sequence:

1. extend current ticker-detail and leaderboard dashboards to consume analytics outputs where appropriate
2. activate and finish existing generator-backed indicator and overview dashboards if still useful
3. add the remaining end-state dashboards through the same generator workflow

Long-term dashboard target remains:

1. Trend Regime
2. Momentum
3. Volatility and Risk
4. Volume Confirmation
5. Breakout and Breakdown
6. Market Structure
7. Relative Strength
8. Mean Reversion and Stretch
9. Multi-Timeframe Alignment
10. Universe Ranking and Report Control

Rules:

- generated dashboards are authored in `scripts/generate_dashboards.py`
- committed outputs live in `grafana/dashboards/`
- dashboard provisioning remains file-based through current Grafana config

### Phase 7: Reporting Layer

Goal:

- Produce weekly and monthly report artifacts using the same analytics outputs that power Grafana.

Preferred outputs:

- Markdown
- HTML

Optional later output:

- PDF

Implementation rule:

- use deterministic templates and structured fields
- do not depend on opaque LLM-only freeform generation for the core report body

Report sections:

- market summary
- top bullish names
- top bearish names
- watchlists
- risk commentary
- explicit non-advisory disclaimer

### Phase 8: End-to-End Validation

Goal:

- Validate repo health using the lightest checks that match project conventions.

Required checks:

- `python -m compileall fetcher/src scripts`
- `python scripts/generate_dashboards.py`
- `docker compose config`

Preferred additional checks when Docker is available:

- `docker compose up -d`
- targeted `docker compose logs` review for `stock-fetcher` and `grafana`

## Track B: Optional Architecture Extraction

Track B starts only if Track A implementation becomes too large, too slow to iterate on, or too operationally messy inside `stock-fetcher`.

Possible extraction steps:

- move analytics execution into a dedicated container
- split reporting if artifact generation needs independent cadence
- keep the same schema and dashboard contracts established in Track A
- preserve compose-first deployment

Track B must not break:

- current compose usability
- committed dashboard JSON
- the schema source-of-truth model
- the established analytics table contracts

## Suggested Implementation Order

Use this order unless a blocker forces a dependency change:

1. Baseline audit and data contract
2. Additive analytics schema
3. Signal and scoring engine
4. Ranking and breadth snapshots
5. Scheduler integration
6. Dashboard expansion through the existing generator
7. Reporting layer
8. End-to-end validation
9. Optional architecture extraction only if Track A proves insufficient

## Suggested Prompt Pack for Subagents

### Prompt for Baseline Explorer Agent

```text
Read README.md, docker-compose.yml, grafana provisioning, scripts/generate_dashboards.py, and the current generated dashboard files. Produce a concise repo-truth summary covering current services, deployment assumptions, dashboard generation workflow, and which dashboard generator functions exist versus which dashboards are actually emitted today.
```

### Prompt for Schema Contract Agent

```text
Inspect scripts/init-db.sql and fetcher/src/database.py. Define additive analytics snapshot tables and any required indexes or views so ranking, signal interpretation, Grafana queries, and reporting can all use a stable contract. Keep the existing tables as raw-data sources and preserve schema source-of-truth discipline.
```

### Prompt for Indicator Inventory Agent

```text
Inspect fetcher/src/indicators.py plus the indicator_catalog and stock_indicators schema contracts. Produce an inventory of indicator families already available, how they map to signal blocks, which joins downstream analytics should use, and where derived metrics are still needed beyond persisted indicator rows.
```

### Prompt for Analytics Engine Agent

```text
Implement a repo-native analytics engine under fetcher/src that converts OHLCV and persisted indicators into explainable signal blocks, composite scores, ranking outputs, and breadth metrics. Do not introduce a new service in Track A. Make outputs deterministic and suitable for both Grafana and report generation.
```

### Prompt for Scheduler Integration Agent

```text
Extend fetcher/src/main.py and fetcher/src/config.py so analytics refreshes and report generation integrate with the existing APScheduler flow. Preserve current startup semantics for empty versus populated databases and keep scheduled jobs incremental and idempotent.
```

### Prompt for Dashboard Generator Agent

```text
Expand scripts/generate_dashboards.py so dashboards are authored and regenerated through the existing generator workflow. Do not hand-edit generated JSON. Prefer analytics snapshot tables and views for new panels once those contracts are available.
```

### Prompt for Reporting Agent

```text
Build a repo-native reporting layer under fetcher/src and/or scripts that generates weekly and monthly Markdown and HTML reports from ranking and breadth snapshot data. Use deterministic templates, machine-readable drivers, and include a clear non-financial-advice disclaimer.
```

### Prompt for Docs and Config Agent

```text
Update README.md and configuration documentation only where new runtime interfaces are introduced. Keep existing environment variable names unless there is a concrete coordinated change across docker-compose.yml, fetcher/src/config.py, and README.md.
```

### Prompt for Validation Agent

```text
Validate the implementation using project-standard checks: python -m compileall fetcher/src scripts, python scripts/generate_dashboards.py, docker compose config, and targeted container log review when Docker is available. Focus on catching schema drift, broken generator output, and startup regressions.
```

## Definition of Done

The overall project is done when all of the following are true:

- additive analytics snapshot outputs exist in TimescaleDB
- per-ticker signal interpretation is machine-readable and explainable
- bullish and bearish ranking is queryable directly from stored outputs
- weekly and monthly reports can be generated systematically
- Grafana dashboards are generated through the existing script workflow and provision on startup
- `docker compose up -d` remains the standard way to run the stack
- committed dashboard JSON remains in the repo
- schema changes remain anchored in `scripts/init-db.sql`
- current startup semantics are preserved
- every recommendation-oriented output includes a clear non-advisory disclaimer

## Validation Checklist for This Document

This rewritten plan is only acceptable if all of the following remain true:

- it does not claim the repo already has `services/analytics`
- it does not claim the repo already has `reporter`
- it does not claim the repo already has `sql/schema`
- it does not claim the repo already has 10 provisioned dashboards today
- it explicitly preserves committed dashboard JSON
- it explicitly preserves schema source-of-truth discipline
- it explicitly preserves current empty-DB versus populated-DB startup behavior
- it explicitly preserves the compose-based deployment model
