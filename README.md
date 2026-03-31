# S&P 500 Stock Analysis with Grafana

## Quick Description

**SnP500Grafana** is a containerized application that collects S&P 500 stock market data and visualizes it in Grafana. It pulls OHLCV (Open, High, Low, Close, Volume) price data from Yahoo Finance, stores it in TimescaleDB (time-series optimized PostgreSQL), computes additive analytics snapshots and rankings, and generates both Grafana dashboards and deterministic weekly/monthly reports. The fetcher performs a full historical load only when the database is empty and schedules incremental updates after the US market close.

## Architecture

- **TimescaleDB**: time-series optimized PostgreSQL for stock prices, indicators, analytics snapshots, and stored report metadata
- **stock-fetcher**: Python service that fetches data from Wikipedia (ticker list) and Yahoo Finance (OHLCV), persists indicators, refreshes analytics/rank snapshots, and writes weekly/monthly reports
- **Grafana**: custom image with baked-in datasource provisioning and generated dashboards driven by ticker metadata, analytics snapshots, and report rows. The leaderboards dashboard links to a manual monthly report UI and the stack includes remote rendering support for downloadable report artifacts.

## Prerequisites

- Docker and Docker Compose
- Portainer (optional, for web-based deployment)
- Python if you want to regenerate dashboard JSON locally

## Quick Start

1. Clone or copy this repository.

2. Create a `.env` file from the example:
   ```bash
   cp .env.example .env
   ```

3. Set required variables in `.env`:
   - `DB_PASSWORD` - PostgreSQL/TimescaleDB password
   - `GRAFANA_ADMIN_USER` - optional Grafana admin username, defaults to `admin`
   - `GRAFANA_ADMIN_PASSWORD` - Grafana admin password
   - Optional Grafana overrides if you need remote access:
     - `GRAFANA_BIND_IP` - defaults to `127.0.0.1`
     - `GRAFANA_PORT` - defaults to `3001`
     - `GRAFANA_HOST` - defaults to `localhost`
     - `GRAFANA_ROOT_URL` - defaults to `http://localhost:3001`
   - Optional manual report UI overrides:
     - `REPORT_UI_BIND_IP` - defaults to `127.0.0.1`
     - `REPORT_UI_PORT` - defaults to `3002`
     - `REPORT_UI_PUBLIC_URL` - defaults to `http://localhost:3002`
   - Optional backfill override if you need to extend existing history earlier than the current earliest stored date:
     - `BACKFILL_START` - example `2000-01-01`

4. Deploy the stack:
   ```bash
   docker compose up -d
   ```

5. Wait for the initial data load if the database is empty. Monitor logs:
   ```bash
   docker compose logs -f stock-fetcher
   ```

   On restarts against an already-populated database, the fetcher now becomes ready after schema init and the startup incremental OHLCV sync. Indicator refresh, analytics refresh, and report generation continue in a background catch-up job by default. Set `STARTUP_POST_SYNC_MODE=blocking` to restore the legacy behavior and keep startup blocked until those post-sync tasks finish.

   For an older-history backfill on a populated database, set `BACKFILL_START` and restart the stack. The fetcher will still run its normal startup incremental sync first, then fetch older data from `BACKFILL_START` up to the earliest date already in `stock_prices`.

6. Open Grafana at `http://localhost:3001`, log in with the admin credentials, and open the dashboards under **Dashboards > S&P 500**. On the `S&P 500 Leaderboards` dashboard, use the `Generate Monthly Report` panel to open the report UI and start a monthly export.

## Dashboard Generation

Dashboard JSON is committed to the repo and baked into the Grafana image, so Grafana can start without any generation step. If you update the dashboard templates, regenerate the JSON files locally:

```bash
pip install -r requirements-dev.txt
python scripts/generate_dashboards.py
```

If the manual report UI is hosted somewhere other than `http://localhost:3002`, set `REPORT_UI_PUBLIC_URL` before regenerating so the leaderboards launch panel points at the correct endpoint.

Generated dashboards:

- `S&P 500 Ticker Detail`
- `S&P 500 Leaderboards`
- `S&P 500 Trend Regime`
- `S&P 500 Momentum`
- `S&P 500 Volatility and Risk`
- `S&P 500 Volume Confirmation`
- `S&P 500 Breakout and Breakdown`
- `S&P 500 Market Structure`
- `S&P 500 Relative Strength`
- `S&P 500 Mean Reversion`

The `S&P 500 Leaderboards` dashboard also includes:

- a monthly-report launch panel that opens `REPORT_UI_PUBLIC_URL/monthly-report?autostart=1`
- a latest manual export panel backed by `report_export_jobs`

## Analytics and Reports

The fetcher now materializes additive analytics tables after OHLCV and indicator refreshes:

- `signal_snapshots`
- `rank_snapshots`
- `market_breadth_snapshots`
- `report_snapshots`

The analytics layer produces machine-readable block scores for trend, momentum, volume, relative strength, structure, mean reversion, and risk. Those snapshots drive both Grafana and the report generator.

Analytics snapshot refreshes now backfill any missing historical snapshot dates from the full stored OHLCV range and only append newly missing dates on later runs. That means ticker score history can cover the full database period without manually clearing and rebuilding the analytics tables. Historical backfills are written in snapshot-date windows controlled by `ANALYTICS_BACKFILL_BATCH_DATES`, which reduces transaction size and makes progress visible in `stock-fetcher` logs during long rebuilds.

Weekly and monthly reports are generated as Markdown and HTML under `reports/` by default. The fetcher also stores report metadata and summary rows in `report_snapshots` so Grafana can surface the latest report output alongside rankings.

For manual exports, the stack now includes a dedicated report UI at `REPORT_UI_PUBLIC_URL`. The Grafana launch panel opens that page, which generates a downloadable HTML report and PDF using Grafana-rendered panel images plus dashboard-by-dashboard explanations.

## Fetch Hardening

Yahoo Finance can partially fail within a multi-symbol request: one symbol may come back missing or malformed while the rest of the chunk succeeds. The fetcher now detects that condition, preserves the successful rows, retries only the missing symbols in smaller recovery batches, and finally falls back to one-symbol retries before marking a symbol as failed for that run.

Runs remain non-fatal by default when only a subset of symbols fail. The fetcher logs a structured summary after each sync and warns explicitly about any symbols that still failed after targeted retries.

Example warning shape:

```text
Fetch summary: requested=503 succeeded=502 recovered=1 failed=1
Permanently failed symbols after targeted retries: EXR
```

## Deployment via Portainer

1. In Portainer, go to **Stacks > Add stack**
2. Deploy from a repository checkout or upload the full project directory so Portainer has the Docker build context for the custom Grafana image
3. Under environment variables, add:
   - `DB_PASSWORD` (required)
   - `GRAFANA_ADMIN_PASSWORD` (required)
   - Override `GRAFANA_BIND_IP`, `GRAFANA_PORT`, `GRAFANA_HOST`, and `GRAFANA_ROOT_URL` if the stack should be reachable off-host
4. Deploy the stack and wait for Portainer to build the Grafana image

Pasting only the contents of `docker-compose.yml` is not sufficient unless the stack also has access to the repository files referenced by the build context. The compose defaults are tuned for local development, so remote deployments should supply explicit Grafana host and URL values.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_PASSWORD` | *(required)* | Database password |
| `GRAFANA_ADMIN_USER` | `admin` | Grafana admin username |
| `GRAFANA_ADMIN_PASSWORD` | *(required)* | Grafana admin password |
| `GRAFANA_BIND_IP` | `127.0.0.1` | Host IP for published Grafana port |
| `GRAFANA_PORT` | `3001` | Published host port for Grafana |
| `GRAFANA_HOST` | `localhost` | Grafana server domain |
| `GRAFANA_ROOT_URL` | `http://localhost:3001` | Grafana public URL |
| `REPORT_UI_BIND_IP` | `127.0.0.1` | Host IP for publishing the manual report UI |
| `REPORT_UI_PORT` | `3002` | Published host port for the manual report UI |
| `REPORT_UI_PUBLIC_URL` | `http://localhost:3002` | Public URL used by Grafana links to the manual report UI |
| `DB_NAME` | `stocks` | Database name |
| `DB_USER` | `postgres` | Database user |
| `UPDATE_CRON` | `0 23 * * *` | Cron: daily at 11 PM UTC (6 PM ET) |
| `YFINANCE_CHUNK_SIZE` | `50` | Tickers per batch (rate limit mitigation) |
| `YFINANCE_SYMBOL_RETRIES` | `2` | Additional targeted retry attempts for missing symbols during recovery |
| `YFINANCE_RECOVERY_CHUNK_SIZE` | `5` | Small-batch size used when retrying only missing symbols from a partial chunk |
| `YFINANCE_FAILED_SYMBOL_LOG_LIMIT` | `20` | Maximum failed symbols to print in the final warning summary for a run |
| `YFINANCE_DELAY_SEC` | `2.5` | Delay between batches (seconds) |
| `YFINANCE_MAX_RETRIES` | `5` | Retry attempts per ticker chunk when Yahoo rate-limits requests |
| `YFINANCE_RETRY_DELAY` | `60` | Base retry delay in seconds before exponential backoff is applied |
| `HISTORICAL_START` | `2020-01-01` | Start date for initial historical load |
| `BACKFILL_START` | *(unset)* | Optional one-time startup backfill start date for older history on a populated database |
| `STARTUP_POST_SYNC_MODE` | `background` | Populated-DB startup post-sync behavior: `background` starts the scheduler after OHLCV sync and defers indicators/analytics/reports to a one-shot catch-up job; `blocking` restores the legacy inline behavior |
| `INDICATORS_ENABLED` | `true` | Enable persisted technical-indicator calculation after OHLCV syncs |
| `INDICATOR_INCREMENTAL_LOOKBACK_ROWS` | `1000` | Row window used for normal incremental indicator refreshes per touched ticker |
| `INDICATOR_REBUILD_ON_STARTUP` | `false` | Recompute indicators from full history for symbols explicitly rebuilt during startup processing; it does not force a full-universe populated-DB startup block |
| `INDICATOR_BATCH_SIZE` | `25` | Number of symbols processed per indicator refresh batch |
| `ANALYTICS_ENABLED` | `true` | Enable additive analytics snapshot refresh after price and indicator updates |
| `ANALYTICS_TIMEFRAMES` | `daily,weekly,monthly` | Comma-separated analytics snapshot cadences to materialize |
| `ANALYTICS_BACKFILL_BATCH_DATES` | `50` | Maximum number of missing snapshot dates written per analytics backfill window within each timeframe |
| `REPORTS_ENABLED` | `true` | Enable deterministic weekly and monthly report generation |
| `REPORT_OUTPUT_DIR` | `/app/reports` | In-container output path for generated Markdown and HTML reports |
| `REPORT_WEEKLY_CRON` | `15 0 * * 1` | Cron schedule for the weekly report job |
| `REPORT_MONTHLY_CRON` | `30 0 1 * *` | Cron schedule for the monthly report job |

## Services

| Service | Port | Description |
|---------|------|-------------|
| TimescaleDB | 5432 | PostgreSQL + TimescaleDB |
| Grafana | 3001 | Dashboards and visualization, bound to `127.0.0.1:3001` by default |
| Report UI | 3002 | Manual monthly report workflow, bound to `127.0.0.1:3002` by default |
| stock-fetcher | - | Data fetcher and daily scheduler |

## Data Flow

1. **Initial load**: On first start with an empty database, the fetcher downloads the S&P 500 ticker set from Wikipedia and historical OHLCV data from Yahoo Finance.
2. **Restart behavior**: On later restarts, the fetcher performs an incremental sync instead of replaying the full historical backfill. By default, it starts the scheduler immediately after that sync and finishes indicators, analytics, and reports in the background for only the symbols touched by the startup sync.
3. **Optional backfill**: If `BACKFILL_START` is set and the database already contains newer rows, the fetcher can backfill older history without clearing the database first.
4. **Daily updates**: At the configured cron time, the fetcher reloads the last 7 days of data and upserts into the database.
5. **Indicator derivation**: After each OHLCV sync, derived indicators are computed from the stored bars and persisted for Grafana use.
6. **Analytics refresh**: The fetcher computes machine-readable signal snapshots, cross-sectional rankings, and market breadth summaries and stores them in additive analytics tables.
7. **Report generation**: Weekly and monthly reports are rendered to Markdown/HTML under `reports/` and summarized in `report_snapshots`.
8. **Grafana**: Starts from a custom image that already contains the provisioned TimescaleDB datasource and generated dashboards that query `stock_prices`, `tickers`, analytics snapshot tables, and stored report metadata.

## Monitoring Backfill Progress

Use fetcher logs to monitor chunk-level progress:

```bash
docker compose logs -f stock-fetcher
```

During a backfill, the fetcher logs:

- the requested backfill window
- each chunk number and ticker count
- rows returned per chunk and the date span for that chunk
- any partial-chunk recovery attempts for missing symbols
- a final per-run fetch summary including recovered and permanently failed symbols
- the final upsert count

You can also inspect the stored date range directly in PostgreSQL:

```sql
SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
FROM stock_prices;
```

## Limitations

- `yfinance` is community-maintained and depends on Yahoo Finance. For production-critical deployments, consider paid APIs such as Polygon.io or Alpha Vantage.
- Rate limiting still affects the initial load. Do not reduce chunk delays aggressively.
- Dashboard generation requires a local Python environment.
- TA-Lib is built into the fetcher image, which increases image build time compared with the pure-`pandas-ta` setup.
- Report files are written to the host-mounted `reports/` directory by default; if you change `REPORT_OUTPUT_DIR`, keep `docker-compose.yml` and your runtime mount path aligned.

## License

MIT
