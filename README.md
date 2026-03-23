# S&P 500 Stock Analysis with Grafana

## Quick Description

**SnP500Grafana** is a containerized application that collects S&P 500 stock market data and visualizes it in Grafana. It pulls OHLCV (Open, High, Low, Close, Volume) price data from Yahoo Finance, stores it in TimescaleDB (time-series optimized PostgreSQL), and provides generated dashboards for ticker, sector, and industry analysis. The fetcher performs a full historical load only when the database is empty and schedules incremental updates after the US market close.

## Architecture

- **TimescaleDB**: time-series optimized PostgreSQL for stock prices
- **stock-fetcher**: Python service that fetches data from Wikipedia (ticker list) and Yahoo Finance (OHLCV), with retries and cached-symbol fallback
- **Grafana**: custom image with baked-in datasource provisioning and generated dashboards driven by ticker metadata

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

   For an older-history backfill on a populated database, set `BACKFILL_START` and restart the stack. The fetcher will still run its normal startup incremental sync first, then fetch older data from `BACKFILL_START` up to the earliest date already in `stock_prices`.

6. Open Grafana at `http://localhost:3001`, log in with the admin credentials, and open the dashboards under **Dashboards > S&P 500**.

## Dashboard Generation

Dashboard JSON is committed to the repo and baked into the Grafana image, so Grafana can start without any generation step. If you update the dashboard templates, regenerate the JSON files locally:

```bash
pip install -r requirements-dev.txt
python scripts/generate_dashboards.py
```

Generated dashboards:

- `S&P 500 Ticker Detail`
- `S&P 500 Stock Overview`
- `S&P 500 Leaderboards`
- `S&P 500 Sector Overview`
- `S&P 500 Industry Overview`

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
| `DB_NAME` | `stocks` | Database name |
| `DB_USER` | `postgres` | Database user |
| `UPDATE_CRON` | `0 23 * * *` | Cron: daily at 11 PM UTC (6 PM ET) |
| `YFINANCE_CHUNK_SIZE` | `50` | Tickers per batch (rate limit mitigation) |
| `YFINANCE_DELAY_SEC` | `2.5` | Delay between batches (seconds) |
| `YFINANCE_MAX_RETRIES` | `5` | Retry attempts per ticker chunk when Yahoo rate-limits requests |
| `YFINANCE_RETRY_DELAY` | `60` | Base retry delay in seconds before exponential backoff is applied |
| `HISTORICAL_START` | `2020-01-01` | Start date for initial historical load |
| `BACKFILL_START` | *(unset)* | Optional one-time startup backfill start date for older history on a populated database |

## Services

| Service | Port | Description |
|---------|------|-------------|
| TimescaleDB | 5432 | PostgreSQL + TimescaleDB |
| Grafana | 3001 | Dashboards and visualization, bound to `127.0.0.1:3001` by default |
| stock-fetcher | - | Data fetcher and daily scheduler |

## Data Flow

1. **Initial load**: On first start with an empty database, the fetcher downloads the S&P 500 ticker set from Wikipedia and historical OHLCV data from Yahoo Finance.
2. **Restart behavior**: On later restarts, the fetcher performs an incremental sync instead of replaying the full historical backfill.
3. **Optional backfill**: If `BACKFILL_START` is set and the database already contains newer rows, the fetcher can backfill older history without clearing the database first.
4. **Daily updates**: At the configured cron time, the fetcher reloads the last 7 days of data and upserts into the database.
5. **Grafana**: Starts from a custom image that already contains the provisioned TimescaleDB datasource and generated dashboards that query `stock_prices` and `tickers`.

## Monitoring Backfill Progress

Use fetcher logs to monitor chunk-level progress:

```bash
docker compose logs -f stock-fetcher
```

During a backfill, the fetcher logs:

- the requested backfill window
- each chunk number and ticker count
- rows returned per chunk and the date span for that chunk
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

## License

MIT
