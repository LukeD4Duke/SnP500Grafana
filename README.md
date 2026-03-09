# S&P 500 Stock Analysis with Grafana

## Quick Description

**SnP500Grafana** is a containerized application that collects S&P 500 stock market data and visualizes it in Grafana. It pulls OHLCV (Open, High, Low, Close, Volume) price data from Yahoo Finance, stores it in TimescaleDB (time-series optimized PostgreSQL), and provides interactive dashboards with ticker selection, price charts, and volume analysis. The fetcher runs an initial historical load and schedules daily updates after US market close.

## Architecture

- **TimescaleDB**: Time-series optimized PostgreSQL for stock prices
- **stock-fetcher**: Python service that fetches data from Wikipedia (ticker list) and Yahoo Finance (OHLCV), with rate limiting and retries
- **Grafana**: Dashboards with ticker selection, close price, volume, and OHLC charts

## Prerequisites

- Docker and Docker Compose
- Portainer (optional, for web-based deployment)

## Quick Start

1. Clone or copy this repository.

2. Create a `.env` file from the example:
   ```bash
   cp .env.example .env
   ```

3. Set **required** variables in `.env`:
   - `DB_PASSWORD` – PostgreSQL/TimescaleDB password
   - `GRAFANA_ADMIN_PASSWORD` – Grafana admin password

4. Deploy the stack:
   ```bash
   docker compose up -d
   ```

5. Wait for the initial data load (15–30 minutes for full S&P 500 history). Monitor logs:
   ```bash
   docker compose logs -f stock-fetcher
   ```

6. Open Grafana at `http://localhost:3000`, log in with admin credentials, and go to **Dashboards → S&P 500 → S&P 500 Stock Overview**.

## Deployment via Portainer

1. In Portainer: **Stacks** → **Add stack**
2. Paste the contents of `docker-compose.yml` (or upload the file)
3. Under **Environment variables**, add:
   - `DB_PASSWORD` (required)
   - `GRAFANA_ADMIN_PASSWORD` (required)
4. Deploy the stack

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_PASSWORD` | *(required)* | Database password |
| `GRAFANA_ADMIN_PASSWORD` | *(required)* | Grafana admin password |
| `DB_NAME` | `stocks` | Database name |
| `DB_USER` | `postgres` | Database user |
| `UPDATE_CRON` | `0 23 * * *` | Cron: daily at 11 PM UTC (6 PM ET) |
| `YFINANCE_CHUNK_SIZE` | `50` | Tickers per batch (rate limit mitigation) |
| `YFINANCE_DELAY_SEC` | `2.5` | Delay between batches (seconds) |
| `HISTORICAL_START` | `2020-01-01` | Start date for initial historical load |

## Services

| Service | Port | Description |
|---------|------|-------------|
| TimescaleDB | 5432 | PostgreSQL + TimescaleDB |
| Grafana | 3000 | Dashboards and visualization |
| stock-fetcher | - | Data fetcher and daily scheduler |

## Data Flow

1. **Initial load**: On first start, the fetcher downloads all S&P 500 tickers from Wikipedia and historical OHLCV data from Yahoo Finance (chunked with delays to avoid rate limits).
2. **Daily updates**: At the configured cron time (default 6 PM ET), the fetcher fetches the last 7 days of data and upserts into the database.
3. **Grafana**: Connects to TimescaleDB and queries `stock_prices` and `tickers` for dashboards.

## Limitations

- **yfinance** is community-maintained and depends on Yahoo Finance. For production-critical deployments, consider paid APIs (Polygon.io, Alpha Vantage).
- Rate limiting: Initial load may take 15–30 minutes. Do not reduce chunk delay excessively.

## License

MIT
