# Agents Guide

## Project Summary

This repository contains a small containerized S&P 500 market-data stack:

- `timescaledb`: stores ticker metadata and OHLCV time-series data
- `stock-fetcher`: Python service that initializes the schema, backfills historical data, and schedules daily updates
- `grafana`: custom Grafana image with provisioned datasource and committed dashboard JSON

The main workflow is: fetch ticker metadata from Wikipedia, fetch price history from Yahoo Finance through `yfinance`, upsert into TimescaleDB, and visualize through Grafana dashboards.

## Repository Map

- `fetcher/src/`
  - `main.py`: startup flow, initial sync, scheduler setup
  - `fetcher.py`: remote data fetching
  - `database.py`: schema setup and upsert/query helpers
  - `config.py`: environment-driven configuration
- `scripts/`
  - `init-db.sql`: canonical DB schema
  - `generate_dashboards.py`: regenerates committed Grafana dashboard JSON
- `grafana/provisioning/`: datasource and dashboard provisioning config
- `grafana/dashboards/`: generated dashboard JSON files that are baked into the image
- `docker-compose.yml`: local deployment entry point
- `README.md`: user-facing setup and deployment notes

## Agent Priorities

1. Preserve the current deployment model: `docker compose up -d` from the repo root should remain the standard path.
2. Keep dashboard JSON checked into the repo. If dashboard generation logic changes, regenerate the JSON files in the same change.
3. Treat `scripts/init-db.sql` as the source of truth for schema changes. Keep `fetcher/src/database.py` fallback SQL aligned with it when relevant.
4. Avoid changing environment variable names unless the README, compose file, and Python config are all updated together.

## Safe Working Conventions

- Work from the repo root unless a command clearly belongs in a subdirectory.
- Prefer small, reviewable changes. This project is simple and tightly coupled.
- Do not commit secrets from `.env`.
- Do not hardcode host-specific values beyond the existing local-server assumptions already present in `docker-compose.yml`.
- When changing fetch logic, keep startup behavior intact:
  - empty DB => full historical sync
  - populated DB => incremental sync
  - scheduled job => incremental sync

## Common Commands

From the repository root:

```powershell
docker compose up -d
docker compose logs -f stock-fetcher
docker compose logs -f grafana
docker compose config
```

For dashboard regeneration:

```powershell
pip install -r requirements-dev.txt
python scripts/generate_dashboards.py
```

If you need Python dependencies for the fetcher locally:

```powershell
pip install -r fetcher/requirements.txt
```

## Validation Expectations

There is no formal automated test suite in this repo yet. After making changes, validate with the smallest relevant checks:

- Python-only changes:
  - run `python -m compileall fetcher/src scripts`
- Dashboard generation changes:
  - run `python scripts/generate_dashboards.py`
  - confirm the expected JSON files under `grafana/dashboards/` changed
- Compose or container changes:
  - run `docker compose config`
- End-to-end behavior changes:
  - prefer validating with `docker compose up -d` and targeted logs if Docker is available

## Change-Specific Notes

### Fetcher

- The fetcher depends on external sources and may hit rate limits or transient failures.
- Keep retry and batching behavior conservative unless there is a clear reason to change it.
- Wikipedia ticker refresh failure is intentionally allowed to fall back to cached DB symbols.

### Database

- The schema assumes TimescaleDB and creates a hypertable on `stock_prices`.
- Be careful with migrations: this repo currently relies on initialization SQL, not a formal migration tool.

### Grafana

- Dashboard JSON is generated code. Edit `scripts/generate_dashboards.py` first, then regenerate JSON.
- Provisioning files under `grafana/provisioning/` must stay consistent with the baked dashboard paths and datasource UID.

## When Updating Documentation

Update `README.md` in the same change when you alter:

- required environment variables
- startup commands
- dashboard generation workflow
- service names, ports, or deployment assumptions

## Good First Inspection Points

If you need to understand current behavior quickly, read files in this order:

1. `README.md`
2. `docker-compose.yml`
3. `fetcher/src/main.py`
4. `fetcher/src/database.py`
5. `scripts/generate_dashboards.py`
