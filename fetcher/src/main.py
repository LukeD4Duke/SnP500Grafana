"""Entrypoint: wait for DB, init schema, run initial load, schedule daily updates."""

import logging
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import get_database_config, get_fetcher_config
from .database import (
    get_all_symbols,
    has_stock_price_data,
    init_schema,
    schema_exists,
    upsert_stock_prices,
    upsert_tickers,
    wait_for_db,
)
from .fetcher import fetch_historical_data, fetch_sp500_tickers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_sync(full_historical: bool = False) -> None:
    """Fetch S&P 500 data and upsert into database."""
    db_config = get_database_config()
    fetcher_config = get_fetcher_config()

    try:
        tickers_meta = fetch_sp500_tickers()
    except Exception as exc:
        symbols = get_all_symbols(db_config)
        if not symbols:
            raise
        tickers_meta = []
        logger.warning(
            "Failed to refresh tickers from Wikipedia, using %d cached symbols: %s",
            len(symbols),
            exc,
        )
    else:
        upsert_tickers(db_config, tickers_meta)
        symbols = [t[0] for t in tickers_meta]

    if full_historical:
        start = fetcher_config.historical_start
        logger.info("Running full historical sync from %s", start)
    else:
        # Daily update: fetch last 7 days to handle weekends/holidays
        from datetime import datetime, timedelta

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=7)
        start = start_dt.strftime("%Y-%m-%d")
        logger.info("Running incremental sync from %s", start)

    df = fetch_historical_data(
        symbols,
        start=start,
        config=fetcher_config,
    )

    if df.empty:
        logger.warning("No data fetched")
        return

    rows = []
    for _, r in df.iterrows():
        rows.append(
            (
                str(r["Symbol"]),
                r["Date"].isoformat() if hasattr(r["Date"], "isoformat") else str(r["Date"]),
                float(r["Open"]) if r.get("Open") is not None and str(r["Open"]) != "nan" else None,
                float(r["High"]) if r.get("High") is not None and str(r["High"]) != "nan" else None,
                float(r["Low"]) if r.get("Low") is not None and str(r["Low"]) != "nan" else None,
                float(r["Close"]),
                int(r.get("Volume", 0) or 0),
                float(r.get("Dividends", 0) or 0),
                float(r.get("Stock Splits", 0) or 0),
            )
        )

    count = upsert_stock_prices(db_config, rows)
    logger.info("Upserted %d price records", count)


def main() -> None:
    """Bootstrap: wait for DB, init schema, initial load, schedule updates."""
    db_config = get_database_config()
    fetcher_config = get_fetcher_config()

    if not wait_for_db(db_config):
        logger.error("Database not available after max retries. Exiting.")
        sys.exit(1)

    if not schema_exists(db_config):
        logger.info("Initializing database schema")
        init_schema(db_config)
    else:
        logger.info("Schema already exists")

    if has_stock_price_data(db_config):
        logger.info("Existing stock price data found, running startup incremental sync")
        run_sync(full_historical=False)
    else:
        logger.info("No stock price data found, running initial historical load (this may take 15-30 minutes)")
        run_sync(full_historical=True)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_sync,
        CronTrigger.from_crontab(fetcher_config.update_cron),
        args=[False],
        id="daily_update",
        replace_existing=True,
    )
    logger.info("Scheduled daily update with cron: %s", fetcher_config.update_cron)
    scheduler.start()


if __name__ == "__main__":
    main()
