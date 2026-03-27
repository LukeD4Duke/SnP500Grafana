"""Entrypoint: wait for DB, init schema, run initial load, schedule daily updates."""

import logging
import sys
from typing import Iterable

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import get_database_config, get_fetcher_config, get_indicator_config
from .database import (
    delete_stock_indicators,
    get_stock_price_history,
    get_price_date_bounds,
    has_stock_price_data,
    init_schema,
    schema_exists,
    upsert_indicator_catalog,
    upsert_stock_indicators,
    upsert_stock_prices,
    upsert_tickers,
    wait_for_db,
)
from .fetcher import FetchResult, fetch_historical_data, fetch_sp500_tickers
from .indicators import calculate_indicators, indicators_available

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
    chunk_size = max(size, 1)
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def refresh_indicators(symbols: list[str], force_rebuild: bool = False) -> None:
    """Refresh persisted indicators from stored OHLCV history for the given symbols."""
    indicator_config = get_indicator_config()
    if not indicator_config.enabled:
        logger.info("Indicator refresh disabled via INDICATORS_ENABLED")
        return
    if not symbols:
        logger.info("Indicator refresh skipped because no symbols were provided")
        return
    if not indicators_available():
        logger.warning("Indicator refresh skipped because pandas-ta is not installed")
        return

    db_config = get_database_config()
    unique_symbols = sorted(set(symbols))
    logger.info(
        "Refreshing indicators for %d symbols%s",
        len(unique_symbols),
        " with full rebuild" if force_rebuild else "",
    )

    for batch_num, batch in enumerate(_chunked(unique_symbols, indicator_config.batch_size), start=1):
        logger.info(
            "Refreshing indicator batch %d containing %d symbols",
            batch_num,
            len(batch),
        )
        for symbol in batch:
            try:
                history_rows = get_stock_price_history(db_config, symbol)
                if not history_rows:
                    logger.warning("Skipping indicator refresh for %s because no OHLCV history was found", symbol)
                    continue

                price_df = pd.DataFrame(history_rows)
                price_df["symbol"] = symbol
                result = calculate_indicators(price_df)

                if force_rebuild:
                    delete_stock_indicators(db_config, symbol=symbol)
                else:
                    min_timestamp = price_df["timestamp"].min()
                    max_timestamp = price_df["timestamp"].max()
                    delete_stock_indicators(
                        db_config,
                        symbol=symbol,
                        start_timestamp=min_timestamp.isoformat() if hasattr(min_timestamp, "isoformat") else str(min_timestamp),
                        end_timestamp=max_timestamp.isoformat() if hasattr(max_timestamp, "isoformat") else str(max_timestamp),
                    )

                catalog_rows = [
                    (
                        entry.indicator_key,
                        entry.indicator,
                        entry.output_name,
                        entry.display_name,
                        entry.category,
                        entry.library,
                        entry.default_params,
                        entry.warmup_periods,
                        True,
                    )
                    for entry in result.catalog_entries
                ]
                value_rows = [
                    (
                        row.symbol,
                        row.timestamp,
                        row.indicator_key,
                        row.value,
                    )
                    for row in result.value_rows
                ]
                upsert_indicator_catalog(db_config, catalog_rows)
                upsert_stock_indicators(db_config, value_rows)
                logger.info(
                    "Refreshed %d indicator outputs for %s (%d rows)",
                    result.successful_outputs,
                    symbol,
                    len(value_rows),
                )
            except Exception as exc:
                logger.exception("Indicator refresh failed for %s: %s", symbol, exc)


def _log_fetch_summary(result: FetchResult, failed_symbol_log_limit: int) -> None:
    """Log a structured summary of a fetch run."""
    logger.info(
        "Fetch summary: requested=%d succeeded=%d recovered=%d failed=%d",
        len(result.requested_symbols),
        len(result.successful_symbols),
        len(result.recovered_symbols),
        len(result.failed_symbols),
    )
    if result.recovered_symbols:
        logger.info(
            "Recovered %d symbols after targeted retries",
            len(result.recovered_symbols),
        )
    if result.failed_symbols:
        visible = result.failed_symbols[: max(failed_symbol_log_limit, 0)]
        suffix = ""
        if len(result.failed_symbols) > len(visible):
            suffix = f" ... (+{len(result.failed_symbols) - len(visible)} more)"
        logger.warning(
            "Permanently failed symbols after targeted retries: %s%s",
            ", ".join(visible),
            suffix,
        )


def run_scheduled_sync() -> None:
    """Run the scheduled OHLCV sync followed by indicator refresh."""
    result = run_sync(full_historical=False)
    refresh_indicators(result.successful_symbols)


def run_sync(
    full_historical: bool = False,
    start_override: str | None = None,
    end_override: str | None = None,
    mode_label: str | None = None,
) -> FetchResult:
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

    end = end_override
    if start_override:
        start = start_override
    elif full_historical:
        start = fetcher_config.historical_start
    else:
        # Daily update: fetch last 7 days to handle weekends/holidays
        from datetime import datetime, timedelta

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=7)
        start = start_dt.strftime("%Y-%m-%d")

    if mode_label:
        if end:
            logger.info("%s from %s to %s", mode_label, start, end)
        else:
            logger.info("%s from %s", mode_label, start)
    elif full_historical:
        if end:
            logger.info("Running full historical sync from %s to %s", start, end)
        else:
            logger.info("Running full historical sync from %s", start)
    else:
        if end:
            logger.info("Running incremental sync from %s to %s", start, end)
        else:
            logger.info("Running incremental sync from %s", start)

    fetch_result = fetch_historical_data(
        symbols,
        start=start,
        end=end,
        config=fetcher_config,
    )
    _log_fetch_summary(fetch_result, fetcher_config.failed_symbol_log_limit)
    df = fetch_result.dataframe

    if df.empty:
        logger.warning("No data fetched")
        return fetch_result

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
    return fetch_result


def main() -> None:
    """Bootstrap: wait for DB, init schema, initial load, schedule updates."""
    db_config = get_database_config()
    fetcher_config = get_fetcher_config()
    indicator_config = get_indicator_config()

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
        startup_result = run_sync(full_historical=False)
        refresh_indicators(
            startup_result.successful_symbols,
            force_rebuild=indicator_config.rebuild_on_startup,
        )
        min_price_date, max_price_date = get_price_date_bounds(db_config)
        logger.info(
            "Current price data range after incremental sync: %s to %s",
            min_price_date or "n/a",
            max_price_date or "n/a",
        )

        backfill_start = fetcher_config.backfill_start
        if backfill_start and min_price_date and backfill_start < min_price_date:
            logger.info(
                "Backfill requested; filling older history from %s up to existing earliest date %s",
                backfill_start,
                min_price_date,
            )
            backfill_result = run_sync(
                full_historical=True,
                start_override=backfill_start,
                end_override=min_price_date,
                mode_label="Running startup backfill",
            )
            refresh_indicators(backfill_result.successful_symbols, force_rebuild=True)
            min_price_date, max_price_date = get_price_date_bounds(db_config)
            logger.info(
                "Price data range after backfill: %s to %s",
                min_price_date or "n/a",
                max_price_date or "n/a",
            )
        elif backfill_start:
            logger.info(
                "BACKFILL_START=%s already covered by existing earliest date %s; skipping startup backfill",
                backfill_start,
                min_price_date or "n/a",
            )
    else:
        logger.info("No stock price data found, running initial historical load (this may take 15-30 minutes)")
        full_result = run_sync(full_historical=True)
        refresh_indicators(full_result.successful_symbols, force_rebuild=True)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_scheduled_sync,
        CronTrigger.from_crontab(fetcher_config.update_cron),
        id="daily_update",
        replace_existing=True,
    )
    logger.info("Scheduled daily update with cron: %s", fetcher_config.update_cron)
    scheduler.start()


if __name__ == "__main__":
    main()
