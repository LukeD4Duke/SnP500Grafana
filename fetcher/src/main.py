"""Entrypoint: wait for DB, init schema, run initial load, schedule daily updates."""

import logging
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Iterable

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger

from .analytics import refresh_analytics_snapshots
from .config import (
    get_analytics_config,
    get_database_config,
    get_fetcher_config,
    get_indicator_config,
    get_reporting_config,
)
from .database import (
    delete_stock_indicators,
    get_all_symbols,
    get_max_enabled_indicator_warmup_period,
    get_recent_stock_price_history,
    get_stock_price_history,
    get_price_date_bounds,
    has_stock_split_in_window,
    has_stock_price_data,
    init_schema,
    normalize_invalid_stock_splits,
    schema_exists,
    upsert_indicator_catalog,
    upsert_stock_indicators,
    upsert_stock_prices,
    upsert_tickers,
    wait_for_db,
)
from .fetcher import (
    FetchResult,
    fetch_historical_data,
    fetch_sp500_tickers,
    normalize_corporate_action_value,
)
from .indicators import calculate_indicators, indicators_available
from .reporting import generate_report_artifacts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
INCREMENTAL_LOOKBACK_BUFFER_ROWS = 50
STARTUP_CATCHUP_JOB_ID = "startup_post_sync_catchup"
PIPELINE_LOCK = Lock()


def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
    chunk_size = max(size, 1)
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def refresh_indicators(
    symbols: list[str],
    price_frame: pd.DataFrame | None = None,
    force_rebuild: bool = False,
) -> None:
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
    symbol_windows = _build_symbol_windows(price_frame)
    incremental_lookback_rows = None
    if not force_rebuild:
        max_warmup_period = get_max_enabled_indicator_warmup_period(db_config)
        incremental_lookback_rows = max(
            indicator_config.incremental_lookback_rows,
            max_warmup_period + INCREMENTAL_LOOKBACK_BUFFER_ROWS,
        )
        logger.info(
            "Refreshing indicators for %d symbols incrementally using %d lookback rows "
            "(floor=%d, max_warmup=%d, buffer=%d)",
            len(unique_symbols),
            incremental_lookback_rows,
            indicator_config.incremental_lookback_rows,
            max_warmup_period,
            INCREMENTAL_LOOKBACK_BUFFER_ROWS,
        )
    else:
        logger.info("Refreshing indicators for %d symbols with full rebuild", len(unique_symbols))

    for batch_num, batch in enumerate(_chunked(unique_symbols, indicator_config.batch_size), start=1):
        logger.info(
            "Refreshing indicator batch %d containing %d symbols",
            batch_num,
            len(batch),
        )
        for symbol in batch:
            try:
                symbol_window = symbol_windows.get(symbol)
                split_detected = False
                if not force_rebuild and symbol_window is not None:
                    window_start, window_end = symbol_window
                    split_detected = has_stock_split_in_window(
                        db_config,
                        symbol,
                        _format_timestamp(window_start),
                        _format_timestamp(window_end),
                    )
                    if split_detected:
                        logger.info(
                            "Detected stock split for %s in synced window %s to %s; forcing full rebuild",
                            symbol,
                            _format_timestamp(window_start),
                            _format_timestamp(window_end),
                        )

                rebuild_full_history = force_rebuild or split_detected or symbol_window is None
                if rebuild_full_history:
                    history_rows = get_stock_price_history(db_config, symbol)
                    if not history_rows:
                        logger.warning("Skipping indicator refresh for %s because no OHLCV history was found", symbol)
                        continue

                    price_df = pd.DataFrame(history_rows)
                    price_df["symbol"] = symbol
                    result = calculate_indicators(price_df)
                    delete_stock_indicators(db_config, symbol=symbol)
                    logger.info(
                        "Rebuilt indicators for %s using full history (%d rows)",
                        symbol,
                        len(price_df),
                    )
                else:
                    assert incremental_lookback_rows is not None
                    history_rows = get_recent_stock_price_history(
                        db_config,
                        symbol,
                        incremental_lookback_rows,
                    )
                    if not history_rows:
                        logger.warning("Skipping indicator refresh for %s because no OHLCV history was found", symbol)
                        continue

                    price_df = pd.DataFrame(history_rows)
                    price_df["symbol"] = symbol
                    result = calculate_indicators(price_df)
                    recompute_start = price_df["timestamp"].min()
                    delete_stock_indicators(
                        db_config,
                        symbol=symbol,
                        start_timestamp=_format_timestamp(recompute_start),
                    )
                    logger.info(
                        "Refreshed indicators for %s incrementally using %d rows from %s",
                        symbol,
                        len(price_df),
                        _format_timestamp(recompute_start),
                    )

                catalog_rows = [
                    (
                        entry.indicator_key,
                        entry.indicator,
                        entry.output_name,
                        entry.display_name,
                        entry.category,
                        entry.purpose_description,
                        entry.value_interpretation,
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
    with PIPELINE_LOCK:
        logger.info("Starting scheduled sync pipeline")
        result = run_sync(full_historical=False)
        run_post_sync_tasks(result.changed_symbols, price_frame=result.dataframe)


def refresh_analytics() -> None:
    """Refresh additive analytics snapshots when enabled."""
    analytics_config = get_analytics_config()
    if not analytics_config.enabled:
        logger.info("Analytics refresh disabled via ANALYTICS_ENABLED")
        return

    db_config = get_database_config()
    analytics_result = refresh_analytics_snapshots(
        db_config,
        analytics_config.timeframes,
        batch_size=analytics_config.backfill_batch_dates,
    )
    if analytics_result is None:
        return
    logger.info(
        "Analytics snapshot refresh completed for %s across %s (%d signals, %d ranks, %d breadth rows)",
        analytics_result.snapshot_date,
        ", ".join(analytics_result.timeframes),
        analytics_result.signal_rows,
        analytics_result.rank_rows,
        analytics_result.breadth_rows,
    )


def generate_configured_reports(report_kind: str | None = None) -> None:
    """Generate deterministic report artifacts when enabled."""
    reporting_config = get_reporting_config()
    if not reporting_config.enabled:
        logger.info("Report generation disabled via REPORTS_ENABLED")
        return

    db_config = get_database_config()
    if report_kind is None:
        report_kinds = ("weekly", "monthly")
    else:
        report_kinds = (report_kind,)

    generated_count = 0
    for kind in report_kinds:
        artifact = generate_report_artifacts(
            db_config,
            reporting_config.output_dir,
            kind,
        )
        if artifact is None:
            logger.warning("Skipped %s report generation because no matching analytics snapshot was available", kind)
            continue
        generated_count += 1
        logger.info(
            "Generated %s report for %s at %s and %s (%d stored rows)",
            artifact.report_kind,
            artifact.snapshot_date,
            artifact.markdown_path,
            artifact.html_path,
            artifact.row_count,
        )
    if generated_count:
        logger.info("Generated %d report artifact sets", generated_count)


def run_post_sync_tasks(
    symbols: list[str],
    price_frame: pd.DataFrame | None = None,
    force_rebuild: bool = False,
) -> None:
    """Refresh indicators, then analytics, then reports in order."""
    if not symbols:
        logger.info("Skipping indicator refresh because no persisted OHLCV changes were detected")
    else:
        refresh_indicators(
            symbols,
            price_frame=price_frame,
            force_rebuild=force_rebuild,
        )
    refresh_analytics()
    generate_configured_reports()


def run_startup_post_sync_catchup(
    symbols: list[str],
    price_frame: pd.DataFrame | None = None,
    force_rebuild: bool = False,
) -> None:
    """Run populated-DB startup catch-up work outside the blocking startup path."""
    with PIPELINE_LOCK:
        logger.info(
            "Running startup post-sync catch-up for %d symbols",
            len(set(symbols)),
        )
        run_post_sync_tasks(
            symbols,
            price_frame=price_frame,
            force_rebuild=force_rebuild,
        )


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
        return replace(fetch_result, changed_symbols=[], upserted_row_count=0)

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
                normalize_corporate_action_value(r.get("Dividends", 0)),
                normalize_corporate_action_value(r.get("Stock Splits", 0)),
            )
        )

    upsert_result = upsert_stock_prices(db_config, rows)
    logger.info(
        "Fetched %d symbols; %d symbols had persisted OHLCV changes",
        len(fetch_result.successful_symbols),
        len(upsert_result.changed_symbols),
    )
    logger.info("Upserted %d price records", upsert_result.affected_row_count)
    if not upsert_result.changed_symbols:
        logger.info("No persisted OHLCV changes were detected during this sync")
    else:
        logger.info(
            "Refreshing downstream work will use %d changed symbols",
            len(upsert_result.changed_symbols),
        )
    return replace(
        fetch_result,
        changed_symbols=upsert_result.changed_symbols,
        upserted_row_count=upsert_result.affected_row_count,
    )


def main() -> None:
    """Bootstrap: wait for DB, init schema, initial load, schedule updates."""
    db_config = get_database_config()
    fetcher_config = get_fetcher_config()
    indicator_config = get_indicator_config()
    reporting_config = get_reporting_config()
    scheduler = BlockingScheduler()

    if not wait_for_db(db_config):
        logger.error("Database not available after max retries. Exiting.")
        sys.exit(1)

    if not schema_exists(db_config):
        logger.info("Initializing database schema")
    else:
        logger.info("Schema already exists; applying idempotent schema updates")
    init_schema(db_config)
    normalized_split_rows = normalize_invalid_stock_splits(db_config)
    if normalized_split_rows:
        logger.info("Normalized %d persisted stock split rows from NaN to 0", normalized_split_rows)

    startup_catchup_kwargs: dict[str, object] | None = None
    if has_stock_price_data(db_config):
        logger.info("Existing stock price data found, running startup incremental sync")
        startup_result = run_sync(full_historical=False)
        if fetcher_config.startup_post_sync_mode == "blocking":
            logger.info("Running populated-DB startup post-sync tasks inline (STARTUP_POST_SYNC_MODE=blocking)")
            run_post_sync_tasks(
                startup_result.changed_symbols,
                price_frame=startup_result.dataframe,
                force_rebuild=indicator_config.rebuild_on_startup,
            )
        else:
            logger.info(
                "Deferring populated-DB startup post-sync tasks to background catch-up "
                "(STARTUP_POST_SYNC_MODE=background)"
            )
            startup_catchup_kwargs = {
                "symbols": startup_result.changed_symbols,
                "price_frame": startup_result.dataframe,
                "force_rebuild": indicator_config.rebuild_on_startup,
            }
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
            run_post_sync_tasks(
                backfill_result.changed_symbols,
                price_frame=backfill_result.dataframe,
                force_rebuild=True,
            )
            startup_catchup_kwargs = None
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
        run_post_sync_tasks(
            full_result.changed_symbols,
            price_frame=full_result.dataframe,
            force_rebuild=True,
        )

    scheduler.add_job(
        run_scheduled_sync,
        CronTrigger.from_crontab(fetcher_config.update_cron),
        id="daily_update",
        max_instances=1,
        replace_existing=True,
    )
    logger.info("Scheduled daily update with cron: %s", fetcher_config.update_cron)
    if reporting_config.enabled:
        scheduler.add_job(
            lambda: generate_configured_reports("weekly"),
            CronTrigger.from_crontab(reporting_config.weekly_cron),
            id="weekly_report",
            replace_existing=True,
        )
        logger.info("Scheduled weekly report with cron: %s", reporting_config.weekly_cron)
        scheduler.add_job(
            lambda: generate_configured_reports("monthly"),
            CronTrigger.from_crontab(reporting_config.monthly_cron),
            id="monthly_report",
            replace_existing=True,
        )
        logger.info("Scheduled monthly report with cron: %s", reporting_config.monthly_cron)

    if startup_catchup_kwargs is not None:
        run_date = datetime.now(timezone.utc) + timedelta(seconds=1)
        scheduler.add_job(
            run_startup_post_sync_catchup,
            DateTrigger(run_date=run_date),
            kwargs=startup_catchup_kwargs,
            id=STARTUP_CATCHUP_JOB_ID,
            max_instances=1,
            replace_existing=True,
        )
        logger.info(
            "Scheduled startup post-sync catch-up for %s",
            run_date.isoformat(),
        )
    scheduler.start()


def _build_symbol_windows(price_frame: pd.DataFrame | None) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    if price_frame is None or price_frame.empty:
        return {}
    if "Symbol" not in price_frame.columns or "Date" not in price_frame.columns:
        return {}

    windows: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    for symbol, group in price_frame.groupby("Symbol", sort=True):
        timestamps = pd.to_datetime(group["Date"], utc=True, errors="coerce").dropna()
        if timestamps.empty:
            continue
        windows[str(symbol)] = (timestamps.min(), timestamps.max())
    return windows


def _format_timestamp(value: object) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat()


if __name__ == "__main__":
    main()
