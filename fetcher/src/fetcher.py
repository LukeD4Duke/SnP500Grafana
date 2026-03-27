"""yfinance data fetching with rate limiting and retry logic."""

import io
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Set, Tuple

import pandas as pd
import requests
import yfinance as yf

from .config import FetcherConfig

# YFRateLimitError added in yfinance 0.2.58; fallback for older versions
try:
    from yfinance.const import YFRateLimitError
except ImportError:
    YFRateLimitError = None


def _is_rate_limit_error(exc: BaseException) -> bool:
    """Detect rate limit from yfinance (works across versions)."""
    msg = str(exc).lower()
    return "rate limit" in msg or "too many requests" in msg or "429" in msg

logger = logging.getLogger(__name__)

WIKIPEDIA_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# User-Agent required by Wikipedia to avoid 403 Forbidden
WIKIPEDIA_HEADERS = {
    "User-Agent": "StockAnalysisGrafana/1.0 (https://github.com/stock-analysis; data collection)",
}


@dataclass(frozen=True)
class FetchResult:
    """Historical fetch result including partial-failure metadata."""

    dataframe: pd.DataFrame
    requested_symbols: list[str]
    successful_symbols: list[str]
    failed_symbols: list[str]
    recovered_symbols: list[str]

    @property
    def partial_failure(self) -> bool:
        return bool(self.failed_symbols)


def fetch_sp500_tickers() -> List[Tuple[str, str, str, str]]:
    """
    Fetch S&P 500 ticker list from Wikipedia.
    Returns list of (symbol, name, sector, industry).
    """
    logger.info("Fetching S&P 500 tickers from Wikipedia")
    resp = requests.get(WIKIPEDIA_SP500_URL, headers=WIKIPEDIA_HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.BytesIO(resp.content))
    df = tables[0]
    # Standard Wikipedia table columns: Symbol, Security, GICS Sector, GICS Sub-Industry
    # Some pages use different column names; handle variations
    symbol_col = "Symbol" if "Symbol" in df.columns else df.columns[0]
    name_col = "Security" if "Security" in df.columns else df.columns[1]
    sector_col = "GICS Sector" if "GICS Sector" in df.columns else "Sector"
    if sector_col not in df.columns:
        sector_col = df.columns[2] if len(df.columns) > 2 else "Sector"
    industry_col = "GICS Sub-Industry" if "GICS Sub-Industry" in df.columns else "Industry"
    if industry_col not in df.columns:
        industry_col = df.columns[3] if len(df.columns) > 3 else "Industry"

    tickers = []
    for _, row in df.iterrows():
        symbol = str(row[symbol_col]).strip()
        if "." in symbol:
            # Yahoo uses different ticker format (e.g., BRK.B -> BRK-B)
            symbol = symbol.replace(".", "-")
        name = str(row.get(name_col, ""))[:255] if pd.notna(row.get(name_col)) else ""
        sector = str(row.get(sector_col, ""))[:100] if pd.notna(row.get(sector_col)) else ""
        industry = str(row.get(industry_col, ""))[:150] if pd.notna(row.get(industry_col)) else ""
        tickers.append((symbol, name, sector, industry))

    logger.info("Fetched %d tickers from Wikipedia", len(tickers))
    return tickers


def _get_value(row: pd.Series, col: str, ticker: str) -> float:
    """Extract value from row. Supports MultiIndex (col, ticker) or flat columns."""
    try:
        val = row.get((col, ticker), row.get(col, float("nan")))
        return float(val) if pd.notna(val) else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _fetch_chunk(
    tickers: List[str],
    start: str,
    end: str,
) -> pd.DataFrame:
    """Fetch historical data for a chunk of tickers."""
    try:
        data = yf.download(
            tickers,
            start=start,
            end=end,
            group_by="column",
            auto_adjust=False,
            prepost=False,
            threads=False,
            progress=False,
        )
    except Exception as e:
        raise RuntimeError(f"yfinance download failed: {e}") from e

    if data.empty:
        return pd.DataFrame()

    # yfinance returns MultiIndex columns: (Open, AAPL), (Close, AAPL), etc.
    # or flat columns for single ticker
    records = []

    for dt, row in data.iterrows():
        for ticker in tickers:
            close = _get_value(row, "Close", ticker)
            if pd.notna(close):
                open_ = _get_value(row, "Open", ticker)
                high = _get_value(row, "High", ticker)
                low = _get_value(row, "Low", ticker)
                vol = _get_value(row, "Volume", ticker)
                volume = int(vol) if pd.notna(vol) else 0
                dividends = _get_value(row, "Dividends", ticker) or 0
                splits = _get_value(row, "Stock Splits", ticker) or 0
                records.append({
                    "Symbol": ticker,
                    "Date": dt,
                    "Open": open_,
                    "High": high,
                    "Low": low,
                    "Close": close,
                    "Volume": volume,
                    "Dividends": dividends,
                    "Stock Splits": splits,
                })

    return pd.DataFrame(records) if records else pd.DataFrame()


def _extract_successful_symbols(df: pd.DataFrame) -> set[str]:
    """Return symbols that produced at least one valid OHLCV row."""
    if df.empty or "Symbol" not in df.columns:
        return set()
    return {str(symbol) for symbol in df["Symbol"].dropna().astype(str).unique().tolist()}


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine result frames into a deduplicated DataFrame."""
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True)
    return combined.drop_duplicates(subset=["Symbol", "Date"], keep="last")


def _fetch_chunk_with_retries(
    tickers: list[str],
    start: str,
    end: str,
    config: FetcherConfig,
    *,
    context_label: str,
) -> pd.DataFrame:
    """Fetch a symbol set using chunk-level retry rules."""
    for attempt in range(config.max_retries):
        try:
            return _fetch_chunk(tickers, start, end)
        except Exception as exc:
            is_rate_limit = _is_rate_limit_error(exc) or (
                YFRateLimitError is not None and isinstance(exc, YFRateLimitError)
            )
            attempt_num = attempt + 1
            total_attempts = config.max_retries
            if is_rate_limit and attempt_num < total_attempts:
                wait = config.retry_delay_seconds * (2 ** attempt)
                logger.warning(
                    "%s rate limited, retry %d/%d in %.0fs for symbols [%s]: %s",
                    context_label,
                    attempt_num,
                    total_attempts,
                    wait,
                    ", ".join(tickers),
                    str(exc),
                )
                time.sleep(wait)
                continue
            if is_rate_limit:
                logger.error(
                    "%s exhausted chunk retries after %d/%d attempts for symbols [%s]: %s",
                    context_label,
                    attempt_num,
                    total_attempts,
                    ", ".join(tickers),
                    str(exc),
                )
            else:
                logger.error(
                    "%s hard failure for symbols [%s]: %s",
                    context_label,
                    ", ".join(tickers),
                    str(exc),
                )
            return pd.DataFrame()
    return pd.DataFrame()


def _recover_symbol_batch(
    tickers: list[str],
    start: str,
    end: str,
    config: FetcherConfig,
    *,
    context_label: str,
) -> tuple[pd.DataFrame, set[str]]:
    """Retry a reduced symbol batch with symbol-level retry logic."""
    total_attempts = max(config.symbol_retry_count + 1, 1)
    for attempt in range(total_attempts):
        try:
            df = _fetch_chunk(tickers, start, end)
        except Exception as exc:
            attempt_num = attempt + 1
            if attempt_num >= total_attempts:
                logger.warning(
                    "%s exhausted symbol retries for [%s]: %s",
                    context_label,
                    ", ".join(tickers),
                    str(exc),
                )
                return pd.DataFrame(), set()
            wait = config.retry_delay_seconds * (2 ** attempt)
            logger.warning(
                "%s retry %d/%d in %.0fs for symbols [%s]: %s",
                context_label,
                attempt_num,
                total_attempts,
                wait,
                ", ".join(tickers),
                str(exc),
            )
            time.sleep(wait)
            continue

        successful_symbols = _extract_successful_symbols(df)
        if successful_symbols:
            return df, successful_symbols

        attempt_num = attempt + 1
        if attempt_num >= total_attempts:
            logger.warning("%s returned no rows for symbols [%s]", context_label, ", ".join(tickers))
            return pd.DataFrame(), set()
        wait = config.retry_delay_seconds * (2 ** attempt)
        logger.warning(
            "%s returned no rows, retry %d/%d in %.0fs for symbols [%s]",
            context_label,
            attempt_num,
            total_attempts,
            wait,
            ", ".join(tickers),
        )
        time.sleep(wait)

    return pd.DataFrame(), set()


def _recover_missing_symbols(
    missing_symbols: set[str],
    start: str,
    end: str,
    config: FetcherConfig,
    *,
    chunk_num: int,
    total_chunks: int,
) -> tuple[pd.DataFrame, set[str], set[str]]:
    """Recover missing symbols using smaller batches and single-symbol retries."""
    if not missing_symbols:
        return pd.DataFrame(), set(), set()

    recovered_frames: list[pd.DataFrame] = []
    recovered_symbols: set[str] = set()
    unresolved_symbols: set[str] = set()
    symbols = sorted(missing_symbols)
    batch_size = max(config.recovery_chunk_size, 1)

    logger.warning(
        "Chunk %d/%d partial success: retrying %d missing symbols in recovery batches of %d",
        chunk_num,
        total_chunks,
        len(symbols),
        batch_size,
    )

    for batch_index in range(0, len(symbols), batch_size):
        batch = symbols[batch_index : batch_index + batch_size]
        batch_label = f"Recovery batch {chunk_num}/{total_chunks}"
        batch_df, batch_success = _recover_symbol_batch(
            batch,
            start,
            end,
            config,
            context_label=batch_label,
        )
        if not batch_df.empty:
            recovered_frames.append(batch_df)
        recovered_symbols.update(batch_success)

        still_missing = [symbol for symbol in batch if symbol not in batch_success]
        for symbol in still_missing:
            symbol_df, symbol_success = _recover_symbol_batch(
                [symbol],
                start,
                end,
                config,
                context_label=f"Single-symbol recovery {chunk_num}/{total_chunks}",
            )
            if not symbol_df.empty:
                recovered_frames.append(symbol_df)
            if symbol_success:
                recovered_symbols.update(symbol_success)
            else:
                unresolved_symbols.add(symbol)

    return _concat_frames(recovered_frames), recovered_symbols, unresolved_symbols


def fetch_historical_data(
    tickers: List[str],
    start: str,
    end: Optional[str] = None,
    config: Optional[FetcherConfig] = None,
) -> FetchResult:
    """
    Fetch historical OHLCV data in chunks with rate limiting and retries.
    Returns rows plus metadata about recovered and failed symbols.
    """
    config = config or FetcherConfig(
        chunk_size=50,
        symbol_retry_count=2,
        recovery_chunk_size=5,
        failed_symbol_log_limit=20,
        delay_seconds=2.5,
        historical_start="2020-01-01",
        backfill_start=None,
        update_cron="0 23 * * *",
        max_retries=5,
        retry_delay_seconds=60,
    )
    end = end or datetime.now().strftime("%Y-%m-%d")

    all_records: list[pd.DataFrame] = []
    successful_symbols: set[str] = set()
    failed_symbols: set[str] = set()
    recovered_symbols: set[str] = set()
    chunks = [
        tickers[i : i + config.chunk_size]
        for i in range(0, len(tickers), config.chunk_size)
    ]

    for i, chunk in enumerate(chunks):
        chunk_num = i + 1
        chunk_symbols = ", ".join(chunk)
        logger.info(
            "Fetching chunk %d/%d (%d tickers) from %s to %s",
            chunk_num,
            len(chunks),
            len(chunk),
            start,
            end,
        )
        df = _fetch_chunk_with_retries(
            chunk,
            start,
            end,
            config,
            context_label=f"Chunk {chunk_num}/{len(chunks)}",
        )
        chunk_success = _extract_successful_symbols(df)
        missing_symbols = set(chunk) - chunk_success

        if not df.empty:
            min_date = df["Date"].min()
            max_date = df["Date"].max()
            logger.info(
                "Fetched chunk %d/%d with %d rows spanning %s to %s",
                chunk_num,
                len(chunks),
                len(df),
                min_date.date().isoformat() if hasattr(min_date, "date") else str(min_date),
                max_date.date().isoformat() if hasattr(max_date, "date") else str(max_date),
            )
            all_records.append(df)
        else:
            logger.warning(
                "Chunk %d/%d returned no rows for symbols [%s]",
                chunk_num,
                len(chunks),
                chunk_symbols,
            )

        if missing_symbols and chunk_success:
            logger.warning(
                "Chunk %d/%d partial success: %d/%d symbols returned; missing [%s]",
                chunk_num,
                len(chunks),
                len(chunk_success),
                len(chunk),
                ", ".join(sorted(missing_symbols)),
            )

        recovered_df, recovered_chunk_symbols, unresolved_symbols = _recover_missing_symbols(
            missing_symbols,
            start,
            end,
            config,
            chunk_num=chunk_num,
            total_chunks=len(chunks),
        )
        if not recovered_df.empty:
            all_records.append(recovered_df)
        recovered_symbols.update(recovered_chunk_symbols)
        successful_symbols.update(chunk_success)
        successful_symbols.update(recovered_chunk_symbols)
        failed_symbols.update(unresolved_symbols)

        if i < len(chunks) - 1:
            time.sleep(config.delay_seconds)

    failed_symbols.difference_update(successful_symbols)
    combined = _concat_frames(all_records)
    return FetchResult(
        dataframe=combined,
        requested_symbols=list(tickers),
        successful_symbols=sorted(successful_symbols),
        failed_symbols=sorted(failed_symbols),
        recovered_symbols=sorted(recovered_symbols),
    )
