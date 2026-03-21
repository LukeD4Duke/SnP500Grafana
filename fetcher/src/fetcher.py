"""yfinance data fetching with rate limiting and retry logic."""

import io
import logging
import time
from datetime import datetime
from typing import List, Optional, Tuple

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


def fetch_historical_data(
    tickers: List[str],
    start: str,
    end: Optional[str] = None,
    config: Optional[FetcherConfig] = None,
) -> pd.DataFrame:
    """
    Fetch historical OHLCV data in chunks with rate limiting and retries.
    Returns DataFrame with columns: Symbol, Date, Open, High, Low, Close, Volume, Dividends, Stock Splits.
    """
    config = config or FetcherConfig(
        chunk_size=50,
        delay_seconds=2.5,
        historical_start="2020-01-01",
        update_cron="0 23 * * *",
        max_retries=5,
        retry_delay_seconds=60,
    )
    end = end or datetime.now().strftime("%Y-%m-%d")

    all_records = []
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
        for attempt in range(config.max_retries):
            try:
                df = _fetch_chunk(chunk, start, end)
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
                        "Fetched chunk %d/%d but received no rows for symbols [%s]",
                        chunk_num,
                        len(chunks),
                        chunk_symbols,
                    )
                break
            except Exception as e:
                is_rate_limit = _is_rate_limit_error(e) or (
                    YFRateLimitError is not None and isinstance(e, YFRateLimitError)
                )
                if is_rate_limit:
                    attempt_num = attempt + 1
                    total_attempts = config.max_retries
                    wait = config.retry_delay_seconds * (2 ** attempt)
                    if attempt_num >= total_attempts:
                        logger.error(
                            "Rate limited (chunk %d/%d) exhausted after %d/%d retries for symbols [%s]: %s",
                            chunk_num,
                            len(chunks),
                            attempt_num,
                            total_attempts,
                            chunk_symbols,
                            str(e),
                        )
                        raise RuntimeError(
                            "Rate limit retries exhausted for chunk "
                            f"{chunk_num}/{len(chunks)} with symbols [{chunk_symbols}]"
                        ) from e
                    logger.warning(
                        "Rate limited (chunk %d/%d), retry %d/%d in %.0fs: %s",
                        chunk_num,
                        len(chunks),
                        attempt_num,
                        total_attempts,
                        wait,
                        str(e),
                    )
                    time.sleep(wait)
                else:
                    logger.error("Chunk %d/%d failed: %s", chunk_num, len(chunks), str(e))
                    raise

        if i < len(chunks) - 1:
            time.sleep(config.delay_seconds)

    if not all_records:
        return pd.DataFrame()

    combined = pd.concat(all_records, ignore_index=True)
    return combined
