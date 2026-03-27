"""Technical indicator discovery and calculation using pandas-ta."""

from __future__ import annotations

import contextlib
import io
import json
import logging
import math
from dataclasses import dataclass
from typing import Iterable, List

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import pandas_ta as pta
except ImportError:  # pragma: no cover - handled at runtime
    pta = None

try:
    import talib  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime
    talib = None

EXCLUDED_INDICATORS = {
    "constants",
    "cross",
    "cross_value",
    "long_run",
    "short_run",
    "tsignals",
    "xsignals",
}


@dataclass(frozen=True)
class IndicatorCatalogEntry:
    """Metadata for a persisted indicator output series."""

    indicator_key: str
    indicator: str
    output_name: str
    display_name: str
    category: str
    library: str
    default_params: str
    warmup_periods: int


@dataclass(frozen=True)
class IndicatorValueRow:
    """Persistable normalized indicator row."""

    symbol: str
    timestamp: str
    indicator_key: str
    value: float


@dataclass(frozen=True)
class IndicatorComputationResult:
    """Indicator rows and metadata generated for a price frame."""

    catalog_entries: List[IndicatorCatalogEntry]
    value_rows: List[IndicatorValueRow]
    attempted_indicators: int
    successful_outputs: int


def indicators_available() -> bool:
    """Return whether pandas-ta is importable."""
    return pta is not None


def using_talib() -> bool:
    """Return whether native TA-Lib is importable."""
    return talib is not None


def discover_indicator_names(price_df: pd.DataFrame) -> list[str]:
    """Return supported indicator names from pandas-ta on this runtime."""
    if pta is None:
        return []
    names: list[str] = []
    try:
        names = list(price_df.ta.indicators(as_list=True))
    except Exception as exc:
        logger.warning("Unable to enumerate pandas-ta indicators: %s", exc)
        return []
    return [name for name in names if name and name not in EXCLUDED_INDICATORS]


def calculate_indicators(price_df: pd.DataFrame) -> IndicatorComputationResult:
    """Calculate all default-parameter indicators for the provided OHLCV frame."""
    if pta is None:
        raise RuntimeError("pandas-ta is not installed")
    if price_df.empty:
        return IndicatorComputationResult([], [], 0, 0)

    normalized = _normalize_price_frame(price_df)
    symbol = str(normalized["symbol"].iloc[0])
    working = normalized[["open", "high", "low", "close", "volume"]].copy()

    catalog_entries: list[IndicatorCatalogEntry] = []
    value_rows: list[IndicatorValueRow] = []
    attempted = 0
    successful = 0

    for indicator_name in discover_indicator_names(working):
        attempted += 1
        try:
            indicator_frame = working.copy()
            existing_columns = set(indicator_frame.columns)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                output = getattr(indicator_frame.ta, indicator_name)(append=True, verbose=False)
        except Exception as exc:
            logger.debug("Skipping indicator %s for %s: %s", indicator_name, symbol, exc)
            continue

        series_outputs = _coerce_outputs(output, indicator_frame, existing_columns)
        if not series_outputs:
            continue

        for output_name, series in series_outputs:
            non_null = series.dropna()
            if non_null.empty:
                continue

            indicator_key = _build_indicator_key(indicator_name, output_name)
            warmup_periods = _infer_warmup_periods(series)
            catalog_entries.append(
                IndicatorCatalogEntry(
                    indicator_key=indicator_key,
                    indicator=indicator_name,
                    output_name=output_name,
                    display_name=_build_display_name(indicator_name, output_name),
                    category=_resolve_category(indicator_name),
                    library="pandas-ta+talib" if talib is not None else "pandas-ta",
                    default_params=json.dumps({}, sort_keys=True),
                    warmup_periods=warmup_periods,
                )
            )
            for timestamp, value in non_null.items():
                numeric_value = float(value)
                if not math.isfinite(numeric_value):
                    continue
                value_rows.append(
                    IndicatorValueRow(
                        symbol=symbol,
                        timestamp=_to_timestamp_string(timestamp),
                        indicator_key=indicator_key,
                        value=numeric_value,
                    )
                )
            successful += 1

    return IndicatorComputationResult(
        catalog_entries=_dedupe_catalog(catalog_entries),
        value_rows=value_rows,
        attempted_indicators=attempted,
        successful_outputs=successful,
    )


def compute_indicators_for_symbols(price_df: pd.DataFrame) -> IndicatorComputationResult:
    """Compute indicators for a multi-symbol OHLCV frame."""
    if price_df.empty:
        return IndicatorComputationResult([], [], 0, 0)

    catalog: list[IndicatorCatalogEntry] = []
    values: list[IndicatorValueRow] = []
    attempted = 0
    successful = 0
    for _, symbol_df in price_df.groupby("symbol", sort=True):
        result = calculate_indicators(symbol_df)
        catalog.extend(result.catalog_entries)
        values.extend(result.value_rows)
        attempted += result.attempted_indicators
        successful += result.successful_outputs

    return IndicatorComputationResult(
        catalog_entries=_dedupe_catalog(catalog),
        value_rows=values,
        attempted_indicators=attempted,
        successful_outputs=successful,
    )


def get_max_warmup_period(price_df: pd.DataFrame) -> int:
    """Best-effort estimate for the largest default warmup used by discovered indicators."""
    result = calculate_indicators(price_df)
    if not result.catalog_entries:
        return 0
    return max(entry.warmup_periods for entry in result.catalog_entries)


def _normalize_price_frame(price_df: pd.DataFrame) -> pd.DataFrame:
    renamed = price_df.rename(
        columns={
            "Symbol": "symbol",
            "Date": "timestamp",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    ).copy()
    required = {"symbol", "timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(renamed.columns)
    if missing:
        raise ValueError(f"Price frame missing required columns: {sorted(missing)}")
    renamed["timestamp"] = pd.to_datetime(renamed["timestamp"], utc=True)
    renamed = renamed.sort_values("timestamp").set_index("timestamp")
    renamed["volume"] = pd.to_numeric(renamed["volume"], errors="coerce").fillna(0)
    for column in ("open", "high", "low", "close"):
        renamed[column] = pd.to_numeric(renamed[column], errors="coerce")
    return renamed


def _coerce_outputs(
    output: object,
    frame: pd.DataFrame | None = None,
    existing_columns: set[str] | None = None,
) -> list[tuple[str, pd.Series]]:
    if output is None:
        if frame is None or existing_columns is None:
            return []
        appended_columns = [column for column in frame.columns if column not in existing_columns]
        return [
            (str(column), frame[column])
            for column in appended_columns
            if isinstance(frame[column], pd.Series)
        ]
    if isinstance(output, pd.Series):
        name = str(output.name or "value")
        return [(name, output)]
    if isinstance(output, pd.DataFrame):
        outputs: list[tuple[str, pd.Series]] = []
        for column in output.columns:
            series = output[column]
            if not isinstance(series, pd.Series):
                continue
            outputs.append((str(column), series))
        return outputs
    return []


def _build_indicator_key(indicator: str, output_name: str) -> str:
    slug = output_name.lower().replace(" ", "_")
    slug = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in slug)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return f"{indicator.lower()}__{slug}".strip("_")


def _build_display_name(indicator: str, output_name: str) -> str:
    if output_name.lower().startswith(indicator.lower()):
        return output_name
    return f"{indicator.upper()} {output_name}"


def _infer_warmup_periods(series: pd.Series) -> int:
    first_valid = series.first_valid_index()
    if first_valid is None:
        return len(series)
    positions = list(series.index)
    return positions.index(first_valid)


def _resolve_category(indicator_name: str) -> str:
    if pta is None:
        return "unknown"
    categories = getattr(pta, "Category", None)
    if isinstance(categories, dict):
        for category, indicator_names in categories.items():
            if indicator_name in indicator_names:
                return str(category)
    return "other"


def _dedupe_catalog(entries: Iterable[IndicatorCatalogEntry]) -> list[IndicatorCatalogEntry]:
    deduped: dict[str, IndicatorCatalogEntry] = {}
    for entry in entries:
        deduped[entry.indicator_key] = entry
    return list(deduped.values())


def _to_timestamp_string(value: object) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()
