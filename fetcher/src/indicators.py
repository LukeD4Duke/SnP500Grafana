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

INDICATOR_PURPOSE_DESCRIPTIONS = {
    "aberration": "Shows whether price is stretching away from its recent trading envelope.",
    "accbands": "Highlights whether price is pressing against expanded volatility bands.",
    "ad": "Tracks whether volume is flowing into or out of the ticker.",
    "adosc": "Measures changes in accumulation and distribution momentum.",
    "adx": "Measures trend strength regardless of direction.",
    "amat": "Flags whether trend conditions are strengthening or weakening.",
    "aobv": "Tracks on-balance-volume trend direction and persistence.",
    "apo": "Shows momentum as the gap between faster and slower moving averages.",
    "aroon": "Estimates whether recent highs or lows dominate the current trend.",
    "atr": "Measures recent trading range expansion to gauge volatility.",
    "bbands": "Shows whether price is near statistically high or low volatility bands.",
    "bias": "Shows how far price is stretched from its moving-average baseline.",
    "bop": "Estimates whether buyers or sellers controlled the recent candle range.",
    "brar": "Compares buying pressure versus selling pressure over the lookback window.",
    "cci": "Measures how far price has deviated from its typical level.",
    "cg": "Tracks turning-point pressure in recent price movement.",
    "chop": "Shows whether price action is trending cleanly or moving sideways.",
    "cksp": "Places a trailing stop reference based on volatility-adjusted swings.",
    "cmf": "Measures whether price advances are supported by volume accumulation.",
    "cmo": "Measures directional momentum by comparing recent gains and losses.",
    "coppock": "Blends long-horizon rate-of-change signals to spot broad momentum shifts.",
    "decay": "Shows how quickly the effect of recent moves is fading.",
    "decreasing": "Flags whether the series has been consistently declining.",
    "dema": "Smooths price while reacting faster than a standard moving average.",
    "donchian": "Shows the recent breakout range defined by rolling highs and lows.",
    "efi": "Combines price change and volume to estimate force behind the move.",
    "ema": "Smooths recent prices to show the prevailing short-to-medium trend.",
    "entropy": "Measures how orderly or noisy recent price movement has been.",
    "eom": "Shows how easily price is moving relative to traded volume.",
    "er": "Measures how efficient the recent move has been versus its path noise.",
    "fisher": "Transforms price movement to emphasize potential turning points.",
    "fwma": "Smooths trend using Fibonacci-style weighting toward recent prices.",
    "ha": "Uses Heikin-Ashi candles to reduce noise and show directional bias.",
    "hma": "Smooths trend while reducing lag versus many classic averages.",
    "hwc": "Builds a volatility channel around a smoothed price center.",
    "ichimoku": "Summarizes trend direction, support/resistance, and momentum in one view.",
    "increasing": "Flags whether the series has been consistently rising.",
    "kama": "Adapts smoothing speed based on whether price action is noisy or efficient.",
    "kc": "Places volatility envelopes around a trend baseline to show stretch.",
    "kst": "Combines several rate-of-change windows to measure multi-horizon momentum.",
    "kurtosis": "Shows whether returns are producing unusually sharp extremes.",
    "linreg": "Fits a local regression line to show direction and slope of trend.",
    "log_return": "Measures compounded return over the chosen lookback period.",
    "macd": "Shows momentum shifts through the relationship of fast and slow trend lines.",
    "mad": "Measures typical deviation from the recent average level.",
    "massi": "Looks for range expansion that can precede reversals.",
    "mfi": "Blends price and volume to gauge buying or selling pressure.",
    "midpoint": "Shows the center of the recent price range.",
    "midprice": "Shows the midpoint between recent highs and lows.",
    "mom": "Measures simple price momentum over the lookback window.",
    "natr": "Measures volatility as a percentage of price.",
    "nvi": "Tracks price behavior on lower-volume days to infer smart-money participation.",
    "obv": "Tracks cumulative volume flow behind price direction.",
    "pdist": "Measures price distance traveled within recent candles.",
    "percent_return": "Shows percentage change over the chosen lookback period.",
    "pgo": "Compares price distance from its average against recent volatility.",
    "ppo": "Shows percentage momentum between faster and slower moving averages.",
    "psar": "Provides a trailing trend and reversal reference level.",
    "pvi": "Tracks price behavior on higher-volume days to gauge crowd participation.",
    "pvol": "Measures price trend with direct volume weighting.",
    "pvo": "Shows momentum shifts in trading volume itself.",
    "pvt": "Tracks cumulative price-volume trend pressure.",
    "pwma": "Smooths prices with a weighted emphasis toward the center of the window.",
    "qstick": "Measures candlestick body bias to show buying or selling pressure.",
    "quantile": "Shows where recent values sit within their rolling distribution.",
    "roc": "Measures percentage rate of change to show momentum speed.",
    "rsi": "Measures whether recent gains or losses have dominated momentum.",
    "rvi": "Measures whether volatility is favoring bullish or bearish closes.",
    "rvgi": "Compares closing strength to the recent trading range.",
    "rvol": "Shows current volume relative to its recent norm.",
    "rwi": "Tests whether a move is strong enough to qualify as a trend.",
    "slope": "Measures the slope of the recent trend line.",
    "sma": "Shows the baseline average price over the lookback window.",
    "smi": "Measures momentum relative to the recent price range.",
    "squeeze": "Shows whether volatility compression may be setting up expansion.",
    "squeeze_pro": "Compares multiple squeeze conditions to gauge compression intensity.",
    "ssf": "Smooths price with reduced lag and less short-term noise.",
    "stc": "Blends cycle analysis and MACD-style momentum to detect trend shifts.",
    "stdev": "Measures standard deviation of recent values to gauge variability.",
    "stoch": "Shows where price sits within its recent high-low range.",
    "stochrsi": "Shows whether RSI itself is near its recent extremes.",
    "supertrend": "Uses volatility-adjusted trend bands to identify directional bias.",
    "swma": "Smooths price with symmetrically weighted recent observations.",
    "t3": "Provides extra-smoothed trend tracking with low visual noise.",
    "tema": "Smooths price while reducing lag versus a standard EMA.",
    "thermo": "Shows whether directional pressure is expanding enough to matter.",
    "trima": "Smooths price with triangular weighting to emphasize the middle of the window.",
    "trix": "Measures the rate of change in a triple-smoothed trend line.",
    "true_range": "Measures the full trading range including overnight gaps.",
    "tsi": "Measures double-smoothed momentum strength and direction.",
    "uo": "Blends multiple lookback windows to show broad momentum balance.",
    "variance": "Measures how dispersed recent values are around their mean.",
    "vhf": "Shows whether movement is directional enough to be called a trend.",
    "vortex": "Tracks whether bullish or bearish trend movement is dominant.",
    "vp": "Builds a volume profile to show where trading activity has concentrated.",
    "vwap": "Shows the volume-weighted average price as a session reference level.",
    "vwma": "Shows the average price weighted by recent trading volume.",
    "wcp": "Shows a weighted typical price level for recent candles.",
    "willr": "Shows whether price is near the top or bottom of its recent range.",
    "wma": "Places more weight on recent prices to show near-term trend direction.",
    "zlma": "Tracks trend with an average designed to reduce lag.",
}

CATEGORY_PURPOSE_DESCRIPTIONS = {
    "candle": "Flags candlestick-derived signals that can hint at short-term sentiment shifts.",
    "candles": "Shows alternative candle-derived structure to reduce noise in the ticker's price action.",
    "cycles": "Looks for repeating turning-point behavior in the ticker's price movement.",
    "momentum": "Measures how strongly the ticker has been moving up or down recently.",
    "overlap": "Shows the ticker's trend baseline or dynamic support and resistance levels.",
    "performance": "Measures how the ticker's returns are evolving over the selected lookback.",
    "statistics": "Quantifies how noisy, extreme, or dispersed the ticker's recent moves have been.",
    "trend": "Estimates whether the ticker is in a persistent directional move and how strong it is.",
    "volatility": "Measures how wide the ticker's recent price swings have been.",
    "volume": "Shows whether trading activity is confirming or contradicting the ticker's move.",
}


@dataclass(frozen=True)
class IndicatorCatalogEntry:
    """Metadata for a persisted indicator output series."""

    indicator_key: str
    indicator: str
    output_name: str
    display_name: str
    category: str
    purpose_description: str
    value_interpretation: str
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
            category = _resolve_category(indicator_name)
            warmup_periods = _infer_warmup_periods(series)
            catalog_entries.append(
                IndicatorCatalogEntry(
                    indicator_key=indicator_key,
                    indicator=indicator_name,
                    output_name=output_name,
                    display_name=_build_display_name(indicator_name, output_name),
                    category=category,
                    purpose_description=_build_purpose_description(
                        indicator_name,
                        output_name,
                        indicator_key,
                        category,
                    ),
                    value_interpretation=_build_value_interpretation(
                        indicator_name,
                        output_name,
                        indicator_key,
                        category,
                    ),
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


def _build_purpose_description(indicator_name: str, output_name: str, indicator_key: str, category: str) -> str:
    indicator_base = indicator_name.lower()
    output_key = output_name.lower()
    key_blob = f"{indicator_base} {output_key} {indicator_key.lower()}"

    if indicator_base.startswith("cdl") or category.lower() in {"candle", "candles"}:
        return "Flags a candlestick pattern and whether it points bullish or bearish."

    if indicator_base in INDICATOR_PURPOSE_DESCRIPTIONS:
        base_description = INDICATOR_PURPOSE_DESCRIPTIONS[indicator_base]
        if indicator_base in {"macd", "ppo"}:
            if _looks_like_histogram(output_key, key_blob):
                return "Shows whether momentum is accelerating or fading around the zero line."
            if _looks_like_signal_line(output_key, key_blob):
                return "Provides the smoothed reference line used to confirm momentum crossovers."
        return base_description

    if "macd" in indicator_base or "macd" in key_blob:
        if _looks_like_histogram(output_key, key_blob):
            return "Shows whether MACD momentum is accelerating or fading."
        if _looks_like_signal_line(output_key, key_blob):
            return "Smooths MACD so crossovers confirm trend direction."
        return "Compares fast and slow averages to show momentum shifts."

    if "ppo" in indicator_base or "ppo" in key_blob:
        if _looks_like_histogram(output_key, key_blob):
            return "Shows whether percentage momentum is strengthening or weakening."
        return "Compares moving averages as a percentage of price."

    if indicator_base in {"rsi", "mfi", "stoch", "stochrsi", "willr", "cci"}:
        return "Shows whether the ticker is stretched toward an extreme or near neutral."

    if indicator_base in {"adx", "atr", "natr", "rvol", "vhf", "stdev", "variance", "kurtosis", "entropy"}:
        return "Measures trend strength, volatility, or market noise for the ticker."

    if indicator_base in {"mom", "roc", "log_return", "percent_return", "cmo", "tsi", "uo", "apo", "pvo", "efi", "rvgi", "slope"}:
        return "Shows direction and strength of recent momentum."

    if indicator_base in {"sma", "ema", "dema", "tema", "hma", "wma", "vwma", "vwap", "pwma", "fwma", "ssf", "t3", "zlma", "kama", "midpoint", "midprice"}:
        return "Provides a smoothed price reference that the ticker can trade above or below."

    if indicator_base in {"bbands", "kc", "donchian", "supertrend", "psar", "ichimoku", "hwc", "pgo", "bias"}:
        return "Shows a price reference, channel, or trailing guide for trend and stretch."

    if indicator_base in {"obv", "ad", "adosc", "aobv", "cmf", "pvt", "pvi", "nvi", "efi", "massi", "pvol"}:
        return "Shows whether volume is confirming the ticker's price move."

    category_key = category.lower()
    if category_key in CATEGORY_PURPOSE_DESCRIPTIONS:
        return CATEGORY_PURPOSE_DESCRIPTIONS[category_key]

    return "Provides a technical view of the ticker's recent behavior."


def _build_value_interpretation(indicator_name: str, output_name: str, indicator_key: str, category: str) -> str:
    indicator_base = indicator_name.lower()
    output_key = output_name.lower()
    key_blob = f"{indicator_base} {output_key} {indicator_key.lower()}"

    if indicator_base.startswith("cdl") or category.lower() in {"candle", "candles"}:
        return "0 means no pattern. Positive values usually flag bullish patterns, negative values bearish ones. Larger absolute values mean a stronger detected pattern."

    if indicator_base in {"rsi", "mfi"}:
        return "Values are usually 0 to 100. Below 30 is often treated as oversold, 30 to 70 as neutral, and above 70 as overbought."

    if indicator_base in {"stoch", "stochrsi"}:
        return "Values are usually 0 to 100. Below 20 is often oversold, 20 to 80 is the middle zone, and above 80 is often overbought."

    if indicator_base == "willr":
        return "Values run from -100 to 0. Readings near -80 or lower are often oversold, around -80 to -20 are neutral, and near -20 or higher are often overbought."

    if indicator_base == "cci":
        return "Values near 0 are normal. Roughly between -100 and 100 is a common band, below -100 is weak, and above 100 is strong."

    if indicator_base == "adx":
        return "Values are usually 0 to 100. Below 20 often means a weak trend, 20 to 25 is a developing trend, and above 25 suggests a stronger trend."

    if _looks_like_histogram(output_key, key_blob):
        return "Near 0 means momentum is close to neutral. Positive values point to bullish acceleration, negative values to bearish acceleration, and larger absolute values mean a stronger move."

    if _looks_like_signal_line(output_key, key_blob):
        return "This is a smoothed reference line. Crosses with the main line matter more than the raw number: above it is usually more bullish, below it more bearish."

    if _looks_like_zero_centered(indicator_base, key_blob):
        return "0 is neutral. Positive values indicate bullish or upward bias, negative values bearish or downward bias, and larger absolute values mean a stronger signal."

    if _looks_like_strength_only(indicator_base, category, key_blob):
        if indicator_base == "rvol":
            return "Around 1 means normal volume, below 1 is quieter than usual, and above 1 is heavier than usual. Larger values mean stronger volume expansion."
        if indicator_base == "adx":
            return "Values below 20 are typically weak, 20 to 25 are forming, and above 25 are strong. Higher values mean a more directional trend."
        return "Low values usually mean a weak or noisy market state, medium values mean an emerging signal, and high values mean a stronger trend or volatility condition. Compare to the ticker's own history for context."

    if _looks_like_price_relative(indicator_base, output_key, key_blob):
        if _looks_like_band_output(output_key, key_blob):
            return "These are price levels, not scores. Price above the upper side of the channel or below the lower side is more extreme; values in the middle are more normal."
        if _looks_like_trailing_line(output_key, key_blob):
            return "These are price levels, not scores. Price above the line is usually bullish, below it bearish; the size of the gap matters more than the raw number."
        return "This is a price-level reference, so there is no universal good or bad range. Compare it directly with the ticker's latest close and recent history."

    if indicator_base in {"mom", "roc", "log_return", "percent_return", "cmo", "tsi", "uo", "apo", "pvo", "efi", "rvgi", "slope"}:
        return "0 is the neutral point. Positive values mean upward momentum, negative values mean downward momentum, and larger absolute values mean a stronger move. Exact ranges depend on the lookback and the ticker."

    category_key = category.lower()
    if category_key in CATEGORY_PURPOSE_DESCRIPTIONS:
        return "Interpret this against the ticker's recent history rather than a universal range."

    return "Interpret this relative to the ticker's own recent history; there is no universal range for this output."


def _looks_like_histogram(output_key: str, key_blob: str) -> bool:
    return (
        any(token in output_key for token in ("hist", "histogram", "macdh", "ppoh"))
        or "hist" in key_blob
        or "macdh" in key_blob
        or "ppoh" in key_blob
    )


def _looks_like_signal_line(output_key: str, key_blob: str) -> bool:
    return any(token in output_key for token in ("signal", "sig", "macds", "ppos")) or "signal" in key_blob


def _looks_like_zero_centered(indicator_base: str, key_blob: str) -> bool:
    return indicator_base in {"macd", "ppo", "cmo", "mom", "roc", "log_return", "percent_return", "tsi", "apo", "efi", "rvgi", "slope", "pvo"} or any(
        token in key_blob for token in ("hist", "signal", "momentum", "roc", "return")
    )


def _looks_like_strength_only(indicator_base: str, category: str, key_blob: str) -> bool:
    return indicator_base in {"adx", "atr", "natr", "rvol", "vhf", "stdev", "variance", "kurtosis", "entropy", "rwi"} or category.lower() in {"volatility", "trend", "statistics"} or any(
        token in key_blob for token in ("adx", "atr", "vol", "var", "stdev", "entropy", "vhf", "rwi")
    )


def _looks_like_price_relative(indicator_base: str, output_key: str, key_blob: str) -> bool:
    return (
        indicator_base in {"sma", "ema", "dema", "tema", "hma", "wma", "vwma", "vwap", "pwma", "fwma", "ssf", "t3", "zlma", "kama", "midpoint", "midprice", "bbands", "kc", "donchian", "supertrend", "psar", "ichimoku", "hwc", "pgo", "bias"}
        or _looks_like_band_output(output_key, key_blob)
        or _looks_like_trailing_line(output_key, key_blob)
    )


def _looks_like_band_output(output_key: str, key_blob: str) -> bool:
    return any(token in output_key for token in ("upper", "lower", "mid", "middle", "basis", "band", "width")) or any(
        token in key_blob for token in ("bb", "band", "donchian", "kc", "supertrend")
    )


def _looks_like_trailing_line(output_key: str, key_blob: str) -> bool:
    return any(token in output_key for token in ("trend", "stop", "sar", "line", "basis", "mid")) or any(
        token in key_blob for token in ("psar", "supertrend", "ichimoku", "zlma")
    )


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
