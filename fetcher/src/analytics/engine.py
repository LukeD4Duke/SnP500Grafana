"""Deterministic analytics snapshot generation for dashboards and reports."""

from __future__ import annotations

import logging
import math
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

import pandas as pd

from ..config import DatabaseConfig
from ..database import (
    get_existing_signal_snapshot_dates,
    get_price_history_dataset,
    upsert_market_breadth_snapshots,
    upsert_rank_snapshots,
    upsert_signal_snapshots,
)

logger = logging.getLogger(__name__)

TIMEFRAME_SETTINGS = {
    "daily": {
        "rule": None,
        "fast": 20,
        "medium": 50,
        "slow": 200,
        "momentum": 21,
        "breakout": 20,
        "rsi": 14,
        "atr": 14,
        "volume": 20,
        "slope": 5,
        "volatility": 20,
        "stale_days": 7,
        "return_scale": 0.12,
        "macd_scale": 0.05,
    },
    "weekly": {
        "rule": "W-FRI",
        "fast": 10,
        "medium": 20,
        "slow": 40,
        "momentum": 13,
        "breakout": 13,
        "rsi": 14,
        "atr": 14,
        "volume": 10,
        "slope": 4,
        "volatility": 10,
        "stale_days": 21,
        "return_scale": 0.18,
        "macd_scale": 0.08,
    },
    "monthly": {
        "rule": "ME",
        "fast": 6,
        "medium": 12,
        "slow": 24,
        "momentum": 12,
        "breakout": 12,
        "rsi": 12,
        "atr": 12,
        "volume": 6,
        "slope": 3,
        "volatility": 6,
        "stale_days": 45,
        "return_scale": 0.25,
        "macd_scale": 0.12,
    },
}


@dataclass(frozen=True)
class AnalyticsRefreshResult:
    """Summary of a snapshot refresh run."""

    snapshot_date: str
    timeframes: list[str]
    signal_rows: int
    rank_rows: int
    breadth_rows: int


def refresh_analytics_snapshots(
    db_config: DatabaseConfig,
    timeframes: Iterable[str] | None = None,
    batch_size: int = 50,
) -> AnalyticsRefreshResult | None:
    """Backfill and extend analytics outputs from stored OHLCV history."""
    normalized_timeframes = _normalize_timeframes(timeframes)
    normalized_batch_size = max(int(batch_size), 1)
    price_rows = get_price_history_dataset(db_config)
    if not price_rows:
        return None

    prices = pd.DataFrame(price_rows)
    prices["timestamp"] = pd.to_datetime(prices["timestamp"], utc=True, errors="coerce")
    prices = prices.dropna(subset=["timestamp", "close"]).copy()
    prices["volume"] = pd.to_numeric(prices["volume"], errors="coerce").fillna(0)
    for column in ("open", "high", "low", "close", "dividends", "stock_splits"):
        prices[column] = pd.to_numeric(prices[column], errors="coerce")
    prices = prices.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    if prices.empty:
        return None

    snapshot_date = prices["timestamp"].max().date().isoformat()
    breadth_reference = _build_daily_breadth_history(prices)

    signal_row_count = 0
    rank_row_count = 0
    breadth_row_count = 0

    for timeframe in normalized_timeframes:
        metrics = _build_timeframe_metrics(prices, timeframe)
        if metrics.empty:
            continue
        metrics = metrics.merge(breadth_reference, on=["symbol", "snapshot_date"], how="left")
        expected_dates = set(metrics["snapshot_date"].astype(str))
        existing_dates = set(get_existing_signal_snapshot_dates(db_config, timeframe))
        missing_dates = sorted(expected_dates - existing_dates)
        extra_dates = sorted(existing_dates - expected_dates)
        if extra_dates:
            logger.warning(
                "Analytics timeframe %s has %d stored snapshot date(s) not produced by the current resampling logic: %s",
                timeframe,
                len(extra_dates),
                _format_date_preview(extra_dates),
            )
        unexpected_missing_dates = sorted(expected_dates - existing_dates)
        if unexpected_missing_dates:
            logger.info(
                "Analytics timeframe %s is missing %d expected snapshot date(s): %s",
                timeframe,
                len(unexpected_missing_dates),
                _format_date_preview(unexpected_missing_dates),
            )
        if not missing_dates:
            logger.info("Analytics timeframe %s already up to date; no missing snapshot dates", timeframe)
            continue

        date_windows = list(_chunked_snapshot_dates(missing_dates, normalized_batch_size))
        logger.info(
            "Analytics timeframe %s has %d missing snapshot dates; processing in %d window(s) of up to %d date(s)",
            timeframe,
            len(missing_dates),
            len(date_windows),
            normalized_batch_size,
        )
        for window_index, window_dates in enumerate(date_windows, start=1):
            signal_rows = _build_signal_rows(metrics, timeframe, window_dates)
            rank_rows = _build_rank_rows(metrics, timeframe, window_dates)
            breadth_rows = _build_breadth_rows(metrics, timeframe, window_dates)
            upsert_signal_snapshots(db_config, signal_rows)
            upsert_rank_snapshots(db_config, rank_rows)
            upsert_market_breadth_snapshots(db_config, breadth_rows)
            signal_row_count += len(signal_rows)
            rank_row_count += len(rank_rows)
            breadth_row_count += len(breadth_rows)
            logger.info(
                "Analytics timeframe %s window %d/%d wrote %d signal rows, %d rank rows, and %d breadth rows for %s to %s",
                timeframe,
                window_index,
                len(date_windows),
                len(signal_rows),
                len(rank_rows),
                len(breadth_rows),
                window_dates[0],
                window_dates[-1],
            )

    return AnalyticsRefreshResult(
        snapshot_date=snapshot_date,
        timeframes=normalized_timeframes,
        signal_rows=signal_row_count,
        rank_rows=rank_row_count,
        breadth_rows=breadth_row_count,
    )


def _normalize_timeframes(timeframes: Iterable[str] | None) -> list[str]:
    if timeframes is None:
        return list(TIMEFRAME_SETTINGS.keys())
    normalized: list[str] = []
    for timeframe in timeframes:
        candidate = str(timeframe).strip().lower()
        if candidate in TIMEFRAME_SETTINGS and candidate not in normalized:
            normalized.append(candidate)
    return normalized or list(TIMEFRAME_SETTINGS.keys())


def _chunked_snapshot_dates(dates: list[str], batch_size: int) -> Iterable[list[str]]:
    normalized_batch_size = max(batch_size, 1)
    for index in range(0, len(dates), normalized_batch_size):
        yield dates[index : index + normalized_batch_size]


def _format_date_preview(dates: list[str], limit: int = 5) -> str:
    if not dates:
        return "none"
    visible = dates[:limit]
    suffix = ""
    if len(dates) > len(visible):
        suffix = f" ... (+{len(dates) - len(visible)} more)"
    return ", ".join(visible) + suffix


def _build_daily_breadth_history(prices: pd.DataFrame) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    for symbol, symbol_frame in prices.groupby("symbol", sort=True):
        symbol_frame = symbol_frame.sort_values("timestamp")
        close = symbol_frame["close"].astype(float)
        high = symbol_frame["high"].astype(float)
        low = symbol_frame["low"].astype(float)
        if close.empty:
            continue
        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        ema200 = close.ewm(span=200, adjust=False).mean()
        high_20 = high.rolling(20, min_periods=5).max()
        low_20 = low.rolling(20, min_periods=5).min()
        high_252 = high.rolling(252, min_periods=20).max()
        low_252 = low.rolling(252, min_periods=20).min()
        records.append(
            pd.DataFrame(
                {
                    "symbol": symbol,
                    "snapshot_date": pd.to_datetime(symbol_frame["timestamp"]).dt.date.astype(str),
                    "above_ema20": close.gt(ema20.fillna(close)),
                    "above_ema50": close.gt(ema50.fillna(close)),
                    "above_ema200": close.gt(ema200.fillna(close)),
                    "new_20d_high": close.ge(high_20.fillna(close) * 0.995),
                    "new_20d_low": close.le(low_20.fillna(close) * 1.005),
                    "near_52w_high": close.ge(high_252.fillna(close) * 0.98),
                    "near_52w_low": close.le(low_252.fillna(close) * 1.02),
                }
            )
        )
    if not records:
        return pd.DataFrame(columns=["symbol", "snapshot_date"])
    return pd.concat(records, ignore_index=True)


def _build_timeframe_metrics(prices: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    settings = TIMEFRAME_SETTINGS[timeframe]
    records: list[pd.DataFrame] = []
    for symbol, symbol_frame in prices.groupby("symbol", sort=True):
        bars = _resample_symbol_frame(symbol_frame, timeframe)
        if bars.empty:
            continue
        metrics = _compute_symbol_metric_history(symbol, bars, settings)
        if metrics is not None and not metrics.empty:
            records.append(metrics)
    if not records:
        return pd.DataFrame()

    metrics = pd.concat(records, ignore_index=True)
    metrics["relative_strength_score"] = (
        metrics.groupby("snapshot_date")["momentum_return"].rank(pct=True).mul(100).round(2)
    )
    metrics["atr_percentile"] = metrics.groupby("snapshot_date")["atr_pct"].rank(pct=True).mul(100)
    metrics["realized_vol_percentile"] = (
        metrics.groupby("snapshot_date")["realized_vol"].rank(pct=True).mul(100)
    )

    stack_score = (
        metrics["ema_fast"].gt(metrics["ema_medium"]).astype(float) * 50
        + metrics["ema_medium"].gt(metrics["ema_slow"]).astype(float) * 50
    )
    metrics["trend_score"] = _average_scores(
        _score_ratio(metrics["close"] / metrics["ema_fast"], 1.0, 0.10),
        _score_ratio(metrics["close"] / metrics["ema_medium"], 1.0, 0.14),
        _score_ratio(metrics["close"] / metrics["ema_slow"], 1.0, 0.20),
        stack_score,
        _score_centered(metrics["ema_medium_slope"], 0.04),
    )
    metrics["momentum_score"] = _average_scores(
        ((metrics["rsi"] - 30) / 40 * 100).clip(0, 100),
        _score_centered(metrics["momentum_return"], settings["return_scale"]),
        _score_centered(metrics["macd_proxy"], settings["macd_scale"]),
    )
    metrics["volume_score"] = _average_scores(
        _score_ratio(metrics["volume_ratio"], 1.0, 1.0),
        ((metrics["volume_pressure"] + 1) * 50).clip(0, 100),
        metrics["breakout_flag"].astype(float).mul(100).where(
            metrics["volume_ratio"] > 1.1,
            50.0,
        ),
    )
    metrics["structure_score"] = _average_scores(
        metrics["range_position"].mul(100).clip(0, 100),
        metrics["swing_structure"].map({-1: 0.0, 0: 50.0, 1: 100.0}).fillna(50.0),
        metrics["breakout_flag"].map({True: 100.0, False: 50.0}) - metrics["breakdown_flag"].astype(float) * 50,
    ).clip(0, 100)
    metrics["mean_reversion_score"] = _average_scores(
        (100 - (metrics["rsi"] - 50).abs() * 2).clip(0, 100),
        (100 - (metrics["stretch_pct"].abs() / 0.15 * 100)).clip(0, 100),
        (100 - metrics["range_position"].sub(0.5).abs().mul(200)).clip(0, 100),
    )
    metrics["volatility_risk_score"] = _average_scores(
        metrics["atr_percentile"],
        metrics["realized_vol_percentile"],
        _score_ratio(metrics["gap_risk"], 0.0, 0.08),
    )
    metrics["risk_penalty"] = (
        metrics["volatility_risk_score"] * 0.18
        + metrics["data_quality_flag"].astype(float) * 14
        + metrics["breakdown_flag"].astype(float) * 8
        + metrics["overbought_flag"].astype(float) * 4
    ).clip(0, 35)
    metrics["final_score"] = (
        metrics["trend_score"] * 0.24
        + metrics["momentum_score"] * 0.18
        + metrics["volume_score"] * 0.12
        + metrics["relative_strength_score"] * 0.18
        + metrics["structure_score"] * 0.16
        + metrics["mean_reversion_score"] * 0.12
        - metrics["risk_penalty"]
    ).clip(0, 100)
    metrics["trend_state"] = metrics["trend_score"].map(_score_state)
    metrics["momentum_state"] = metrics["momentum_score"].map(_score_state)
    metrics["volume_state"] = metrics["volume_score"].map(_score_state)
    metrics["relative_strength_state"] = metrics["relative_strength_score"].map(_score_state)
    metrics["structure_state"] = metrics["structure_score"].map(_score_state)
    metrics["volatility_state"] = metrics["volatility_risk_score"].map(_risk_state)
    metrics["regime_label"] = metrics.apply(_regime_label, axis=1)
    metrics["recommendation_label"] = metrics.apply(_recommendation_label, axis=1)
    metrics["drivers_json"] = metrics.apply(_build_drivers, axis=1)
    return metrics.sort_values(["snapshot_date", "symbol"]).reset_index(drop=True)


def _resample_symbol_frame(symbol_frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    rule = TIMEFRAME_SETTINGS[timeframe]["rule"]
    ordered = symbol_frame.sort_values("timestamp").copy()
    ordered["timestamp"] = pd.to_datetime(ordered["timestamp"], utc=True, errors="coerce")
    ordered = ordered.dropna(subset=["timestamp"]).set_index("timestamp")
    if ordered.empty:
        return pd.DataFrame()
    if rule is None:
        bars = ordered[["open", "high", "low", "close", "volume"]].copy()
        bars["last_timestamp"] = bars.index
        bars = bars.reset_index()
        return bars

    ohlcv = ordered.resample(rule).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    last_timestamp = ordered.index.to_series().resample(rule).max()
    bars = ohlcv.join(last_timestamp.rename("last_timestamp"))
    bars = bars.dropna(subset=["close", "last_timestamp"]).reset_index()
    return bars


def _compute_symbol_metric_history(symbol: str, bars: pd.DataFrame, settings: dict) -> pd.DataFrame | None:
    bars = bars.sort_values("timestamp").copy()
    close = pd.to_numeric(bars["close"], errors="coerce")
    high = pd.to_numeric(bars["high"], errors="coerce")
    low = pd.to_numeric(bars["low"], errors="coerce")
    volume = pd.to_numeric(bars["volume"], errors="coerce").fillna(0)
    if close.dropna().empty:
        return None

    min_required = max(settings["slow"] + settings["slope"], settings["breakout"] + 5)

    ema_fast = close.ewm(span=settings["fast"], adjust=False).mean()
    ema_medium = close.ewm(span=settings["medium"], adjust=False).mean()
    ema_slow = close.ewm(span=settings["slow"], adjust=False).mean()
    previous_medium = ema_medium.shift(settings["slope"])
    ema_medium_slope = ((ema_medium / previous_medium) - 1).where(previous_medium.ne(0), 0.0).fillna(0.0)

    rsi = _calculate_rsi(close, settings["rsi"]).fillna(50.0)
    atr = _calculate_atr(high, low, close, settings["atr"])
    atr_pct = (atr / close.replace(0, pd.NA)).fillna(0.0)
    realized_vol = close.pct_change().rolling(settings["volatility"], min_periods=3).std().fillna(0.0)
    volume_avg = volume.rolling(settings["volume"], min_periods=3).mean()
    volume_ratio = (volume / volume_avg.replace(0, pd.NA)).fillna(1.0)
    momentum_return = close.pct_change(settings["momentum"]).fillna(0.0)
    macd_proxy = ((ema_fast - ema_medium) / close.replace(0, pd.NA)).fillna(0.0)
    recent_high = high.rolling(settings["breakout"], min_periods=5).max().fillna(high)
    recent_low = low.rolling(settings["breakout"], min_periods=5).min().fillna(low)
    range_span = recent_high - recent_low
    range_position = ((close - recent_low) / range_span.where(range_span.gt(0), pd.NA)).fillna(0.5)
    breakout_flag = recent_high.gt(0) & close.ge(recent_high * 0.995)
    breakdown_flag = recent_low.gt(0) & close.le(recent_low * 1.005)
    stretch_pct = ((close / ema_fast.replace(0, pd.NA)) - 1).fillna(0.0)
    overbought_flag = rsi.ge(70) | stretch_pct.ge(0.10)
    oversold_flag = rsi.le(30) | stretch_pct.le(-0.10)

    swing_structure = pd.Series(0, index=bars.index, dtype="int64")
    higher_highs = high.gt(high.shift(1)) & high.shift(1).gt(high.shift(2))
    higher_lows = low.gt(low.shift(1)) & low.shift(1).gt(low.shift(2))
    lower_highs = high.lt(high.shift(1)) & high.shift(1).lt(high.shift(2))
    lower_lows = low.lt(low.shift(1)) & low.shift(1).lt(low.shift(2))
    swing_structure.loc[higher_highs & higher_lows] = 1
    swing_structure.loc[lower_highs & lower_lows] = -1

    signed_volume = close.diff().fillna(0).map(lambda value: 1 if value > 0 else (-1 if value < 0 else 0))
    rolling_volume = volume.rolling(settings["volume"], min_periods=3).sum()
    volume_pressure = ((signed_volume * volume).rolling(settings["volume"], min_periods=3).sum() / rolling_volume.clip(lower=1)).fillna(0.0)
    previous_close = close.shift(1)
    gap_risk = (((close - previous_close).abs() / previous_close.replace(0, pd.NA)).rolling(5, min_periods=2).max()).fillna(0.0)
    trend_alignment_flag = (
        ema_fast.notna()
        & ema_medium.notna()
        & ema_slow.notna()
        & ((ema_fast.gt(ema_medium) & ema_medium.gt(ema_slow)) | (ema_fast.lt(ema_medium) & ema_medium.lt(ema_slow)))
    )
    bar_counts = pd.Series(range(1, len(bars) + 1), index=bars.index)
    data_quality_flag = bar_counts.lt(min_required)

    metrics = pd.DataFrame(
        {
            "symbol": symbol,
            "last_timestamp": pd.to_datetime(bars["last_timestamp"], utc=True).map(lambda value: value.isoformat()),
            "snapshot_date": pd.to_datetime(bars["last_timestamp"], utc=True).dt.date.astype(str),
            "close": close.astype(float),
            "volume": volume.fillna(0).astype(int),
            "ema_fast": ema_fast.fillna(close).astype(float),
            "ema_medium": ema_medium.fillna(close).astype(float),
            "ema_slow": ema_slow.fillna(close).astype(float),
            "ema_medium_slope": ema_medium_slope.astype(float),
            "rsi": rsi.astype(float),
            "atr_pct": atr_pct.astype(float),
            "realized_vol": realized_vol.astype(float),
            "volume_ratio": volume_ratio.astype(float),
            "momentum_return": momentum_return.astype(float),
            "macd_proxy": macd_proxy.astype(float),
            "range_position": range_position.astype(float),
            "breakout_flag": breakout_flag.astype(bool),
            "breakdown_flag": breakdown_flag.astype(bool),
            "stretch_pct": stretch_pct.astype(float),
            "overbought_flag": overbought_flag.astype(bool),
            "oversold_flag": oversold_flag.astype(bool),
            "swing_structure": swing_structure.astype(int),
            "volume_pressure": volume_pressure.astype(float),
            "gap_risk": gap_risk.astype(float),
            "trend_alignment_flag": trend_alignment_flag.astype(bool),
            "data_quality_flag": data_quality_flag.astype(bool),
        }
    )
    return metrics


def _calculate_rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / max(period, 1), min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / max(period, 1), min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)


def _calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low).abs(),
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(period, min_periods=max(2, period // 2)).mean().bfill()


def _score_ratio(series: pd.Series, center: float, scale: float) -> pd.Series:
    return ((series - center) / scale * 50 + 50).clip(0, 100)


def _score_centered(series: pd.Series, scale: float) -> pd.Series:
    return (series / scale * 50 + 50).clip(0, 100)


def _average_scores(*series_list: pd.Series | float) -> pd.Series:
    frames = []
    for series in series_list:
        frames.append(series if isinstance(series, pd.Series) else pd.Series(series))
    return pd.concat(frames, axis=1).mean(axis=1)


def _score_state(score: float) -> str:
    if score >= 67:
        return "bullish"
    if score <= 33:
        return "bearish"
    return "neutral"


def _risk_state(score: float) -> str:
    if score >= 67:
        return "high"
    if score <= 33:
        return "low"
    return "moderate"


def _regime_label(row: pd.Series) -> str:
    if row["breakout_flag"] and row["trend_score"] >= 65:
        return "breakout"
    if row["breakdown_flag"] and row["trend_score"] <= 40:
        return "breakdown"
    if row["trend_score"] >= 67 and row["relative_strength_score"] >= 60:
        return "uptrend"
    if row["trend_score"] <= 33 and row["relative_strength_score"] <= 40:
        return "downtrend"
    return "range"


def _recommendation_label(row: pd.Series) -> str:
    if row["final_score"] >= 80 and not row["overbought_flag"]:
        return "bullish watch"
    if row["final_score"] >= 65:
        return "bullish"
    if row["final_score"] <= 25:
        return "avoid"
    if row["final_score"] <= 40:
        return "defensive"
    return "neutral"


def _build_drivers(row: pd.Series) -> list[dict]:
    return [
        {"key": "trend_score", "label": "Trend score", "value": _finite_or_none(row["trend_score"])},
        {"key": "momentum_score", "label": "Momentum score", "value": _finite_or_none(row["momentum_score"])},
        {
            "key": "relative_strength_score",
            "label": "Relative strength percentile",
            "value": _finite_or_none(row["relative_strength_score"]),
        },
        {"key": "rsi", "label": "RSI", "value": _finite_or_none(row["rsi"])},
        {"key": "volume_ratio", "label": "Volume ratio", "value": _finite_or_none(row["volume_ratio"])},
        {"key": "atr_pct", "label": "ATR % of price", "value": _finite_or_none(float(row["atr_pct"]) * 100)},
    ]


def _finite_or_none(value: object) -> float | None:
    numeric = float(value)
    if not math.isfinite(numeric):
        return None
    return round(numeric, 2)


def _build_signal_rows(metrics: pd.DataFrame, timeframe: str, missing_dates: list[str]) -> list[dict]:
    rows: list[dict] = []
    date_filter = set(missing_dates)
    for _, row in metrics.loc[metrics["snapshot_date"].isin(date_filter)].iterrows():
        rows.append(
            {
                "snapshot_date": row["snapshot_date"],
                "symbol": row["symbol"],
                "timeframe": timeframe,
                "last_timestamp": row["last_timestamp"],
                "close": round(float(row["close"]), 4),
                "volume": int(row["volume"]),
                "trend_score": round(float(row["trend_score"]), 4),
                "momentum_score": round(float(row["momentum_score"]), 4),
                "volume_score": round(float(row["volume_score"]), 4),
                "relative_strength_score": round(float(row["relative_strength_score"]), 4),
                "structure_score": round(float(row["structure_score"]), 4),
                "mean_reversion_score": round(float(row["mean_reversion_score"]), 4),
                "volatility_risk_score": round(float(row["volatility_risk_score"]), 4),
                "risk_penalty": round(float(row["risk_penalty"]), 4),
                "final_score": round(float(row["final_score"]), 4),
                "trend_state": row["trend_state"],
                "momentum_state": row["momentum_state"],
                "volume_state": row["volume_state"],
                "relative_strength_state": row["relative_strength_state"],
                "structure_state": row["structure_state"],
                "volatility_state": row["volatility_state"],
                "regime_label": row["regime_label"],
                "recommendation_label": row["recommendation_label"],
                "breakout_flag": bool(row["breakout_flag"]),
                "breakdown_flag": bool(row["breakdown_flag"]),
                "overbought_flag": bool(row["overbought_flag"]),
                "oversold_flag": bool(row["oversold_flag"]),
                "trend_alignment_flag": bool(row["trend_alignment_flag"]),
                "data_quality_flag": bool(row["data_quality_flag"]),
                "drivers_json": row["drivers_json"],
            }
        )
    return rows


def _build_rank_rows(
    metrics: pd.DataFrame,
    timeframe: str,
    missing_dates: list[str],
) -> list[dict]:
    rows: list[dict] = []
    if metrics.empty:
        return rows

    missing_date_set = set(missing_dates)
    prior_lookup = _build_prior_score_lookup(metrics)
    for snapshot_date, date_metrics in metrics.groupby("snapshot_date", sort=True):
        if snapshot_date not in missing_date_set:
            continue
        bullish = date_metrics.sort_values(["final_score", "symbol"], ascending=[False, True]).copy()
        bullish["bull_rank"] = range(1, len(bullish) + 1)
        bearish = date_metrics.sort_values(["final_score", "symbol"], ascending=[True, True]).copy()
        bearish["bear_rank"] = range(1, len(bearish) + 1)
        ranked = bullish.merge(
            bearish[["symbol", "bear_rank"]],
            on="symbol",
            how="left",
        ).sort_values("symbol")

        for _, row in ranked.iterrows():
            previous_1w = prior_lookup.get((row["symbol"], snapshot_date, 7))
            previous_1m = prior_lookup.get((row["symbol"], snapshot_date, 30))
            final_score = float(row["final_score"])
            bull_rank = int(row["bull_rank"])
            bear_rank = int(row["bear_rank"])
            rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "timeframe": timeframe,
                    "symbol": row["symbol"],
                    "final_score": round(final_score, 4),
                    "bull_rank": bull_rank,
                    "bear_rank": bear_rank,
                    "regime_label": row["regime_label"],
                    "recommendation_label": row["recommendation_label"],
                    "score_change_1w": None if previous_1w is None else round(final_score - previous_1w, 4),
                    "score_change_1m": None if previous_1m is None else round(final_score - previous_1m, 4),
                    "in_top20_bull": bull_rank <= 20,
                    "in_top20_bear": bear_rank <= 20,
                }
            )
    return rows


def _build_prior_score_lookup(metrics: pd.DataFrame) -> dict[tuple[str, str, int], float]:
    lookup: dict[tuple[str, str, int], float] = {}
    if metrics.empty:
        return lookup

    for symbol, symbol_rows in metrics.groupby("symbol", sort=True):
        ordered = symbol_rows.sort_values("snapshot_date")
        dates = [date.fromisoformat(str(value)) for value in ordered["snapshot_date"]]
        scores = [float(value) for value in ordered["final_score"]]
        ordinals = [value.toordinal() for value in dates]
        for current_date in dates:
            for delta_days in (7, 30):
                target_ordinal = (current_date - timedelta(days=delta_days)).toordinal()
                index = bisect_right(ordinals, target_ordinal) - 1
                if index >= 0:
                    lookup[(str(symbol), current_date.isoformat(), delta_days)] = scores[index]
    return lookup


def _build_breadth_rows(metrics: pd.DataFrame, timeframe: str, missing_dates: list[str]) -> list[dict]:
    rows: list[dict] = []
    missing_date_set = set(missing_dates)
    for snapshot_date, date_metrics in metrics.groupby("snapshot_date", sort=True):
        if snapshot_date not in missing_date_set:
            continue
        total = len(date_metrics)
        bullish_count = int(date_metrics["final_score"].ge(65).sum())
        bearish_count = int(date_metrics["final_score"].le(35).sum())
        neutral_count = max(total - bullish_count - bearish_count, 0)
        median_score = float(date_metrics["final_score"].median()) if total else 0.0
        rows.append(
            {
                "snapshot_date": snapshot_date,
                "timeframe": timeframe,
                "universe_size": total,
                "bullish_count": bullish_count,
                "neutral_count": neutral_count,
                "bearish_count": bearish_count,
                "pct_above_ema20": _boolean_pct(date_metrics.get("above_ema20")),
                "pct_above_ema50": _boolean_pct(date_metrics.get("above_ema50")),
                "pct_above_ema200": _boolean_pct(date_metrics.get("above_ema200")),
                "pct_new_20d_high": _boolean_pct(date_metrics.get("new_20d_high")),
                "pct_new_20d_low": _boolean_pct(date_metrics.get("new_20d_low")),
                "pct_near_52w_high": _boolean_pct(date_metrics.get("near_52w_high")),
                "pct_near_52w_low": _boolean_pct(date_metrics.get("near_52w_low")),
                "avg_final_score": round(float(date_metrics["final_score"].mean()) if total else 0.0, 4),
                "median_final_score": round(median_score, 4),
            }
        )
    return rows


def _boolean_pct(series: pd.Series | None) -> float:
    if series is None or len(series) == 0:
        return 0.0
    return round(float(series.fillna(False).astype(bool).mean() * 100), 4)
