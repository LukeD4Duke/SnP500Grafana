"""Deterministic analytics snapshot generation for dashboards and reports."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

import pandas as pd

from ..config import DatabaseConfig
from ..database import (
    get_price_history_dataset,
    get_prior_signal_scores,
    upsert_market_breadth_snapshots,
    upsert_rank_snapshots,
    upsert_signal_snapshots,
)

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
) -> AnalyticsRefreshResult | None:
    """Rebuild latest analytics outputs from stored OHLCV history."""
    normalized_timeframes = _normalize_timeframes(timeframes)
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
    breadth_reference = _build_daily_breadth_reference(prices)
    prior_scores = pd.DataFrame(
        get_prior_signal_scores(
            db_config,
            snapshot_date=snapshot_date,
            timeframes=normalized_timeframes,
            symbols=sorted(prices["symbol"].dropna().astype(str).unique()),
        )
    )
    if not prior_scores.empty:
        prior_scores["snapshot_date"] = pd.to_datetime(prior_scores["snapshot_date"]).dt.date

    signal_rows: list[dict] = []
    rank_rows: list[dict] = []
    breadth_rows: list[dict] = []

    for timeframe in normalized_timeframes:
        metrics = _build_timeframe_metrics(prices, timeframe)
        if metrics.empty:
            continue
        metrics = metrics.merge(breadth_reference, on="symbol", how="left")
        signal_rows.extend(_build_signal_rows(metrics, timeframe, snapshot_date))
        rank_rows.extend(_build_rank_rows(metrics, timeframe, snapshot_date, prior_scores))
        breadth_rows.append(_build_breadth_row(metrics, timeframe, snapshot_date))

    upsert_signal_snapshots(db_config, signal_rows)
    upsert_rank_snapshots(db_config, rank_rows)
    upsert_market_breadth_snapshots(db_config, breadth_rows)
    return AnalyticsRefreshResult(
        snapshot_date=snapshot_date,
        timeframes=normalized_timeframes,
        signal_rows=len(signal_rows),
        rank_rows=len(rank_rows),
        breadth_rows=len(breadth_rows),
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


def _build_daily_breadth_reference(prices: pd.DataFrame) -> pd.DataFrame:
    records: list[dict] = []
    for symbol, symbol_frame in prices.groupby("symbol", sort=True):
        symbol_frame = symbol_frame.sort_values("timestamp")
        close = symbol_frame["close"].astype(float)
        high = symbol_frame["high"].astype(float)
        low = symbol_frame["low"].astype(float)
        if close.empty:
            continue
        ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
        ema50 = close.ewm(span=50, adjust=False).mean().iloc[-1]
        ema200 = close.ewm(span=200, adjust=False).mean().iloc[-1]
        high_20 = high.rolling(20, min_periods=5).max().iloc[-1]
        low_20 = low.rolling(20, min_periods=5).min().iloc[-1]
        high_252 = high.rolling(252, min_periods=20).max().iloc[-1]
        low_252 = low.rolling(252, min_periods=20).min().iloc[-1]
        latest_close = float(close.iloc[-1])
        records.append(
            {
                "symbol": symbol,
                "above_ema20": bool(pd.notna(ema20) and latest_close > float(ema20)),
                "above_ema50": bool(pd.notna(ema50) and latest_close > float(ema50)),
                "above_ema200": bool(pd.notna(ema200) and latest_close > float(ema200)),
                "new_20d_high": bool(pd.notna(high_20) and latest_close >= float(high_20) * 0.995),
                "new_20d_low": bool(pd.notna(low_20) and latest_close <= float(low_20) * 1.005),
                "near_52w_high": bool(pd.notna(high_252) and latest_close >= float(high_252) * 0.98),
                "near_52w_low": bool(pd.notna(low_252) and latest_close <= float(low_252) * 1.02),
            }
        )
    return pd.DataFrame(records)


def _build_timeframe_metrics(prices: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    settings = TIMEFRAME_SETTINGS[timeframe]
    records: list[dict] = []
    for symbol, symbol_frame in prices.groupby("symbol", sort=True):
        bars = _resample_symbol_frame(symbol_frame, timeframe)
        if bars.empty:
            continue
        metrics = _compute_symbol_metrics(symbol, bars, settings)
        if metrics is not None:
            records.append(metrics)
    if not records:
        return pd.DataFrame()

    metrics = pd.DataFrame(records)
    metrics["relative_strength_score"] = metrics["momentum_return"].rank(pct=True).mul(100).round(2)
    metrics["atr_percentile"] = metrics["atr_pct"].rank(pct=True).mul(100)
    metrics["realized_vol_percentile"] = metrics["realized_vol"].rank(pct=True).mul(100)

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
    metrics["snapshot_date"] = pd.to_datetime(metrics["last_timestamp"]).dt.date.astype(str)
    return metrics


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


def _compute_symbol_metrics(symbol: str, bars: pd.DataFrame, settings: dict) -> dict | None:
    bars = bars.sort_values("timestamp").copy()
    close = pd.to_numeric(bars["close"], errors="coerce")
    high = pd.to_numeric(bars["high"], errors="coerce")
    low = pd.to_numeric(bars["low"], errors="coerce")
    volume = pd.to_numeric(bars["volume"], errors="coerce").fillna(0)
    if close.dropna().empty:
        return None

    min_required = max(settings["slow"] + settings["slope"], settings["breakout"] + 5)
    latest_close = float(close.iloc[-1])
    latest_high = float(high.iloc[-1]) if pd.notna(high.iloc[-1]) else latest_close
    latest_low = float(low.iloc[-1]) if pd.notna(low.iloc[-1]) else latest_close

    ema_fast = close.ewm(span=settings["fast"], adjust=False).mean()
    ema_medium = close.ewm(span=settings["medium"], adjust=False).mean()
    ema_slow = close.ewm(span=settings["slow"], adjust=False).mean()
    ema_medium_slope = 0.0
    if len(ema_medium) > settings["slope"] and pd.notna(ema_medium.iloc[-settings["slope"] - 1]):
        previous_medium = float(ema_medium.iloc[-settings["slope"] - 1])
        if previous_medium:
            ema_medium_slope = float(ema_medium.iloc[-1] / previous_medium - 1)

    rsi = _calculate_rsi(close, settings["rsi"]).iloc[-1]
    atr = _calculate_atr(high, low, close, settings["atr"]).iloc[-1]
    atr_pct = float(atr / latest_close) if pd.notna(atr) and latest_close else 0.0
    realized_vol = float(close.pct_change().rolling(settings["volatility"], min_periods=3).std().iloc[-1] or 0.0)
    volume_avg = float(volume.rolling(settings["volume"], min_periods=3).mean().iloc[-1] or 0.0)
    volume_ratio = float(volume.iloc[-1] / volume_avg) if volume_avg else 1.0
    momentum_return = float(close.pct_change(settings["momentum"]).iloc[-1] or 0.0)
    macd_proxy = float((ema_fast.iloc[-1] - ema_medium.iloc[-1]) / latest_close) if latest_close else 0.0
    recent_high = float(high.rolling(settings["breakout"], min_periods=5).max().iloc[-1] or latest_high)
    recent_low = float(low.rolling(settings["breakout"], min_periods=5).min().iloc[-1] or latest_low)
    range_span = recent_high - recent_low
    range_position = 0.5 if range_span <= 0 else (latest_close - recent_low) / range_span
    breakout_flag = recent_high > 0 and latest_close >= recent_high * 0.995
    breakdown_flag = recent_low > 0 and latest_close <= recent_low * 1.005
    stretch_pct = float(latest_close / ema_fast.iloc[-1] - 1) if pd.notna(ema_fast.iloc[-1]) and ema_fast.iloc[-1] else 0.0
    overbought_flag = bool(pd.notna(rsi) and float(rsi) >= 70) or stretch_pct >= 0.10
    oversold_flag = bool(pd.notna(rsi) and float(rsi) <= 30) or stretch_pct <= -0.10

    swing_structure = 0
    if len(high) >= 3 and len(low) >= 3:
        if high.iloc[-1] > high.iloc[-2] > high.iloc[-3] and low.iloc[-1] > low.iloc[-2] > low.iloc[-3]:
            swing_structure = 1
        elif high.iloc[-1] < high.iloc[-2] < high.iloc[-3] and low.iloc[-1] < low.iloc[-2] < low.iloc[-3]:
            swing_structure = -1

    signed_volume = close.diff().fillna(0).apply(lambda value: 1 if value > 0 else (-1 if value < 0 else 0))
    volume_pressure = float(
        (signed_volume * volume).rolling(settings["volume"], min_periods=3).sum().iloc[-1]
        / max(volume.rolling(settings["volume"], min_periods=3).sum().iloc[-1], 1)
    )
    previous_close = close.shift(1)
    gap_risk = float(((close - previous_close).abs() / previous_close).rolling(5, min_periods=2).max().iloc[-1] or 0.0)
    latest_timestamp = pd.Timestamp(bars["last_timestamp"].iloc[-1])
    age_days = max((pd.Timestamp.utcnow() - latest_timestamp).days, 0)
    trend_alignment_flag = bool(
        pd.notna(ema_fast.iloc[-1])
        and pd.notna(ema_medium.iloc[-1])
        and pd.notna(ema_slow.iloc[-1])
        and (
            (ema_fast.iloc[-1] > ema_medium.iloc[-1] > ema_slow.iloc[-1])
            or (ema_fast.iloc[-1] < ema_medium.iloc[-1] < ema_slow.iloc[-1])
        )
    )
    data_quality_flag = bool(len(bars) < min_required or age_days > settings["stale_days"])

    return {
        "symbol": symbol,
        "last_timestamp": latest_timestamp.isoformat(),
        "close": latest_close,
        "volume": int(volume.iloc[-1]) if pd.notna(volume.iloc[-1]) else 0,
        "ema_fast": float(ema_fast.iloc[-1]) if pd.notna(ema_fast.iloc[-1]) else latest_close,
        "ema_medium": float(ema_medium.iloc[-1]) if pd.notna(ema_medium.iloc[-1]) else latest_close,
        "ema_slow": float(ema_slow.iloc[-1]) if pd.notna(ema_slow.iloc[-1]) else latest_close,
        "ema_medium_slope": ema_medium_slope,
        "rsi": float(rsi) if pd.notna(rsi) else 50.0,
        "atr_pct": atr_pct,
        "realized_vol": realized_vol,
        "volume_ratio": volume_ratio,
        "momentum_return": momentum_return,
        "macd_proxy": macd_proxy,
        "range_position": float(range_position),
        "breakout_flag": bool(breakout_flag),
        "breakdown_flag": bool(breakdown_flag),
        "stretch_pct": stretch_pct,
        "overbought_flag": bool(overbought_flag),
        "oversold_flag": bool(oversold_flag),
        "swing_structure": swing_structure,
        "volume_pressure": volume_pressure,
        "gap_risk": gap_risk,
        "trend_alignment_flag": trend_alignment_flag,
        "data_quality_flag": data_quality_flag,
    }


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


def _build_signal_rows(metrics: pd.DataFrame, timeframe: str, snapshot_date: str) -> list[dict]:
    rows: list[dict] = []
    for _, row in metrics.iterrows():
        rows.append(
            {
                "snapshot_date": snapshot_date,
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
    snapshot_date: str,
    prior_scores: pd.DataFrame,
) -> list[dict]:
    ranked = metrics.sort_values(["final_score", "symbol"], ascending=[False, True]).copy()
    ranked["bull_rank"] = range(1, len(ranked) + 1)
    ranked = ranked.sort_values(["final_score", "symbol"], ascending=[True, True]).copy()
    ranked["bear_rank"] = range(1, len(ranked) + 1)
    ranked = ranked.sort_values("symbol").copy()
    prior_lookup = _build_prior_score_lookup(prior_scores, timeframe, snapshot_date)

    rows: list[dict] = []
    for _, row in ranked.iterrows():
        previous_1w = prior_lookup.get((row["symbol"], 7))
        previous_1m = prior_lookup.get((row["symbol"], 30))
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


def _build_prior_score_lookup(prior_scores: pd.DataFrame, timeframe: str, snapshot_date: str) -> dict[tuple[str, int], float]:
    if prior_scores.empty:
        return {}
    current_date = date.fromisoformat(snapshot_date)
    timeframe_rows = prior_scores.loc[prior_scores["timeframe"] == timeframe].copy()
    if timeframe_rows.empty:
        return {}
    lookup: dict[tuple[str, int], float] = {}
    for symbol, symbol_rows in timeframe_rows.groupby("symbol", sort=True):
        symbol_rows = symbol_rows.sort_values("snapshot_date")
        for delta_days in (7, 30):
            target_date = current_date - timedelta(days=delta_days)
            eligible = symbol_rows.loc[symbol_rows["snapshot_date"] <= target_date]
            if not eligible.empty:
                lookup[(str(symbol), delta_days)] = float(eligible.iloc[-1]["final_score"])
    return lookup


def _build_breadth_row(metrics: pd.DataFrame, timeframe: str, snapshot_date: str) -> dict:
    total = len(metrics)
    bullish_count = int(metrics["final_score"].ge(65).sum())
    bearish_count = int(metrics["final_score"].le(35).sum())
    neutral_count = max(total - bullish_count - bearish_count, 0)
    median_score = float(metrics["final_score"].median()) if total else 0.0
    return {
        "snapshot_date": snapshot_date,
        "timeframe": timeframe,
        "universe_size": total,
        "bullish_count": bullish_count,
        "neutral_count": neutral_count,
        "bearish_count": bearish_count,
        "pct_above_ema20": _boolean_pct(metrics.get("above_ema20")),
        "pct_above_ema50": _boolean_pct(metrics.get("above_ema50")),
        "pct_above_ema200": _boolean_pct(metrics.get("above_ema200")),
        "pct_new_20d_high": _boolean_pct(metrics.get("new_20d_high")),
        "pct_new_20d_low": _boolean_pct(metrics.get("new_20d_low")),
        "pct_near_52w_high": _boolean_pct(metrics.get("near_52w_high")),
        "pct_near_52w_low": _boolean_pct(metrics.get("near_52w_low")),
        "avg_final_score": round(float(metrics["final_score"].mean()) if total else 0.0, 4),
        "median_final_score": round(median_score, 4),
    }


def _boolean_pct(series: pd.Series | None) -> float:
    if series is None or len(series) == 0:
        return 0.0
    return round(float(series.fillna(False).astype(bool).mean() * 100), 4)
