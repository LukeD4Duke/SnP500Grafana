"""Generate Grafana dashboard JSON from analytics snapshot tables."""

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "grafana" / "dashboards"
DATASOURCE = {"type": "postgres", "uid": "timescaledb"}
TIME_RANGE = {"from": "now-1y", "to": "now"}
ALL_SENTINEL = "__all"
GRAFANA_ALL_SENTINEL = "$__all"
REPORT_UI_PUBLIC_URL = os.environ.get("REPORT_UI_PUBLIC_URL", "http://localhost:3002").rstrip("/")
TIMEFRAME_VALUES = ["daily", "weekly", "monthly"]
REPORT_KIND_VALUES = ["weekly", "monthly"]
SECTOR_VALUE_SQL = "'${sector}'"
REPORT_KIND_FROM_TIMEFRAME_SQL = "CASE WHEN '${timeframe}' IN ('weekly', 'monthly') THEN '${timeframe}' ELSE '__none__' END"
SECTOR_FILTER_SQL = (
    f"({SECTOR_VALUE_SQL} IN ('{ALL_SENTINEL}', '{GRAFANA_ALL_SENTINEL}', 'All') "
    f"OR COALESCE(t.sector, '') = {SECTOR_VALUE_SQL})"
)
TICKER_VARIABLE_SECTOR_FILTER_SQL = (
    f"({SECTOR_VALUE_SQL} IN ('{ALL_SENTINEL}', '{GRAFANA_ALL_SENTINEL}', 'All') "
    f"OR COALESCE(sector, '') = {SECTOR_VALUE_SQL})"
)


@dataclass(frozen=True)
class DashboardFile:
    filename: str
    builder: Callable[[], dict]


@dataclass(frozen=True)
class AnalyticsDashboardSpec:
    uid: str
    title: str
    tags: list[str]
    bar_title: str
    bar_sql: str
    table_title: str
    table_sql: str


def base_dashboard(title: str, uid: str, tags: list[str]) -> dict:
    return {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": dashboard_links(uid),
        "liveNow": False,
        "panels": [],
        "refresh": "30s",
        "schemaVersion": 39,
        "style": "dark",
        "tags": tags,
        "templating": {"list": []},
        "time": TIME_RANGE,
        "timepicker": {},
        "timezone": "browser",
        "title": title,
        "uid": uid,
        "version": 1,
        "weekStart": "",
    }


def dashboard_link(uid: str, title: str) -> dict:
    return {
        "asDropdown": False,
        "icon": "dashboard",
        "includeVars": True,
        "keepTime": False,
        "targetBlank": False,
        "title": title,
        "type": "link",
        "url": f"/d/{uid}",
    }


def dashboard_links(current_uid: str) -> list[dict]:
    all_links = [
        ("sp500-ticker-detail", "Ticker Detail"),
        ("sp500-leaderboards", "Leaderboards"),
        ("sp500-trend-regime", "Trend Regime"),
        ("sp500-momentum", "Momentum"),
        ("sp500-volatility-risk", "Volatility and Risk"),
        ("sp500-volume-confirmation", "Volume Confirmation"),
        ("sp500-breakout-breakdown", "Breakout and Breakdown"),
        ("sp500-market-structure", "Market Structure"),
        ("sp500-relative-strength", "Relative Strength"),
        ("sp500-mean-reversion", "Mean Reversion"),
    ]
    return [dashboard_link(uid, title) for uid, title in all_links if uid != current_uid]


def query_variable(
    name: str,
    label: str,
    query: str,
    refresh: int,
    *,
    include_all: bool = True,
    hide: int = 0,
) -> dict:
    variable = {
        "current": {"selected": True, "text": "All", "value": ALL_SENTINEL} if include_all else {},
        "datasource": DATASOURCE,
        "definition": query,
        "hide": hide,
        "includeAll": include_all,
        "label": label,
        "multi": False,
        "name": name,
        "options": [],
        "query": query,
        "refresh": refresh,
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,
        "type": "query",
    }
    if include_all:
        variable["allValue"] = ALL_SENTINEL
    return variable


def custom_variable(name: str, label: str, values: list[str], current: str) -> dict:
    return {
        "current": {"selected": True, "text": current, "value": current},
        "definition": ",".join(values),
        "hide": 0,
        "includeAll": False,
        "label": label,
        "multi": False,
        "name": name,
        "options": [{"selected": value == current, "text": value, "value": value} for value in values],
        "query": ",".join(values),
        "refresh": 0,
        "skipUrlSync": False,
        "sort": 0,
        "type": "custom",
    }


def analytics_variables(
    *,
    include_ticker: bool = False,
    include_sector: bool = True,
    include_report_kind: bool = False,
) -> list[dict]:
    variables = [custom_variable("timeframe", "Timeframe", TIMEFRAME_VALUES, "daily")]
    if include_sector:
        variables.append(
            query_variable(
                "sector",
                "Sector",
                "SELECT DISTINCT sector FROM tickers WHERE sector IS NOT NULL AND sector <> '' ORDER BY 1",
                1,
            )
        )
    if include_ticker:
        variables.append(
            query_variable(
                "ticker",
                "Ticker",
                (
                    "SELECT name || ' (' || symbol || ')' AS __text, symbol AS __value "
                    "FROM tickers "
                    f"WHERE {TICKER_VARIABLE_SECTOR_FILTER_SQL} "
                    "ORDER BY symbol"
                ),
                2,
                include_all=False,
            )
        )
    if include_report_kind:
        variables.append(custom_variable("report_kind", "Report", REPORT_KIND_VALUES, "weekly"))
    return variables


def latest_snapshot_cte(table: str, *, filters: list[str] | None = None) -> str:
    where_clause = ""
    if filters:
        where_clause = " WHERE " + " AND ".join(filters)
    return (
        "WITH latest_snapshot AS (\n"
        f"    SELECT MAX(snapshot_date) AS snapshot_date FROM {table}{where_clause}\n"
        ")\n"
    )


def latest_signal_sql(
    select_sql: str,
    *,
    extra_filters: list[str] | None = None,
    order_by: str = "",
    limit: int | None = None,
) -> str:
    filters = ["ss.timeframe = '${timeframe}'", SECTOR_FILTER_SQL]
    if extra_filters:
        filters.extend(extra_filters)
    sql = (
        latest_snapshot_cte("signal_snapshots", filters=["timeframe = '${timeframe}'"])
        + "SELECT "
        + select_sql
        + "\nFROM signal_snapshots ss\n"
        + "JOIN latest_snapshot ls ON ls.snapshot_date = ss.snapshot_date\n"
        + "LEFT JOIN tickers t ON t.symbol = ss.symbol\n"
        + "WHERE "
        + " AND ".join(filters)
        + "\n"
    )
    if order_by:
        sql += f"ORDER BY {order_by}\n"
    if limit is not None:
        sql += f"LIMIT {limit}"
    return sql


def latest_rank_sql(
    select_sql: str,
    *,
    extra_filters: list[str] | None = None,
    order_by: str = "",
    limit: int | None = None,
) -> str:
    filters = ["rs.timeframe = '${timeframe}'", SECTOR_FILTER_SQL]
    if extra_filters:
        filters.extend(extra_filters)
    sql = (
        latest_snapshot_cte("rank_snapshots", filters=["timeframe = '${timeframe}'"])
        + "SELECT "
        + select_sql
        + "\nFROM rank_snapshots rs\n"
        + "JOIN latest_snapshot ls ON ls.snapshot_date = rs.snapshot_date\n"
        + "LEFT JOIN tickers t ON t.symbol = rs.symbol\n"
        + "WHERE "
        + " AND ".join(filters)
        + "\n"
    )
    if order_by:
        sql += f"ORDER BY {order_by}\n"
    if limit is not None:
        sql += f"LIMIT {limit}"
    return sql


def latest_breadth_sql(select_sql: str) -> str:
    return (
        latest_snapshot_cte("market_breadth_snapshots", filters=["timeframe = '${timeframe}'"])
        + "SELECT "
        + select_sql
        + "\nFROM market_breadth_snapshots mbs\n"
        + "JOIN latest_snapshot ls ON ls.snapshot_date = mbs.snapshot_date\n"
        + "WHERE mbs.timeframe = '${timeframe}'"
    )


def latest_report_sql(select_sql: str) -> str:
    return (
        latest_snapshot_cte(
            "report_snapshots",
            filters=["timeframe = '${timeframe}'", f"report_kind = {REPORT_KIND_FROM_TIMEFRAME_SQL}"],
        )
        + "SELECT "
        + select_sql
        + "\nFROM report_snapshots rs\n"
        + "JOIN latest_snapshot ls ON ls.snapshot_date = rs.snapshot_date\n"
        + f"WHERE rs.timeframe = '${{timeframe}}' AND rs.report_kind = {REPORT_KIND_FROM_TIMEFRAME_SQL}"
    )


def ticker_link_override(field_name: str = "ticker") -> dict:
    return {
        "matcher": {"id": "byName", "options": field_name},
        "properties": [
            {
                "id": "links",
                "value": [
                    {
                        "targetBlank": False,
                        "title": "Open ticker detail",
                        "url": "/d/sp500-ticker-detail?var-ticker=${__data.fields.ticker}",
                    }
                ],
            }
        ],
    }


def markdown_cell_override(field_name: str, *, align: str = "center") -> dict:
    return {
        "matcher": {"id": "byName", "options": field_name},
        "properties": [
            {"id": "custom.cellOptions", "value": {"type": "markdown"}},
            {"id": "custom.align", "value": align},
        ],
    }


def fixed_color_override(field_name: str, color: str) -> dict:
    return {
        "matcher": {"id": "byName", "options": field_name},
        "properties": [{"id": "color", "value": {"fixedColor": color, "mode": "fixed"}}],
    }


def table_panel(
    *,
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    show_header: bool = True,
    overrides: list[dict] | None = None,
    repeat_variable: str | None = None,
) -> dict:
    panel = {
        "datasource": DATASOURCE,
        "fieldConfig": {
            "defaults": {
                "custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
            },
            "overrides": overrides or [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {"cellHeight": "sm", "footer": {"show": False}, "showHeader": show_header},
        "targets": [
            {
                "datasource": DATASOURCE,
                "editorMode": "code",
                "format": "table",
                "rawQuery": True,
                "rawSql": sql,
                "refId": "A",
            }
        ],
        "title": title,
        "type": "table",
    }
    if repeat_variable:
        panel["repeat"] = repeat_variable
        panel["repeatDirection"] = "h"
        panel["maxPerRow"] = 1
    return panel


def text_panel(
    *,
    panel_id: int,
    title: str,
    content: str,
    x: int,
    y: int,
    w: int,
    h: int,
    mode: str = "html",
) -> dict:
    return {
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {"content": content, "mode": mode},
        "transparent": True,
        "title": title,
        "type": "text",
    }


def stat_panel(
    *,
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "none",
    decimals: int | None = None,
) -> dict:
    defaults: dict[str, object] = {
        "color": {"mode": "thresholds"},
        "mappings": [],
        "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
        "unit": unit,
    }
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "datasource": DATASOURCE,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "center",
            "orientation": "auto",
            "percentChangeColorMode": "standard",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "showPercentChange": False,
            "textMode": "auto",
            "wideLayout": True,
        },
        "targets": [
            {
                "datasource": DATASOURCE,
                "editorMode": "code",
                "format": "table",
                "rawQuery": True,
                "rawSql": sql,
                "refId": "A",
            }
        ],
        "title": title,
        "type": "stat",
    }


def link_override(field_name: str, title: str) -> dict:
    return {
        "matcher": {"id": "byName", "options": field_name},
        "properties": [
            {
                "id": "links",
                "value": [
                    {
                        "targetBlank": True,
                        "title": title,
                        "url": "${__value.raw}",
                    }
                ],
            }
        ],
    }


def timeseries_panel(
    *,
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "none",
    overrides: list[dict] | None = None,
) -> dict:
    return {
        "datasource": DATASOURCE,
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {
                    "axisCenteredZero": False,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "drawStyle": "line",
                    "fillOpacity": 12,
                    "gradientMode": "none",
                    "hideFrom": {"legend": False, "tooltip": False, "viz": False},
                    "lineInterpolation": "smooth",
                    "lineWidth": 2,
                    "pointSize": 4,
                    "scaleDistribution": {"type": "linear"},
                    "showPoints": "auto",
                    "spanNulls": True,
                    "stacking": {"group": "A", "mode": "none"},
                    "thresholdsStyle": {"mode": "off"},
                },
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
                "unit": unit,
            },
            "overrides": overrides or [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "single", "sort": "none"},
        },
        "targets": [
            {
                "datasource": DATASOURCE,
                "editorMode": "code",
                "format": "time_series",
                "rawQuery": True,
                "rawSql": sql,
                "refId": "A",
            }
        ],
        "title": title,
        "type": "timeseries",
    }


def pie_chart_panel(
    *,
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    overrides: list[dict] | None = None,
) -> dict:
    return {
        "datasource": DATASOURCE,
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
            },
            "overrides": overrides or [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "displayLabels": ["name", "value", "percent"],
            "legend": {"displayMode": "list", "placement": "right", "showLegend": True, "values": ["value"]},
            "pieType": "pie",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "tooltip": {"mode": "single", "sort": "none"},
        },
        "targets": [
            {
                "datasource": DATASOURCE,
                "editorMode": "code",
                "format": "table",
                "rawQuery": True,
                "rawSql": sql,
                "refId": "A",
            }
        ],
        "title": title,
        "type": "piechart",
    }


def bar_chart_panel(
    *,
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    x_field: str,
    overrides: list[dict] | None = None,
    show_value: str = "auto",
    value_text_size: int | None = None,
) -> dict:
    options = {
        "barRadius": 0,
        "barWidth": 0.9,
        "colorByField": None,
        "fullHighlight": False,
        "groupWidth": 0.8,
        "legend": {"displayMode": "list", "placement": "bottom", "showLegend": False},
        "orientation": "vertical",
        "showValue": show_value,
        "stacking": "none",
        "tooltip": {"mode": "single", "sort": "none"},
        "xField": x_field,
        "xTickLabelMaxLength": 12,
        "xTickLabelRotation": 30,
        "xTickLabelSpacing": 0,
    }
    if value_text_size is not None:
        options["text"] = {"valueSize": value_text_size}

    return {
        "datasource": DATASOURCE,
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {
                    "axisBorderShow": False,
                    "axisCenteredZero": False,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "fillOpacity": 80,
                    "gradientMode": "none",
                    "hideFrom": {"legend": False, "tooltip": False, "viz": False},
                    "lineWidth": 1,
                    "scaleDistribution": {"type": "linear"},
                },
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
            },
            "overrides": overrides or [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": options,
        "targets": [
            {
                "datasource": DATASOURCE,
                "editorMode": "code",
                "format": "table",
                "rawQuery": True,
                "rawSql": sql,
                "refId": "A",
            }
        ],
        "title": title,
        "type": "barchart",
    }


def market_overview_panels() -> list[dict]:
    return [
        stat_panel(
            panel_id=1,
            title="Average Final Score",
            sql=latest_breadth_sql("avg_final_score AS value"),
            x=0,
            y=0,
            w=6,
            h=4,
            decimals=1,
        ),
        stat_panel(
            panel_id=2,
            title="Bullish Count",
            sql=latest_breadth_sql("bullish_count AS value"),
            x=6,
            y=0,
            w=6,
            h=4,
        ),
        stat_panel(
            panel_id=3,
            title="Bearish Count",
            sql=latest_breadth_sql("bearish_count AS value"),
            x=12,
            y=0,
            w=6,
            h=4,
        ),
        stat_panel(
            panel_id=4,
            title="% Above EMA50",
            sql=latest_breadth_sql("pct_above_ema50 AS value"),
            x=18,
            y=0,
            w=6,
            h=4,
            unit="percentunit",
            decimals=2,
        ),
    ]


def ticker_detail_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Ticker Detail",
        "sp500-ticker-detail",
        ["sp500", "ticker-detail", "generated"],
    )
    dashboard["templating"]["list"] = analytics_variables(include_ticker=True)
    dashboard["panels"] = [
        table_panel(
            panel_id=1,
            title="Company",
            sql=(
                "SELECT '<div style=\"font-size:26px;font-weight:700;text-align:center;line-height:1.2;"
                "color:#4ea1ff;\">' || COALESCE(name, symbol) || '</div>' AS company "
                "FROM tickers WHERE symbol = '${ticker}'"
            ),
            x=0,
            y=0,
            w=6,
            h=4,
            show_header=False,
            overrides=[markdown_cell_override("company")],
        ),
        table_panel(
            panel_id=2,
            title="Last Close",
            sql=(
                "SELECT '<div style=\"font-size:28px;font-weight:700;text-align:center;color:#73BF69;\">$' "
                "|| TO_CHAR(close, 'FM999,999,999,990.0') || '</div>' AS last_close "
                "FROM stock_prices WHERE symbol = '${ticker}' ORDER BY timestamp DESC LIMIT 1"
            ),
            x=6,
            y=0,
            w=6,
            h=4,
            show_header=False,
            overrides=[markdown_cell_override("last_close")],
        ),
        table_panel(
            panel_id=3,
            title="Final Score",
            sql=(
                "SELECT "
                "CASE "
                "WHEN recommendation_label IN ('bullish', 'bullish watch') THEN "
                "    '<div style=\"font-size:28px;font-weight:700;text-align:center;color:#2da44e;\">' "
                "    || ROUND(final_score::numeric, 1)::text || '</div>' "
                "WHEN recommendation_label = 'neutral' THEN "
                "    '<div style=\"font-size:28px;font-weight:700;text-align:center;color:#c9d1d9;\">' "
                "    || ROUND(final_score::numeric, 1)::text || '</div>' "
                "ELSE "
                "    '<div style=\"font-size:28px;font-weight:700;text-align:center;color:#f85149;\">' "
                "    || ROUND(final_score::numeric, 1)::text || '</div>' "
                "END AS final_score "
                "FROM signal_snapshots "
                "WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' "
                "ORDER BY snapshot_date DESC LIMIT 1"
            ),
            x=12,
            y=0,
            w=6,
            h=4,
            show_header=False,
            overrides=[markdown_cell_override("final_score")],
        ),
        table_panel(
            panel_id=4,
            title="Recommendation",
            sql=(
                "SELECT "
                "'<div style=\"font-size:14px;line-height:1.35;white-space:normal;overflow:visible;\">"
                "<strong>Recommendation:</strong> ' || recommendation_label "
                "|| ' | <strong>Regime:</strong> ' || regime_label "
                "|| ' | <strong>Volatility:</strong> ' || volatility_state "
                "|| '</div>' AS recommendation "
                "FROM signal_snapshots "
                "WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' "
                "ORDER BY snapshot_date DESC LIMIT 1"
            ),
            x=18,
            y=0,
            w=6,
            h=4,
            show_header=False,
            overrides=[markdown_cell_override("recommendation", align="left")],
        ),
        timeseries_panel(
            panel_id=5,
            title="Close Price",
            sql=(
                "SELECT timestamp AS time, close AS \"Close\" "
                "FROM stock_prices WHERE symbol = '${ticker}' AND $__timeFilter(timestamp) ORDER BY timestamp"
            ),
            x=0,
            y=4,
            w=24,
            h=8,
            unit="currencyUSD",
        ),
        timeseries_panel(
            panel_id=6,
            title="Score History",
            sql=(
                "SELECT snapshot_date::timestamp AS time, final_score AS \"Final Score\", trend_score AS \"Trend\", "
                "momentum_score AS \"Momentum\", relative_strength_score AS \"Relative Strength\" "
                "FROM signal_snapshots "
                "WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' "
                "ORDER BY snapshot_date"
            ),
            x=0,
            y=12,
            w=24,
            h=8,
        ),
        table_panel(
            panel_id=7,
            title="Analytics Snapshot",
            sql=(
                "SELECT snapshot_date, timeframe, close, volume, final_score, trend_score, momentum_score, "
                "volume_score, relative_strength_score, structure_score, mean_reversion_score, "
                "volatility_risk_score, risk_penalty, regime_label, recommendation_label, "
                "breakout_flag, breakdown_flag, overbought_flag, oversold_flag, trend_alignment_flag, "
                "data_quality_flag "
                "FROM signal_snapshots "
                "WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' "
                "ORDER BY snapshot_date DESC LIMIT 1"
            ),
            x=0,
            y=20,
            w=24,
            h=8,
        ),
        table_panel(
            panel_id=8,
            title="Key Drivers",
            sql=(
                "WITH latest_signal AS ("
                "    SELECT snapshot_date, drivers_json "
                "    FROM signal_snapshots "
                "    WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' "
                "    ORDER BY snapshot_date DESC LIMIT 1"
                ") "
                "SELECT "
                "    latest_signal.snapshot_date, "
                "    COALESCE(driver.value->>'label', driver.value->>'metric', driver.value->>'name', 'driver') AS driver, "
                "    COALESCE(NULLIF(driver.value->>'score', ''), driver.value->>'value', '') AS score, "
                "    CASE COALESCE(driver.value->>'key', '') "
                "        WHEN 'trend_score' THEN 'Direction and persistence strength' "
                "        WHEN 'momentum_score' THEN 'Speed of recent price move' "
                "        WHEN 'relative_strength_score' THEN 'Performance versus peer stocks' "
                "        WHEN 'rsi' THEN 'Overbought or oversold pressure' "
                "        WHEN 'volume_ratio' THEN 'Trading activity versus normal' "
                "        WHEN 'atr_pct' THEN 'Volatility relative to price' "
                "        ELSE 'Short driver summary' "
                "    END AS explanation, "
                "    COALESCE(driver.value->>'state', '') AS state, "
                "    COALESCE(driver.value->>'detail', driver.value->>'explanation', '') AS detail "
                "FROM latest_signal "
                "CROSS JOIN LATERAL jsonb_array_elements(latest_signal.drivers_json) AS driver(value)"
            ),
            x=0,
            y=28,
            w=24,
            h=8,
        ),
    ]
    return dashboard


def leaderboard_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Leaderboards",
        "sp500-leaderboards",
        ["sp500", "leaderboards", "generated"],
    )
    dashboard["templating"]["list"] = analytics_variables(include_sector=True)
    dashboard["templating"]["list"].append(
        query_variable(
            "report_snapshot_visible",
            "Report Snapshot Visible",
            "SELECT 'show' WHERE '${timeframe}' IN ('weekly', 'monthly')",
            2,
            include_all=False,
            hide=2,
        )
    )
    dashboard["panels"] = [
        text_panel(
            panel_id=1,
            title="Monthly Report",
            content=(
                "<div style=\"display:flex;flex-direction:column;gap:12px;height:100%;justify-content:center;"
                "padding:8px 4px;\">"
                "<div style=\"font-size:18px;font-weight:700;letter-spacing:0.01em;\">Generate Monthly Report</div>"
                "<div style=\"color:#a5adba;line-height:1.4;\">"
                "Open the report UI, generate the latest monthly export, and download HTML or PDF artifacts."
                "</div>"
                f"<a href=\"{REPORT_UI_PUBLIC_URL}/monthly-report?autostart=1\" "
                "style=\"display:inline-flex;align-items:center;justify-content:center;gap:8px;"
                "padding:12px 16px;border-radius:10px;background:linear-gradient(135deg,#1f78c1,#165a96);"
                "color:#ffffff;font-weight:700;text-decoration:none;width:max-content;\">"
                "Generate Monthly Report"
                "</a>"
                "</div>"
            ),
            x=0,
            y=26,
            w=8,
            h=5,
        ),
        table_panel(
            panel_id=2,
            title="Latest Manual Export",
            sql=(
                "WITH latest_job AS ("
                "    SELECT job_id, report_kind, timeframe, scope, status, snapshot_date, created_at, started_at, "
                "           completed_at, error_message, html_download_url, pdf_download_url "
                "    FROM report_export_jobs "
                "    WHERE report_kind = 'monthly' "
                "      AND timeframe = 'monthly' "
                "      AND scope = 'full_market' "
                "    ORDER BY created_at DESC, job_id DESC "
                "    LIMIT 1"
                ") "
                "SELECT job_id, report_kind, timeframe, scope, status, snapshot_date, created_at, started_at, "
                "       completed_at, error_message, html_download_url, pdf_download_url "
                "FROM latest_job"
            ),
            x=8,
            y=26,
            w=16,
            h=5,
            overrides=[
                link_override("html_download_url", "Download HTML"),
                link_override("pdf_download_url", "Download PDF"),
            ],
        ),
        bar_chart_panel(
            panel_id=3,
            title="Top Bullish Ranks",
            sql=latest_rank_sql(
                "rs.symbol AS ticker, rs.final_score AS score",
                order_by="rs.bull_rank ASC, rs.symbol",
                limit=20,
            ),
            x=0,
            y=5,
            w=12,
            h=9,
            x_field="ticker",
            show_value="always",
            value_text_size=10,
        ),
        bar_chart_panel(
            panel_id=4,
            title="Top Bearish Ranks",
            sql=latest_rank_sql(
                "rs.symbol AS ticker, rs.final_score AS score",
                order_by="rs.bear_rank ASC, rs.symbol",
                limit=20,
            ),
            x=12,
            y=5,
            w=12,
            h=9,
            x_field="ticker",
            overrides=[fixed_color_override("score", "red")],
            show_value="always",
            value_text_size=10,
        ),
        table_panel(
            panel_id=5,
            title="Bullish Table",
            sql=latest_rank_sql(
                "rs.symbol AS ticker, COALESCE(t.name, rs.symbol) AS company_name, rs.bull_rank, rs.final_score, "
                "rs.score_change_1w, rs.score_change_1m, rs.regime_label, rs.recommendation_label",
                order_by="rs.bull_rank ASC, rs.symbol",
                limit=25,
            ),
            x=0,
            y=14,
            w=12,
            h=12,
            overrides=[ticker_link_override()],
        ),
        table_panel(
            panel_id=6,
            title="Bearish Table",
            sql=latest_rank_sql(
                "rs.symbol AS ticker, COALESCE(t.name, rs.symbol) AS company_name, rs.bear_rank, rs.final_score, "
                "rs.score_change_1w, rs.score_change_1m, rs.regime_label, rs.recommendation_label",
                order_by="rs.bear_rank ASC, rs.symbol",
                limit=25,
            ),
            x=12,
            y=14,
            w=12,
            h=12,
            overrides=[ticker_link_override()],
        ),
        table_panel(
            panel_id=7,
            title="Latest Report Snapshot",
            sql=(
                latest_report_sql("rs.report_kind, rs.title, rs.summary_text, rs.risk_text, rs.storage_path")
                + " AND rs.symbol = '__MARKET__'"
            ),
            x=0,
            y=31,
            w=24,
            h=7,
            repeat_variable="report_snapshot_visible",
        ),
        pie_chart_panel(
            panel_id=8,
            title="Setup Distribution",
            sql=(
                "SELECT bullish_count AS \"Bullish\", neutral_count AS \"Neutral\", bearish_count AS \"Bearish\" "
                "FROM ("
                + latest_breadth_sql("bullish_count, neutral_count, bearish_count")
                + ") breadth"
            ),
            x=0,
            y=0,
            w=8,
            h=5,
            overrides=[fixed_color_override("Bearish", "red")],
        ),
        timeseries_panel(
            panel_id=9,
            title="Composite Score History",
            sql=(
                "SELECT snapshot_date::timestamp AS time, avg_final_score AS \"Average\", "
                "median_final_score AS \"Median\" "
                "FROM market_breadth_snapshots "
                "WHERE timeframe = '${timeframe}' "
                "  AND $__timeFilter(snapshot_date::timestamp) "
                "ORDER BY snapshot_date"
            ),
            x=8,
            y=0,
            w=16,
            h=5,
            overrides=[
                fixed_color_override("Average", "#4EA1FF"),
                fixed_color_override("Median", "#8AB8FF"),
            ],
        ),
    ]
    return dashboard


def analytics_family_dashboard(spec: AnalyticsDashboardSpec) -> dict:
    dashboard = base_dashboard(spec.title, spec.uid, spec.tags)
    dashboard["templating"]["list"] = analytics_variables()
    dashboard["panels"] = market_overview_panels() + [
        bar_chart_panel(
            panel_id=5,
            title=spec.bar_title,
            sql=spec.bar_sql,
            x=0,
            y=4,
            w=24,
            h=9,
            x_field="ticker",
        ),
        table_panel(
            panel_id=6,
            title=spec.table_title,
            sql=spec.table_sql,
            x=0,
            y=13,
            w=24,
            h=13,
            overrides=[ticker_link_override()],
        ),
    ]
    return dashboard


def trend_regime_dashboard() -> dict:
    return analytics_family_dashboard(
        AnalyticsDashboardSpec(
            uid="sp500-trend-regime",
            title="S&P 500 Trend Regime",
            tags=["sp500", "trend", "generated"],
            bar_title="Top Trend Scores",
            bar_sql=latest_signal_sql(
                "ss.symbol AS ticker, ss.trend_score AS score",
                order_by="ss.trend_score DESC, ss.symbol",
                limit=20,
            ),
            table_title="Trend Regime Table",
            table_sql=latest_signal_sql(
                "ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.trend_score, "
                "ss.trend_state, ss.regime_label, ss.recommendation_label, ss.final_score",
                order_by="ss.trend_score DESC, ss.symbol",
                limit=50,
            ),
        )
    )


def momentum_dashboard() -> dict:
    return analytics_family_dashboard(
        AnalyticsDashboardSpec(
            uid="sp500-momentum",
            title="S&P 500 Momentum",
            tags=["sp500", "momentum", "generated"],
            bar_title="Top Momentum Scores",
            bar_sql=latest_signal_sql(
                "ss.symbol AS ticker, ss.momentum_score AS score",
                order_by="ss.momentum_score DESC, ss.symbol",
                limit=20,
            ),
            table_title="Overbought and Oversold",
            table_sql=latest_signal_sql(
                "ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.momentum_score, "
                "ss.momentum_state, ss.mean_reversion_score, ss.overbought_flag, ss.oversold_flag, "
                "ss.recommendation_label",
                extra_filters=["(ss.overbought_flag OR ss.oversold_flag)"],
                order_by="ss.momentum_score DESC, ss.symbol",
                limit=50,
            ),
        )
    )


def volatility_risk_dashboard() -> dict:
    return analytics_family_dashboard(
        AnalyticsDashboardSpec(
            uid="sp500-volatility-risk",
            title="S&P 500 Volatility and Risk",
            tags=["sp500", "risk", "generated"],
            bar_title="Highest Volatility Risk",
            bar_sql=latest_signal_sql(
                "ss.symbol AS ticker, ss.volatility_risk_score AS score",
                order_by="ss.volatility_risk_score DESC, ss.symbol",
                limit=20,
            ),
            table_title="Risk Table",
            table_sql=latest_signal_sql(
                "ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.volatility_risk_score, "
                "ss.risk_penalty, ss.volatility_state, ss.data_quality_flag, ss.final_score, "
                "ss.recommendation_label",
                order_by="ss.volatility_risk_score DESC, ss.symbol",
                limit=50,
            ),
        )
    )


def volume_confirmation_dashboard() -> dict:
    return analytics_family_dashboard(
        AnalyticsDashboardSpec(
            uid="sp500-volume-confirmation",
            title="S&P 500 Volume Confirmation",
            tags=["sp500", "volume", "generated"],
            bar_title="Top Volume Scores",
            bar_sql=latest_signal_sql(
                "ss.symbol AS ticker, ss.volume_score AS score",
                order_by="ss.volume_score DESC, ss.symbol",
                limit=20,
            ),
            table_title="Volume Confirmation Table",
            table_sql=latest_signal_sql(
                "ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.volume_score, "
                "ss.volume_state, ss.breakout_flag, ss.trend_alignment_flag, ss.final_score, "
                "ss.recommendation_label",
                order_by="ss.volume_score DESC, ss.symbol",
                limit=50,
            ),
        )
    )


def breakout_breakdown_dashboard() -> dict:
    return analytics_family_dashboard(
        AnalyticsDashboardSpec(
            uid="sp500-breakout-breakdown",
            title="S&P 500 Breakout Breakdown",
            tags=["sp500", "breakout", "generated"],
            bar_title="Breakout Candidates",
            bar_sql=latest_signal_sql(
                "ss.symbol AS ticker, ss.final_score AS score",
                extra_filters=["ss.breakout_flag"],
                order_by="ss.final_score DESC, ss.symbol",
                limit=20,
            ),
            table_title="Breakout and Breakdown Table",
            table_sql=latest_signal_sql(
                "ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.breakout_flag, "
                "ss.breakdown_flag, ss.trend_alignment_flag, ss.trend_score, ss.volume_score, "
                "ss.final_score, ss.recommendation_label",
                extra_filters=["(ss.breakout_flag OR ss.breakdown_flag)"],
                order_by="ss.final_score DESC, ss.symbol",
                limit=50,
            ),
        )
    )


def market_structure_dashboard() -> dict:
    return analytics_family_dashboard(
        AnalyticsDashboardSpec(
            uid="sp500-market-structure",
            title="S&P 500 Market Structure",
            tags=["sp500", "structure", "generated"],
            bar_title="Top Structure Scores",
            bar_sql=latest_signal_sql(
                "ss.symbol AS ticker, ss.structure_score AS score",
                order_by="ss.structure_score DESC, ss.symbol",
                limit=20,
            ),
            table_title="Structure Alignment Table",
            table_sql=latest_signal_sql(
                "ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.structure_score, "
                "ss.structure_state, ss.trend_alignment_flag, ss.regime_label, ss.final_score, "
                "ss.recommendation_label",
                order_by="ss.structure_score DESC, ss.symbol",
                limit=50,
            ),
        )
    )


def relative_strength_dashboard() -> dict:
    return analytics_family_dashboard(
        AnalyticsDashboardSpec(
            uid="sp500-relative-strength",
            title="S&P 500 Relative Strength",
            tags=["sp500", "relative-strength", "generated"],
            bar_title="Top Relative Strength",
            bar_sql=latest_signal_sql(
                "ss.symbol AS ticker, ss.relative_strength_score AS score",
                order_by="ss.relative_strength_score DESC, ss.symbol",
                limit=20,
            ),
            table_title="Relative Strength Table",
            table_sql=latest_signal_sql(
                "ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.relative_strength_score, "
                "ss.relative_strength_state, ss.trend_score, ss.momentum_score, ss.final_score, "
                "ss.recommendation_label",
                order_by="ss.relative_strength_score DESC, ss.symbol",
                limit=50,
            ),
        )
    )


def mean_reversion_dashboard() -> dict:
    return analytics_family_dashboard(
        AnalyticsDashboardSpec(
            uid="sp500-mean-reversion",
            title="S&P 500 Mean Reversion",
            tags=["sp500", "mean-reversion", "generated"],
            bar_title="Top Mean Reversion Scores",
            bar_sql=latest_signal_sql(
                "ss.symbol AS ticker, ss.mean_reversion_score AS score",
                order_by="ss.mean_reversion_score DESC, ss.symbol",
                limit=20,
            ),
            table_title="Mean Reversion Table",
            table_sql=latest_signal_sql(
                "ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.mean_reversion_score, "
                "ss.momentum_score, ss.overbought_flag, ss.oversold_flag, ss.final_score, "
                "ss.recommendation_label",
                order_by="ss.mean_reversion_score DESC, ss.symbol",
                limit=50,
            ),
        )
    )


DASHBOARD_FILES = (
    DashboardFile("sp500-ticker-detail.json", ticker_detail_dashboard),
    DashboardFile("sp500-leaderboards.json", leaderboard_dashboard),
    DashboardFile("sp500-trend-regime.json", trend_regime_dashboard),
    DashboardFile("sp500-momentum.json", momentum_dashboard),
    DashboardFile("sp500-volatility-risk.json", volatility_risk_dashboard),
    DashboardFile("sp500-volume-confirmation.json", volume_confirmation_dashboard),
    DashboardFile("sp500-breakout-breakdown.json", breakout_breakdown_dashboard),
    DashboardFile("sp500-market-structure.json", market_structure_dashboard),
    DashboardFile("sp500-relative-strength.json", relative_strength_dashboard),
    DashboardFile("sp500-mean-reversion.json", mean_reversion_dashboard),
)

EXPECTED_DASHBOARD_FILES = tuple(spec.filename for spec in DASHBOARD_FILES)


def build_dashboards() -> dict[str, dict]:
    return {spec.filename: spec.builder() for spec in DASHBOARD_FILES}


def write_dashboards(output_dir: Path = DASHBOARD_DIR) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written_paths: list[Path] = []
    for filename, dashboard in build_dashboards().items():
        path = output_dir / filename
        with path.open("w", encoding="utf-8", newline="\r\n") as handle:
            handle.write(json.dumps(dashboard, indent=2) + "\n")
        written_paths.append(path)
    return written_paths


def main() -> None:
    written_paths = write_dashboards()
    for path in written_paths:
        print(f"Wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
