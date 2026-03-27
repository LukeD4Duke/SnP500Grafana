"""Generate Grafana dashboard JSON from analytics snapshot tables."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "grafana" / "dashboards"
DATASOURCE = {"type": "postgres", "uid": "timescaledb"}
TIME_RANGE = {"from": "now-1y", "to": "now"}


def base_dashboard(title: str, uid: str, tags: list[str], links: list[dict] | None = None) -> dict:
    return {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": links or [],
        "liveNow": False,
        "panels": [],
        "refresh": "30s",
        "schemaVersion": 39,
        "style": "dark",
        "tags": tags,
        "time": TIME_RANGE,
        "timepicker": {},
        "timezone": "browser",
        "title": title,
        "uid": uid,
        "version": 1,
        "weekStart": "",
    }


def query_variable(name: str, label: str, query: str, refresh: int) -> dict:
    return {
        "current": {},
        "datasource": DATASOURCE,
        "definition": query,
        "hide": 0,
        "includeAll": False,
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


def custom_variable(name: str, label: str, options: list[str], current: str) -> dict:
    return {
        "current": {"selected": True, "text": current.title(), "value": current},
        "hide": 0,
        "includeAll": False,
        "label": label,
        "multi": False,
        "name": name,
        "options": [
            {"selected": option == current, "text": option.title(), "value": option}
            for option in options
        ],
        "query": ",".join(options),
        "skipUrlSync": False,
        "type": "custom",
    }


def _table_defaults() -> dict:
    return {
        "fieldConfig": {
            "defaults": {
                "custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
            },
            "overrides": [],
        },
        "targets": [
            {
                "datasource": DATASOURCE,
                "editorMode": "code",
                "format": "table",
                "rawQuery": True,
                "refId": "A",
            }
        ],
        "type": "table",
    }


def table_panel(*, panel_id: int, title: str, sql: str, x: int, y: int, w: int, h: int, show_header: bool = True) -> dict:
    panel = _table_defaults()
    panel["datasource"] = DATASOURCE
    panel["gridPos"] = {"h": h, "w": w, "x": x, "y": y}
    panel["id"] = panel_id
    panel["options"] = {"cellHeight": "sm", "footer": {"show": False}, "showHeader": show_header}
    panel["targets"][0]["rawSql"] = sql
    panel["title"] = title
    return panel


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
    defaults: dict = {
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
                "rawQueryText": sql,
                "rawSql": sql,
                "refId": "A",
            }
        ],
        "title": title,
        "type": "stat",
    }


def query_variable(name: str, label: str, query: str, refresh: int, include_all: bool = True) -> dict:
    variable = {
        "current": {"selected": True, "text": "All", "value": ALL_SENTINEL} if include_all else {},
        "datasource": DATASOURCE,
        "definition": query,
        "hide": 0,
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


def apply_dashboard_links(dashboard: dict, current_uid: str) -> None:
    links = [
        ("sp500-ticker-detail", "Ticker Detail"),
        ("sp500-leaderboards", "Universe Ranking"),
        ("sp500-trend-regime", "Trend Regime"),
        ("sp500-momentum", "Momentum"),
        ("sp500-volatility-risk", "Volatility and Risk"),
        ("sp500-volume-confirmation", "Volume Confirmation"),
        ("sp500-breakout-breakdown", "Breakout and Breakdown"),
        ("sp500-market-structure", "Market Structure"),
        ("sp500-relative-strength", "Relative Strength"),
        ("sp500-mean-reversion", "Mean Reversion"),
    ]
    dashboard["links"] = [dashboard_link(uid, title) for uid, title in links if uid != current_uid]


def analytics_variables(*, include_ticker: bool = False, include_sector: bool = True, include_report_kind: bool = False) -> list[dict]:
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
                    f"WHERE (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR sector IN (${{sector:sqlstring}})) "
                    "ORDER BY symbol"
                ),
                2,
                include_all=False,
            )
        )
    if include_report_kind:
        variables.append(custom_variable("report_kind", "Report", ["weekly", "monthly"], "weekly"))
    return variables


def latest_snapshot_cte(table: str) -> str:
    return (
        "WITH latest_snapshot AS (\n"
        f"    SELECT MAX(snapshot_date) AS snapshot_date FROM {table} WHERE timeframe = '${{timeframe}}'\n"
        ")\n"
    )


def latest_report_cte() -> str:
    return (
        "WITH latest_report AS (\n"
        "    SELECT MAX(snapshot_date) AS snapshot_date\n"
        "    FROM report_snapshots\n"
        "    WHERE timeframe = '${timeframe}' AND report_kind = '${report_kind}'\n"
        ")\n"
    )


def latest_signal_sql(select_sql: str, *, extra_filters: str = "", order_by: str = "", limit: int | None = None) -> str:
    sql = (
        latest_snapshot_cte("signal_snapshots")
        + "SELECT "
        + select_sql
        + "\nFROM signal_snapshots ss\n"
        + "JOIN latest_snapshot ls ON ls.snapshot_date = ss.snapshot_date\n"
        + "LEFT JOIN tickers t ON t.symbol = ss.symbol\n"
        + "WHERE ss.timeframe = '${timeframe}'\n"
        + f"  AND (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR t.sector IN (${{sector:sqlstring}}))\n"
        + extra_filters
    )
    if order_by:
        sql += f"ORDER BY {order_by}\n"
    if limit is not None:
        sql += f"LIMIT {limit}"
    return sql


def latest_rank_sql(select_sql: str, *, extra_filters: str = "", order_by: str = "", limit: int | None = None) -> str:
    sql = (
        latest_snapshot_cte("rank_snapshots")
        + "SELECT "
        + select_sql
        + "\nFROM rank_snapshots rs\n"
        + "JOIN latest_snapshot ls ON ls.snapshot_date = rs.snapshot_date\n"
        + "LEFT JOIN tickers t ON t.symbol = rs.symbol\n"
        + "WHERE rs.timeframe = '${timeframe}'\n"
        + f"  AND (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR t.sector IN (${{sector:sqlstring}}))\n"
        + extra_filters
    )
    if order_by:
        sql += f"ORDER BY {order_by}\n"
    if limit is not None:
        sql += f"LIMIT {limit}"
    return sql


def latest_breadth_sql(select_sql: str) -> str:
    return (
        latest_snapshot_cte("market_breadth_snapshots")
        + "SELECT "
        + select_sql
        + "\nFROM market_breadth_snapshots mbs\n"
        + "JOIN latest_snapshot ls ON ls.snapshot_date = mbs.snapshot_date\n"
        + "WHERE mbs.timeframe = '${timeframe}'"
    )


def ranked_bar_overrides() -> list[dict]:
    return [
        {
            "matcher": {"id": "byName", "options": "company_name"},
            "properties": [{"id": "custom.hideFrom", "value": {"legend": True, "tooltip": False, "viz": True}}],
        },
        {
            "matcher": {"id": "byName", "options": "color_code"},
            "properties": [{"id": "custom.hideFrom", "value": {"legend": True, "tooltip": True, "viz": True}}],
        },
    ]


def timeseries_panel(*, panel_id: int, title: str, sql: str, x: int, y: int, w: int, h: int, unit: str = "none") -> dict:
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
            "overrides": [],
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


def bar_chart_panel(*, panel_id: int, title: str, sql: str, x: int, y: int, w: int, h: int, x_field: str) -> dict:
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
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "barRadius": 0,
            "barWidth": 0.9,
            "colorByField": None,
            "fullHighlight": False,
            "groupWidth": 0.8,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": False},
            "orientation": "vertical",
            "showValue": "auto",
            "stacking": "none",
            "tooltip": {"mode": "single", "sort": "none"},
            "xField": x_field,
            "xTickLabelMaxLength": 12,
            "xTickLabelRotation": 30,
            "xTickLabelSpacing": 0,
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
        "type": "barchart",
    }


def dashboard_links() -> list[dict]:
    return [
        {
            "asDropdown": False,
            "icon": "dashboard",
            "includeVars": False,
            "keepTime": False,
            "targetBlank": False,
            "title": "Ticker Detail",
            "type": "link",
            "url": "/d/sp500-ticker-detail/sandp-500-ticker-detail",
        },
        {
            "asDropdown": False,
            "icon": "dashboard",
            "includeVars": False,
            "keepTime": False,
            "targetBlank": False,
            "title": "Leaderboards",
            "type": "link",
            "url": "/d/sp500-leaderboards/sandp-500-leaderboards",
        },
    ]


def latest_signal_cte() -> str:
    return (
        "WITH latest_snapshot AS (\n"
        "    SELECT MAX(snapshot_date) AS snapshot_date\n"
        "    FROM signal_snapshots WHERE timeframe = '${timeframe}'\n"
        ")\n"
    )


def latest_rank_cte() -> str:
    return (
        "WITH latest_snapshot AS (\n"
        "    SELECT MAX(snapshot_date) AS snapshot_date\n"
        "    FROM rank_snapshots WHERE timeframe = '${timeframe}'\n"
        ")\n"
    )


def latest_breadth_cte() -> str:
    return (
        "WITH latest_snapshot AS (\n"
        "    SELECT MAX(snapshot_date) AS snapshot_date\n"
        "    FROM market_breadth_snapshots WHERE timeframe = '${timeframe}'\n"
        ")\n"
    )


def latest_report_cte() -> str:
    return (
        "WITH latest_snapshot AS (\n"
        "    SELECT MAX(snapshot_date) AS snapshot_date\n"
        "    FROM report_snapshots WHERE timeframe = '${timeframe}'\n"
        ")\n"
    )


def ticker_detail_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Ticker Detail",
        "sp500-ticker-detail",
        ["sp500", "analytics", "generated"],
        dashboard_links(),
    )
    dashboard["templating"] = {
        "list": [
            query_variable(
                "ticker",
                "Ticker",
                "SELECT name || ' (' || symbol || ')' AS __text, symbol AS __value FROM tickers ORDER BY symbol",
                1,
            ),
            custom_variable("timeframe", "Timeframe", ["daily", "weekly", "monthly"], "daily"),
        ]
    }
    dashboard["panels"] = [
        table_panel(
            panel_id=1,
            title="Company",
            sql=(
                "SELECT '<div style=\"font-size:26px;font-weight:600;text-align:center;line-height:1.2;\">' "
                "|| COALESCE(name, symbol) || '</div>' AS company "
                "FROM tickers WHERE symbol = '${ticker}'"
            ),
            x=0,
            y=0,
            w=6,
            h=4,
            show_header=False,
        ),
        stat_panel(
            panel_id=2,
            title="Last Close",
            sql="SELECT close AS value FROM stock_prices WHERE symbol = '${ticker}' ORDER BY timestamp DESC LIMIT 1",
            x=6,
            y=0,
            w=6,
            h=4,
            unit="currencyUSD",
            decimals=2,
        ),
        stat_panel(
            panel_id=3,
            title="Latest Final Score",
            sql=(
                "SELECT final_score AS value FROM signal_snapshots "
                "WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' ORDER BY snapshot_date DESC LIMIT 1"
            ),
            x=12,
            y=0,
            w=6,
            h=4,
            decimals=1,
        ),
        stat_panel(
            panel_id=4,
            title="Latest Regime",
            sql=(
                "SELECT regime_label AS value FROM signal_snapshots "
                "WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' ORDER BY snapshot_date DESC LIMIT 1"
            ),
            x=18,
            y=0,
            w=6,
            h=4,
        ),
        timeseries_panel(
            panel_id=5,
            title="Price History",
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
            title="Analytics Score History",
            sql=(
                "SELECT snapshot_date::timestamp AS time, final_score AS \"Final Score\", trend_score AS \"Trend\", "
                "momentum_score AS \"Momentum\", relative_strength_score AS \"Relative Strength\" "
                "FROM signal_snapshots WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' ORDER BY snapshot_date"
            ),
            x=0,
            y=12,
            w=24,
            h=8,
        ),
        table_panel(
            panel_id=7,
            title="Latest Snapshot Interpretation",
            sql=(
                "SELECT snapshot_date, timeframe, close, volume, final_score, trend_score, momentum_score, "
                "volume_score, relative_strength_score, structure_score, mean_reversion_score, "
                "volatility_risk_score, regime_label, recommendation_label, breakout_flag, breakdown_flag, "
                "overbought_flag, oversold_flag, trend_alignment_flag, data_quality_flag, drivers_json "
                "FROM signal_snapshots WHERE symbol = '${ticker}' AND timeframe = '${timeframe}' "
                "ORDER BY snapshot_date DESC LIMIT 1"
            ),
            x=0,
            y=20,
            w=24,
            h=8,
        ),
        table_panel(
            panel_id=8,
            title="Latest Stored Reports",
            sql=(
                "SELECT snapshot_date, report_kind, title, summary_text, risk_text, storage_path "
                "FROM report_snapshots WHERE symbol = '__MARKET__' "
                "ORDER BY snapshot_date DESC, report_kind LIMIT 6"
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
        dashboard_links(),
    )
    dashboard["templating"] = {"list": [custom_variable("timeframe", "Timeframe", ["daily", "weekly", "monthly"], "daily")]}
    dashboard["panels"] = [
        bar_chart_panel(
            panel_id=1,
            title="Top 20 Bullish Scores",
            sql=(
                latest_rank_cte()
                + "SELECT rs.symbol AS ticker, rs.final_score AS score "
                  "FROM rank_snapshots rs JOIN latest_snapshot ls ON ls.snapshot_date = rs.snapshot_date "
                  "WHERE rs.timeframe = '${timeframe}' ORDER BY rs.bull_rank ASC LIMIT 20"
            ),
            x=0,
            y=0,
            w=12,
            h=9,
            x_field="ticker",
        ),
        bar_chart_panel(
            panel_id=2,
            title="Top 20 Bearish Scores",
            sql=(
                latest_rank_cte()
                + "SELECT rs.symbol AS ticker, rs.final_score AS score "
                  "FROM rank_snapshots rs JOIN latest_snapshot ls ON ls.snapshot_date = rs.snapshot_date "
                  "WHERE rs.timeframe = '${timeframe}' ORDER BY rs.bear_rank ASC LIMIT 20"
            ),
            x=12,
            y=0,
            w=12,
            h=9,
            x_field="ticker",
        ),
        table_panel(
            panel_id=3,
            title="Ranking Table",
            sql=(
                latest_rank_cte()
                + "SELECT rs.symbol AS ticker, COALESCE(t.name, rs.symbol) AS company_name, rs.final_score, "
                  "rs.bull_rank, rs.bear_rank, rs.score_change_1w, rs.score_change_1m, rs.regime_label, rs.recommendation_label "
                  "FROM rank_snapshots rs JOIN latest_snapshot ls ON ls.snapshot_date = rs.snapshot_date "
                  "LEFT JOIN tickers t ON t.symbol = rs.symbol "
                  "WHERE rs.timeframe = '${timeframe}' ORDER BY rs.bull_rank ASC"
            ),
            x=0,
            y=9,
            w=24,
            h=10,
        ),
        table_panel(
            panel_id=4,
            title="Market Breadth Snapshot",
            sql=(
                latest_breadth_cte()
                + "SELECT snapshot_date, timeframe, universe_size, bullish_count, neutral_count, bearish_count, "
                  "pct_above_ema20, pct_above_ema50, pct_above_ema200, pct_new_20d_high, pct_new_20d_low, "
                  "pct_near_52w_high, pct_near_52w_low, avg_final_score, median_final_score "
                  "FROM market_breadth_snapshots WHERE timeframe = '${timeframe}' "
                  "AND snapshot_date = (SELECT snapshot_date FROM latest_snapshot)"
            ),
            x=0,
            y=19,
            w=24,
            h=6,
        ),
        table_panel(
            panel_id=5,
            title="Latest Report Summary",
            sql=(
                latest_report_cte()
                + "SELECT report_kind, title, summary_text, risk_text, storage_path "
                  "FROM report_snapshots WHERE timeframe = '${timeframe}' AND symbol = '__MARKET__' "
                  "AND snapshot_date = (SELECT snapshot_date FROM latest_snapshot) ORDER BY report_kind"
            ),
            x=0,
            y=25,
            w=24,
            h=7,
        ),
    ]
    return dashboard


def trend_regime_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Trend Regime",
        "sp500-trend-regime",
        ["sp500", "trend", "generated"],
        dashboard_links(),
    )
    dashboard["templating"] = {"list": [custom_variable("timeframe", "Timeframe", ["daily", "weekly", "monthly"], "daily")]}
    dashboard["panels"] = [
        stat_panel(
            panel_id=1,
            title="Average Final Score",
            sql=(
                latest_breadth_cte()
                + "SELECT avg_final_score AS value FROM market_breadth_snapshots "
                  "WHERE timeframe = '${timeframe}' AND snapshot_date = (SELECT snapshot_date FROM latest_snapshot)"
            ),
            x=0,
            y=0,
            w=8,
            h=4,
            decimals=1,
        ),
        stat_panel(
            panel_id=2,
            title="Bullish Count",
            sql=(
                latest_breadth_cte()
                + "SELECT bullish_count AS value FROM market_breadth_snapshots "
                  "WHERE timeframe = '${timeframe}' AND snapshot_date = (SELECT snapshot_date FROM latest_snapshot)"
            ),
            x=8,
            y=0,
            w=8,
            h=4,
        ),
        stat_panel(
            panel_id=3,
            title="Bearish Count",
            sql=(
                latest_breadth_cte()
                + "SELECT bearish_count AS value FROM market_breadth_snapshots "
                  "WHERE timeframe = '${timeframe}' AND snapshot_date = (SELECT snapshot_date FROM latest_snapshot)"
            ),
            x=16,
            y=0,
            w=8,
            h=4,
        ),
        table_panel(
            panel_id=4,
            title="Top Trend Regimes",
            sql=(
                latest_signal_cte()
                + "SELECT ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.trend_score, "
                  "ss.trend_state, ss.regime_label, ss.recommendation_label "
                  "FROM signal_snapshots ss JOIN latest_snapshot ls ON ls.snapshot_date = ss.snapshot_date "
                  "LEFT JOIN tickers t ON t.symbol = ss.symbol "
                  "WHERE ss.timeframe = '${timeframe}' ORDER BY ss.trend_score DESC, ss.symbol LIMIT 25"
            ),
            x=0,
            y=4,
            w=12,
            h=10,
        ),
        table_panel(
            panel_id=5,
            title="Regime Distribution",
            sql=(
                latest_signal_cte()
                + "SELECT regime_label, COUNT(*) AS names, AVG(final_score) AS avg_score "
                  "FROM signal_snapshots WHERE timeframe = '${timeframe}' "
                  "AND snapshot_date = (SELECT snapshot_date FROM latest_snapshot) "
                  "GROUP BY regime_label ORDER BY names DESC, regime_label"
            ),
            x=12,
            y=4,
            w=12,
            h=10,
        ),
    ]
    return dashboard


def momentum_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Momentum",
        "sp500-momentum",
        ["sp500", "momentum", "generated"],
        dashboard_links(),
    )
    dashboard["templating"] = {"list": [custom_variable("timeframe", "Timeframe", ["daily", "weekly", "monthly"], "daily")]}
    dashboard["panels"] = [
        bar_chart_panel(
            panel_id=1,
            title="Top Momentum Scores",
            sql=(
                latest_signal_cte()
                + "SELECT ss.symbol AS ticker, ss.momentum_score AS score "
                  "FROM signal_snapshots ss JOIN latest_snapshot ls ON ls.snapshot_date = ss.snapshot_date "
                  "WHERE ss.timeframe = '${timeframe}' ORDER BY ss.momentum_score DESC, ss.symbol LIMIT 20"
            ),
            x=0,
            y=0,
            w=24,
            h=9,
            x_field="ticker",
        ),
        table_panel(
            panel_id=2,
            title="Overbought and Oversold Names",
            sql=(
                latest_signal_cte()
                + "SELECT ss.symbol AS ticker, COALESCE(t.name, ss.symbol) AS company_name, ss.momentum_score, "
                  "ss.mean_reversion_score, ss.overbought_flag, ss.oversold_flag, ss.recommendation_label "
                  "FROM signal_snapshots ss JOIN latest_snapshot ls ON ls.snapshot_date = ss.snapshot_date "
                  "LEFT JOIN tickers t ON t.symbol = ss.symbol "
                  "WHERE ss.timeframe = '${timeframe}' AND (ss.overbought_flag OR ss.oversold_flag) "
                  "ORDER BY ss.momentum_score DESC, ss.symbol"
            ),
            x=0,
            y=9,
            w=24,
            h=10,
        ),
    ]
    return dashboard
