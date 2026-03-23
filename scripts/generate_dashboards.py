"""Generate Grafana dashboard JSON from metadata-aware templates."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "grafana" / "dashboards"
DATASOURCE = {"type": "postgres", "uid": "timescaledb"}
TIME_RANGE = {"from": "now-1y", "to": "now"}
ALL_SENTINEL = "__all"


def base_dashboard(title: str, uid: str, tags: list[str]) -> dict:
    return {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": [],
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


def timeseries_defaults(unit: str, draw_style: str, fill_opacity: int, line_width: int) -> dict:
    return {
        "color": {"mode": "palette-classic"},
        "custom": {
            "axisCenteredZero": False,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": draw_style,
            "fillOpacity": fill_opacity,
            "gradientMode": "none",
            "hideFrom": {"legend": False, "tooltip": False, "viz": False},
            "lineInterpolation": "smooth" if draw_style == "line" else "linear",
            "lineWidth": line_width,
            "pointSize": 5,
            "scaleDistribution": {"type": "linear"},
            "showPoints": "auto",
            "spanNulls": True,
            "stacking": {"group": "A", "mode": "none"},
            "thresholdsStyle": {"mode": "off"},
        },
        "mappings": [],
        "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
        "unit": unit,
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
    unit: str,
    draw_style: str,
    fill_opacity: int,
    line_width: int,
) -> dict:
    return {
        "datasource": DATASOURCE,
        "fieldConfig": {
            "defaults": timeseries_defaults(unit, draw_style, fill_opacity, line_width),
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


def apply_timeseries_color(panel: dict, color: str) -> dict:
    panel["fieldConfig"]["defaults"]["color"] = {"mode": "fixed", "fixedColor": color}
    return panel


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
) -> dict:
    return {
        "datasource": DATASOURCE,
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "align": "auto",
                    "cellOptions": {"type": "auto"},
                    "inspect": False,
                },
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
            },
            "overrides": overrides or [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "cellHeight": "sm",
            "footer": {"show": False},
            "showHeader": show_header,
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
        "type": "table",
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
    color_by_field: str | None = None,
    overrides: list[dict] | None = None,
) -> dict:
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
                "links": [
                    {
                        "targetBlank": False,
                        "title": "Open ticker detail",
                        "url": "/d/sp500-ticker-detail?var-ticker=${__data.fields.ticker}",
                    }
                ],
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
            },
            "overrides": overrides or [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "barRadius": 0,
            "barWidth": 0.9,
            "colorByField": color_by_field,
            "fullHighlight": False,
            "groupWidth": 0.8,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "orientation": "vertical",
            "showValue": "auto",
            "stacking": "none",
            "tooltip": {"mode": "single", "sort": "none"},
            "xField": x_field,
            "xTickLabelMaxLength": 12,
            "xTickLabelRotation": 45,
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
    color_mode: str = "value",
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
            "colorMode": color_mode,
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


def stock_overview_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Stock Overview",
        "sp500-stock-overview",
        ["sp500", "stocks", "generated"],
    )
    dashboard["templating"] = {
        "list": [
            query_variable(
                "sector",
                "Sector",
                "SELECT DISTINCT sector FROM tickers WHERE sector IS NOT NULL AND sector <> '' ORDER BY 1",
                1,
            ),
            query_variable(
                "industry",
                "Industry",
                (
                    "SELECT DISTINCT industry FROM tickers "
                    "WHERE industry IS NOT NULL AND industry <> '' "
                    f"AND (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR sector IN (${{sector:sqlstring}})) ORDER BY 1"
                ),
                2,
            ),
            query_variable(
                "ticker",
                "Ticker",
                (
                    "SELECT symbol FROM tickers "
                    f"WHERE (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR sector IN (${{sector:sqlstring}})) "
                    f"AND (${{industry:sqlstring}} = '{ALL_SENTINEL}' OR industry IN (${{industry:sqlstring}})) "
                    "ORDER BY symbol"
                ),
                2,
                include_all=False,
            ),
        ]
    }
    dashboard["panels"] = [
        timeseries_panel(
            panel_id=1,
            title="Close Price",
            sql=(
                "SELECT timestamp AS time, close AS \"Close Price\"\n"
                "FROM stock_prices\n"
                "WHERE symbol = '${ticker}'\n"
                "AND $__timeFilter(timestamp)\n"
                "ORDER BY timestamp"
            ),
            x=0,
            y=0,
            w=24,
            h=8,
            unit="currencyUSD",
            draw_style="line",
            fill_opacity=10,
            line_width=2,
        ),
        timeseries_panel(
            panel_id=2,
            title="Volume",
            sql=(
                "SELECT timestamp AS time, volume AS \"Volume\"\n"
                "FROM stock_prices\n"
                "WHERE symbol = '${ticker}'\n"
                "AND $__timeFilter(timestamp)\n"
                "ORDER BY timestamp"
            ),
            x=0,
            y=8,
            w=24,
            h=8,
            unit="short",
            draw_style="bars",
            fill_opacity=80,
            line_width=1,
        ),
        timeseries_panel(
            panel_id=3,
            title="OHLC",
            sql=(
                "SELECT timestamp AS time, open AS \"Open\", high AS \"High\", low AS \"Low\", close AS \"Close\"\n"
                "FROM stock_prices\n"
                "WHERE symbol = '${ticker}'\n"
                "AND $__timeFilter(timestamp)\n"
                "ORDER BY timestamp"
            ),
            x=0,
            y=16,
            w=12,
            h=8,
            unit="currencyUSD",
            draw_style="line",
            fill_opacity=10,
            line_width=1,
        ),
        table_panel(
            panel_id=4,
            title="Ticker Metadata",
            sql="SELECT symbol, name, sector, industry\nFROM tickers\nWHERE symbol = '${ticker}'",
            x=12,
            y=16,
            w=12,
            h=8,
        ),
    ]
    return dashboard


def leaderboard_return_sql(interval_literal: str, limit: int, include_color_code: bool = True) -> str:
    color_select = (
        ",\n"
        "    CASE\n"
        "        WHEN return_pct >= 0 AND MOD(side_rank, 2) = 1 THEN 1\n"
        "        WHEN return_pct >= 0 THEN 2\n"
        "        WHEN MOD(side_rank, 2) = 1 THEN 3\n"
        "        ELSE 4\n"
        "    END AS bar_color_code\n"
        if include_color_code
        else "\n"
    )
    return (
        "WITH latest_prices AS (\n"
        "    SELECT DISTINCT ON (symbol)\n"
        "        symbol,\n"
        "        timestamp AS latest_timestamp,\n"
        "        close AS latest_close\n"
        "    FROM stock_prices\n"
        "    ORDER BY symbol, timestamp DESC\n"
        "), prior_prices AS (\n"
        "    SELECT\n"
        "        lp.symbol,\n"
        "        prior.close AS prior_close\n"
        "    FROM latest_prices lp\n"
        "    JOIN LATERAL (\n"
        "        SELECT close\n"
        "        FROM stock_prices\n"
        f"        WHERE symbol = lp.symbol AND timestamp <= lp.latest_timestamp - INTERVAL '{interval_literal}'\n"
        "        ORDER BY timestamp DESC\n"
        "        LIMIT 1\n"
        "    ) prior ON TRUE\n"
        "), returns AS (\n"
        "    SELECT\n"
        "        lp.symbol,\n"
        "        COALESCE(t.name, lp.symbol) AS company_name,\n"
        "        ((lp.latest_close - pp.prior_close) / pp.prior_close) * 100 AS return_pct\n"
        "    FROM latest_prices lp\n"
        "    JOIN prior_prices pp ON pp.symbol = lp.symbol\n"
        "    LEFT JOIN tickers t ON t.symbol = lp.symbol\n"
        "    WHERE pp.prior_close IS NOT NULL AND pp.prior_close <> 0\n"
        "), ranked AS (\n"
        "    SELECT\n"
        "        symbol,\n"
        "        company_name,\n"
        "        return_pct,\n"
        "        CASE WHEN return_pct >= 0 THEN 0 ELSE 1 END AS sort_bucket,\n"
        "        ROW_NUMBER() OVER (\n"
        "            PARTITION BY CASE WHEN return_pct >= 0 THEN 'positive' ELSE 'negative' END\n"
        "            ORDER BY CASE WHEN return_pct >= 0 THEN return_pct END DESC,\n"
        "                     CASE WHEN return_pct < 0 THEN return_pct END ASC,\n"
        "                     symbol\n"
        "        ) AS side_rank\n"
        "    FROM returns\n"
        ")\n"
        "SELECT\n"
        "    symbol AS ticker,\n"
        "    company_name,\n"
        "    CASE WHEN return_pct >= 0 THEN return_pct END AS \"Positive Change %\",\n"
        "    CASE WHEN return_pct < 0 THEN return_pct END AS \"Negative Change %\""
        + color_select
        + "FROM ranked\n"
        + f"WHERE side_rank <= {limit}\n"
        "ORDER BY sort_bucket, ABS(return_pct) DESC, ticker"
    )


def leaderboard_52w_low_sql(limit: int, include_color_code: bool = True) -> str:
    color_select = (
        ",\n"
        "    CASE WHEN MOD(display_rank, 2) = 1 THEN 1 ELSE 2 END AS bar_color_code\n"
        if include_color_code
        else "\n"
    )
    return (
        "WITH latest_prices AS (\n"
        "    SELECT DISTINCT ON (symbol)\n"
        "        symbol,\n"
        "        close AS latest_close\n"
        "    FROM stock_prices\n"
        "    ORDER BY symbol, timestamp DESC\n"
        "), yearly_range AS (\n"
        "    SELECT\n"
        "        symbol,\n"
        "        MIN(low) AS low_52w\n"
        "    FROM stock_prices\n"
        "    WHERE timestamp >= NOW() - INTERVAL '52 weeks'\n"
        "    GROUP BY symbol\n"
        "), ranked AS (\n"
        "    SELECT\n"
        "        lp.symbol AS ticker,\n"
        "        COALESCE(t.name, lp.symbol) AS company_name,\n"
        "        ((lp.latest_close - yr.low_52w) / yr.low_52w) * 100 AS pct_above_52w_low,\n"
        "        ROW_NUMBER() OVER (\n"
        "            ORDER BY ((lp.latest_close - yr.low_52w) / yr.low_52w) * 100 ASC, lp.symbol\n"
        "        ) AS display_rank\n"
        "    FROM latest_prices lp\n"
        "    JOIN yearly_range yr ON yr.symbol = lp.symbol\n"
        "    LEFT JOIN tickers t ON t.symbol = lp.symbol\n"
        "    WHERE yr.low_52w IS NOT NULL AND yr.low_52w <> 0\n"
        ")\n"
        "SELECT\n"
        "    ticker,\n"
        "    company_name,\n"
        "    pct_above_52w_low AS \"% Above 52W Low\""
        + color_select
        + "FROM ranked\n"
        + "ORDER BY pct_above_52w_low ASC, ticker\n"
        f"LIMIT {limit}"
    )


def ticker_detail_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Ticker Detail",
        "sp500-ticker-detail",
        ["sp500", "ticker", "generated"],
    )
    dashboard["links"] = [
        {
            "asDropdown": False,
            "icon": "dashboard",
            "includeVars": False,
            "keepTime": False,
            "targetBlank": False,
            "title": "Leaderboards",
            "type": "link",
            "url": "/d/sp500-leaderboards/sandp-500-leaderboards",
        }
    ]
    dashboard["templating"] = {
        "list": [
            query_variable(
                "ticker",
                "Ticker",
                (
                    "SELECT name || ' (' || symbol || ')' AS __text, symbol AS __value "
                    "FROM tickers ORDER BY symbol"
                ),
                1,
                include_all=False,
            ),
        ]
    }
    latest_price_cte = (
        "WITH latest_price AS (\n"
        "    SELECT close\n"
        "    FROM stock_prices\n"
        "    WHERE symbol = '${ticker}'\n"
        "    ORDER BY timestamp DESC\n"
        "    LIMIT 1\n"
        "), yearly_range AS (\n"
        "    SELECT MIN(low) AS low_52w, MAX(high) AS high_52w\n"
        "    FROM stock_prices\n"
        "    WHERE symbol = '${ticker}'\n"
        "    AND timestamp >= NOW() - INTERVAL '52 weeks'\n"
        ")\n"
    )
    dashboard["panels"] = [
        table_panel(
            panel_id=1,
            title="Company",
            sql=(
                "SELECT '<div style=\"color:#4c78a8;font-size:26px;font-weight:600;text-align:center;line-height:1.2;\">' "
                "|| COALESCE(name, symbol) || '</div>' AS company\n"
                "FROM tickers\n"
                "WHERE symbol = '${ticker}'"
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
            sql=(
                "SELECT close AS value\n"
                "FROM stock_prices\n"
                "WHERE symbol = '${ticker}'\n"
                "ORDER BY timestamp DESC\n"
                "LIMIT 1"
            ),
            x=6,
            y=0,
            w=3,
            h=4,
            unit="currencyUSD",
            decimals=2,
        ),
        stat_panel(
            panel_id=3,
            title="52-Week Low",
            sql=(
                "SELECT MIN(low) AS value\n"
                "FROM stock_prices\n"
                "WHERE symbol = '${ticker}'\n"
                "AND timestamp >= NOW() - INTERVAL '52 weeks'"
            ),
            x=9,
            y=0,
            w=3,
            h=4,
            unit="currencyUSD",
            decimals=2,
        ),
        stat_panel(
            panel_id=4,
            title="52-Week High",
            sql=(
                "SELECT MAX(high) AS value\n"
                "FROM stock_prices\n"
                "WHERE symbol = '${ticker}'\n"
                "AND timestamp >= NOW() - INTERVAL '52 weeks'"
            ),
            x=12,
            y=0,
            w=3,
            h=4,
            unit="currencyUSD",
            decimals=2,
        ),
        stat_panel(
            panel_id=5,
            title="% Above 52-Week Low",
            sql=(
                latest_price_cte
                + "SELECT CASE\n"
                "    WHEN yearly_range.low_52w IS NULL OR yearly_range.low_52w = 0 OR latest_price.close IS NULL THEN NULL\n"
                "    ELSE ((latest_price.close - yearly_range.low_52w) / yearly_range.low_52w) * 100\n"
                "END AS value\n"
                "FROM latest_price\n"
                "CROSS JOIN yearly_range"
            ),
            x=15,
            y=0,
            w=4,
            h=4,
            unit="percent",
            decimals=2,
        ),
        stat_panel(
            panel_id=6,
            title="% Below 52-Week High",
            sql=(
                latest_price_cte
                + "SELECT CASE\n"
                "    WHEN yearly_range.high_52w IS NULL OR yearly_range.high_52w = 0 OR latest_price.close IS NULL THEN NULL\n"
                "    ELSE ((yearly_range.high_52w - latest_price.close) / yearly_range.high_52w) * 100\n"
                "END AS value\n"
                "FROM latest_price\n"
                "CROSS JOIN yearly_range"
            ),
            x=19,
            y=0,
            w=5,
            h=4,
            unit="percent",
            decimals=2,
        ),
        apply_timeseries_color(
            timeseries_panel(
                panel_id=7,
                title="Close Price",
                sql=(
                    "SELECT timestamp AS time, close AS \"Close Price\"\n"
                    "FROM stock_prices\n"
                    "WHERE symbol = '${ticker}'\n"
                    "AND $__timeFilter(timestamp)\n"
                    "ORDER BY timestamp"
                ),
                x=0,
                y=4,
                w=24,
                h=10,
                unit="currencyUSD",
                draw_style="line",
                fill_opacity=18,
                line_width=2,
            ),
            "semi-dark-green",
        ),
        table_panel(
            panel_id=8,
            title="Ticker Metadata",
            sql=(
                "SELECT symbol, name, sector, industry\n"
                "FROM tickers\n"
                "WHERE symbol = '${ticker}'"
            ),
            x=0,
            y=14,
            w=24,
            h=6,
        ),
    ]
    for panel in dashboard["panels"]:
        if panel["type"] == "stat":
            panel["fieldConfig"]["defaults"]["color"] = {"mode": "fixed", "fixedColor": "dark-green"}
            panel["fieldConfig"]["defaults"]["thresholds"] = {
                "mode": "absolute",
                "steps": [
                    {"color": "dark-blue", "value": None},
                    {"color": "dark-green", "value": 0},
                ],
            }
            panel["options"]["colorMode"] = "background"
            panel["options"]["graphMode"] = "none"
        elif panel["type"] == "table":
            panel["fieldConfig"]["defaults"]["thresholds"] = {
                "mode": "absolute",
                "steps": [{"color": "dark-blue", "value": None}],
            }
    for panel in dashboard["panels"]:
        if panel["id"] == 1:
            panel["fieldConfig"]["defaults"]["custom"]["align"] = "center"
            panel["fieldConfig"]["defaults"]["custom"]["cellOptions"] = {"type": "markdown"}
            panel["options"]["cellHeight"] = "lg"
        elif panel["id"] == 5:
            panel["fieldConfig"]["defaults"]["color"] = {"mode": "fixed", "fixedColor": "green"}
            panel["fieldConfig"]["defaults"]["thresholds"] = {
                "mode": "absolute",
                "steps": [
                    {"color": "dark-green", "value": None},
                    {"color": "green", "value": 0},
                ],
            }
        elif panel["id"] == 6:
            panel["fieldConfig"]["defaults"]["color"] = {"mode": "fixed", "fixedColor": "red"}
            panel["fieldConfig"]["defaults"]["thresholds"] = {
                "mode": "absolute",
                "steps": [
                    {"color": "dark-red", "value": None},
                    {"color": "red", "value": 0},
                ],
            }
    return dashboard


def leaderboard_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Leaderboards",
        "sp500-leaderboards",
        ["sp500", "leaderboards", "generated"],
    )
    dashboard["time"] = {"from": "now-3y", "to": "now"}
    dashboard["panels"] = [
        bar_chart_panel(
            panel_id=1,
            title="Top 50 Closest to 52-Week Low",
            sql=leaderboard_52w_low_sql(50, include_color_code=True),
            x=0,
            y=0,
            w=24,
            h=10,
            x_field="ticker",
            color_by_field="bar_color_code",
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "company_name"},
                    "properties": [{"id": "custom.hideFrom", "value": {"legend": True, "tooltip": False, "viz": True}}],
                },
                {
                    "matcher": {"id": "byName", "options": "bar_color_code"},
                    "properties": [
                        {"id": "custom.hideFrom", "value": {"legend": True, "tooltip": True, "viz": True}},
                        {
                            "id": "mappings",
                            "value": [
                                {"type": "value", "options": {"1": {"text": "Dark Blue", "color": "dark-blue"}}},
                                {"type": "value", "options": {"2": {"text": "Blue", "color": "blue"}}},
                            ],
                        },
                    ],
                },
            ],
        ),
        table_panel(
            panel_id=11,
            title="Top 50 Closest to 52-Week Low Links",
            sql=leaderboard_52w_low_sql(50, include_color_code=False),
            x=0,
            y=10,
            w=24,
            h=8,
            overrides=[ticker_link_override()],
        ),
        bar_chart_panel(
            panel_id=2,
            title="Top 20 Weekly Gainers and Losers",
            sql=leaderboard_return_sql("7 days", 20, include_color_code=True),
            x=0,
            y=18,
            w=24,
            h=10,
            x_field="ticker",
            color_by_field="bar_color_code",
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "company_name"},
                    "properties": [{"id": "custom.hideFrom", "value": {"legend": True, "tooltip": False, "viz": True}}],
                },
                {
                    "matcher": {"id": "byName", "options": "bar_color_code"},
                    "properties": [
                        {"id": "custom.hideFrom", "value": {"legend": True, "tooltip": True, "viz": True}},
                        {
                            "id": "mappings",
                            "value": [
                                {"type": "value", "options": {"1": {"text": "Dark Green", "color": "dark-green"}}},
                                {"type": "value", "options": {"2": {"text": "Green", "color": "green"}}},
                                {"type": "value", "options": {"3": {"text": "Dark Red", "color": "dark-red"}}},
                                {"type": "value", "options": {"4": {"text": "Red", "color": "red"}}},
                            ],
                        },
                    ],
                },
            ],
        ),
        table_panel(
            panel_id=12,
            title="Weekly Gainers and Losers Links",
            sql=leaderboard_return_sql("7 days", 20, include_color_code=False),
            x=0,
            y=28,
            w=24,
            h=8,
            overrides=[ticker_link_override()],
        ),
        bar_chart_panel(
            panel_id=3,
            title="Top 20 Monthly Gainers and Losers",
            sql=leaderboard_return_sql("1 month", 20, include_color_code=True),
            x=0,
            y=36,
            w=24,
            h=10,
            x_field="ticker",
            color_by_field="bar_color_code",
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "company_name"},
                    "properties": [{"id": "custom.hideFrom", "value": {"legend": True, "tooltip": False, "viz": True}}],
                },
                {
                    "matcher": {"id": "byName", "options": "bar_color_code"},
                    "properties": [
                        {"id": "custom.hideFrom", "value": {"legend": True, "tooltip": True, "viz": True}},
                        {
                            "id": "mappings",
                            "value": [
                                {"type": "value", "options": {"1": {"text": "Dark Green", "color": "dark-green"}}},
                                {"type": "value", "options": {"2": {"text": "Green", "color": "green"}}},
                                {"type": "value", "options": {"3": {"text": "Dark Red", "color": "dark-red"}}},
                                {"type": "value", "options": {"4": {"text": "Red", "color": "red"}}},
                            ],
                        },
                    ],
                },
            ],
        ),
        table_panel(
            panel_id=13,
            title="Monthly Gainers and Losers Links",
            sql=leaderboard_return_sql("1 month", 20, include_color_code=False),
            x=0,
            y=46,
            w=24,
            h=8,
            overrides=[ticker_link_override()],
        ),
        bar_chart_panel(
            panel_id=4,
            title="Top 20 Three-Month Gainers and Losers",
            sql=leaderboard_return_sql("3 months", 20, include_color_code=True),
            x=0,
            y=54,
            w=24,
            h=10,
            x_field="ticker",
            color_by_field="bar_color_code",
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "company_name"},
                    "properties": [{"id": "custom.hideFrom", "value": {"legend": True, "tooltip": False, "viz": True}}],
                },
                {
                    "matcher": {"id": "byName", "options": "bar_color_code"},
                    "properties": [
                        {"id": "custom.hideFrom", "value": {"legend": True, "tooltip": True, "viz": True}},
                        {
                            "id": "mappings",
                            "value": [
                                {"type": "value", "options": {"1": {"text": "Dark Green", "color": "dark-green"}}},
                                {"type": "value", "options": {"2": {"text": "Green", "color": "green"}}},
                                {"type": "value", "options": {"3": {"text": "Dark Red", "color": "dark-red"}}},
                                {"type": "value", "options": {"4": {"text": "Red", "color": "red"}}},
                            ],
                        },
                    ],
                },
            ],
        ),
        table_panel(
            panel_id=14,
            title="Three-Month Gainers and Losers Links",
            sql=leaderboard_return_sql("3 months", 20, include_color_code=False),
            x=0,
            y=64,
            w=24,
            h=8,
            overrides=[ticker_link_override()],
        ),
    ]
    return dashboard


def sector_overview_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Sector Overview",
        "sp500-sector-overview",
        ["sp500", "sector", "generated"],
    )
    dashboard["templating"] = {
        "list": [
            query_variable(
                "sector",
                "Sector",
                "SELECT DISTINCT sector FROM tickers WHERE sector IS NOT NULL AND sector <> '' ORDER BY 1",
                1,
            )
        ]
    }
    dashboard["panels"] = [
        timeseries_panel(
            panel_id=1,
            title="Average Close by Sector",
            sql=(
                "SELECT sp.timestamp AS time, AVG(sp.close) AS \"Average Close\"\n"
                "FROM stock_prices sp\n"
                "JOIN tickers t ON t.symbol = sp.symbol\n"
                f"WHERE (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR t.sector IN (${{sector:sqlstring}}))\n"
                "AND $__timeFilter(sp.timestamp)\n"
                "GROUP BY sp.timestamp\n"
                "ORDER BY sp.timestamp"
            ),
            x=0,
            y=0,
            w=24,
            h=8,
            unit="currencyUSD",
            draw_style="line",
            fill_opacity=10,
            line_width=2,
        ),
        timeseries_panel(
            panel_id=2,
            title="Total Volume by Sector",
            sql=(
                "SELECT sp.timestamp AS time, SUM(sp.volume) AS \"Total Volume\"\n"
                "FROM stock_prices sp\n"
                "JOIN tickers t ON t.symbol = sp.symbol\n"
                f"WHERE (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR t.sector IN (${{sector:sqlstring}}))\n"
                "AND $__timeFilter(sp.timestamp)\n"
                "GROUP BY sp.timestamp\n"
                "ORDER BY sp.timestamp"
            ),
            x=0,
            y=8,
            w=24,
            h=8,
            unit="short",
            draw_style="bars",
            fill_opacity=80,
            line_width=1,
        ),
        table_panel(
            panel_id=3,
            title="Sector Constituents",
            sql=(
                "SELECT symbol, name, sector, industry\n"
                "FROM tickers\n"
                f"WHERE (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR sector IN (${{sector:sqlstring}}))\n"
                "ORDER BY symbol"
            ),
            x=0,
            y=16,
            w=24,
            h=10,
        ),
    ]
    return dashboard


def industry_overview_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Industry Overview",
        "sp500-industry-overview",
        ["sp500", "industry", "generated"],
    )
    dashboard["templating"] = {
        "list": [
            query_variable(
                "sector",
                "Sector",
                "SELECT DISTINCT sector FROM tickers WHERE sector IS NOT NULL AND sector <> '' ORDER BY 1",
                1,
            ),
            query_variable(
                "industry",
                "Industry",
                (
                    "SELECT DISTINCT industry FROM tickers "
                    "WHERE industry IS NOT NULL AND industry <> '' "
                    f"AND (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR sector IN (${{sector:sqlstring}})) ORDER BY 1"
                ),
                2,
            ),
        ]
    }
    dashboard["panels"] = [
        timeseries_panel(
            panel_id=1,
            title="Average Close by Industry",
            sql=(
                "SELECT sp.timestamp AS time, AVG(sp.close) AS \"Average Close\"\n"
                "FROM stock_prices sp\n"
                "JOIN tickers t ON t.symbol = sp.symbol\n"
                f"WHERE (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR t.sector IN (${{sector:sqlstring}}))\n"
                f"AND (${{industry:sqlstring}} = '{ALL_SENTINEL}' OR t.industry IN (${{industry:sqlstring}}))\n"
                "AND $__timeFilter(sp.timestamp)\n"
                "GROUP BY sp.timestamp\n"
                "ORDER BY sp.timestamp"
            ),
            x=0,
            y=0,
            w=24,
            h=8,
            unit="currencyUSD",
            draw_style="line",
            fill_opacity=10,
            line_width=2,
        ),
        timeseries_panel(
            panel_id=2,
            title="Total Volume by Industry",
            sql=(
                "SELECT sp.timestamp AS time, SUM(sp.volume) AS \"Total Volume\"\n"
                "FROM stock_prices sp\n"
                "JOIN tickers t ON t.symbol = sp.symbol\n"
                f"WHERE (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR t.sector IN (${{sector:sqlstring}}))\n"
                f"AND (${{industry:sqlstring}} = '{ALL_SENTINEL}' OR t.industry IN (${{industry:sqlstring}}))\n"
                "AND $__timeFilter(sp.timestamp)\n"
                "GROUP BY sp.timestamp\n"
                "ORDER BY sp.timestamp"
            ),
            x=0,
            y=8,
            w=24,
            h=8,
            unit="short",
            draw_style="bars",
            fill_opacity=80,
            line_width=1,
        ),
        table_panel(
            panel_id=3,
            title="Industry Constituents",
            sql=(
                "SELECT symbol, name, sector, industry\n"
                "FROM tickers\n"
                f"WHERE (${{sector:sqlstring}} = '{ALL_SENTINEL}' OR sector IN (${{sector:sqlstring}}))\n"
                f"AND (${{industry:sqlstring}} = '{ALL_SENTINEL}' OR industry IN (${{industry:sqlstring}}))\n"
                "ORDER BY symbol"
            ),
            x=0,
            y=16,
            w=24,
            h=10,
        ),
    ]
    return dashboard


def write_dashboard(filename: str, dashboard: dict) -> None:
    (DASHBOARD_DIR / filename).write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    write_dashboard("sp500-ticker-detail.json", ticker_detail_dashboard())
    write_dashboard("sp500-stock-overview.json", stock_overview_dashboard())
    write_dashboard("sp500-leaderboards.json", leaderboard_dashboard())
    write_dashboard("sp500-sector-overview.json", sector_overview_dashboard())
    write_dashboard("sp500-industry-overview.json", industry_overview_dashboard())


if __name__ == "__main__":
    main()
