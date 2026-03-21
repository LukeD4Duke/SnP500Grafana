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


def table_panel(*, panel_id: int, title: str, sql: str, x: int, y: int, w: int, h: int) -> dict:
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
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "cellHeight": "sm",
            "footer": {"show": False},
            "showHeader": True,
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


def ticker_detail_dashboard() -> dict:
    dashboard = base_dashboard(
        "S&P 500 Ticker Detail",
        "sp500-ticker-detail",
        ["sp500", "ticker", "generated"],
    )
    dashboard["time"] = {"from": "now-3y", "to": "now"}
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
        stat_panel(
            panel_id=1,
            title="Ticker",
            sql=(
                "SELECT COALESCE(name || ' (' || symbol || ')', symbol) AS value\n"
                "FROM tickers\n"
                "WHERE symbol = '${ticker}'"
            ),
            x=0,
            y=0,
            w=6,
            h=4,
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
        timeseries_panel(
            panel_id=7,
            title="Close Price (3 Years)",
            sql=(
                "SELECT timestamp AS time, close AS \"Close Price\"\n"
                "FROM stock_prices\n"
                "WHERE symbol = '${ticker}'\n"
                "AND timestamp >= NOW() - INTERVAL '3 years'\n"
                "AND $__timeFilter(timestamp)\n"
                "ORDER BY timestamp"
            ),
            x=0,
            y=4,
            w=24,
            h=10,
            unit="currencyUSD",
            draw_style="line",
            fill_opacity=10,
            line_width=2,
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
    write_dashboard("sp500-sector-overview.json", sector_overview_dashboard())
    write_dashboard("sp500-industry-overview.json", industry_overview_dashboard())


if __name__ == "__main__":
    main()
