"""Generate Grafana dashboard JSON from metadata-aware templates."""

from __future__ import annotations

import json
from pathlib import Path

from grafanalib.core import Dashboard


ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "grafana" / "dashboards"
DATASOURCE = {"type": "postgres", "uid": "timescaledb"}
TIME_RANGE = {"from": "now-1y", "to": "now"}


def base_dashboard(title: str, uid: str, tags: list[str]) -> dict:
    dashboard = Dashboard(title=title).to_json_data()
    dashboard.update(
        {
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
    )
    return dashboard


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


def query_variable(name: str, label: str, query: str, refresh: int, include_all: bool = True) -> dict:
    variable = {
        "current": {"selected": True, "text": "All", "value": "*"} if include_all else {},
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
        variable["allValue"] = "*"
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
                    "AND ('${sector}' = '*' OR sector IN (${sector:sqlstring})) ORDER BY 1"
                ),
                2,
            ),
            query_variable(
                "ticker",
                "Ticker",
                (
                    "SELECT symbol FROM tickers "
                    "WHERE ('${sector}' = '*' OR sector IN (${sector:sqlstring})) "
                    "AND ('${industry}' = '*' OR industry IN (${industry:sqlstring})) "
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
                "WHERE ('${sector}' = '*' OR t.sector IN (${sector:sqlstring}))\n"
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
                "WHERE ('${sector}' = '*' OR t.sector IN (${sector:sqlstring}))\n"
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
                "WHERE ('${sector}' = '*' OR sector IN (${sector:sqlstring}))\n"
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
                    "AND ('${sector}' = '*' OR sector IN (${sector:sqlstring})) ORDER BY 1"
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
                "WHERE ('${sector}' = '*' OR t.sector IN (${sector:sqlstring}))\n"
                "AND ('${industry}' = '*' OR t.industry IN (${industry:sqlstring}))\n"
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
                "WHERE ('${sector}' = '*' OR t.sector IN (${sector:sqlstring}))\n"
                "AND ('${industry}' = '*' OR t.industry IN (${industry:sqlstring}))\n"
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
                "WHERE ('${sector}' = '*' OR sector IN (${sector:sqlstring}))\n"
                "AND ('${industry}' = '*' OR industry IN (${industry:sqlstring}))\n"
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
    write_dashboard("sp500-stock-overview.json", stock_overview_dashboard())
    write_dashboard("sp500-sector-overview.json", sector_overview_dashboard())
    write_dashboard("sp500-industry-overview.json", industry_overview_dashboard())


if __name__ == "__main__":
    main()
