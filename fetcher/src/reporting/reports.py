"""Deterministic weekly and monthly report generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..config import DatabaseConfig
from ..database import get_report_snapshot_inputs, upsert_report_snapshots

NON_ADVISORY_DISCLAIMER = (
    "This material is generated for market monitoring and education only. "
    "It is not investment advice, not a solicitation to trade, and not a substitute "
    "for your own research, risk limits, or professional advice."
)

REPORT_TIMEFRAME_MAP = {
    "weekly": "weekly",
    "monthly": "monthly",
}


@dataclass(frozen=True)
class ReportGenerationResult:
    """Summary of a report generation run."""

    report_kind: str
    timeframe: str
    snapshot_date: str
    markdown_path: str
    html_path: str
    row_count: int


def generate_report_artifacts(
    db_config: DatabaseConfig,
    output_dir: str,
    report_kind: str,
    timeframe: str | None = None,
    top_n: int = 10,
) -> ReportGenerationResult | None:
    """Render Markdown/HTML reports and persist report snapshot rows."""
    normalized_kind = str(report_kind).strip().lower()
    normalized_timeframe = str(timeframe or REPORT_TIMEFRAME_MAP.get(normalized_kind, normalized_kind)).strip().lower()
    inputs = get_report_snapshot_inputs(
        db_config,
        report_kind=normalized_kind,
        timeframe=normalized_timeframe,
        top_n=top_n,
    )
    breadth = inputs.get("breadth")
    signals = inputs.get("signals") or []
    ranks = inputs.get("ranks") or []
    if not breadth or not signals or not ranks:
        return None

    snapshot_date = str(breadth["snapshot_date"])
    bullish = [row for row in ranks if row.get("bull_rank") and int(row["bull_rank"]) <= top_n]
    bearish = [row for row in ranks if row.get("bear_rank") and int(row["bear_rank"]) <= top_n]
    signal_map = {row["symbol"]: row for row in signals}
    watchlist = [
        row
        for row in signals
        if row.get("breakout_flag") or row.get("oversold_flag") or row.get("breakdown_flag")
    ][:top_n]

    title = f"S&P 500 {normalized_kind.title()} Report - {snapshot_date}"
    summary_text = _build_market_summary(normalized_kind, normalized_timeframe, breadth)
    risk_text = _build_risk_commentary(breadth, watchlist)
    markdown = _render_markdown(
        title=title,
        timeframe=normalized_timeframe,
        summary_text=summary_text,
        risk_text=risk_text,
        bullish=bullish,
        bearish=bearish,
        watchlist=watchlist,
        signal_map=signal_map,
    )
    html = _render_html(title=title, markdown=markdown)

    report_root = Path(output_dir).expanduser().resolve() / normalized_kind
    report_root.mkdir(parents=True, exist_ok=True)
    base_name = f"{normalized_kind}-report-{snapshot_date}"
    markdown_path = report_root / f"{base_name}.md"
    html_path = report_root / f"{base_name}.html"
    markdown_path.write_text(markdown, encoding="utf-8")
    html_path.write_text(html, encoding="utf-8")

    report_rows = [
        {
            "snapshot_date": snapshot_date,
            "report_kind": normalized_kind,
            "timeframe": normalized_timeframe,
            "symbol": "__MARKET__",
            "title": title,
            "final_score": breadth.get("avg_final_score"),
            "regime_label": _market_regime_label(breadth),
            "recommendation_label": "review",
            "summary_text": summary_text,
            "risk_text": risk_text,
            "key_drivers_json": breadth,
            "report_markdown": markdown,
            "report_html": html,
            "storage_path": str(markdown_path),
        }
    ]

    for ranked_row in bullish + bearish:
        symbol = ranked_row["symbol"]
        signal_row = signal_map.get(symbol, {})
        report_rows.append(
            {
                "snapshot_date": snapshot_date,
                "report_kind": normalized_kind,
                "timeframe": normalized_timeframe,
                "symbol": symbol,
                "title": title,
                "final_score": ranked_row.get("final_score"),
                "regime_label": ranked_row.get("regime_label"),
                "recommendation_label": ranked_row.get("recommendation_label"),
                "summary_text": _build_symbol_summary(symbol, ranked_row, signal_row),
                "risk_text": _build_symbol_risk(signal_row),
                "key_drivers_json": signal_row.get("drivers_json", []),
                "report_markdown": "",
                "report_html": "",
                "storage_path": str(markdown_path),
            }
        )

    upsert_report_snapshots(db_config, report_rows)
    return ReportGenerationResult(
        report_kind=normalized_kind,
        timeframe=normalized_timeframe,
        snapshot_date=snapshot_date,
        markdown_path=str(markdown_path),
        html_path=str(html_path),
        row_count=len(report_rows),
    )


def _build_market_summary(report_kind: str, timeframe: str, breadth: dict) -> str:
    return (
        f"The {report_kind} {timeframe} snapshot covers {breadth['universe_size']} names. "
        f"Bullish setups account for {breadth['bullish_count']} names, neutral for {breadth['neutral_count']}, "
        f"and bearish for {breadth['bearish_count']}. Average composite score is "
        f"{float(breadth['avg_final_score']):.1f} with median {float(breadth['median_final_score']):.1f}. "
        f"{float(breadth['pct_above_ema50']):.1f}% of the universe is above the 50-day trend proxy and "
        f"{float(breadth['pct_near_52w_high']):.1f}% is trading near 52-week highs."
    )


def _build_risk_commentary(breadth: dict, watchlist: list[dict]) -> str:
    breakdowns = sum(1 for row in watchlist if row.get("breakdown_flag"))
    overbought = sum(1 for row in watchlist if row.get("overbought_flag"))
    return (
        f"Breakdown pressure remains visible in {float(breadth['pct_new_20d_low']):.1f}% of the universe, while "
        f"{float(breadth['pct_near_52w_low']):.1f}% sits near 52-week lows. "
        f"The watchlist currently contains {breakdowns} names flagged for breakdown risk and {overbought} names "
        "flagged as overbought. Treat high-ranked names as candidates for review rather than trade instructions. "
        f"{NON_ADVISORY_DISCLAIMER}"
    )


def _build_symbol_summary(symbol: str, rank_row: dict, signal_row: dict) -> str:
    drivers = signal_row.get("drivers_json") or []
    driver_text = ", ".join(
        f"{driver.get('label')}: {driver.get('value')}" for driver in drivers[:3]
    )
    return (
        f"{symbol} carries a final score of {float(rank_row.get('final_score') or 0):.1f}, "
        f"regime `{rank_row.get('regime_label', 'range')}`, recommendation `{rank_row.get('recommendation_label', 'neutral')}`. "
        f"Key drivers: {driver_text or 'insufficient driver detail'}."
    )


def _build_symbol_risk(signal_row: dict) -> str:
    return (
        f"Volatility risk score {float(signal_row.get('volatility_risk_score') or 0):.1f}; "
        f"breakout={bool(signal_row.get('breakout_flag'))}, breakdown={bool(signal_row.get('breakdown_flag'))}, "
        f"overbought={bool(signal_row.get('overbought_flag'))}, oversold={bool(signal_row.get('oversold_flag'))}. "
        f"{NON_ADVISORY_DISCLAIMER}"
    )


def _render_markdown(
    *,
    title: str,
    timeframe: str,
    summary_text: str,
    risk_text: str,
    bullish: list[dict],
    bearish: list[dict],
    watchlist: list[dict],
    signal_map: dict[str, dict],
) -> str:
    lines = [
        f"# {title}",
        "",
        f"Timeframe: `{timeframe}`",
        "",
        "## Market Summary",
        summary_text,
        "",
        "## Top Bullish Names",
        "| Symbol | Bull Rank | Score | Recommendation |",
        "| --- | ---: | ---: | --- |",
    ]
    for row in bullish:
        lines.append(
            f"| {row['symbol']} | {row.get('bull_rank', '')} | {float(row.get('final_score') or 0):.1f} | {row.get('recommendation_label', '')} |"
        )
    lines.extend(
        [
            "",
            "## Top Bearish Names",
            "| Symbol | Bear Rank | Score | Recommendation |",
            "| --- | ---: | ---: | --- |",
        ]
    )
    for row in bearish:
        lines.append(
            f"| {row['symbol']} | {row.get('bear_rank', '')} | {float(row.get('final_score') or 0):.1f} | {row.get('recommendation_label', '')} |"
        )
    lines.extend(["", "## Watchlist"])
    for row in watchlist:
        lines.append(f"- {row['symbol']}: {_build_symbol_risk(signal_map.get(row['symbol'], row))}")
    lines.extend(["", "## Risk Commentary", risk_text, "", "## Disclaimer", NON_ADVISORY_DISCLAIMER, ""])
    return "\n".join(lines)


def _render_html(*, title: str, markdown: str) -> str:
    escaped = (
        markdown.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br/>\n")
    )
    return (
        "<!doctype html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        f"  <title>{title}</title>\n"
        "  <style>body{font-family:Georgia,serif;max-width:960px;margin:2rem auto;padding:0 1rem;line-height:1.5;color:#1e293b;}h1,h2{color:#0f172a;}code{background:#e2e8f0;padding:0.1rem 0.3rem;border-radius:4px;}</style>\n"
        "</head>\n"
        "<body>\n"
        f"{escaped}\n"
        "</body>\n"
        "</html>\n"
    )


def _market_regime_label(breadth: dict) -> str:
    if float(breadth.get("avg_final_score") or 0) >= 65:
        return "bullish"
    if float(breadth.get("avg_final_score") or 0) <= 35:
        return "bearish"
    return "mixed"
