"""Manual market-report exports with rendered Grafana panel images."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from html import escape
from pathlib import Path

import requests

from ..config import DatabaseConfig
from ..database import (
    get_latest_report_snapshot_rows,
    get_latest_signal_snapshot_date,
    get_market_breadth_snapshot,
    get_rank_snapshots,
    get_signal_snapshots,
)
from .reports import NON_ADVISORY_DISCLAIMER, _build_market_summary, _build_risk_commentary

MONTHLY_TIMEFRAME = "monthly"


@dataclass(frozen=True)
class ManualMarketReportArtifact:
    """Output metadata for a generated manual market report."""

    job_id: str
    snapshot_date: str
    html_path: str
    pdf_path: str


@dataclass(frozen=True)
class PanelImageSpec:
    """One rendered panel image to include in a report section."""

    panel_id: int
    caption: str


@dataclass(frozen=True)
class DashboardSectionSpec:
    """Definition for one report section sourced from a Grafana dashboard."""

    uid: str
    slug: str
    title: str
    description: str
    signal_guide: str
    panels: tuple[PanelImageSpec, ...]


@dataclass(frozen=True)
class RenderedPanelImage:
    """Panel image prepared for HTML embedding."""

    caption: str
    data_uri: str


@dataclass(frozen=True)
class RenderedDashboardSection:
    """Rendered content for a dashboard section."""

    title: str
    description: str
    signal_guide: str
    findings: str
    images: tuple[RenderedPanelImage, ...]


MONTHLY_REPORT_SECTIONS = (
    DashboardSectionSpec(
        uid="sp500-leaderboards",
        slug="sp-500-leaderboards",
        title="Leaderboards",
        description="Cross-sectional monthly rankings that surface the strongest and weakest composite setups in the index.",
        signal_guide=(
            "Read this as a ranking surface. Names near the top of the bullish table have the strongest "
            "aggregate setup across the scoring blocks, while the bearish side shows the weakest relative conditions."
        ),
        panels=(
            PanelImageSpec(panel_id=3, caption="Top Bullish Ranks"),
            PanelImageSpec(panel_id=4, caption="Top Bearish Ranks"),
        ),
    ),
    DashboardSectionSpec(
        uid="sp500-trend-regime",
        slug="sp-500-trend-regime",
        title="Trend Regime",
        description="Monthly trend leadership based on the persistent trend block in the analytics snapshot.",
        signal_guide=(
            "Higher trend scores indicate cleaner multi-period trend structure. Use the regime and recommendation "
            "labels to separate durable leadership from names that only have short-lived strength."
        ),
        panels=(PanelImageSpec(panel_id=5, caption="Top Trend Scores"),),
    ),
    DashboardSectionSpec(
        uid="sp500-momentum",
        slug="sp-500-momentum",
        title="Momentum",
        description="Monthly momentum leadership and exhaustion signals such as overbought and oversold conditions.",
        signal_guide=(
            "Use this section to spot acceleration and exhaustion. A high momentum score supports leadership, "
            "while overbought and oversold flags indicate where follow-through or snapback risk is elevated."
        ),
        panels=(PanelImageSpec(panel_id=5, caption="Top Momentum Scores"),),
    ),
    DashboardSectionSpec(
        uid="sp500-volatility-risk",
        slug="sp-500-volatility-and-risk",
        title="Volatility and Risk",
        description="Cross-sectional risk view for names carrying the highest volatility or data-quality penalties.",
        signal_guide=(
            "Treat higher volatility-risk scores as a caution flag. Strong setups with elevated risk may still rank well, "
            "but position sizing and confirmation requirements should be tighter."
        ),
        panels=(PanelImageSpec(panel_id=5, caption="Highest Volatility Risk"),),
    ),
    DashboardSectionSpec(
        uid="sp500-volume-confirmation",
        slug="sp-500-volume-confirmation",
        title="Volume Confirmation",
        description="Monthly participation view showing whether leadership is confirmed by volume and trend alignment.",
        signal_guide=(
            "Higher volume scores indicate broader participation behind a move. Breakout and trend-alignment flags "
            "matter more when they coincide with strong volume confirmation."
        ),
        panels=(PanelImageSpec(panel_id=5, caption="Top Volume Scores"),),
    ),
    DashboardSectionSpec(
        uid="sp500-breakout-breakdown",
        slug="sp-500-breakout-breakdown",
        title="Breakout and Breakdown",
        description="Monthly breakout and breakdown candidates based on the composite signal stack.",
        signal_guide=(
            "Use this section to separate expansion candidates from deterioration. Breakouts with trend alignment are "
            "constructive; breakdown flags point to areas where risk is expanding faster than strength."
        ),
        panels=(PanelImageSpec(panel_id=5, caption="Breakout Candidates"),),
    ),
    DashboardSectionSpec(
        uid="sp500-market-structure",
        slug="sp-500-market-structure",
        title="Market Structure",
        description="Monthly structure ranking focused on alignment between internal trend and setup quality.",
        signal_guide=(
            "Higher structure scores indicate cleaner price organization. The most useful names are those where "
            "structure, regime, and recommendation all point in the same direction."
        ),
        panels=(PanelImageSpec(panel_id=5, caption="Top Structure Scores"),),
    ),
    DashboardSectionSpec(
        uid="sp500-relative-strength",
        slug="sp-500-relative-strength",
        title="Relative Strength",
        description="Monthly leadership relative to the rest of the S&P 500 universe.",
        signal_guide=(
            "Relative-strength leaders are the names winning on a cross-sectional basis. Treat this as confirmation "
            "of leadership rather than a standalone entry signal."
        ),
        panels=(PanelImageSpec(panel_id=5, caption="Top Relative Strength"),),
    ),
    DashboardSectionSpec(
        uid="sp500-mean-reversion",
        slug="sp-500-mean-reversion",
        title="Mean Reversion",
        description="Monthly mean-reversion opportunities that overlap with momentum extremes and oversold conditions.",
        signal_guide=(
            "A high mean-reversion score highlights names stretched away from their usual equilibrium. Combine it with "
            "momentum and oversold/overbought context before treating it as actionable."
        ),
        panels=(PanelImageSpec(panel_id=5, caption="Top Mean Reversion Scores"),),
    ),
)


class GrafanaRenderError(RuntimeError):
    """Raised when a Grafana panel image cannot be rendered."""


class _GrafanaRenderClient:
    def __init__(
        self,
        grafana_base_url: str,
        username: str,
        password: str,
        *,
        timeout_seconds: int = 120,
    ) -> None:
        self._grafana_base_url = grafana_base_url.rstrip("/")
        self._auth = (username, password)
        self._timeout_seconds = timeout_seconds

    def render_panel(
        self,
        section: DashboardSectionSpec,
        panel: PanelImageSpec,
        output_path: Path,
    ) -> Path:
        params = {
            "orgId": 1,
            "panelId": panel.panel_id,
            "theme": "light",
            "width": 1600,
            "height": 900,
            "tz": "UTC",
            "var-timeframe": MONTHLY_TIMEFRAME,
            "var-sector": "$__all",
        }
        response = requests.get(
            f"{self._grafana_base_url}/render/d-solo/{section.uid}/{section.slug}",
            params=params,
            auth=self._auth,
            timeout=self._timeout_seconds,
        )
        content_type = response.headers.get("content-type", "")
        if response.status_code != 200 or not content_type.startswith("image/"):
            raise GrafanaRenderError(
                f"Failed to render {section.uid} panel {panel.panel_id}: "
                f"status={response.status_code} content_type={content_type or 'unknown'}"
            )
        output_path.write_bytes(response.content)
        return output_path


def generate_manual_monthly_market_report(
    db_config: DatabaseConfig,
    output_dir: str,
    grafana_base_url: str,
    grafana_username: str,
    grafana_password: str,
    *,
    job_id: str,
    snapshot_date: str | None = None,
) -> ManualMarketReportArtifact:
    """Generate a downloadable monthly market report with embedded panel images."""
    resolved_snapshot_date = snapshot_date or get_latest_signal_snapshot_date(db_config, timeframe=MONTHLY_TIMEFRAME)
    if not resolved_snapshot_date:
        raise ValueError("No monthly analytics snapshot is available yet")

    context = _build_report_context(db_config, resolved_snapshot_date)
    report_root = Path(output_dir).expanduser().resolve() / "manual" / MONTHLY_TIMEFRAME / f"{resolved_snapshot_date}-{job_id}"
    images_root = report_root / "images"
    images_root.mkdir(parents=True, exist_ok=True)

    render_client = _GrafanaRenderClient(
        grafana_base_url,
        grafana_username,
        grafana_password,
    )
    rendered_sections = []
    for section in MONTHLY_REPORT_SECTIONS:
        rendered_images = []
        for panel in section.panels:
            image_path = images_root / f"{section.uid}-panel-{panel.panel_id}.png"
            render_client.render_panel(section, panel, image_path)
            rendered_images.append(
                RenderedPanelImage(
                    caption=panel.caption,
                    data_uri=_image_data_uri(image_path),
                )
            )
        rendered_sections.append(
            RenderedDashboardSection(
                title=section.title,
                description=section.description,
                signal_guide=section.signal_guide,
                findings=_build_section_findings(section.uid, context),
                images=tuple(rendered_images),
            )
        )

    html = _render_manual_report_html(
        snapshot_date=resolved_snapshot_date,
        executive_summary=context["executive_summary"],
        risk_commentary=context["risk_commentary"],
        rendered_sections=tuple(rendered_sections),
    )
    html_path = report_root / "monthly-market-report.html"
    pdf_path = report_root / "monthly-market-report.pdf"
    html_path.write_text(html, encoding="utf-8")
    _write_pdf(html, html_path, pdf_path)
    return ManualMarketReportArtifact(
        job_id=job_id,
        snapshot_date=resolved_snapshot_date,
        html_path=str(html_path),
        pdf_path=str(pdf_path),
    )


def _build_report_context(db_config: DatabaseConfig, snapshot_date: str) -> dict:
    signals = get_signal_snapshots(db_config, snapshot_date, MONTHLY_TIMEFRAME)
    breadth = get_market_breadth_snapshot(db_config, snapshot_date, MONTHLY_TIMEFRAME)
    bullish = get_rank_snapshots(db_config, snapshot_date, MONTHLY_TIMEFRAME, "bull", 10)
    bearish = get_rank_snapshots(db_config, snapshot_date, MONTHLY_TIMEFRAME, "bear", 10)
    if not signals or breadth is None or not bullish or not bearish:
        raise ValueError(f"Monthly analytics snapshot {snapshot_date} is incomplete")

    monthly_report_rows = get_latest_report_snapshot_rows(db_config, "monthly", MONTHLY_TIMEFRAME)
    market_report = next(
        (
            row
            for row in monthly_report_rows
            if row.get("symbol") == "__MARKET__" and str(row.get("snapshot_date")) == snapshot_date
        ),
        {},
    )
    signal_map = {row["symbol"]: row for row in signals}
    watchlist = [
        row
        for row in signals
        if row.get("breakout_flag") or row.get("breakdown_flag") or row.get("oversold_flag") or row.get("overbought_flag")
    ][:10]
    executive_summary = market_report.get("summary_text") or _build_market_summary("monthly", MONTHLY_TIMEFRAME, breadth)
    risk_commentary = market_report.get("risk_text") or _build_risk_commentary(breadth, watchlist)
    return {
        "signals": signals,
        "signal_map": signal_map,
        "breadth": breadth,
        "bullish": bullish,
        "bearish": bearish,
        "watchlist": watchlist,
        "executive_summary": executive_summary,
        "risk_commentary": risk_commentary,
    }


def _build_section_findings(section_uid: str, context: dict) -> str:
    signals = context["signals"]
    breadth = context["breadth"]
    bullish = context["bullish"]
    bearish = context["bearish"]

    if section_uid == "sp500-leaderboards":
        return (
            f"Monthly composite leadership is led by {_symbol_list(bullish)}, while the weakest relative profiles "
            f"cluster in {_symbol_list(bearish)}."
        )
    if section_uid == "sp500-trend-regime":
        leaders = _top_signal_symbols(signals, "trend_score")
        bullish_count = sum(1 for row in signals if row.get("regime_label") == "bullish")
        return (
            f"Trend leadership is concentrated in {leaders}. {bullish_count} names currently carry a bullish regime "
            f"label on the monthly snapshot."
        )
    if section_uid == "sp500-momentum":
        overbought = sum(1 for row in signals if row.get("overbought_flag"))
        oversold = sum(1 for row in signals if row.get("oversold_flag"))
        leaders = _top_signal_symbols(signals, "momentum_score")
        return (
            f"Momentum leaders are {leaders}. The universe contains {overbought} overbought names and {oversold} "
            f"oversold names at the monthly cadence."
        )
    if section_uid == "sp500-volatility-risk":
        risky = _top_signal_symbols(signals, "volatility_risk_score")
        quality_flags = sum(1 for row in signals if row.get("data_quality_flag"))
        return (
            f"Risk concentrations are highest in {risky}. {quality_flags} names also carry an explicit data-quality "
            f"flag in the latest monthly snapshot."
        )
    if section_uid == "sp500-volume-confirmation":
        leaders = _top_signal_symbols(signals, "volume_score")
        confirmed = sum(1 for row in signals if row.get("breakout_flag") and row.get("trend_alignment_flag"))
        return (
            f"Volume-backed leadership is led by {leaders}. {confirmed} names currently combine breakout and "
            f"trend-alignment flags on monthly data."
        )
    if section_uid == "sp500-breakout-breakdown":
        breakouts = sum(1 for row in signals if row.get("breakout_flag"))
        breakdowns = sum(1 for row in signals if row.get("breakdown_flag"))
        leaders = _top_breakout_symbols(signals)
        return (
            f"The monthly snapshot flags {breakouts} breakout candidates and {breakdowns} breakdown candidates. "
            f"Highest-scoring breakout names include {leaders}."
        )
    if section_uid == "sp500-market-structure":
        leaders = _top_signal_symbols(signals, "structure_score")
        aligned = sum(1 for row in signals if row.get("trend_alignment_flag"))
        return (
            f"Market-structure leadership is led by {leaders}. {aligned} names show explicit trend-alignment "
            f"confirmation in the monthly snapshot."
        )
    if section_uid == "sp500-relative-strength":
        leaders = _top_signal_symbols(signals, "relative_strength_score")
        return (
            f"Cross-sectional relative-strength leadership is concentrated in {leaders}. These names are outperforming "
            f"the rest of the S&P 500 universe on the monthly score stack."
        )
    if section_uid == "sp500-mean-reversion":
        leaders = _top_signal_symbols(signals, "mean_reversion_score")
        oversold = sum(1 for row in signals if row.get("oversold_flag"))
        return (
            f"Mean-reversion candidates are led by {leaders}. {oversold} names carry an oversold flag, which is the "
            f"main confirmation to watch alongside this dashboard."
        )
    return (
        f"The monthly market breadth snapshot covers {breadth['universe_size']} names with an average final score of "
        f"{float(breadth['avg_final_score']):.1f}."
    )


def _top_signal_symbols(rows: list[dict], score_key: str, limit: int = 3) -> str:
    filtered = [row for row in rows if row.get(score_key) is not None]
    filtered.sort(key=lambda row: float(row.get(score_key) or 0), reverse=True)
    return _symbol_list(filtered[:limit])


def _top_breakout_symbols(rows: list[dict], limit: int = 3) -> str:
    filtered = [row for row in rows if row.get("breakout_flag")]
    filtered.sort(key=lambda row: float(row.get("final_score") or 0), reverse=True)
    return _symbol_list(filtered[:limit])


def _symbol_list(rows: list[dict]) -> str:
    symbols = [str(row.get("symbol")) for row in rows if row.get("symbol")]
    return ", ".join(symbols) if symbols else "no names met the filter"


def _image_data_uri(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def _write_pdf(html: str, html_path: Path, pdf_path: Path) -> None:
    from weasyprint import HTML

    HTML(string=html, base_url=html_path.parent.as_uri()).write_pdf(target=str(pdf_path))


def _render_manual_report_html(
    *,
    snapshot_date: str,
    executive_summary: str,
    risk_commentary: str,
    rendered_sections: tuple[RenderedDashboardSection, ...],
) -> str:
    section_blocks = "\n".join(_render_section_html(section, index) for index, section in enumerate(rendered_sections, start=1))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>S&amp;P 500 Monthly Market Report - {escape(snapshot_date)}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #112031;
      --muted: #506072;
      --line: #d6dde5;
      --surface: #f6f7f9;
      --accent: #0b6e4f;
      --accent-soft: #e5f5ef;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      background: #ffffff;
      line-height: 1.55;
    }}
    .report {{
      max-width: 1080px;
      margin: 0 auto;
      padding: 28px 24px 48px;
    }}
    .cover {{
      border: 1px solid var(--line);
      background: linear-gradient(135deg, #f8fbfd 0%, #eef5f8 100%);
      padding: 28px;
      margin-bottom: 28px;
    }}
    .eyebrow {{
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
      color: var(--accent);
      font-weight: 700;
    }}
    h1, h2 {{
      margin: 0 0 12px;
      line-height: 1.2;
    }}
    h1 {{ font-size: 34px; }}
    h2 {{ font-size: 26px; margin-top: 0; }}
    p {{ margin: 0 0 14px; }}
    .lead {{ font-size: 18px; color: var(--muted); max-width: 760px; }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
      margin-top: 20px;
    }}
    .summary-card {{
      padding: 18px;
      background: #fff;
      border: 1px solid var(--line);
    }}
    .section {{
      margin-top: 26px;
      padding-top: 26px;
      border-top: 1px solid var(--line);
    }}
    .section-note {{
      padding: 14px 16px;
      background: var(--surface);
      border-left: 4px solid var(--accent);
      margin: 14px 0 18px;
    }}
    .images {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 18px;
      margin: 18px 0;
    }}
    .panel {{
      margin: 0;
      border: 1px solid var(--line);
      background: #fff;
      padding: 12px;
    }}
    .panel img {{
      display: block;
      width: 100%;
      height: auto;
      border: 1px solid var(--line);
    }}
    figcaption {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 14px;
    }}
    .footer {{
      margin-top: 34px;
      padding: 18px 20px;
      background: var(--accent-soft);
      border: 1px solid #bfded2;
      font-size: 14px;
    }}
    @page {{
      size: A4;
      margin: 0.6in;
    }}
    @media print {{
      .report {{ max-width: none; padding: 0; }}
      .cover, .summary-card, .panel, .footer {{ break-inside: avoid; }}
      .section {{ break-inside: avoid; }}
    }}
  </style>
</head>
<body>
  <main class="report">
    <section class="cover">
      <div class="eyebrow">Monthly Market Report</div>
      <h1>S&amp;P 500 Dashboard Findings</h1>
      <p class="lead">Snapshot date: {escape(snapshot_date)}. This manual report packages the latest monthly analytics view across the market-wide Grafana dashboards and excludes the individual ticker-detail dashboard.</p>
      <div class="summary-grid">
        <article class="summary-card">
          <h2>Executive Summary</h2>
          <p>{escape(executive_summary)}</p>
        </article>
        <article class="summary-card">
          <h2>Risk Commentary</h2>
          <p>{escape(risk_commentary)}</p>
        </article>
      </div>
    </section>
    {section_blocks}
    <section class="footer">
      <strong>Reading the score framework.</strong> Final score is the aggregate of trend, momentum, volume, relative-strength, structure, mean-reversion, and volatility/risk inputs. Flags such as breakout, breakdown, overbought, oversold, and trend alignment are confirmation markers, not standalone trade instructions. {escape(NON_ADVISORY_DISCLAIMER)}
    </section>
  </main>
</body>
</html>
"""


def _render_section_html(section: RenderedDashboardSection, index: int) -> str:
    images_html = "\n".join(
        (
            "<figure class=\"panel\">"
            f"<img alt=\"{escape(image.caption)}\" src=\"{image.data_uri}\" />"
            f"<figcaption>{escape(image.caption)}</figcaption>"
            "</figure>"
        )
        for image in section.images
    )
    return f"""
    <section class="section">
      <div class="eyebrow">Section {index}</div>
      <h2>{escape(section.title)}</h2>
      <p>{escape(section.description)}</p>
      <div class="section-note">
        <strong>Findings.</strong> {escape(section.findings)}
      </div>
      <div class="images">
        {images_html}
      </div>
      <p><strong>How to read this dashboard.</strong> {escape(section.signal_guide)}</p>
    </section>
"""
