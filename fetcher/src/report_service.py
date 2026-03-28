"""HTTP service for manual monthly market-report exports."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from threading import Lock, Thread
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from .config import get_database_config, get_report_export_config
from .database import (
    get_latest_report_export_job,
    get_latest_signal_snapshot_date,
    get_report_export_job,
    init_schema,
    insert_report_export_job,
    update_report_export_job,
    wait_for_db,
)
from .reporting import generate_manual_monthly_market_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

REPORT_KIND = "monthly"
REPORT_TIMEFRAME = "monthly"
REPORT_SCOPE = "full_market"
ACTIVE_JOB_STATUSES = ["queued", "running"]
JOB_LOCK = Lock()

app = FastAPI(title="S&P 500 Report Service", version="1.0.0")


@app.on_event("startup")
def startup() -> None:
    db_config = get_database_config()
    if not wait_for_db(db_config):
        raise RuntimeError("Database not available after max retries")
    init_schema(db_config)
    logger.info("Report service initialized")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/monthly-report")


@app.get("/monthly-report", response_class=HTMLResponse)
def monthly_report_page(autostart: int = 0) -> HTMLResponse:
    return HTMLResponse(_build_report_page_html(autostart=bool(autostart)))


@app.post("/api/report-jobs/monthly-market")
def create_monthly_market_report_job() -> JSONResponse:
    db_config = get_database_config()
    export_config = get_report_export_config()
    snapshot_date = get_latest_signal_snapshot_date(db_config, timeframe=REPORT_TIMEFRAME)
    if not snapshot_date:
        raise HTTPException(status_code=409, detail="No monthly analytics snapshot is available yet")

    with JOB_LOCK:
        active_job = get_latest_report_export_job(
            db_config,
            report_kind=REPORT_KIND,
            timeframe=REPORT_TIMEFRAME,
            scope=REPORT_SCOPE,
            statuses=ACTIVE_JOB_STATUSES,
        )
        if active_job is not None:
            return JSONResponse({"created": False, "job": _serialize_job(active_job)})

        job_id = uuid4().hex
        created_at = datetime.now(timezone.utc)
        insert_report_export_job(
            db_config,
            {
                "job_id": job_id,
                "report_kind": REPORT_KIND,
                "timeframe": REPORT_TIMEFRAME,
                "scope": REPORT_SCOPE,
                "status": "queued",
                "snapshot_date": snapshot_date,
                "created_at": created_at,
                "started_at": None,
                "completed_at": None,
                "error_message": "",
                "html_path": "",
                "pdf_path": "",
                "html_download_url": "",
                "pdf_download_url": "",
            },
        )

        worker = Thread(
            target=_run_monthly_market_report_job,
            args=(job_id, str(snapshot_date), export_config.public_url),
            daemon=True,
        )
        worker.start()

    created_job = get_report_export_job(db_config, job_id)
    return JSONResponse({"created": True, "job": _serialize_job(created_job)})


@app.get("/api/report-jobs/latest/monthly-market")
def get_latest_monthly_market_report_job() -> JSONResponse:
    db_config = get_database_config()
    job = get_latest_report_export_job(
        db_config,
        report_kind=REPORT_KIND,
        timeframe=REPORT_TIMEFRAME,
        scope=REPORT_SCOPE,
    )
    return JSONResponse({"job": _serialize_job(job)})


@app.get("/api/report-jobs/{job_id}")
def get_report_job(job_id: str) -> JSONResponse:
    db_config = get_database_config()
    job = get_report_export_job(db_config, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Report job not found")
    return JSONResponse({"job": _serialize_job(job)})


@app.get("/downloads/{job_id}/report.html")
def download_report_html(job_id: str) -> FileResponse:
    return _download_report_file(job_id, "html")


@app.get("/downloads/{job_id}/report.pdf")
def download_report_pdf(job_id: str) -> FileResponse:
    return _download_report_file(job_id, "pdf")


def _download_report_file(job_id: str, extension: str) -> FileResponse:
    db_config = get_database_config()
    job = get_report_export_job(db_config, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Report job not found")
    path_key = "html_path" if extension == "html" else "pdf_path"
    artifact_path = job.get(path_key) or ""
    if not artifact_path or not os.path.exists(artifact_path):
        raise HTTPException(status_code=404, detail="Requested artifact is not available")
    media_type = "text/html; charset=utf-8" if extension == "html" else "application/pdf"
    return FileResponse(
        artifact_path,
        media_type=media_type,
        filename=os.path.basename(artifact_path),
    )


def _run_monthly_market_report_job(job_id: str, snapshot_date: str, public_url: str) -> None:
    db_config = get_database_config()
    export_config = get_report_export_config()
    started_at = datetime.now(timezone.utc)
    update_report_export_job(
        db_config,
        job_id,
        status="running",
        started_at=started_at,
        error_message="",
    )
    try:
        grafana_username = os.environ.get("GRAFANA_ADMIN_USER", "admin")
        grafana_password = os.environ.get("GRAFANA_ADMIN_PASSWORD")
        if not grafana_password:
            raise RuntimeError("GRAFANA_ADMIN_PASSWORD must be set for report exports")

        artifact = generate_manual_monthly_market_report(
            db_config,
            export_config.output_dir,
            export_config.grafana_internal_url,
            grafana_username,
            grafana_password,
            job_id=job_id,
            snapshot_date=snapshot_date,
        )
        update_report_export_job(
            db_config,
            job_id,
            status="completed",
            snapshot_date=artifact.snapshot_date,
            completed_at=datetime.now(timezone.utc),
            html_path=artifact.html_path,
            pdf_path=artifact.pdf_path,
            html_download_url=f"{public_url}/downloads/{job_id}/report.html",
            pdf_download_url=f"{public_url}/downloads/{job_id}/report.pdf",
        )
        logger.info("Completed manual monthly report job %s for snapshot %s", job_id, artifact.snapshot_date)
    except Exception as exc:
        logger.exception("Manual monthly report job %s failed: %s", job_id, exc)
        update_report_export_job(
            db_config,
            job_id,
            status="failed",
            completed_at=datetime.now(timezone.utc),
            error_message=str(exc),
        )


def _serialize_job(job: dict | None) -> dict | None:
    if job is None:
        return None
    serialized = {}
    for key, value in job.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, date):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized


def _build_report_page_html(*, autostart: bool) -> str:
    autostart_literal = "true" if autostart else "false"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>S&amp;P 500 Monthly Report</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #102030;
      --muted: #566678;
      --line: #d5dde5;
      --surface: #f6f8fb;
      --accent: #135d66;
      --accent-strong: #0b3c49;
      --danger: #9f1c1c;
      --ok: #0b6b3a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(19, 93, 102, 0.08), transparent 42%),
        linear-gradient(180deg, #f9fbfd 0%, #eef3f7 100%);
    }}
    .page {{
      max-width: 900px;
      margin: 48px auto;
      padding: 0 20px;
    }}
    .card {{
      background: rgba(255, 255, 255, 0.94);
      border: 1px solid var(--line);
      box-shadow: 0 18px 50px rgba(16, 32, 48, 0.08);
      padding: 28px;
    }}
    .eyebrow {{
      text-transform: uppercase;
      letter-spacing: 0.14em;
      font-size: 12px;
      color: var(--accent);
      font-weight: 700;
      margin-bottom: 12px;
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: 34px;
      line-height: 1.15;
    }}
    p {{
      margin: 0 0 14px;
      line-height: 1.55;
      color: var(--muted);
    }}
    .actions {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 20px;
    }}
    button, a.button {{
      appearance: none;
      border: 0;
      text-decoration: none;
      background: var(--accent-strong);
      color: #fff;
      padding: 12px 16px;
      border-radius: 999px;
      font-weight: 700;
      cursor: pointer;
    }}
    button[disabled] {{
      opacity: 0.6;
      cursor: default;
    }}
    .status-card {{
      margin-top: 18px;
      padding: 18px;
      background: var(--surface);
      border: 1px solid var(--line);
    }}
    dl {{
      display: grid;
      grid-template-columns: 180px 1fr;
      gap: 8px 14px;
      margin: 0;
    }}
    dt {{ font-weight: 700; color: var(--ink); }}
    dd {{ margin: 0; color: var(--muted); word-break: break-word; }}
    .status {{
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .queued, .running {{ color: var(--accent-strong); }}
    .completed {{ color: var(--ok); }}
    .failed {{ color: var(--danger); }}
    .downloads {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 14px;
    }}
    .hint {{
      margin-top: 16px;
      font-size: 14px;
      color: var(--muted);
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="card">
      <div class="eyebrow">Manual Export</div>
      <h1>S&amp;P 500 Monthly Market Report</h1>
      <p>This page generates the downloadable monthly market report built from the latest stored monthly analytics snapshot and the market-wide Grafana dashboards.</p>
      <p>The export includes chart images from the non-ticker dashboards, dashboard-by-dashboard findings, and plain-language guidance on how to read each signal surface.</p>
      <div class="actions">
        <button id="start-button" type="button">Generate Monthly Report</button>
      </div>
      <div class="status-card">
        <dl>
          <dt>Status</dt>
          <dd id="status-text">Checking for the latest job...</dd>
          <dt>Job ID</dt>
          <dd id="job-id">-</dd>
          <dt>Snapshot Date</dt>
          <dd id="snapshot-date">-</dd>
          <dt>Created</dt>
          <dd id="created-at">-</dd>
          <dt>Completed</dt>
          <dd id="completed-at">-</dd>
          <dt>Error</dt>
          <dd id="error-text">-</dd>
        </dl>
        <div class="downloads" id="downloads"></div>
      </div>
      <p class="hint">If a job is already queued or running, the existing job is reused and this page will follow its progress.</p>
    </section>
  </main>
  <script>
    const autostart = {autostart_literal};
    const startButton = document.getElementById("start-button");
    const downloads = document.getElementById("downloads");
    let currentJobId = null;

    function setText(id, value) {{
      document.getElementById(id).textContent = value || "-";
    }}

    function renderDownloads(job) {{
      downloads.innerHTML = "";
      if (!job || job.status !== "completed") {{
        return;
      }}
      if (job.html_download_url) {{
        const htmlLink = document.createElement("a");
        htmlLink.href = job.html_download_url;
        htmlLink.textContent = "Download HTML";
        htmlLink.className = "button";
        downloads.appendChild(htmlLink);
      }}
      if (job.pdf_download_url) {{
        const pdfLink = document.createElement("a");
        pdfLink.href = job.pdf_download_url;
        pdfLink.textContent = "Download PDF";
        pdfLink.className = "button";
        downloads.appendChild(pdfLink);
      }}
    }}

    function renderJob(job) {{
      if (!job) {{
        setText("status-text", "No export has been requested yet.");
        setText("job-id", "-");
        setText("snapshot-date", "-");
        setText("created-at", "-");
        setText("completed-at", "-");
        setText("error-text", "-");
        downloads.innerHTML = "";
        startButton.disabled = false;
        return;
      }}
      currentJobId = job.job_id;
      const statusText = document.getElementById("status-text");
      statusText.textContent = job.status || "-";
      statusText.className = "status " + (job.status || "").toLowerCase();
      setText("job-id", job.job_id);
      setText("snapshot-date", job.snapshot_date);
      setText("created-at", job.created_at);
      setText("completed-at", job.completed_at);
      setText("error-text", job.error_message);
      renderDownloads(job);
      startButton.disabled = job.status === "queued" || job.status === "running";
    }}

    async function fetchLatestJob() {{
      const response = await fetch("/api/report-jobs/latest/monthly-market", {{ cache: "no-store" }});
      const payload = await response.json();
      renderJob(payload.job);
      if (payload.job && (payload.job.status === "queued" || payload.job.status === "running")) {{
        currentJobId = payload.job.job_id;
      }}
    }}

    async function pollJob() {{
      if (!currentJobId) {{
        await fetchLatestJob();
        return;
      }}
      const response = await fetch("/api/report-jobs/" + currentJobId, {{ cache: "no-store" }});
      if (response.status === 404) {{
        currentJobId = null;
        await fetchLatestJob();
        return;
      }}
      const payload = await response.json();
      renderJob(payload.job);
      if (payload.job && !["queued", "running"].includes(payload.job.status)) {{
        currentJobId = payload.job.job_id;
      }}
    }}

    async function startJob() {{
      startButton.disabled = true;
      const response = await fetch("/api/report-jobs/monthly-market", {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
      }});
      if (!response.ok) {{
        const payload = await response.json().catch(() => ({{ detail: "Request failed" }}));
        setText("status-text", "failed: " + (payload.detail || "Request failed"));
        startButton.disabled = false;
        return;
      }}
      const payload = await response.json();
      renderJob(payload.job);
      currentJobId = payload.job ? payload.job.job_id : null;
    }}

    startButton.addEventListener("click", startJob);
    fetchLatestJob().then(() => {{
      if (autostart) {{
        startJob();
      }}
    }});
    setInterval(pollJob, 3000);
  </script>
</body>
</html>
"""
