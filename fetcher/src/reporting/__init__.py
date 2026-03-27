"""Scheduled report generation from stored analytics snapshots."""

from .reports import ReportGenerationResult, generate_report_artifacts

__all__ = ["ReportGenerationResult", "generate_report_artifacts"]
