import unittest
from pathlib import Path
from unittest import mock

from scripts import generate_dashboards

ROOT = Path(__file__).resolve().parents[1]


class GenerateDashboardsTests(unittest.TestCase):
    def test_ticker_variable_query_uses_unaliased_sector_filter(self):
        variables = generate_dashboards.analytics_variables(include_ticker=True)
        ticker_variable = next(variable for variable in variables if variable["name"] == "ticker")

        self.assertIn("FROM tickers", ticker_variable["query"])
        self.assertNotIn("t.sector", ticker_variable["query"])
        self.assertIn("COALESCE(sector, '')", ticker_variable["query"])
        self.assertIn("'${sector}'", ticker_variable["query"])

    def test_sector_filter_quotes_variable_directly(self):
        self.assertIn("'${sector}'", generate_dashboards.SECTOR_FILTER_SQL)
        self.assertNotIn("${sector:singlequote}", generate_dashboards.SECTOR_FILTER_SQL)
        self.assertNotIn("${sector:sqlstring}", generate_dashboards.SECTOR_FILTER_SQL)
        self.assertIn("'$__all'", generate_dashboards.SECTOR_FILTER_SQL)

    def test_leaderboard_dashboard_maps_report_kind_from_timeframe(self):
        dashboard = generate_dashboards.leaderboard_dashboard()
        report_panel = next(panel for panel in dashboard["panels"] if panel["title"] == "Latest Report Snapshot")

        self.assertIn(generate_dashboards.REPORT_KIND_FROM_TIMEFRAME_SQL, report_panel["targets"][0]["rawSql"])
        self.assertEqual(report_panel["repeat"], "report_snapshot_visible")

    def test_leaderboard_dashboard_hides_report_panel_for_daily(self):
        dashboard = generate_dashboards.leaderboard_dashboard()
        variables = {variable["name"]: variable for variable in dashboard["templating"]["list"]}

        self.assertNotIn("report_kind", variables)
        self.assertIn("report_snapshot_visible", variables)
        self.assertEqual(variables["report_snapshot_visible"]["hide"], 2)

    def test_write_dashboards_emits_expected_filenames(self):
        output_dir = ROOT / "tests"
        with mock.patch("pathlib.Path.open", mock.mock_open(), create=True) as open_mock:
            written_paths = generate_dashboards.write_dashboards(output_dir)

        self.assertEqual(len(written_paths), len(generate_dashboards.EXPECTED_DASHBOARD_FILES))
        self.assertEqual(
            {path.name for path in written_paths},
            set(generate_dashboards.EXPECTED_DASHBOARD_FILES),
        )
        self.assertEqual(open_mock.call_count, len(generate_dashboards.EXPECTED_DASHBOARD_FILES))

    def test_build_dashboards_matches_expected_registry(self):
        dashboards = generate_dashboards.build_dashboards()

        self.assertEqual(set(dashboards), set(generate_dashboards.EXPECTED_DASHBOARD_FILES))
        self.assertEqual(len(dashboards), len(generate_dashboards.EXPECTED_DASHBOARD_FILES))


if __name__ == "__main__":
    unittest.main()
