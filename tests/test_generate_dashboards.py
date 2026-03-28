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
