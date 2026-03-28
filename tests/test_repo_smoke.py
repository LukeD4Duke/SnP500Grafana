import ast
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

from scripts import generate_dashboards


ROOT = Path(__file__).resolve().parents[1]


def top_level_function_counts(path: Path) -> Counter[str]:
    module = ast.parse(path.read_text(encoding="utf-8"))
    return Counter(
        node.name
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    )


class DashboardGeneratorSmokeTests(unittest.TestCase):
    def test_write_dashboards_emits_expected_files(self):
        output_dir = ROOT / "tests"
        dashboards = generate_dashboards.build_dashboards()
        with mock.patch("pathlib.Path.write_text", autospec=True, return_value=0):
            written_paths = generate_dashboards.write_dashboards(output_dir)

        self.assertEqual(
            {path.name for path in written_paths},
            set(generate_dashboards.EXPECTED_DASHBOARD_FILES),
        )

        for filename, payload in dashboards.items():
            self.assertEqual(payload["uid"], Path(filename).stem)
            self.assertTrue(payload["panels"])
            self.assertEqual(payload["templating"]["list"][0]["name"], "timeframe")

    def test_generator_has_single_copy_of_critical_helpers(self):
        counts = top_level_function_counts(ROOT / "scripts" / "generate_dashboards.py")

        for name in (
            "query_variable",
            "custom_variable",
            "latest_report_sql",
            "build_dashboards",
            "write_dashboards",
            "main",
        ):
            self.assertEqual(counts[name], 1, name)


class DatabaseModuleSmokeTests(unittest.TestCase):
    def test_database_has_single_copy_of_snapshot_helpers(self):
        counts = top_level_function_counts(ROOT / "fetcher" / "src" / "database.py")

        for name in (
            "upsert_signal_snapshots",
            "upsert_rank_snapshots",
            "upsert_market_breadth_snapshots",
            "upsert_report_snapshots",
            "get_ticker_metadata",
        ):
            self.assertEqual(counts[name], 1, name)


if __name__ == "__main__":
    unittest.main()
