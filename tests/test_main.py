import unittest
from unittest import mock

import pandas as pd

from fetcher.src.fetcher import FetchResult
from fetcher.src.main import STARTUP_CATCHUP_JOB_ID


class MainStartupFlowTests(unittest.TestCase):
    def setUp(self):
        self.sync_result = FetchResult(
            dataframe=pd.DataFrame(
                [
                    {
                        "Symbol": "AAA",
                        "Date": pd.Timestamp("2024-01-02"),
                        "Open": 1.0,
                        "High": 1.5,
                        "Low": 0.5,
                        "Close": 1.2,
                        "Volume": 100,
                        "Dividends": 0.0,
                        "Stock Splits": 0.0,
                    }
                ]
            ),
            requested_symbols=["AAA"],
            successful_symbols=["AAA"],
            changed_symbols=["AAA"],
            upserted_row_count=1,
            failed_symbols=[],
            recovered_symbols=[],
        )

    def _build_fetcher_config(self, mode: str = "background"):
        return mock.Mock(
            update_cron="0 23 * * *",
            backfill_start=None,
            startup_post_sync_mode=mode,
        )

    def test_populated_db_background_mode_schedules_startup_catchup(self):
        scheduler = mock.Mock()

        with mock.patch("fetcher.src.main.get_database_config", return_value=mock.Mock()), \
             mock.patch("fetcher.src.main.get_fetcher_config", return_value=self._build_fetcher_config("background")), \
             mock.patch("fetcher.src.main.get_indicator_config", return_value=mock.Mock(rebuild_on_startup=False)), \
             mock.patch("fetcher.src.main.get_reporting_config", return_value=mock.Mock(enabled=False)), \
             mock.patch("fetcher.src.main.wait_for_db", return_value=True), \
             mock.patch("fetcher.src.main.schema_exists", return_value=True), \
             mock.patch("fetcher.src.main.init_schema"), \
             mock.patch("fetcher.src.main.normalize_invalid_stock_splits", return_value=0), \
             mock.patch("fetcher.src.main.has_stock_price_data", return_value=True), \
             mock.patch("fetcher.src.main.run_sync", return_value=self.sync_result), \
             mock.patch("fetcher.src.main.get_price_date_bounds", return_value=("2024-01-01", "2024-03-01")), \
             mock.patch("fetcher.src.main.run_post_sync_tasks") as run_post_sync_tasks, \
             mock.patch("fetcher.src.main.BlockingScheduler", return_value=scheduler):
            from fetcher.src import main

            main.main()

        run_post_sync_tasks.assert_not_called()
        self.assertTrue(scheduler.start.called)
        startup_calls = [
            call
            for call in scheduler.add_job.call_args_list
            if call.kwargs.get("id") == STARTUP_CATCHUP_JOB_ID
        ]
        self.assertEqual(len(startup_calls), 1)
        self.assertEqual(startup_calls[0].args[0], main.run_startup_post_sync_catchup)
        self.assertEqual(startup_calls[0].kwargs["kwargs"]["symbols"], ["AAA"])
        self.assertFalse(startup_calls[0].kwargs["kwargs"]["force_rebuild"])

    def test_populated_db_blocking_mode_runs_post_sync_inline(self):
        scheduler = mock.Mock()

        with mock.patch("fetcher.src.main.get_database_config", return_value=mock.Mock()), \
             mock.patch("fetcher.src.main.get_fetcher_config", return_value=self._build_fetcher_config("blocking")), \
             mock.patch("fetcher.src.main.get_indicator_config", return_value=mock.Mock(rebuild_on_startup=True)), \
             mock.patch("fetcher.src.main.get_reporting_config", return_value=mock.Mock(enabled=False)), \
             mock.patch("fetcher.src.main.wait_for_db", return_value=True), \
             mock.patch("fetcher.src.main.schema_exists", return_value=True), \
             mock.patch("fetcher.src.main.init_schema"), \
             mock.patch("fetcher.src.main.normalize_invalid_stock_splits", return_value=0), \
             mock.patch("fetcher.src.main.has_stock_price_data", return_value=True), \
             mock.patch("fetcher.src.main.run_sync", return_value=self.sync_result), \
             mock.patch("fetcher.src.main.get_price_date_bounds", return_value=("2024-01-01", "2024-03-01")), \
             mock.patch("fetcher.src.main.run_post_sync_tasks") as run_post_sync_tasks, \
             mock.patch("fetcher.src.main.BlockingScheduler", return_value=scheduler):
            from fetcher.src import main

            main.main()

        run_post_sync_tasks.assert_called_once_with(
            ["AAA"],
            price_frame=self.sync_result.dataframe,
            force_rebuild=True,
        )
        startup_calls = [
            call
            for call in scheduler.add_job.call_args_list
            if call.kwargs.get("id") == STARTUP_CATCHUP_JOB_ID
        ]
        self.assertEqual(startup_calls, [])

    def test_populated_db_background_mode_skips_catchup_when_no_changes(self):
        scheduler = mock.Mock()
        no_change_result = FetchResult(
            dataframe=self.sync_result.dataframe,
            requested_symbols=["AAA"],
            successful_symbols=["AAA"],
            changed_symbols=[],
            upserted_row_count=0,
            failed_symbols=[],
            recovered_symbols=[],
        )

        with mock.patch("fetcher.src.main.get_database_config", return_value=mock.Mock()), \
             mock.patch("fetcher.src.main.get_fetcher_config", return_value=self._build_fetcher_config("background")), \
             mock.patch("fetcher.src.main.get_indicator_config", return_value=mock.Mock(rebuild_on_startup=False)), \
             mock.patch("fetcher.src.main.get_reporting_config", return_value=mock.Mock(enabled=False)), \
             mock.patch("fetcher.src.main.wait_for_db", return_value=True), \
             mock.patch("fetcher.src.main.schema_exists", return_value=True), \
             mock.patch("fetcher.src.main.init_schema"), \
             mock.patch("fetcher.src.main.normalize_invalid_stock_splits", return_value=0), \
             mock.patch("fetcher.src.main.has_stock_price_data", return_value=True), \
             mock.patch("fetcher.src.main.run_sync", return_value=no_change_result), \
             mock.patch("fetcher.src.main.get_price_date_bounds", return_value=("2024-01-01", "2024-03-01")), \
             mock.patch("fetcher.src.main.run_post_sync_tasks") as run_post_sync_tasks, \
             mock.patch("fetcher.src.main.BlockingScheduler", return_value=scheduler):
            from fetcher.src import main

            main.main()

        run_post_sync_tasks.assert_not_called()
        startup_calls = [
            call
            for call in scheduler.add_job.call_args_list
            if call.kwargs.get("id") == STARTUP_CATCHUP_JOB_ID
        ]
        self.assertEqual(startup_calls, [])


if __name__ == "__main__":
    unittest.main()
