import unittest
from unittest import mock

import pandas as pd

from fetcher.src.config import FetcherConfig
from fetcher.src.fetcher import fetch_historical_data


class FetchHistoricalDataTests(unittest.TestCase):
    def setUp(self):
        self.config = FetcherConfig(
            chunk_size=2,
            delay_seconds=0,
            historical_start="2020-01-01",
            update_cron="0 23 * * *",
            max_retries=3,
            retry_delay_seconds=0,
        )

    def test_rate_limit_exhaustion_raises_terminal_error(self):
        with mock.patch("fetcher.src.fetcher._fetch_chunk", side_effect=RuntimeError("429 rate limit")), \
             mock.patch("fetcher.src.fetcher.time.sleep"), \
             self.assertLogs("fetcher.src.fetcher", level="ERROR") as logs:
            with self.assertRaisesRegex(RuntimeError, "AAA, BBB"):
                fetch_historical_data(["AAA", "BBB"], start="2024-01-01", config=self.config)

        self.assertTrue(any("exhausted" in message for message in logs.output))

    def test_transient_rate_limit_failure_retries_then_succeeds(self):
        successful_df = pd.DataFrame(
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
        )

        with mock.patch(
            "fetcher.src.fetcher._fetch_chunk",
            side_effect=[RuntimeError("429 rate limit"), successful_df],
        ) as fetch_chunk, mock.patch("fetcher.src.fetcher.time.sleep"):
            result = fetch_historical_data(["AAA"], start="2024-01-01", config=self.config)

        self.assertEqual(fetch_chunk.call_count, 2)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["Symbol"], "AAA")

    def test_non_rate_limit_error_raises_immediately(self):
        with mock.patch("fetcher.src.fetcher._fetch_chunk", side_effect=RuntimeError("network down")) as fetch_chunk, \
             mock.patch("fetcher.src.fetcher.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "network down"):
                fetch_historical_data(["AAA"], start="2024-01-01", config=self.config)

        self.assertEqual(fetch_chunk.call_count, 1)


if __name__ == "__main__":
    unittest.main()
