import os
import unittest
from unittest import mock

from fetcher.src.config import get_fetcher_config, get_report_export_config


class FetcherConfigTests(unittest.TestCase):
    @mock.patch.dict(
        os.environ,
        {
            "DB_PASSWORD": "secret",
        },
        clear=False,
    )
    def test_startup_post_sync_mode_defaults_to_background(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("STARTUP_POST_SYNC_MODE", None)
            config = get_fetcher_config()

        self.assertEqual(config.startup_post_sync_mode, "background")

    @mock.patch.dict(
        os.environ,
        {
            "STARTUP_POST_SYNC_MODE": "blocking",
        },
        clear=False,
    )
    def test_startup_post_sync_mode_accepts_blocking(self):
        config = get_fetcher_config()

        self.assertEqual(config.startup_post_sync_mode, "blocking")

    @mock.patch.dict(
        os.environ,
        {
            "STARTUP_POST_SYNC_MODE": "invalid",
        },
        clear=False,
    )
    def test_startup_post_sync_mode_rejects_invalid_value(self):
        with self.assertRaises(ValueError):
            get_fetcher_config()

    @mock.patch.dict(
        os.environ,
        {
            "REPORT_OUTPUT_DIR": "/tmp/reports",
            "REPORT_UI_PUBLIC_URL": "http://localhost:3002/",
            "GRAFANA_INTERNAL_URL": "http://grafana:3000/",
        },
        clear=False,
    )
    def test_report_export_config_normalizes_urls(self):
        config = get_report_export_config()

        self.assertEqual(config.output_dir, "/tmp/reports")
        self.assertEqual(config.public_url, "http://localhost:3002")
        self.assertEqual(config.grafana_internal_url, "http://grafana:3000")


if __name__ == "__main__":
    unittest.main()
