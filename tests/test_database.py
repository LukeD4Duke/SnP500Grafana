import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

from fetcher.src import database
from fetcher.src.config import DatabaseConfig


class ResolveInitScriptPathTests(unittest.TestCase):
    def test_resolve_init_script_path_uses_repo_script_for_local_runs(self):
        expected = Path(__file__).resolve().parents[1] / "scripts" / "init-db.sql"

        self.assertEqual(database.resolve_init_script_path(), expected)


class InitSchemaTests(unittest.TestCase):
    def test_init_schema_uses_embedded_sql_only_when_script_is_missing(self):
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            name="stocks",
            user="postgres",
            password="secret",
        )
        missing_script = Path(__file__).resolve().parent / "missing-init-db.sql"
        executed = []

        @contextmanager
        def stub_connection(_config):
            connection = mock.MagicMock()
            cursor = connection.cursor.return_value.__enter__.return_value
            cursor.execute.side_effect = executed.append
            yield connection

        with mock.patch("fetcher.src.database.get_connection", stub_connection):
            database.init_schema(config, missing_script)

        self.assertTrue(executed)
        self.assertIn("CREATE EXTENSION IF NOT EXISTS timescaledb", executed[0])


if __name__ == "__main__":
    unittest.main()
