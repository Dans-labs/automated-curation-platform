import importlib
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[2]))


class TestDbConfig(TestCase):
    def _reload_commons(self):
        import src.acp.commons as commons

        return importlib.reload(commons)

    @patch.dict(
        "os.environ",
        {
            "DB_URL": "",
            "DB_USER": "acp",
            "DB_PASSWORD": "secret",
            "DB_HOST": "postgres",
            "DB_PORT": "5432",
        },
        clear=False,
    )
    def test_build_db_url_from_individual_env_vars(self):
        commons = self._reload_commons()

        self.assertEqual(commons.db_url, "acp:secret@postgres:5432")

    @patch.dict(
        "os.environ",
        {
            "DB_URL": "acp:override@db.example:5432",
            "DB_USER": "acp",
            "DB_PASSWORD": "secret",
            "DB_HOST": "postgres",
            "DB_PORT": "5432",
        },
        clear=False,
    )
    def test_db_url_env_takes_precedence(self):
        commons = self._reload_commons()

        self.assertEqual(commons.db_url, "acp:override@db.example:5432")
