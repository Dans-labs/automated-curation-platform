import sys
from pathlib import Path
from unittest import TestCase

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.acp.db.dbz import _normalize_database_name


class TestDatabaseName(TestCase):
    def test_normalizes_hyphenated_app_name(self):
        self.assertEqual(_normalize_database_name("harvest-doi-jsonfolder"), "acp_harvest_doi_jsonfolder")

    def test_empty_app_name_defaults_to_acp(self):
        self.assertEqual(_normalize_database_name(""), "acp")
