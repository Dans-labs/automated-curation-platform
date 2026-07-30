import importlib
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[2]))


class TestRetrieveDataverseJson(TestCase):
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
    def _plugin_class(self):
        module = importlib.import_module("src.acp.plugins.retrieve_dataverse_json")
        module = importlib.reload(module)
        return module.RetrieveDataverseJson

    def test_derive_export_endpoint_from_source_base_url_without_oai(self):
        plugin_cls = self._plugin_class()
        endpoint = plugin_cls._derive_export_endpoint_from_source_base_url(
            "https://ssh.datastations.nl"
        )
        self.assertEqual(endpoint, "https://ssh.datastations.nl/api/datasets/export")

    def test_derive_export_endpoint_from_source_base_url_with_oai(self):
        plugin_cls = self._plugin_class()
        endpoint = plugin_cls._derive_export_endpoint_from_source_base_url(
            "https://ssh.datastations.nl/oai"
        )
        self.assertEqual(endpoint, "https://ssh.datastations.nl/api/datasets/export")
