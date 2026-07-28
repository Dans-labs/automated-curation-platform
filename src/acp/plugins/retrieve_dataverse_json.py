from __future__ import annotations

import json
import os
import re
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from src.acp.bridge import Bridge
from src.acp.commons import handle_deposit_exceptions, transform, transform_xml
from src.acp.db.dbz import DepositStatus
from src.acp.models.bridge_output_model import ResponseContentType, TargetDataModel, TargetResponse


class RetrieveDataverseJson(Bridge):
    @staticmethod
    def _sanitize_segment(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]", "-", value).strip("-") or "dataset"

    @staticmethod
    def _safe_child_path(base_dir: Path, *segments: str) -> Path:
        candidate = (base_dir.joinpath(*segments)).resolve()
        if base_dir != candidate and base_dir not in candidate.parents:
            raise ValueError("Resolved path escapes the configured target directory.")
        return candidate

    @staticmethod
    def _first_match(pattern: re.Pattern[str], value: Any) -> str | None:
        if isinstance(value, str):
            match = pattern.search(value)
            return match.group(0) if match else None
        if isinstance(value, dict):
            for child in value.values():
                result = RetrieveDataverseJson._first_match(pattern, child)
                if result:
                    return result
        if isinstance(value, list):
            for child in value:
                result = RetrieveDataverseJson._first_match(pattern, child)
                if result:
                    return result
        return None

    @staticmethod
    def _to_persistent_id(doi_value: str) -> str:
        value = doi_value.strip()
        lowered = value.lower()
        if lowered.startswith("https://doi.org/") or lowered.startswith("http://doi.org/"):
            return f"doi:{urllib.parse.urlparse(value).path.lstrip('/')}"
        if lowered.startswith("doi:"):
            return value
        if value.startswith("10."):
            return f"doi:{value}"
        raise ValueError(f"Could not convert DOI value to persistent ID: {doi_value}")

    def _extract_doi_value(self, metadata_content: str, raw_xml: str | None) -> str:
        metadata_obj = json.loads(metadata_content)

        transformed = (
            self.target.metadata.transformed_metadata
            if self.target.metadata and self.target.metadata.transformed_metadata
            else []
        )
        for tm in transformed:
            if not tm.transformer_url:
                continue
            source = raw_xml or metadata_content
            result = transform_xml(f"{tm.transformer_url}?app_name={self.app_name}", source) if raw_xml else transform(
                f"{tm.transformer_url}?app_name={self.app_name}",
                source,
            )
            try:
                parsed = json.loads(result)
            except json.JSONDecodeError:
                parsed = {}
            doi_value = parsed.get("doi") if isinstance(parsed, dict) else None
            if isinstance(doi_value, str) and doi_value.strip():
                return doi_value.strip()

        doi_pattern = re.compile(r"https?://doi\.org/\S+|doi:\S+|10\.\d{4,9}/\S+")
        doi_value = self._first_match(doi_pattern, metadata_obj)
        if doi_value:
            return doi_value

        raise ValueError("No DOI value found in dataset metadata.")

    @handle_deposit_exceptions
    def job(self) -> TargetDataModel:
        parsed_target_url = urllib.parse.urlparse(self.target.target_url)
        if parsed_target_url.scheme != "file" or not parsed_target_url.path:
            raise ValueError("RetrieveDataverseJson requires a file:// target-url.")

        metadata_content = self.dataset_rec.metadata_content
        metadata_obj = json.loads(metadata_content)
        raw_xml = metadata_obj.get("raw_xml") if isinstance(metadata_obj, dict) else None
        if raw_xml is not None and not isinstance(raw_xml, str):
            raw_xml = None

        doi_value = self._extract_doi_value(metadata_content, raw_xml)
        persistent_id = self._to_persistent_id(doi_value)

        export_endpoint = os.getenv(
            "DATAVERSE_JSON_EXPORT_ENDPOINT",
            "https://dataverse.nl/api/datasets/export",
        )
        query = urllib.parse.urlencode(
            {
                "exporter": "dataverse_json",
                "persistentId": persistent_id,
            }
        )
        export_url = f"{export_endpoint}?{query}"
        response = requests.get(export_url, timeout=60)
        response.raise_for_status()

        exported_json = response.json()
        exported_content = json.dumps(exported_json, indent=2, ensure_ascii=False)
        self.dataset_rec.metadata_content = exported_content
        self.db_manager.update_dataset(self.dataset_rec)

        base_dir = Path(urllib.parse.unquote(parsed_target_url.path)).resolve()
        dataset_dir = self._safe_child_path(base_dir, self._sanitize_segment(self.dataset_id))
        dataset_dir.mkdir(parents=True, exist_ok=True)

        metadata_dir = self._safe_child_path(dataset_dir, "metadata")
        source_dir = self._safe_child_path(dataset_dir, "source")
        metadata_dir.mkdir(parents=True, exist_ok=True)
        source_dir.mkdir(parents=True, exist_ok=True)

        doi_path = self._safe_child_path(metadata_dir, "dataset-metadata.json")
        doi_payload = json.dumps({"doi": doi_value, "persistentId": persistent_id}, indent=2, ensure_ascii=False)
        doi_path.write_text(doi_payload, encoding="utf-8")

        exported_path = self._safe_child_path(source_dir, "dataverse-json-export.json")
        exported_path.write_text(exported_content, encoding="utf-8")

        context_path = self._safe_child_path(dataset_dir, "_pipeline-context.json")
        context_path.write_text(
            json.dumps(
                {
                    "dataset_id": self.dataset_id,
                    "app_name": self.app_name,
                    "target_repo": self.target.repo_name,
                    "target_url": self.target.target_url,
                    "doi": doi_value,
                    "persistentId": persistent_id,
                    "export_url": export_url,
                    "written_at": datetime.now(timezone.utc).isoformat(),
                    "files": [
                        str(doi_path),
                        str(exported_path),
                    ],
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        tdm = TargetDataModel()
        tdm.deposit_status = DepositStatus.FINISH
        tdm.deposited_metadata = {
            "doi": doi_value,
            "persistentId": persistent_id,
            "export_url": export_url,
            "output_dir": str(dataset_dir),
            "doi_file": str(doi_path),
            "exported_json_file": str(exported_path),
            "context_file": str(context_path),
        }
        tdm.response = TargetResponse(
            url=export_url,
            status_code=response.status_code,
            content=tdm.deposited_metadata,
            content_type=ResponseContentType.JSON,
        )
        return tdm
