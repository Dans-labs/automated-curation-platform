from __future__ import annotations

import json
import os
import re
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

from src.acp.bridge import Bridge
from src.acp.commons import handle_deposit_exceptions, transform
from src.acp.db.dbz import DepositStatus
from src.acp.models.bridge_output_model import (
    ResponseContentType,
    TargetDataModel,
    TargetResponse,
)


class FolderPipelineDepositor(Bridge):
    """
    Save the latest pipeline output files into a local folder target.
    """

    @staticmethod
    def _sanitize_segment(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]", "-", value).strip("-") or "dataset"

    @staticmethod
    def _safe_child_path(base_dir: Path, *segments: str) -> Path:
        candidate = (base_dir.joinpath(*segments)).resolve()
        if base_dir != candidate and base_dir not in candidate.parents:
            raise ValueError("Resolved path escapes the configured target directory.")
        return candidate

    @handle_deposit_exceptions
    def job(self) -> TargetDataModel:
        parsed_url = urllib.parse.urlparse(self.target.target_url)
        if parsed_url.scheme != "file" or not parsed_url.path:
            raise ValueError("FolderPipelineDepositor requires a file:// target-url.")

        base_dir = Path(urllib.parse.unquote(parsed_url.path)).resolve()
        dataset_dir = self._safe_child_path(base_dir, self._sanitize_segment(self.dataset_id))
        dataset_dir.mkdir(parents=True, exist_ok=True)

        transformed = (
            self.target.metadata.transformed_metadata
            if self.target.metadata and self.target.metadata.transformed_metadata
            else []
        )

        written_files: list[dict[str, str]] = []

        if not transformed:
            output_file = self._safe_child_path(dataset_dir, "metadata-content.json")
            output_file.write_text(self.dataset_rec.metadata_content, encoding="utf-8")
            written_files.append({"name": "metadata-content.json", "path": str(output_file)})
        else:
            for tm in transformed:
                content = self.dataset_rec.metadata_content
                if tm.transformer_url:
                    content = transform(
                        f"{tm.transformer_url}?app_name={self.app_name}",
                        self.dataset_rec.metadata_content,
                    )

                rel_dir = tm.dir or ""
                output_dir = self._safe_child_path(dataset_dir, rel_dir)
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = self._safe_child_path(output_dir, tm.name)
                output_file.write_text(content, encoding="utf-8")
                written_files.append(
                    {
                        "name": tm.name,
                        "dir": rel_dir,
                        "path": str(output_file),
                    }
                )

        context_file = self._safe_child_path(dataset_dir, "_pipeline-context.json")
        context_file.write_text(
            json.dumps(
                {
                    "dataset_id": self.dataset_id,
                    "app_name": self.app_name,
                    "target_repo": self.target.repo_name,
                    "target_url": self.target.target_url,
                    "written_at": datetime.now(timezone.utc).isoformat(),
                    "files": written_files,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        tdm = TargetDataModel()
        tdm.deposit_status = DepositStatus.FINISH
        tdm.deposited_metadata = {
            "output_dir": str(dataset_dir),
            "files_written": written_files,
            "context_file": str(context_file),
        }
        tdm.response = TargetResponse(
            url=self.target.target_url,
            status_code=200,
            content=tdm.deposited_metadata,
            content_type=ResponseContentType.JSON,
        )
        return tdm
