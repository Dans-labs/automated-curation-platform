from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import httpx
from oaipmh_scythe import Scythe

from src.acp.plugins.harvesters.base import HarvesterPlugin
from src.acp.plugins.harvesters.models import (
    DeletedRecordPolicy,
    HarvestMode,
    HarvestRequest,
    HarvestedRecord,
    OAIPMHSourceConfig,
)


class OAIPMHHarvesterError(RuntimeError):
    """Raised when an OAI-PMH harvesting operation fails."""


class OAIPMHHarvesterPlugin(HarvesterPlugin):
    plugin_name = "oai-pmh"

    def __init__(
        self,
        config: OAIPMHSourceConfig,
    ) -> None:
        self.config = config

    def _build_auth(self) -> httpx.Auth | None:
        authentication = self.config.authentication

        if authentication is None:
            return None

        if not authentication.username_env:
            return None

        username = os.getenv(authentication.username_env)

        password = (
            os.getenv(authentication.password_env)
            if authentication.password_env
            else None
        )

        if not username or password is None:
            raise OAIPMHHarvesterError(
                "OAI-PMH authentication variables are missing"
            )

        return httpx.BasicAuth(
            username=username,
            password=password,
        )

    def _create_client(self) -> Scythe:
        return Scythe(
            str(self.config.base_url),
            auth=self._build_auth(),
        )

    def identify(self) -> dict[str, Any]:
        try:
            with self._create_client() as scythe:
                repository = scythe.identify()

                return {
                    "repository_name": getattr(
                        repository,
                        "repositoryName",
                        None,
                    ),
                    "base_url": getattr(
                        repository,
                        "baseURL",
                        str(self.config.base_url),
                    ),
                    "protocol_version": getattr(
                        repository,
                        "protocolVersion",
                        None,
                    ),
                    "earliest_datestamp": getattr(
                        repository,
                        "earliestDatestamp",
                        None,
                    ),
                    "deleted_record": getattr(
                        repository,
                        "deletedRecord",
                        None,
                    ),
                    "granularity": getattr(
                        repository,
                        "granularity",
                        None,
                    ),
                }

        except Exception as exc:
            raise OAIPMHHarvesterError(
                f"Cannot identify OAI-PMH repository "
                f"{self.config.base_url}: {exc}"
            ) from exc

    def list_metadata_formats(self) -> list[dict[str, Any]]:
        try:
            with self._create_client() as scythe:
                return [
                    {
                        "metadata_prefix": getattr(
                            item,
                            "metadataPrefix",
                            None,
                        ),
                        "schema": getattr(
                            item,
                            "schema",
                            None,
                        ),
                        "namespace": getattr(
                            item,
                            "metadataNamespace",
                            None,
                        ),
                    }
                    for item in scythe.list_metadata_formats()
                ]

        except Exception as exc:
            raise OAIPMHHarvesterError(
                "Cannot retrieve OAI-PMH metadata formats"
            ) from exc

    def list_sets(self) -> list[dict[str, Any]]:
        try:
            with self._create_client() as scythe:
                return [
                    {
                        "set_spec": getattr(item, "setSpec", None),
                        "set_name": getattr(item, "setName", None),
                    }
                    for item in scythe.list_sets()
                ]

        except Exception as exc:
            raise OAIPMHHarvesterError(
                "Cannot retrieve OAI-PMH sets"
            ) from exc

    def validate_source(self) -> dict[str, Any]:
        identity = self.identify()
        formats = self.list_metadata_formats()

        supported_prefixes = {
            item["metadata_prefix"]
            for item in formats
            if item["metadata_prefix"]
        }

        if (
            self.config.metadata_prefix
            not in supported_prefixes
        ):
            raise OAIPMHHarvesterError(
                f"Metadata prefix "
                f"{self.config.metadata_prefix!r} is not supported. "
                f"Supported prefixes: "
                f"{sorted(supported_prefixes)}"
            )

        return {
            "valid": True,
            "identity": identity,
            "metadata_formats": formats,
        }

    def harvest(
        self,
        request: HarvestRequest,
    ) -> Iterator[HarvestedRecord]:
        parameters: dict[str, Any] = {
            "metadata_prefix":
                request.source.metadata_prefix,
        }

        if request.source.set_spec:
            parameters["set_"] = request.source.set_spec

        if request.from_date:
            parameters["from_"] = request.from_date

        if request.until_date:
            parameters["until"] = request.until_date

        ignore_deleted = (
            request.source.deleted_record_policy
            == DeletedRecordPolicy.IGNORE
        )

        harvested_count = 0

        try:
            with self._create_client() as scythe:
                if request.source.mode == HarvestMode.IDENTIFIERS:
                    iterator = scythe.list_identifiers(
                        ignore_deleted=ignore_deleted,
                        **parameters,
                    )
                else:
                    iterator = scythe.list_records(
                        ignore_deleted=ignore_deleted,
                        **parameters,
                    )

                for item in iterator:
                    yield self._convert_item(
                        item=item,
                        metadata_prefix=(
                            request.source.metadata_prefix
                        ),
                        identifiers_only=(
                            request.source.mode
                            == HarvestMode.IDENTIFIERS
                        ),
                    )

                    harvested_count += 1

                    if (
                        request.limit is not None
                        and harvested_count >= request.limit
                    ):
                        break

        except Exception as exc:
            raise OAIPMHHarvesterError(
                f"OAI-PMH harvest failed for "
                f"{request.source.base_url}: {exc}"
            ) from exc

    def get_record(
        self,
        identifier: str,
        metadata_prefix: str | None = None,
    ) -> HarvestedRecord:
        prefix = (
            metadata_prefix
            or self.config.metadata_prefix
        )

        try:
            with self._create_client() as scythe:
                record = scythe.get_record(
                    identifier=identifier,
                    metadata_prefix=prefix,
                )

            return self._convert_item(
                item=record,
                metadata_prefix=prefix,
                identifiers_only=False,
            )

        except Exception as exc:
            raise OAIPMHHarvesterError(
                f"Cannot retrieve OAI-PMH record "
                f"{identifier}: {exc}"
            ) from exc

    @staticmethod
    def _convert_item(
        *,
        item: Any,
        metadata_prefix: str,
        identifiers_only: bool,
    ) -> HarvestedRecord:
        header = getattr(item, "header", item)

        identifier = (
            getattr(header, "identifier", None)
            or getattr(item, "identifier", None)
        )

        if not identifier:
            raise OAIPMHHarvesterError(
                "Harvested OAI-PMH item has no identifier"
            )

        deleted = bool(
            getattr(header, "deleted", False)
            or getattr(header, "status", None) == "deleted"
        )

        set_specs = (
            getattr(header, "setSpecs", None)
            or getattr(header, "set_specs", None)
            or []
        )

        metadata = None

        if not identifiers_only and not deleted:
            metadata = getattr(item, "metadata", None)

        return HarvestedRecord(
            provider_identifier=str(identifier),
            datestamp=(
                str(getattr(header, "datestamp", ""))
                or None
            ),
            set_specs=list(set_specs),
            deleted=deleted,
            metadata_prefix=metadata_prefix,
            metadata=metadata,
            harvested_at=datetime.now(timezone.utc),
        )