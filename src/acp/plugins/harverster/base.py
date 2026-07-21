from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

from src.acp.plugins.harvesters.models import (
    HarvestRequest,
    HarvestedRecord,
)


class HarvesterPlugin(ABC):
    plugin_name: str

    @abstractmethod
    def identify(self) -> dict[str, Any]:
        """Return information describing the source repository."""

    @abstractmethod
    def validate_source(self) -> dict[str, Any]:
        """Validate connectivity and requested metadata format."""

    @abstractmethod
    def harvest(
        self,
        request: HarvestRequest,
    ) -> Iterator[HarvestedRecord]:
        """Yield records without loading an entire harvest into memory."""

    @abstractmethod
    def get_record(
        self,
        identifier: str,
        metadata_prefix: str | None = None,
    ) -> HarvestedRecord:
        """Harvest one source record."""