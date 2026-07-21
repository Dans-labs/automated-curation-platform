from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class HarvestMode(StrEnum):
    IDENTIFIERS = "identifiers"
    RECORDS = "records"


class DeletedRecordPolicy(StrEnum):
    INCLUDE = "include"
    IGNORE = "ignore"
    TOMBSTONE = "tombstone"


class OAIPMHAuthentication(BaseModel):
    username_env: str | None = None
    password_env: str | None = None


class OAIPMHSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plugin: str = "oai-pmh"
    base_url: HttpUrl

    metadata_prefix: str = "oai_dc"
    set_spec: str | None = None

    mode: HarvestMode = HarvestMode.RECORDS
    deleted_record_policy: DeletedRecordPolicy = (
        DeletedRecordPolicy.IGNORE
    )

    timeout_seconds: float = Field(
        default=60,
        gt=0,
        le=600,
    )

    verify_ssl: bool = True
    user_agent: str = "ORCHESTRATOR-ACP-Harvester/0.1"

    authentication: OAIPMHAuthentication | None = None


class HarvestRequest(BaseModel):
    source: OAIPMHSourceConfig

    batch_id: str

    from_date: str | None = None
    until_date: str | None = None

    limit: int | None = Field(
        default=None,
        ge=1,
    )


class HarvestedRecord(BaseModel):
    provider_identifier: str
    datestamp: str | None = None
    set_specs: list[str] = Field(default_factory=list)

    deleted: bool = False

    metadata_prefix: str
    metadata: dict[str, Any] | None = None

    raw_xml: str | None = None

    harvested_at: datetime