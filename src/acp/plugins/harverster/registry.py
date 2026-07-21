from __future__ import annotations

from collections.abc import Callable

from src.acp.plugins.harvesters.base import HarvesterPlugin
from src.acp.plugins.harvesters.models import OAIPMHSourceConfig
from src.acp.plugins.harvesters.oai_pmh import (
    OAIPMHHarvesterPlugin,
)


HarvesterFactory = Callable[
    [dict],
    HarvesterPlugin,
]


def create_oai_pmh_plugin(
    config: dict,
) -> HarvesterPlugin:
    parsed_config = OAIPMHSourceConfig.model_validate(
        config
    )

    return OAIPMHHarvesterPlugin(parsed_config)


HARVESTER_PLUGINS: dict[str, HarvesterFactory] = {
    "oai-pmh": create_oai_pmh_plugin,
}


def create_harvester_plugin(
    plugin_name: str,
    config: dict,
) -> HarvesterPlugin:
    try:
        factory = HARVESTER_PLUGINS[plugin_name]
    except KeyError as exc:
        raise ValueError(
            f"Unknown harvester plugin: {plugin_name}. "
            f"Available plugins: "
            f"{sorted(HARVESTER_PLUGINS)}"
        ) from exc

    return factory(config)