from __future__ import annotations

from typing import Any, Dict, Tuple

from airflow.providers.microsoft.fabric.hooks.run_item.dataflow_gen2 import (
    DataflowGen2Config,
    MSFabricRunDataflowGen2Hook,
)
from airflow.providers.microsoft.fabric.hooks.run_item.model import RunItemTracker
from airflow.providers.microsoft.fabric.triggers.run_item.base import BaseFabricRunItemTrigger


class MSFabricRunDataflowGen2Trigger(BaseFabricRunItemTrigger):
    """
    Trigger that monitors a Microsoft Fabric Dataflows Gen2 refresh until it
    reaches a terminal state (Completed, Failed, Cancelled, or TimedOut).

    This trigger is created by MSFabricRunDataflowGen2Operator when running in
    deferrable mode and polls the Fabric Job Scheduler status endpoint.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        tracker: Dict[str, Any],
    ):
        super().__init__()
        self.config_dict = config
        self.tracker_dict = tracker

    def initialize_hook_and_tracker(self) -> Tuple[MSFabricRunDataflowGen2Hook, RunItemTracker]:
        """Initialize and return the hook and tracker instances from serialized dicts."""
        self.log.info(
            "Initializing Dataflows Gen2 trigger - conn_id: %s",
            self.config_dict.get("fabric_conn_id", "Unknown"),
        )

        config = DataflowGen2Config.from_dict(self.config_dict)
        tracker = RunItemTracker.from_dict(self.tracker_dict)
        hook = MSFabricRunDataflowGen2Hook(config=config)

        self.log.info(
            "Dataflows Gen2 trigger initialized - conn_id: %s, run_id: %s, "
            "workspace_id: %s, item_id: %s",
            config.fabric_conn_id,
            tracker.run_id,
            tracker.item.workspace_id,
            tracker.item.item_id,
        )

        return hook, tracker

    def serialize(self):
        """Serialize the trigger for Airflow's deferral mechanism."""
        return (
            "airflow.providers.microsoft.fabric.triggers.run_item.dataflow_gen2"
            ".MSFabricRunDataflowGen2Trigger",
            {
                "config": self.config_dict,
                "tracker": self.tracker_dict,
            },
        )
