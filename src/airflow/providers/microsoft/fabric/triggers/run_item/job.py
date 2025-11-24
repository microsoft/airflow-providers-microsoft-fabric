from __future__ import annotations
from typing import Any, Dict, Tuple

from airflow.providers.microsoft.fabric.hooks.run_item.job import JobSchedulerConfig, MSFabricRunJobHook
from airflow.providers.microsoft.fabric.hooks.run_item.model import RunItemTracker
from airflow.providers.microsoft.fabric.triggers.run_item.base import BaseFabricRunItemTrigger

class MSFabricRunJobTrigger(BaseFabricRunItemTrigger):
    """Trigger when a Fabric job is scheduled."""

    def __init__(
        self,
        config: Dict[str, Any],
        tracker: Dict[str, Any],
    ):
        super().__init__()
        # Save Dictionaries: used for serialization and initialization
        self.config_dict = config
        self.tracker_dict = tracker

    def initialize_hook_and_tracker(self) -> Tuple[MSFabricRunJobHook, RunItemTracker]:
        """Initialize and return hook and tracker instances."""
        self.log.info("Initializing Job Scheduler trigger with config: %s", self.config_dict.get('fabric_conn_id', 'Unknown'))
        
        # Parse configuration and tracker from dictionaries
        config = JobSchedulerConfig.from_dict(self.config_dict)
        tracker = RunItemTracker.from_dict(self.tracker_dict)
        
        # Create hook
        hook = MSFabricRunJobHook(config=config)
        
        self.log.info(
            "Job trigger initialized - conn_id: %s, run_id: %s, workspace_id: %s, item_id: %s",
            config.fabric_conn_id,
            tracker.run_id,
            tracker.item.workspace_id,
            tracker.item.item_id
        )
        
        return hook, tracker


    def serialize(self):
        """Serialize the MSFabricRunJobTrigger instance."""
        return (
            "airflow.providers.microsoft.fabric.triggers.run_item.MSFabricRunJobTrigger",
            {
                "config": self.config_dict,
                "tracker": self.tracker_dict
            },
        )