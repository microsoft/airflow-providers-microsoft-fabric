from __future__ import annotations
from typing import Any, Dict

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
        # Save Dictionaries: used in operator to serialize
        self.config_dict = config
        self.tracker_dict = tracker

        # Save Actual Objects: used in trigger side
        self.config = JobSchedulerConfig.from_dict(self.config_dict)
        self.tracker = RunItemTracker.from_dict(self.tracker_dict)

        self.hook = MSFabricRunJobHook(config=self.config)

        # Initialize parent to start run
        super().__init__(self.hook, self.tracker)


    def serialize(self):
        """Serialize the MSFabricRunJobTrigger instance."""
        return (
            "airflow.providers.microsoft.fabric.triggers.run_item.MSFabricRunJobTrigger",
            {
                "config": self.config_dict,
                "tracker": self.tracker_dict
            },
        )