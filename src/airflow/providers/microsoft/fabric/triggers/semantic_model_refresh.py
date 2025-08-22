from __future__ import annotations
from typing import Any, Dict

from airflow.providers.microsoft.fabric.hooks.semantic_model_refresh import SemanticModelRefreshConfig, MSFabricSemanticModelRefreshHook
from airflow.providers.microsoft.fabric.hooks.run_item_model import RunItemTracker
from airflow.providers.microsoft.fabric.triggers.run_item import MSFabricRunItemTrigger

class MSFabricSemanticModelRefreshTrigger(MSFabricRunItemTrigger):
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
        self.config = SemanticModelRefreshConfig.from_dict(self.config_dict)
        self.tracker = RunItemTracker.from_dict(self.tracker_dict)

        self.hook = MSFabricSemanticModelRefreshHook(config=self.config)

        # Initialize parent to start run
        super().__init__(self.hook, self.tracker)


    def serialize(self):
        """Serialize the PowerBISemanticModelRefreshTrigger instance."""
        return (
            "airflow.providers.microsoft.fabric.triggers.semantic_model_refresh.PowerBISemanticModelRefreshTrigger",
            {
                "config": self.config_dict,
                "tracker": self.tracker_dict
            },
        )