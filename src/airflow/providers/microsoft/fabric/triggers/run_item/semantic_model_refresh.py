from __future__ import annotations
from typing import Any, Dict

from airflow.providers.microsoft.fabric.hooks.run_item.semantic_model_refresh import SemanticModelRefreshConfig, MSFabricRunSemanticModelRefreshHook
from airflow.providers.microsoft.fabric.hooks.run_item.model import RunItemTracker
from airflow.providers.microsoft.fabric.triggers.run_item.base import BaseFabricRunItemTrigger

class MSFabricRunSemanticModelRefreshTrigger(BaseFabricRunItemTrigger):
    """Trigger for monitoring a semantic model refresh in a Fabric."""
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

        self.hook = MSFabricRunSemanticModelRefreshHook(config=self.config)

        # Initialize parent to start run
        super().__init__(self.hook, self.tracker)


    def serialize(self):
        """Serialize the MSFabricRunSemanticModelRefreshTrigger instance."""
        return (
            "airflow.providers.microsoft.fabric.triggers.run_item.MSFabricRunSemanticModelRefreshTrigger",
            {
                "config": self.config_dict,
                "tracker": self.tracker_dict
            },
        )