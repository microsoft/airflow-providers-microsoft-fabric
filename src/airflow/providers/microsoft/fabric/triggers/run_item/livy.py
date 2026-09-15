"""Deferrable trigger for the Fabric Livy **batch** operator.

Mirrors ``triggers/run_item/job.py``: rebuilds the hook + tracker on the
triggerer and delegates polling to ``BaseFabricRunItemTrigger.run``.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from airflow.providers.microsoft.fabric.hooks.run_item.livy import (
    LivyBatchConfig,
    MSFabricLivyBatchHook,
)
from airflow.providers.microsoft.fabric.hooks.run_item.model import RunItemTracker
from airflow.providers.microsoft.fabric.triggers.run_item.base import BaseFabricRunItemTrigger


class MSFabricLivyBatchTrigger(BaseFabricRunItemTrigger):
    """Trigger that polls a Fabric Livy batch to completion."""

    def __init__(self, config: Dict[str, Any], tracker: Dict[str, Any]):
        super().__init__()
        self.config_dict = config
        self.tracker_dict = tracker

    def initialize_hook_and_tracker(self) -> Tuple[MSFabricLivyBatchHook, RunItemTracker]:
        config = LivyBatchConfig.from_dict(self.config_dict)
        tracker = RunItemTracker.from_dict(self.tracker_dict)
        hook = MSFabricLivyBatchHook(config=config)
        self.log.info(
            "Livy batch trigger initialized - conn_id: %s, batch_id: %s, workspace_id: %s",
            config.fabric_conn_id, tracker.run_id, tracker.item.workspace_id,
        )
        return hook, tracker

    def serialize(self):
        return (
            "airflow.providers.microsoft.fabric.triggers.run_item.MSFabricLivyBatchTrigger",
            {"config": self.config_dict, "tracker": self.tracker_dict},
        )
