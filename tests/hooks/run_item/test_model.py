import pytest
from datetime import datetime, timedelta
from airflow.providers.microsoft.fabric.hooks.run_item.model import (
    ItemDefinition,
    RunItemConfig,
    RunItemTracker,
    RunItemOutput,
    MSFabricRunItemStatus,
    _dump_datetime,
    _load_datetime,
    _dump_timedelta,
    _load_timedelta,
)


class TestModelSerialization:
    """Test serialization and deserialization of model classes."""

    def test_item_definition_serialization(self):
        """Test ItemDefinition to_dict and from_dict methods."""
        # Test with all fields
        item = ItemDefinition(
            workspace_id="workspace-123",
            item_type="notebook",
            item_id="item-456",
            item_name="Test Notebook"
        )
        
        # Test serialization
        data = item.to_dict()
        expected = {
            "workspace_id": "workspace-123",
            "item_type": "notebook", 
            "item_id": "item-456",
            "item_name": "Test Notebook"
        }
        assert data == expected
        
        # Test deserialization
        restored = ItemDefinition.from_dict(data)
        assert restored == item
        assert restored.workspace_id == "workspace-123"
        assert restored.item_type == "notebook"
        assert restored.item_id == "item-456"
        assert restored.item_name == "Test Notebook"

    def test_item_definition_with_empty_name(self):
        """Test ItemDefinition with empty item_name (default)."""
        item = ItemDefinition(
            workspace_id="workspace-123",
            item_type="notebook",
            item_id="item-456"
        )
        
        data = item.to_dict()
        assert data["item_name"] == ""
        
        restored = ItemDefinition.from_dict(data)
        assert restored.item_name == ""

    def test_item_definition_missing_required_fields(self):
        """Test ItemDefinition validation for missing required fields."""
        # Test missing workspace_id
        with pytest.raises(ValueError, match="ItemDefinition missing required keys: \\['workspace_id'\\]"):
            ItemDefinition.from_dict({
                "item_type": "notebook",
                "item_id": "item-456"
            })
            
        # Test missing multiple fields
        with pytest.raises(ValueError, match="ItemDefinition missing required keys:"):
            ItemDefinition.from_dict({
                "item_name": "Test"
            })
            
        # Test empty dict
        with pytest.raises(ValueError, match="ItemDefinition missing required keys:"):
            ItemDefinition.from_dict({})

    def test_run_item_config_serialization(self):
        """Test RunItemConfig to_dict and from_dict methods."""
        config = RunItemConfig(
            fabric_conn_id="fabric_default",
            timeout_seconds=3600,
            poll_interval_seconds=60
        )
        
        # Test serialization (should exclude tenacity_retry)
        data = config.to_dict()
        expected = {
            "fabric_conn_id": "fabric_default",
            "timeout_seconds": 3600,
            "poll_interval_seconds": 60
        }
        assert data == expected
        assert "tenacity_retry" not in data
        
        # Test deserialization
        restored = RunItemConfig.from_dict(data)
        assert restored.fabric_conn_id == "fabric_default"
        assert restored.timeout_seconds == 3600
        assert restored.poll_interval_seconds == 60
        assert restored.tenacity_retry is None

    def test_run_item_tracker_serialization(self):
        """Test RunItemTracker to_dict and from_dict methods."""
        start_time = datetime(2023, 12, 1, 10, 30, 45)
        retry_after = timedelta(seconds=30)
        
        item = ItemDefinition(
            workspace_id="workspace-123",
            item_type="notebook",
            item_id="item-456",
            item_name="Test Item"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://api.fabric.microsoft.com/v1/workspaces/workspace-123/items/item-456/jobs/run-789",
            run_timeout_in_seconds=1800,
            start_time=start_time,
            retry_after=retry_after
        )
        
        # Test serialization
        data = tracker.to_dict()
        expected = {
            "item": {
                "workspace_id": "workspace-123",
                "item_type": "notebook",
                "item_id": "item-456",
                "item_name": "Test Item"
            },
            "run_id": "run-789",
            "location_url": "https://api.fabric.microsoft.com/v1/workspaces/workspace-123/items/item-456/jobs/run-789",
            "run_timeout_in_seconds": 1800,
            "start_time": "2023-12-01T10:30:45",
            "retry_after": 30.0
        }
        assert data == expected
        
        # Test deserialization
        restored = RunItemTracker.from_dict(data)
        assert restored.item == item
        assert restored.run_id == "run-789"
        assert restored.location_url == "https://api.fabric.microsoft.com/v1/workspaces/workspace-123/items/item-456/jobs/run-789"
        assert restored.run_timeout_in_seconds == 1800
        assert restored.start_time == start_time
        assert restored.retry_after == retry_after
        assert restored.output is None

    def test_run_item_tracker_with_none_retry_after(self):
        """Test RunItemTracker serialization with None retry_after."""
        item = ItemDefinition(
            workspace_id="workspace-123",
            item_type="notebook",
            item_id="item-456"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=1800,
            start_time=datetime(2023, 12, 1, 10, 30, 45),
            retry_after=None
        )
        
        data = tracker.to_dict()
        assert data["retry_after"] is None
        
        restored = RunItemTracker.from_dict(data)
        assert restored.retry_after is None

    def test_run_item_tracker_missing_required_fields(self):
        """Test RunItemTracker validation for missing required fields."""
        # Test missing item
        with pytest.raises(ValueError, match="RunItemTracker.item must be a dict"):
            RunItemTracker.from_dict({
                "run_id": "run-123",
                "location_url": "https://example.com",
                "run_timeout_in_seconds": 1800,
                "start_time": "2023-12-01T10:30:45",
                "retry_after": None
            })
            
        # Test invalid start_time
        with pytest.raises(ValueError, match="RunItemTracker.start_time must be ISO string"):
            RunItemTracker.from_dict({
                "item": {"workspace_id": "ws", "item_type": "notebook", "item_id": "item"},
                "run_id": "run-123",
                "location_url": "https://example.com", 
                "run_timeout_in_seconds": 1800,
                "start_time": 12345,  # Invalid - should be string
                "retry_after": None
            })

    def test_run_item_output_serialization(self):
        """Test RunItemOutput to_dict and from_dict methods."""
        start_time = datetime(2023, 12, 1, 10, 30, 45)
        
        item = ItemDefinition(
            workspace_id="workspace-123",
            item_type="notebook",
            item_id="item-456",
            item_name="Test Item"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=1800,
            start_time=start_time,
            retry_after=timedelta(seconds=30)
        )
        
        output = RunItemOutput(
            tracker=tracker,
            status=MSFabricRunItemStatus.COMPLETED,
            failed_reason=None
        )
        
        # Test serialization
        data = output.to_dict()
        assert data["status"] == "Completed"
        assert data["failed_reason"] is None
        assert "tracker" in data
        assert isinstance(data["tracker"], dict)
        
        # Test deserialization
        restored = RunItemOutput.from_dict(data)
        assert restored.tracker == tracker
        assert restored.status == MSFabricRunItemStatus.COMPLETED
        assert restored.failed_reason is None

    def test_run_item_output_with_failure(self):
        """Test RunItemOutput serialization with failure status and reason."""
        item = ItemDefinition(
            workspace_id="workspace-123",
            item_type="notebook", 
            item_id="item-456"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=1800,
            start_time=datetime(2023, 12, 1, 10, 30, 45),
            retry_after=None
        )
        
        output = RunItemOutput(
            tracker=tracker,
            status=MSFabricRunItemStatus.FAILED,
            failed_reason="Connection timeout"
        )
        
        data = output.to_dict()
        assert data["status"] == "Failed"
        assert data["failed_reason"] == "Connection timeout"
        
        restored = RunItemOutput.from_dict(data)
        assert restored.status == MSFabricRunItemStatus.FAILED
        assert restored.failed_reason == "Connection timeout"

    def test_run_item_output_invalid_status(self):
        """Test RunItemOutput validation for invalid status."""
        tracker_data = {
            "item": {"workspace_id": "ws", "item_type": "notebook", "item_id": "item", "item_name": ""},
            "run_id": "run-123",
            "location_url": "https://example.com",
            "run_timeout_in_seconds": 1800,
            "start_time": "2023-12-01T10:30:45",
            "retry_after": None
        }
        
        # Test invalid status
        with pytest.raises(ValueError, match="Invalid status 'InvalidStatus'"):
            RunItemOutput.from_dict({
                "tracker": tracker_data,
                "status": "InvalidStatus",
                "failed_reason": None
            })

    def test_datetime_helpers(self):
        """Test datetime serialization helper functions."""
        dt = datetime(2023, 12, 1, 10, 30, 45, 123456)
        
        # Test dump and load
        dumped = _dump_datetime(dt)
        assert dumped == "2023-12-01T10:30:45.123456"
        
        loaded = _load_datetime(dumped)
        assert loaded == dt

    def test_timedelta_helpers(self):
        """Test timedelta serialization helper functions."""
        # Test with timedelta
        td = timedelta(seconds=30, milliseconds=500)
        dumped = _dump_timedelta(td)
        assert dumped == 30.5
        
        loaded = _load_timedelta(dumped)
        assert loaded == td
        
        # Test with None
        assert _dump_timedelta(None) is None
        assert _load_timedelta(None) is None

    def test_full_roundtrip_serialization(self):
        """Test complete roundtrip serialization for all models."""
        # Create complex nested structure
        item = ItemDefinition(
            workspace_id="workspace-123",
            item_type="notebook",
            item_id="item-456",
            item_name="Integration Test Notebook"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://api.fabric.microsoft.com/v1/workspaces/workspace-123/items/item-456/jobs/run-789",
            run_timeout_in_seconds=3600,
            start_time=datetime(2023, 12, 1, 15, 45, 30, 123456),
            retry_after=timedelta(minutes=2, seconds=15)
        )
        
        output = RunItemOutput(
            tracker=tracker,
            status=MSFabricRunItemStatus.IN_PROGRESS,
            failed_reason=None
        )
        
        # Serialize to dict
        data = output.to_dict()
        
        # Deserialize back
        restored_output = RunItemOutput.from_dict(data)
        
        # Verify everything matches
        assert restored_output.status == MSFabricRunItemStatus.IN_PROGRESS
        assert restored_output.failed_reason is None
        
        restored_tracker = restored_output.tracker
        assert restored_tracker.run_id == "run-789"
        assert restored_tracker.location_url == "https://api.fabric.microsoft.com/v1/workspaces/workspace-123/items/item-456/jobs/run-789"
        assert restored_tracker.run_timeout_in_seconds == 3600
        assert restored_tracker.start_time == datetime(2023, 12, 1, 15, 45, 30, 123456)
        assert restored_tracker.retry_after == timedelta(minutes=2, seconds=15)
        
        restored_item = restored_tracker.item
        assert restored_item.workspace_id == "workspace-123"
        assert restored_item.item_type == "notebook"
        assert restored_item.item_id == "item-456"
        assert restored_item.item_name == "Integration Test Notebook"

    def test_enum_values(self):
        """Test that MSFabricRunItemStatus enum values serialize correctly."""
        test_statuses = [
            MSFabricRunItemStatus.IN_PROGRESS,
            MSFabricRunItemStatus.COMPLETED,
            MSFabricRunItemStatus.FAILED,
            MSFabricRunItemStatus.CANCELLED,
            MSFabricRunItemStatus.NOT_STARTED,
            MSFabricRunItemStatus.DEDUPED,
            MSFabricRunItemStatus.TIMED_OUT,
            MSFabricRunItemStatus.DISABLED
        ]
        
        for status in test_statuses:
            # Create a minimal output to test status serialization
            item = ItemDefinition(workspace_id="ws", item_type="notebook", item_id="item")
            tracker = RunItemTracker(
                item=item,
                run_id="run",
                location_url="url",
                run_timeout_in_seconds=1800,
                start_time=datetime.now(),
                retry_after=None
            )
            output = RunItemOutput(tracker=tracker, status=status)
            
            # Test serialization preserves enum value
            data = output.to_dict()
            assert data["status"] == status.value
            
            # Test deserialization recreates enum
            restored = RunItemOutput.from_dict(data)
            assert restored.status == status