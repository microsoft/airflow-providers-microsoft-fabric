from dataclasses import dataclass, asdict, fields
from datetime import datetime
from typing import Optional

@dataclass
class ItemRunConfig:
    """Represents the configuration for a Microsoft Fabric item run."""
    
    # Operator configuration properties
    fabric_conn_id: str
    workspace_id: str
    item_id: str
    job_type: str
    api_host: str
    scope: str
    check_interval: int # seconds
    api_version: str = "V1"
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create instance from dictionary."""
        # Filter to only valid fields
        valid_keys = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered_data)

@dataclass
class ItemRun(ItemRunConfig):
    """Represents a Microsoft Fabric item run with runtime information."""
    
    # Runtime properties
    run_id: Optional[str] = None
    location_url: Optional[str] = None
    item_name: Optional[str] = None
    start_time: Optional[datetime] = None
    start_polling_time: Optional[datetime] = None
    timeout_time: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary with proper datetime handling."""
        data = asdict(self)
        # Convert all datetime fields to ISO strings
        datetime_fields = ['start_time', 'start_polling_time', 'timeout_time']
        for field in datetime_fields:
            if data.get(field):
                data[field] = getattr(self, field).isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create instance from dictionary with proper datetime handling."""
        data = data.copy()  # Don't modify original
        
        # Convert datetime fields from ISO strings
        datetime_fields = ['start_time', 'start_polling_time', 'timeout_time']
        for field in datetime_fields:
            if data.get(field):
                data[field] = datetime.fromisoformat(data[field])
        
        # Filter to only valid fields
        valid_keys = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered_data)
    
    def is_initialized(self) -> bool:
        """Check if the run has been initialized with runtime data."""
        return bool(self.run_id and self.location_url and self.start_time)
    
    def should_wait_before_polling(self) -> bool:
        """Check if we should wait before starting to poll."""
        if not self.start_polling_time:
            return False
        return datetime.now() < self.start_polling_time
    
    def get_wait_seconds_before_polling(self) -> float:
        """Get seconds to wait before starting polling."""
        if not self.start_polling_time:
            return 0
        wait_seconds = (self.start_polling_time - datetime.now()).total_seconds()
        return max(0, wait_seconds)
    
    def is_timed_out(self) -> bool:
        """Check if the run has exceeded its timeout."""
        if not self.timeout_time:
            return False
        return datetime.now() >= self.timeout_time
    
    def get_elapsed_seconds(self) -> Optional[float]:
        """Get elapsed time since start in seconds."""
        if not self.start_time:
            return None
        return (datetime.now() - self.start_time).total_seconds()
    
    def get_remaining_timeout_seconds(self) -> Optional[float]:
        """Get remaining timeout in seconds, or None if no timeout set."""
        if not self.timeout_time:
            return None
        remaining = (self.timeout_time - datetime.now()).total_seconds()
        return max(0, remaining)
