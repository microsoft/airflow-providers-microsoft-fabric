from dataclasses import dataclass, field, fields as dc_fields
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any
from tenacity import AsyncRetrying

# ---------- Helpers (JSON-safe) ----------
def _dump_datetime(dt: datetime) -> str:
    # Keep as ISO 8601. If you use Zulu times, fromisoformat below handles 'Z' via normalization.
    return dt.isoformat()

def _load_datetime(s: str) -> datetime:
    # Python <3.11 doesn't accept 'Z' -> normalize to +00:00
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

def _dump_timedelta(td: Optional[timedelta]) -> Optional[float]:
    return None if td is None else td.total_seconds()

def _load_timedelta(v: Optional[float]) -> Optional[timedelta]:
    return None if v is None else timedelta(seconds=float(v))

def _filter_kwargs(cls, data: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {f.name for f in dc_fields(cls)}
    return {k: v for k, v in data.items() if k in allowed}

# ---------- Domain ----------
class MSFabricRunItemStatus(Enum):
    IN_PROGRESS = "InProgress"
    COMPLETED   = "Completed"
    FAILED      = "Failed"
    CANCELLED   = "Cancelled"
    NOT_STARTED = "NotStarted"
    DEDUPED     = "Deduped"
    TIMED_OUT   = "TimedOut"
    DISABLED   = "Disabled"

@dataclass(kw_only=True)
class ItemDefinition:
    workspace_id: str
    item_type: str
    item_id: str
    item_name: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "item_type": self.item_type,
            "item_id": self.item_id,
            "item_name": self.item_name,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ItemDefinition":
        d = _filter_kwargs(cls, data or {})
        #item name is not a required parameter
        missing = [k for k in ("workspace_id", "item_type", "item_id") if k not in d]
        if missing:
            raise ValueError(f"ItemDefinition missing required keys: {missing}")
        return cls(**d)

@dataclass(kw_only=True)
class RunItemConfig:
    fabric_conn_id: str
    timeout_seconds: int
    poll_interval_seconds: int
    # Mark non-serializable / runtime-only with metadata so we can drop it
    tenacity_retry: Optional[AsyncRetrying] = field(default=None, repr=False, compare=False, metadata={"dump": False})

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "fabric_conn_id": self.fabric_conn_id,
            "timeout_seconds": self.timeout_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            # tenacity_retry intentionally omitted
        }
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunItemConfig":
        d = _filter_kwargs(cls, data or {})
        # Defaults/fallbacks if you want them:
        d.setdefault("timeout_seconds", 600)
        d.setdefault("poll_interval_seconds", 5)
        d["tenacity_retry"] = None
        missing = [k for k in ("fabric_conn_id",) if k not in d]
        if missing:
            raise ValueError(f"RunItemConfig missing required keys: {missing}")
        return cls(**d)

@dataclass(kw_only=True)
class RunItemTracker:
    item: ItemDefinition
    run_id: str
    location_url: str
    run_timeout_in_seconds: int
    start_time: datetime
    retry_after: Optional[timedelta]

    output: Optional[Any] = None # serialization not supported as this should not go to trigger side.

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item": self.item.to_dict(),
            "run_id": self.run_id,
            "location_url": self.location_url,
            "run_timeout_in_seconds": self.run_timeout_in_seconds,
            "start_time": _dump_datetime(self.start_time),
            "retry_after": _dump_timedelta(self.retry_after),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunItemTracker":
        d = dict(data or {})
        # Rebuild nested + typed fields
        item_raw = d.get("item")
        if not isinstance(item_raw, dict):
            raise ValueError("RunItemTracker.item must be a dict")
        item = ItemDefinition.from_dict(item_raw)

        start_time_raw = d.get("start_time")
        if not isinstance(start_time_raw, str):
            raise ValueError("RunItemTracker.start_time must be ISO string")
        start_time = _load_datetime(start_time_raw)

        retry_after = _load_timedelta(d.get("retry_after"))

        # Validate required keys
        required = ("run_id", "location_url", "run_timeout_in_seconds")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"RunItemTracker missing required keys: {missing}")

        return cls(
            item=item,
            run_id=d["run_id"],
            location_url=d["location_url"],
            run_timeout_in_seconds=int(d["run_timeout_in_seconds"]),
            start_time=start_time,
            retry_after=retry_after,
        )

@dataclass(kw_only=True)
class RunItemOutput:
    tracker: RunItemTracker
    status: MSFabricRunItemStatus
    failed_reason: Optional[str] = None

    result: Optional[Any] = None # for non 202 execution, no trigger usage

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tracker": self.tracker.to_dict(),
            "status": self.status.value,  # Enum -> value
            "failed_reason": self.failed_reason,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunItemOutput":
        d = dict(data or {})
        tr_raw = d.get("tracker")
        if not isinstance(tr_raw, dict):
            raise ValueError("RunItemOutput.tracker must be a dict")
        tracker = RunItemTracker.from_dict(tr_raw)

        status_raw = d.get("status")
        try:
            status = MSFabricRunItemStatus(status_raw)
        except Exception as e:
            raise ValueError(f"Invalid status {status_raw!r}") from e

        return cls(
            tracker=tracker,
            status=status,
            failed_reason=d.get("failed_reason"),
        )