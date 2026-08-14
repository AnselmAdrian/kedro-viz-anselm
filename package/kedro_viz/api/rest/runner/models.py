"""Data models for the runner module."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel


class RunConfig(BaseModel):
    """Request body for POST /api/run."""

    pipeline: Optional[str] = None
    env: Optional[str] = None
    tags: Optional[list[str]] = None
    params: Optional[dict[str, Any]] = None
    from_nodes: Optional[list[str]] = None
    to_nodes: Optional[list[str]] = None


class JobStatus(str, Enum):
    """Status of a pipeline execution job."""

    INITIALIZE = "initialize"
    RUNNING = "running"
    FINISHED = "finished"
    ERROR = "error"
    TERMINATED = "terminated"
    INTERRUPTED = "interrupted"


@dataclass
class NodeEvent:
    """A single node lifecycle event during pipeline execution."""

    node_id: str
    node_name: str
    status: str  # "running", "success", "failed"
    timestamp: datetime
    duration: Optional[float] = None
    error: Optional[str] = None


@dataclass
class RunProgress:
    """Aggregated progress state for a pipeline run."""

    nodes_total: int = 0
    nodes_completed: int = 0
    current_node: Optional[str] = None
    node_events: list[NodeEvent] = field(default_factory=list)


@dataclass
class Job:
    """Internal state for a pipeline execution job."""

    job_id: str
    status: JobStatus
    start_time: datetime
    config: Optional[RunConfig] = None
    cmd: str = ""
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    returncode: Optional[int] = None
    pid: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    error_summary: Optional[str] = None
    progress: Optional[RunProgress] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert Job to a plain dict suitable for JSON serialisation.

        Handles:
        - datetime → ISO 8601 string
        - JobStatus enum → string value
        - RunConfig → dict (via model_dump())
        - RunProgress/NodeEvent → dict (via dataclasses.asdict)
        - Skips stdout/stderr (stored in separate log files)
        """
        data: dict[str, Any] = {
            "job_id": self.job_id,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "cmd": self.cmd,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "returncode": self.returncode,
            "pid": self.pid,
            "error_summary": self.error_summary,
            "config": self.config.model_dump() if self.config else None,
            "progress": self._progress_to_dict() if self.progress else None,
        }
        return data

    def _progress_to_dict(self) -> dict[str, Any]:
        """Convert RunProgress to a dict with datetime handling."""
        if self.progress is None:
            return None  # type: ignore[return-value]
        raw = asdict(self.progress)
        # Convert datetime objects in node_events to ISO strings
        for event in raw.get("node_events", []):
            if isinstance(event.get("timestamp"), datetime):
                event["timestamp"] = event["timestamp"].isoformat()
        return raw

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Job":
        """Reconstruct a Job from a dict (e.g. loaded from meta.json).

        Handles:
        - ISO 8601 string → datetime
        - string → JobStatus enum
        - dict → RunConfig (if present)
        - dict → RunProgress with NodeEvent objects (if present)
        """
        config = RunConfig(**data["config"]) if data.get("config") else None

        progress = None
        if data.get("progress"):
            progress = cls._progress_from_dict(data["progress"])

        return cls(
            job_id=data["job_id"],
            status=JobStatus(data["status"]),
            start_time=datetime.fromisoformat(data["start_time"]),
            config=config,
            cmd=data.get("cmd", ""),
            end_time=(
                datetime.fromisoformat(data["end_time"])
                if data.get("end_time")
                else None
            ),
            duration=data.get("duration"),
            returncode=data.get("returncode"),
            pid=data.get("pid"),
            error_summary=data.get("error_summary"),
            progress=progress,
        )

    @staticmethod
    def _progress_from_dict(data: dict[str, Any]) -> RunProgress:
        """Reconstruct RunProgress from a dict."""
        node_events = []
        for event_data in data.get("node_events", []):
            node_events.append(
                NodeEvent(
                    node_id=event_data["node_id"],
                    node_name=event_data["node_name"],
                    status=event_data["status"],
                    timestamp=datetime.fromisoformat(event_data["timestamp"]),
                    duration=event_data.get("duration"),
                    error=event_data.get("error"),
                )
            )
        return RunProgress(
            nodes_total=data.get("nodes_total", 0),
            nodes_completed=data.get("nodes_completed", 0),
            current_node=data.get("current_node"),
            node_events=node_events,
        )

    def to_json(self) -> str:
        """Serialize Job to a JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "Job":
        """Deserialize a Job from a JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class ValidationResult:
    """Result of validating a run configuration or raw command."""

    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
