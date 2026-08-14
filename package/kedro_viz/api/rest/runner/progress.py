"""Node-level progress tracking for pipeline runs."""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Optional

from kedro_viz.api.rest.runner.models import NodeEvent, RunProgress


class ProgressTracker:
    """Tracks node-level execution progress for active pipeline runs.

    Thread-safe: all methods acquire a lock before accessing state.
    """

    def __init__(self):
        self._progress: dict[str, RunProgress] = {}
        self._node_start_times: dict[str, dict[str, datetime]] = {}
        self._lock = threading.Lock()

    def init_run(self, job_id: str, total_nodes: int = 0) -> None:
        """Initialize progress tracking for a new run.

        Args:
            job_id: The job identifier.
            total_nodes: Expected number of nodes to execute.
        """
        with self._lock:
            self._progress[job_id] = RunProgress(nodes_total=total_nodes)
            self._node_start_times[job_id] = {}

    def node_started(self, job_id: str, node_id: str, node_name: str) -> None:
        """Record that a node has started execution.

        Args:
            job_id: The job identifier.
            node_id: Unique identifier for the node.
            node_name: Human-readable node name.
        """
        with self._lock:
            progress = self._progress.get(job_id)
            if not progress:
                return
            now = datetime.now()
            progress.current_node = node_name
            progress.node_events.append(
                NodeEvent(
                    node_id=node_id,
                    node_name=node_name,
                    status="running",
                    timestamp=now,
                )
            )
            self._node_start_times.setdefault(job_id, {})[node_id] = now

    def node_completed(self, job_id: str, node_id: str, node_name: str) -> None:
        """Record that a node has completed successfully.

        Args:
            job_id: The job identifier.
            node_id: Unique identifier for the node.
            node_name: Human-readable node name.
        """
        with self._lock:
            progress = self._progress.get(job_id)
            if not progress:
                return
            now = datetime.now()
            progress.nodes_completed += 1
            start = self._node_start_times.get(job_id, {}).get(node_id)
            duration = (now - start).total_seconds() if start else None
            progress.node_events.append(
                NodeEvent(
                    node_id=node_id,
                    node_name=node_name,
                    status="success",
                    timestamp=now,
                    duration=duration,
                )
            )
            if progress.current_node == node_name:
                progress.current_node = None

    def node_failed(
        self, job_id: str, node_id: str, node_name: str, error: str = ""
    ) -> None:
        """Record that a node has failed.

        Args:
            job_id: The job identifier.
            node_id: Unique identifier for the node.
            node_name: Human-readable node name.
            error: Error message describing the failure.
        """
        with self._lock:
            progress = self._progress.get(job_id)
            if not progress:
                return
            now = datetime.now()
            start = self._node_start_times.get(job_id, {}).get(node_id)
            duration = (now - start).total_seconds() if start else None
            progress.node_events.append(
                NodeEvent(
                    node_id=node_id,
                    node_name=node_name,
                    status="failed",
                    timestamp=now,
                    duration=duration,
                    error=error,
                )
            )
            if progress.current_node == node_name:
                progress.current_node = None

    def get_progress(self, job_id: str) -> Optional[RunProgress]:
        """Return current progress state for a job.

        Args:
            job_id: The job identifier.

        Returns:
            The RunProgress instance if found, otherwise None.
        """
        with self._lock:
            return self._progress.get(job_id)
