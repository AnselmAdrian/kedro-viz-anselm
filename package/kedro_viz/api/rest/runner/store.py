"""Job store for managing run state."""

from __future__ import annotations

import json
import logging
import shutil
import threading
from pathlib import Path
from typing import Optional

from kedro_viz.api.rest.runner.models import Job, JobStatus

logger = logging.getLogger(__name__)

# In-memory log size limits
MAX_LOG_SIZE = 1_048_576  # 1MB
TRUNCATE_TO = 524_288  # 500KB


class JobStore:
    """Thread-safe in-memory store for pipeline execution jobs.

    Wraps a dict of jobs and a threading lock. All public methods
    acquire the lock before accessing shared state.

    When ``storage_dir`` is provided, job metadata and logs are
    persisted to disk so they survive server restarts. When
    ``storage_dir`` is None the store operates purely in-memory.
    """

    def __init__(
        self, storage_dir: Optional[Path] = None, max_jobs: int = 50
    ) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._storage_dir = storage_dir
        self._max_jobs = max_jobs

        if self._storage_dir:
            self._storage_dir.mkdir(parents=True, exist_ok=True)
            self.hydrate_from_disk()

    def hydrate_from_disk(self) -> None:
        """Load persisted jobs from disk on startup.

        Scans storage_dir for job directories, reads meta.json from each,
        and loads them into memory. Jobs with status "running" or "initialize"
        are marked as "interrupted" (since the server crashed/restarted).
        """
        if not self._storage_dir or not self._storage_dir.exists():
            return

        for job_dir in sorted(
            self._storage_dir.iterdir(), key=lambda p: p.stat().st_mtime
        ):
            if not job_dir.is_dir():
                continue
            meta_path = job_dir / "meta.json"
            if not meta_path.exists():
                continue
            try:
                job = Job.from_json(meta_path.read_text(encoding="utf-8"))
                # Mark stale running jobs as interrupted
                if job.status in (JobStatus.INITIALIZE, JobStatus.RUNNING):
                    job.status = JobStatus.INTERRUPTED
                    self._persist_metadata(job)
                self._jobs[job.job_id] = job
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                logger.warning("Failed to load job from %s: %s", meta_path, exc)

        # Log a summary if any interrupted jobs were found
        interrupted_count = sum(
            1 for j in self._jobs.values() if j.status == JobStatus.INTERRUPTED
        )
        if interrupted_count:
            logger.info(
                "Found %d interrupted job(s) from previous session",
                interrupted_count,
            )

    def add_job(self, job: Job) -> None:
        """Add a job to the store.

        Args:
            job: The Job instance to store.
        """
        with self._lock:
            self._jobs[job.job_id] = job
            self._enforce_cap()
        # Persist outside lock to avoid blocking stream readers
        if self._storage_dir:
            self._persist_metadata(job)

    def update_job(self, job_id: str, **updates) -> None:
        """Update fields on an existing job by keyword arguments.

        Args:
            job_id: The ID of the job to update.
            **updates: Field names and their new values.

        Raises:
            KeyError: If job_id is not found in the store.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"Job not found: {job_id}")
            for key, value in updates.items():
                if not hasattr(job, key):
                    raise AttributeError(
                        f"Job has no attribute '{key}'"
                    )
                setattr(job, key, value)
        # Persist outside lock
        if self._storage_dir:
            self._persist_metadata(job)

    def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieve a job by ID.

        If the job is not in memory but storage_dir is set,
        attempts to load it from disk.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            The Job if found, otherwise None.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                return job
        # Check disk if not in memory
        if self._storage_dir:
            return self._load_job_from_disk(job_id)
        return None

    def append_logs(self, job_id: str, stdout: str = "", stderr: str = "") -> None:
        """Append log content to a job's stdout/stderr fields.

        Persists to disk first (uncapped), then applies in-memory cap.
        If in-memory log exceeds 1MB, truncates to last 500KB (keeps tail).

        Args:
            job_id: The ID of the job to append logs to.
            stdout: Content to append to the job's stdout field.
            stderr: Content to append to the job's stderr field.
        """
        # Persist to disk BEFORE in-memory capping so disk has full logs
        if self._storage_dir and (stdout or stderr):
            self._persist_logs(job_id, stdout, stderr)

        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return  # Job may have been removed
            if stdout:
                job.stdout += stdout
            if stderr:
                job.stderr += stderr
            # Cap in-memory logs
            if len(job.stdout) > MAX_LOG_SIZE:
                job.stdout = job.stdout[-TRUNCATE_TO:]
            if len(job.stderr) > MAX_LOG_SIZE:
                job.stderr = job.stderr[-TRUNCATE_TO:]

    def get_active_job(self) -> Optional[Job]:
        """Return the currently active job (INITIALIZE or RUNNING status).

        Only one job should be active at a time due to the single-process
        execution constraint.

        Returns:
            The active Job, or None if no job is currently active.
        """
        with self._lock:
            for job in self._jobs.values():
                if job.status in (JobStatus.INITIALIZE, JobStatus.RUNNING):
                    return job
            return None

    def get_history(self, limit: int = 50) -> list[Job]:
        """Return recent jobs sorted by start_time descending.

        Args:
            limit: Maximum number of jobs to return. Defaults to 50.

        Returns:
            A list of Job instances, most recent first.
        """
        with self._lock:
            sorted_jobs = sorted(
                self._jobs.values(),
                key=lambda j: j.start_time,
                reverse=True,
            )
            return sorted_jobs[:limit]

    # ------------------------------------------------------------------
    # Job cap enforcement
    # ------------------------------------------------------------------

    def _enforce_cap(self) -> None:
        """Delete oldest jobs beyond max_jobs.

        Must be called while holding self._lock.
        """
        if len(self._jobs) <= self._max_jobs:
            return
        # Sort by start_time, oldest first
        sorted_jobs = sorted(self._jobs.values(), key=lambda j: j.start_time)
        while len(self._jobs) > self._max_jobs:
            oldest = sorted_jobs.pop(0)
            del self._jobs[oldest.job_id]
            # Delete from disk if storage_dir is set
            if self._storage_dir:
                job_dir = self._storage_dir / oldest.job_id
                if job_dir.exists():
                    shutil.rmtree(job_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Disk read helpers
    # ------------------------------------------------------------------

    def _load_job_from_disk(self, job_id: str) -> Optional[Job]:
        """Load a job from disk by reading its meta.json file.

        Args:
            job_id: The job identifier (subdirectory name).

        Returns:
            The deserialized Job, or None if not found or unreadable.
        """
        meta_path = self._storage_dir / job_id / "meta.json"
        if not meta_path.exists():
            return None
        try:
            return Job.from_json(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def get_full_logs(self, job_id: str) -> tuple[str, str]:
        """Read complete logs from disk (not subject to in-memory cap).

        Falls back to in-memory logs if storage_dir is not configured.

        Args:
            job_id: The job identifier.

        Returns:
            A tuple of (stdout, stderr) log content.
        """
        if not self._storage_dir:
            # Fallback to in-memory
            job = self.get_job(job_id)
            return (job.stdout, job.stderr) if job else ("", "")

        job_dir = self._storage_dir / job_id
        stdout = ""
        stderr = ""
        stdout_path = job_dir / "stdout.log"
        stderr_path = job_dir / "stderr.log"
        if stdout_path.exists():
            stdout = stdout_path.read_text(encoding="utf-8")
        if stderr_path.exists():
            stderr = stderr_path.read_text(encoding="utf-8")
        return stdout, stderr

    # ------------------------------------------------------------------
    # Disk persistence (only active when storage_dir is set)
    # ------------------------------------------------------------------

    def _persist_metadata(self, job: Job) -> None:
        """Write job metadata JSON to disk.

        Creates a subdirectory for the job and writes ``meta.json``
        containing the serialised job state (excluding stdout/stderr).
        """
        job_dir = self._storage_dir / job.job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        meta_path = job_dir / "meta.json"
        try:
            meta_path.write_text(job.to_json(), encoding="utf-8")
        except OSError:
            logger.warning(
                "Failed to persist metadata for job %s", job.job_id, exc_info=True
            )

    def _persist_logs(self, job_id: str, stdout: str, stderr: str) -> None:
        """Append stdout/stderr content to log files on disk.

        Args:
            job_id: The job identifier (used as subdirectory name).
            stdout: Content to append to stdout.log.
            stderr: Content to append to stderr.log.
        """
        job_dir = self._storage_dir / job_id
        try:
            job_dir.mkdir(parents=True, exist_ok=True)
            if stdout:
                with open(job_dir / "stdout.log", mode="a", encoding="utf-8") as f:
                    f.write(stdout)
            if stderr:
                with open(job_dir / "stderr.log", mode="a", encoding="utf-8") as f:
                    f.write(stderr)
        except OSError:
            logger.warning(
                "Failed to persist logs for job %s", job_id, exc_info=True
            )
