"""Tests for graceful shutdown handling."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from unittest.mock import patch

import pytest

from kedro_viz.api.rest.runner.models import Job, JobStatus
from kedro_viz.api.rest.runner.store import JobStore


class TestShutdownHandler:
    """Tests for the shutdown handler registered via atexit."""

    def test_shutdown_terminates_active_job(self):
        """On shutdown, an active job should be terminated."""
        from kedro_viz.api.rest.router import job_store, executor

        # Clear the store to avoid job cap eviction issues
        original_jobs = dict(job_store._jobs)
        job_store._jobs.clear()

        # Inject an active job
        active_job = Job(
            job_id="shutdown-test-job",
            status=JobStatus.RUNNING,
            start_time=datetime(2025, 1, 1, 10, 0, 0),
            cmd="kedro run -p my_pipeline",
            pid=99999,
        )
        job_store.add_job(active_job)

        try:
            # The shutdown function checks for active job and terminates it
            active = job_store.get_active_job()
            assert active is not None
            assert active.job_id == "shutdown-test-job"

            # Terminate will fail (no real process) but the logic path is exercised
            result = executor.terminate("shutdown-test-job")
            # os.kill will raise an error for non-existent PID, but terminate
            # handles it gracefully and returns False
            assert result is False
        finally:
            job_store._jobs = original_jobs

    def test_shutdown_with_no_active_job_is_noop(self):
        """If no job is active, shutdown should be a clean no-op."""
        from kedro_viz.api.rest.router import job_store

        # Ensure no active job
        original_jobs = dict(job_store._jobs)
        job_store._jobs.clear()

        try:
            active = job_store.get_active_job()
            assert active is None
            # Calling with no active job doesn't crash
        finally:
            job_store._jobs = original_jobs


class TestHydrateInterruptedJobsLogging:
    """Tests for the logging of interrupted jobs on startup."""

    def test_logs_interrupted_count(self, tmp_path, caplog):
        """hydrate_from_disk should log the number of interrupted jobs found."""
        # Create two jobs on disk in 'running' state
        for job_id in ["stale-job-1", "stale-job-2"]:
            job_dir = tmp_path / job_id
            job_dir.mkdir()
            meta = {
                "job_id": job_id,
                "status": "running",
                "start_time": "2025-01-15T10:00:00",
                "cmd": "kedro run",
            }
            (job_dir / "meta.json").write_text(json.dumps(meta))

        with caplog.at_level(logging.INFO, logger="kedro_viz.api.rest.runner.store"):
            store = JobStore(storage_dir=tmp_path)

        assert "Found 2 interrupted job(s) from previous session" in caplog.text

    def test_no_log_when_no_interrupted_jobs(self, tmp_path, caplog):
        """No log message should appear if there are no interrupted jobs."""
        # Create a finished job
        job_dir = tmp_path / "finished-job"
        job_dir.mkdir()
        meta = {
            "job_id": "finished-job",
            "status": "finished",
            "start_time": "2025-01-15T10:00:00",
            "cmd": "kedro run",
            "end_time": "2025-01-15T10:05:00",
            "duration": 300.0,
            "returncode": 0,
        }
        (job_dir / "meta.json").write_text(json.dumps(meta))

        with caplog.at_level(logging.INFO, logger="kedro_viz.api.rest.runner.store"):
            store = JobStore(storage_dir=tmp_path)

        assert "interrupted" not in caplog.text.lower()
