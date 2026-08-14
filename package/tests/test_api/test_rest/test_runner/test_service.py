"""Tests for the RunnerService class."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from kedro_viz.api.rest.runner.executor import PipelineExecutor
from kedro_viz.api.rest.runner.models import Job, JobStatus
from kedro_viz.api.rest.runner.service import RunnerService
from kedro_viz.api.rest.runner.store import JobStore


@pytest.fixture
def store():
    """Create a fresh JobStore for each test."""
    return JobStore()


@pytest.fixture
def executor(store):
    """Create a PipelineExecutor backed by the test store."""
    return PipelineExecutor(store)


@pytest.fixture
def service(store, executor):
    """Create a RunnerService with the test store and executor."""
    return RunnerService(store=store, executor=executor)


class TestStartRawRun:
    """Tests for RunnerService.start_raw_run."""

    def test_creates_job_with_kedro_prefix(self, service):
        """Command without 'kedro' prefix gets it prepended."""
        job, cmd = service.start_raw_run("run --pipeline=my_pipeline")
        assert cmd[0] == "kedro"
        assert cmd[1] == "run"
        assert cmd[2] == "--pipeline=my_pipeline"
        assert "kedro" in job.cmd

    def test_preserves_kedro_prefix(self, service):
        """Command already starting with 'kedro' is not doubled."""
        job, cmd = service.start_raw_run("kedro run --help")
        assert cmd == ["kedro", "run", "--help"]
        assert cmd[0] == "kedro"
        # Should not have kedro twice
        assert cmd[1] != "kedro"

    def test_returns_job_with_initialize_status(self, service):
        """Returned job should have INITIALIZE status."""
        job, _ = service.start_raw_run("kedro run")
        assert job.status == JobStatus.INITIALIZE

    def test_returns_job_with_valid_id(self, service):
        """Returned job should have a non-empty UUID job_id."""
        import uuid

        job, _ = service.start_raw_run("kedro run")
        # Should not raise ValueError
        uuid.UUID(job.job_id)

    def test_job_added_to_store(self, service, store):
        """Job created by start_raw_run should be retrievable from the store."""
        job, _ = service.start_raw_run("kedro run --help")
        retrieved = store.get_job(job.job_id)
        assert retrieved is not None
        assert retrieved.job_id == job.job_id

    def test_job_has_start_time(self, service):
        """Job should have a start_time set."""
        job, _ = service.start_raw_run("kedro run")
        assert job.start_time is not None
        assert isinstance(job.start_time, datetime)

    def test_job_cmd_field_contains_full_command(self, service):
        """Job.cmd should contain the formatted command string."""
        job, _ = service.start_raw_run("run --pipeline=dp --env base")
        assert "kedro" in job.cmd
        assert "run" in job.cmd
        assert "--pipeline=dp" in job.cmd
        assert "--env" in job.cmd
        assert "base" in job.cmd

    def test_handles_quoted_arguments(self, service):
        """Commands with quoted strings should be parsed correctly."""
        job, cmd = service.start_raw_run('run --params="key:value"')
        assert cmd[0] == "kedro"
        assert cmd[1] == "run"
        assert cmd[2] == "--params=key:value"

    def test_multiple_calls_create_distinct_jobs(self, service, store):
        """Each call to start_raw_run should create a separate job (when no active job)."""
        job1, _ = service.start_raw_run("kedro run --pipeline=a")
        # Complete the first job so the mutex allows a second
        store.update_job(job1.job_id, status=JobStatus.FINISHED)
        job2, _ = service.start_raw_run("kedro run --pipeline=b")
        assert job1.job_id != job2.job_id
        assert store.get_job(job1.job_id) is not None
        assert store.get_job(job2.job_id) is not None


class TestCancelRun:
    """Tests for RunnerService.cancel_run."""

    def test_returns_none_for_nonexistent_job(self, service):
        """cancel_run should return None if the job doesn't exist."""
        result = service.cancel_run("nonexistent-id")
        assert result is None

    def test_returns_false_for_finished_job(self, service, store):
        """cancel_run should return False for a job that already finished."""
        job = Job(
            job_id="test-job-1",
            status=JobStatus.FINISHED,
            start_time=datetime.now(),
            cmd="kedro run",
            pid=99999,
        )
        store.add_job(job)
        result = service.cancel_run("test-job-1")
        assert result is False

    def test_returns_false_for_job_without_pid(self, service, store):
        """cancel_run should return False if the job has no pid."""
        job = Job(
            job_id="test-job-2",
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
            cmd="kedro run",
            pid=None,
        )
        store.add_job(job)
        result = service.cancel_run("test-job-2")
        assert result is False

    @patch("os.kill")
    def test_returns_true_for_running_job(self, mock_kill, service, store):
        """cancel_run should return True when a running job is terminated."""
        job = Job(
            job_id="test-job-3",
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
            cmd="kedro run",
            pid=12345,
        )
        store.add_job(job)
        result = service.cancel_run("test-job-3")
        assert result is True
        mock_kill.assert_called_once()


class TestGetStatus:
    """Tests for RunnerService.get_status."""

    def test_returns_none_for_nonexistent_job(self, service):
        """get_status should return None for an unknown job_id."""
        result = service.get_status("nonexistent-id")
        assert result is None

    def test_returns_job_for_existing_id(self, service, store):
        """get_status should return the Job for a valid job_id."""
        job = Job(
            job_id="status-test-1",
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
            cmd="kedro run",
        )
        store.add_job(job)
        result = service.get_status("status-test-1")
        assert result is not None
        assert result.job_id == "status-test-1"
        assert result.status == JobStatus.RUNNING


class TestGetHistory:
    """Tests for RunnerService.get_history."""

    def test_returns_empty_list_when_no_jobs(self, service):
        """get_history should return empty list for an empty store."""
        result = service.get_history()
        assert result == []

    def test_returns_jobs_sorted_by_start_time(self, service, store):
        """get_history should return jobs sorted most recent first."""
        job1 = Job(
            job_id="hist-1",
            status=JobStatus.FINISHED,
            start_time=datetime(2024, 1, 1, 10, 0, 0),
            cmd="kedro run --pipeline=a",
        )
        job2 = Job(
            job_id="hist-2",
            status=JobStatus.FINISHED,
            start_time=datetime(2024, 1, 2, 10, 0, 0),
            cmd="kedro run --pipeline=b",
        )
        job3 = Job(
            job_id="hist-3",
            status=JobStatus.ERROR,
            start_time=datetime(2024, 1, 3, 10, 0, 0),
            cmd="kedro run --pipeline=c",
        )
        store.add_job(job1)
        store.add_job(job2)
        store.add_job(job3)

        result = service.get_history()
        assert len(result) == 3
        assert result[0].job_id == "hist-3"  # most recent
        assert result[1].job_id == "hist-2"
        assert result[2].job_id == "hist-1"  # oldest

    def test_respects_limit_parameter(self, service, store):
        """get_history should respect the limit parameter."""
        for i in range(5):
            job = Job(
                job_id=f"limit-{i}",
                status=JobStatus.FINISHED,
                start_time=datetime(2024, 1, i + 1, 10, 0, 0),
                cmd=f"kedro run --pipeline=p{i}",
            )
            store.add_job(job)

        result = service.get_history(limit=3)
        assert len(result) == 3

    def test_default_limit_is_50(self, service, store):
        """get_history default limit should be 50."""
        # Add fewer than 50 jobs, verify all are returned
        for i in range(10):
            job = Job(
                job_id=f"default-{i}",
                status=JobStatus.FINISHED,
                start_time=datetime(2024, 1, 1, i, 0, 0),
                cmd=f"kedro run --pipeline=p{i}",
            )
            store.add_job(job)

        result = service.get_history()
        assert len(result) == 10


class TestServiceWiring:
    """Tests verifying service is properly wired to store and executor."""

    def test_service_exposes_store(self, service, store):
        """The service should expose its store for direct access if needed."""
        assert service.store is store

    def test_service_exposes_executor(self, service, executor):
        """The service should expose its executor for background task scheduling."""
        assert service.executor is executor


class TestSingleProcessMutex:
    """Tests for single-process execution enforcement in RunnerService."""

    def test_start_raw_run_raises_active_job_error_when_job_active(self, service, store):
        """start_raw_run should raise ActiveJobError if another job is active."""
        from kedro_viz.api.rest.runner.service import ActiveJobError

        # Manually add an active (RUNNING) job to the store
        active_job = Job(
            job_id="active-job-1",
            status=JobStatus.RUNNING,
            start_time=datetime(2024, 6, 15, 10, 0, 0),
            cmd="kedro run --pipeline=first",
        )
        store.add_job(active_job)

        # Attempting a second run should raise ActiveJobError
        with pytest.raises(ActiveJobError) as exc_info:
            service.start_raw_run("kedro run --pipeline=second")

        assert exc_info.value.active_job_id == "active-job-1"
        assert exc_info.value.started_at == datetime(2024, 6, 15, 10, 0, 0)

    def test_start_raw_run_raises_when_job_in_initialize_status(self, service, store):
        """start_raw_run should raise ActiveJobError for INITIALIZE status jobs too."""
        from kedro_viz.api.rest.runner.service import ActiveJobError

        active_job = Job(
            job_id="init-job-1",
            status=JobStatus.INITIALIZE,
            start_time=datetime(2024, 6, 15, 11, 0, 0),
            cmd="kedro run",
        )
        store.add_job(active_job)

        with pytest.raises(ActiveJobError) as exc_info:
            service.start_raw_run("kedro run --pipeline=new")

        assert exc_info.value.active_job_id == "init-job-1"

    def test_start_raw_run_succeeds_after_active_job_finishes(self, service, store):
        """After an active job finishes, a new run should be allowed."""
        # Add a job that has already finished
        finished_job = Job(
            job_id="finished-job-1",
            status=JobStatus.FINISHED,
            start_time=datetime(2024, 6, 15, 10, 0, 0),
            cmd="kedro run --pipeline=old",
        )
        store.add_job(finished_job)

        # A new run should succeed since there's no active job
        job, cmd = service.start_raw_run("kedro run --pipeline=new")
        assert job.status == JobStatus.INITIALIZE
        assert "kedro" in cmd

    def test_start_raw_run_succeeds_after_active_job_errors(self, service, store):
        """After an active job errors out, a new run should be allowed."""
        errored_job = Job(
            job_id="errored-job-1",
            status=JobStatus.ERROR,
            start_time=datetime(2024, 6, 15, 10, 0, 0),
            cmd="kedro run --pipeline=failed",
        )
        store.add_job(errored_job)

        job, cmd = service.start_raw_run("kedro run --pipeline=new")
        assert job.status == JobStatus.INITIALIZE
