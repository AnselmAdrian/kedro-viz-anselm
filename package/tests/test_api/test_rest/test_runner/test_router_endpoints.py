"""Integration tests verifying existing runner endpoints still work after JobStore refactoring.

Tests the following endpoints:
- POST /api/run-kedro-command
- GET /api/kedro-command-status/{job_id}
- POST /api/kedro-command-cancel/{job_id}
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from kedro_viz.api.rest.router import router


@pytest.fixture
def app():
    """Create a minimal FastAPI app with just the router for testing."""
    test_app = FastAPI()
    test_app.include_router(router)
    return test_app


@pytest.fixture
def test_client(app):
    """Create a TestClient for the test app."""
    return TestClient(app)


def _mock_popen(*args, **kwargs):
    """Create a mock Popen that simulates a quick successful process."""
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.stdout = MagicMock()
    mock_proc.stderr = MagicMock()
    # readline returns empty string to signal EOF
    mock_proc.stdout.readline = MagicMock(return_value="")
    mock_proc.stderr.readline = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=0)
    mock_proc.communicate = MagicMock(return_value=("", ""))
    return mock_proc


class TestRunKedroCommand:
    """Tests for POST /api/run-kedro-command."""

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_returns_202_with_job_id(self, mock_popen, test_client):
        """A valid command should return 202 with a job_id and status."""
        response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        assert body["status"] == "initialize"
        # job_id should be a non-empty string
        assert len(body["job_id"]) > 0

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_command_without_kedro_prefix(self, mock_popen, test_client):
        """A command without 'kedro' prefix should still work (gets added)."""
        response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "run --help"},
        )
        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        assert body["status"] == "initialize"

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_job_id_is_valid_uuid(self, mock_popen, test_client):
        """The returned job_id should be a valid UUID format."""
        import uuid

        response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        body = response.json()
        # Should not raise ValueError
        uuid.UUID(body["job_id"])


class TestKedroCommandStatus:
    """Tests for GET /api/kedro-command-status/{job_id}."""

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_returns_status_for_valid_job(self, mock_popen, test_client):
        """After starting a command, the status endpoint should return its state."""
        # Start a command
        run_response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        job_id = run_response.json()["job_id"]

        # Check status
        status_response = test_client.get(f"/api/kedro-command-status/{job_id}")
        assert status_response.status_code == 200

        body = status_response.json()
        assert "status" in body
        assert "start_time" in body
        assert "cmd" in body
        assert "stdout" in body
        assert "stderr" in body
        assert "returncode" in body
        assert "duration" in body
        assert "end_time" in body

        # Status should be one of the valid values
        valid_statuses = {"initialize", "running", "finished", "error", "terminated", "interrupted"}
        assert body["status"] in valid_statuses

    def test_returns_404_for_invalid_job_id(self, test_client):
        """A nonexistent job_id should return 404."""
        response = test_client.get("/api/kedro-command-status/nonexistent-id-12345")
        assert response.status_code == 404
        body = response.json()
        assert "message" in body
        assert body["message"] == "Job not found"

    def test_returns_404_for_empty_job_id(self, test_client):
        """An arbitrary UUID that was never created should return 404."""
        response = test_client.get(
            "/api/kedro-command-status/00000000-0000-0000-0000-000000000000"
        )
        assert response.status_code == 404

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_status_response_contains_cmd(self, mock_popen, test_client):
        """The status response should include the command that was run."""
        run_response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        job_id = run_response.json()["job_id"]

        status_response = test_client.get(f"/api/kedro-command-status/{job_id}")
        body = status_response.json()
        assert "kedro" in body["cmd"]
        assert "run" in body["cmd"]
        assert "--help" in body["cmd"]


class TestKedroCommandCancel:
    """Tests for POST /api/kedro-command-cancel/{job_id}."""

    def test_returns_404_for_invalid_job_id(self, test_client):
        """A nonexistent job_id should return 404."""
        response = test_client.post("/api/kedro-command-cancel/nonexistent-id-12345")
        assert response.status_code == 404
        body = response.json()
        assert "message" in body
        assert body["message"] == "Job not found"

    def test_returns_404_for_random_uuid(self, test_client):
        """A UUID that was never created should return 404."""
        response = test_client.post(
            "/api/kedro-command-cancel/00000000-0000-0000-0000-000000000000"
        )
        assert response.status_code == 404

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_cancel_already_finished_job_returns_200(self, mock_popen, test_client):
        """Cancelling a job that already finished should return 200 with terminated=False."""
        # Start a fast command (mock will complete instantly)
        run_response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        job_id = run_response.json()["job_id"]

        # Give the background task time to complete
        time.sleep(0.5)

        # Try to cancel it - should succeed but indicate nothing was terminated
        cancel_response = test_client.post(f"/api/kedro-command-cancel/{job_id}")
        assert cancel_response.status_code == 200
        body = cancel_response.json()
        assert body["terminated"] is False


class TestEndpointIntegration:
    """Integration tests verifying the endpoints work together as a workflow."""

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_full_lifecycle_start_status(self, mock_popen, test_client):
        """Start a command, check status - should work end-to-end."""
        # Start
        run_response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        assert run_response.status_code == 202
        job_id = run_response.json()["job_id"]

        # Status immediately available
        status_response = test_client.get(f"/api/kedro-command-status/{job_id}")
        assert status_response.status_code == 200

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_multiple_jobs_have_unique_ids(self, mock_popen, test_client):
        """Starting multiple jobs should create entries with unique IDs."""
        # Start first job
        resp1 = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        job_id_1 = resp1.json()["job_id"]

        # Start second job
        resp2 = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --pipeline=second"},
        )
        job_id_2 = resp2.json()["job_id"]

        # They should be different jobs
        assert job_id_1 != job_id_2

        # Both should be queryable
        status1 = test_client.get(f"/api/kedro-command-status/{job_id_1}")
        status2 = test_client.get(f"/api/kedro-command-status/{job_id_2}")
        assert status1.status_code == 200
        assert status2.status_code == 200


class TestRunKedroCommandMutex:
    """Tests for single-process mutex via POST /api/run-kedro-command."""

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_returns_409_when_run_already_active(self, mock_popen, test_client):
        """A second run while the first is active should return 409 Conflict."""
        from kedro_viz.api.rest.router import runner_service
        from kedro_viz.api.rest.runner.models import Job, JobStatus
        from datetime import datetime

        # Clear the store to avoid job cap eviction issues
        original_jobs = dict(runner_service.store._jobs)
        runner_service.store._jobs.clear()

        # Inject an active job directly into the store
        active_job = Job(
            job_id="active-mutex-job",
            status=JobStatus.RUNNING,
            start_time=datetime(2024, 6, 15, 10, 30, 0),
            cmd="kedro run --pipeline=first",
        )
        runner_service.store.add_job(active_job)

        try:
            # Attempt a second run - should be rejected
            response = test_client.post(
                "/api/run-kedro-command",
                params={"command": "kedro run --pipeline=second"},
            )
            assert response.status_code == 409
            body = response.json()
            assert body["message"] == "A run is already active"
            assert body["active_job_id"] == "active-mutex-job"
            assert "started_at" in body
            # started_at should be ISO 8601 format
            assert "2024-06-15" in body["started_at"]
        finally:
            # Clean up: restore original state
            runner_service.store._jobs = original_jobs


class TestPostApiRun:
    """Tests for POST /api/run with structured RunConfig."""

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_returns_202_with_valid_config(self, mock_popen, test_client):
        """A valid RunConfig should return 202 with job_id and status."""
        response = test_client.post(
            "/api/run",
            json={"pipeline": "data_processing", "env": "local"},
        )
        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        assert body["status"] == "initialize"
        assert len(body["job_id"]) > 0

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_returns_202_with_empty_config(self, mock_popen, test_client):
        """An empty config (default pipeline) should return 202."""
        response = test_client.post(
            "/api/run",
            json={},
        )
        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        assert body["status"] == "initialize"

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_returns_202_with_full_config(self, mock_popen, test_client):
        """A fully-specified config should return 202."""
        response = test_client.post(
            "/api/run",
            json={
                "pipeline": "data_science",
                "env": "staging",
                "tags": ["training", "evaluation"],
                "from_nodes": ["split_data"],
                "to_nodes": ["evaluate_model"],
                "params": {"model.learning_rate": 0.01, "model.epochs": 100},
            },
        )
        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        assert body["status"] == "initialize"

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_returns_409_when_run_already_active(self, mock_popen, test_client):
        """POST /api/run should return 409 if a run is already active."""
        from kedro_viz.api.rest.router import runner_service
        from kedro_viz.api.rest.runner.models import Job, JobStatus
        from datetime import datetime

        # Clear the store to avoid job cap eviction issues
        original_jobs = dict(runner_service.store._jobs)
        runner_service.store._jobs.clear()

        # Inject an active job
        active_job = Job(
            job_id="active-run-job",
            status=JobStatus.RUNNING,
            start_time=datetime(2024, 7, 1, 12, 0, 0),
            cmd="kedro run -p my_pipeline",
        )
        runner_service.store.add_job(active_job)

        try:
            response = test_client.post(
                "/api/run",
                json={"pipeline": "another_pipeline"},
            )
            assert response.status_code == 409
            body = response.json()
            assert body["message"] == "A run is already active"
            assert body["active_job_id"] == "active-run-job"
            assert "started_at" in body
        finally:
            runner_service.store._jobs = original_jobs

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_job_id_is_valid_uuid(self, mock_popen, test_client):
        """The returned job_id should be a valid UUID."""
        import uuid

        response = test_client.post("/api/run", json={})
        body = response.json()
        uuid.UUID(body["job_id"])


class TestGetRunHistory:
    """Tests for GET /api/run-history."""

    def test_returns_empty_list_when_no_jobs(self, test_client):
        """When no jobs have been run, return an empty list."""
        from kedro_viz.api.rest.router import runner_service

        # Ensure the store is empty for this test
        original_jobs = dict(runner_service.store._jobs)
        runner_service.store._jobs.clear()

        try:
            response = test_client.get("/api/run-history")
            assert response.status_code == 200
            assert response.json() == []
        finally:
            runner_service.store._jobs = original_jobs

    def test_returns_jobs_sorted_by_start_time(self, test_client):
        """History should return jobs sorted most-recent first."""
        from kedro_viz.api.rest.router import runner_service
        from kedro_viz.api.rest.runner.models import Job, JobStatus
        from datetime import datetime

        original_jobs = dict(runner_service.store._jobs)
        runner_service.store._jobs.clear()

        # Create jobs with different start times
        job1 = Job(
            job_id="history-job-1",
            status=JobStatus.FINISHED,
            start_time=datetime(2024, 1, 1, 10, 0, 0),
            cmd="kedro run -p pipeline_a",
            end_time=datetime(2024, 1, 1, 10, 5, 0),
            duration=300.0,
            returncode=0,
        )
        job2 = Job(
            job_id="history-job-2",
            status=JobStatus.ERROR,
            start_time=datetime(2024, 1, 2, 14, 0, 0),
            cmd="kedro run -p pipeline_b",
            end_time=datetime(2024, 1, 2, 14, 2, 0),
            duration=120.0,
            returncode=1,
            error_summary="ValueError: invalid input",
        )
        job3 = Job(
            job_id="history-job-3",
            status=JobStatus.FINISHED,
            start_time=datetime(2024, 1, 3, 9, 0, 0),
            cmd="kedro run -p pipeline_c",
            end_time=datetime(2024, 1, 3, 9, 10, 0),
            duration=600.0,
            returncode=0,
        )

        runner_service.store.add_job(job1)
        runner_service.store.add_job(job2)
        runner_service.store.add_job(job3)

        try:
            response = test_client.get("/api/run-history")
            assert response.status_code == 200
            body = response.json()
            assert len(body) == 3

            # Should be sorted by start_time descending (most recent first)
            assert body[0]["job_id"] == "history-job-3"
            assert body[1]["job_id"] == "history-job-2"
            assert body[2]["job_id"] == "history-job-1"

            # Verify structure of each entry
            for entry in body:
                assert "job_id" in entry
                assert "status" in entry
                assert "start_time" in entry
                assert "end_time" in entry
                assert "duration" in entry
                assert "cmd" in entry
                assert "returncode" in entry
                assert "error_summary" in entry
        finally:
            runner_service.store._jobs = original_jobs

    def test_respects_limit_parameter(self, test_client):
        """History should respect the limit query parameter."""
        from kedro_viz.api.rest.router import runner_service
        from kedro_viz.api.rest.runner.models import Job, JobStatus
        from datetime import datetime

        original_jobs = dict(runner_service.store._jobs)
        runner_service.store._jobs.clear()

        # Create 5 jobs
        for i in range(5):
            job = Job(
                job_id=f"limit-job-{i}",
                status=JobStatus.FINISHED,
                start_time=datetime(2024, 1, i + 1, 10, 0, 0),
                cmd=f"kedro run -p pipeline_{i}",
                end_time=datetime(2024, 1, i + 1, 10, 5, 0),
                duration=300.0,
                returncode=0,
            )
            runner_service.store.add_job(job)

        try:
            response = test_client.get("/api/run-history?limit=3")
            assert response.status_code == 200
            body = response.json()
            assert len(body) == 3
        finally:
            runner_service.store._jobs = original_jobs

    def test_timestamps_are_iso_8601(self, test_client):
        """All timestamps should be in ISO 8601 format."""
        from kedro_viz.api.rest.router import runner_service
        from kedro_viz.api.rest.runner.models import Job, JobStatus
        from datetime import datetime

        original_jobs = dict(runner_service.store._jobs)
        runner_service.store._jobs.clear()

        job = Job(
            job_id="iso-job",
            status=JobStatus.FINISHED,
            start_time=datetime(2024, 6, 15, 10, 30, 45),
            cmd="kedro run",
            end_time=datetime(2024, 6, 15, 10, 35, 0),
            duration=255.0,
            returncode=0,
        )
        runner_service.store.add_job(job)

        try:
            response = test_client.get("/api/run-history")
            assert response.status_code == 200
            body = response.json()
            assert len(body) == 1
            entry = body[0]
            # ISO 8601 contains 'T' separator
            assert "T" in entry["start_time"]
            assert "T" in entry["end_time"]
            # Verify it can be parsed back
            datetime.fromisoformat(entry["start_time"])
            datetime.fromisoformat(entry["end_time"])
        finally:
            runner_service.store._jobs = original_jobs

    def test_null_end_time_for_active_job(self, test_client):
        """An active job should have null end_time in history."""
        from kedro_viz.api.rest.router import runner_service
        from kedro_viz.api.rest.runner.models import Job, JobStatus
        from datetime import datetime

        original_jobs = dict(runner_service.store._jobs)
        runner_service.store._jobs.clear()

        job = Job(
            job_id="active-history-job",
            status=JobStatus.RUNNING,
            start_time=datetime(2024, 6, 15, 10, 30, 0),
            cmd="kedro run -p long_pipeline",
        )
        runner_service.store.add_job(job)

        try:
            response = test_client.get("/api/run-history")
            assert response.status_code == 200
            body = response.json()
            assert len(body) == 1
            assert body[0]["end_time"] is None
            assert body[0]["duration"] is None
            assert body[0]["returncode"] is None
        finally:
            runner_service.store._jobs = original_jobs


class TestGetRunProgress:
    """Tests for GET /api/run-progress/{job_id}."""

    def test_returns_404_for_unknown_job(self, test_client):
        """An unknown job_id should return 404."""
        response = test_client.get("/api/run-progress/nonexistent-job-id")
        assert response.status_code == 404
        body = response.json()
        assert "message" in body
        assert body["message"] == "Job not found or no progress available"

    def test_returns_200_with_progress_data(self, test_client):
        """A job with initialized progress should return 200 with progress data."""
        from kedro_viz.api.rest.router import progress_tracker

        # Initialize progress for a test job
        progress_tracker.init_run("progress-test-job", total_nodes=5)
        progress_tracker.node_started("progress-test-job", "n1", "split_data")
        progress_tracker.node_completed("progress-test-job", "n1", "split_data")

        try:
            response = test_client.get("/api/run-progress/progress-test-job")
            assert response.status_code == 200
            body = response.json()
            assert body["nodes_total"] == 5
            assert body["nodes_completed"] == 1
            assert body["current_node"] is None  # cleared after completion
            assert len(body["node_events"]) == 2

            # Verify event structure
            start_event = body["node_events"][0]
            assert start_event["node_id"] == "n1"
            assert start_event["node_name"] == "split_data"
            assert start_event["status"] == "running"
            assert "timestamp" in start_event
            assert "T" in start_event["timestamp"]  # ISO 8601

            complete_event = body["node_events"][1]
            assert complete_event["status"] == "success"
            assert complete_event["duration"] is not None
            assert complete_event["duration"] >= 0
        finally:
            # Clean up
            with progress_tracker._lock:
                progress_tracker._progress.pop("progress-test-job", None)
                progress_tracker._node_start_times.pop("progress-test-job", None)

    def test_returns_current_node_while_running(self, test_client):
        """While a node is running, current_node should be set."""
        from kedro_viz.api.rest.router import progress_tracker

        progress_tracker.init_run("running-node-job", total_nodes=3)
        progress_tracker.node_started("running-node-job", "n1", "train_model")

        try:
            response = test_client.get("/api/run-progress/running-node-job")
            assert response.status_code == 200
            body = response.json()
            assert body["current_node"] == "train_model"
            assert body["nodes_completed"] == 0
        finally:
            with progress_tracker._lock:
                progress_tracker._progress.pop("running-node-job", None)
                progress_tracker._node_start_times.pop("running-node-job", None)

    def test_returns_failed_node_events(self, test_client):
        """A failed node should appear in node_events with error."""
        from kedro_viz.api.rest.router import progress_tracker

        progress_tracker.init_run("failed-node-job", total_nodes=2)
        progress_tracker.node_started("failed-node-job", "n1", "bad_node")
        progress_tracker.node_failed(
            "failed-node-job", "n1", "bad_node", error="ValueError: bad input"
        )

        try:
            response = test_client.get("/api/run-progress/failed-node-job")
            assert response.status_code == 200
            body = response.json()
            assert body["nodes_completed"] == 0
            assert len(body["node_events"]) == 2
            fail_event = body["node_events"][1]
            assert fail_event["status"] == "failed"
            assert fail_event["error"] == "ValueError: bad input"
        finally:
            with progress_tracker._lock:
                progress_tracker._progress.pop("failed-node-job", None)
                progress_tracker._node_start_times.pop("failed-node-job", None)


class TestRunStreamEndpoint:
    """Tests for GET /api/run-stream/{job_id}."""

    def test_returns_404_for_unknown_job(self, test_client):
        """An unknown job_id should return 404."""
        response = test_client.get("/api/run-stream/nonexistent-job-id")
        assert response.status_code == 404
        body = response.json()
        assert body["message"] == "Job not found"

    def test_returns_stream_for_completed_job(self, test_client):
        """A completed job without an event queue should stream status + done."""
        from kedro_viz.api.rest.router import runner_service
        from kedro_viz.api.rest.runner.models import Job, JobStatus
        from datetime import datetime

        # Create a completed job (no event queue)
        job = Job(
            job_id="stream-completed-job",
            status=JobStatus.FINISHED,
            start_time=datetime(2024, 6, 15, 10, 0, 0),
            end_time=datetime(2024, 6, 15, 10, 5, 0),
            cmd="kedro run",
            duration=300.0,
            returncode=0,
        )
        runner_service.store.add_job(job)

        try:
            response = test_client.get("/api/run-stream/stream-completed-job")
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")

            # Parse the streamed events
            import json
            events = []
            for line in response.text.strip().split("\n\n"):
                if line.startswith("data: "):
                    events.append(json.loads(line[len("data: "):]))

            assert len(events) == 2
            assert events[0]["type"] == "status"
            assert events[0]["status"] == "finished"
            assert events[1]["type"] == "done"
            assert events[1]["status"] == "finished"
            assert events[1]["duration"] == 300.0
            assert events[1]["returncode"] == 0
        finally:
            runner_service.store._jobs.pop("stream-completed-job", None)

    def test_returns_stream_for_active_job_with_queue(self, test_client):
        """An active job with an event queue should stream events from the queue."""
        from kedro_viz.api.rest.router import runner_service
        from kedro_viz.api.rest.runner.models import Job, JobStatus
        from kedro_viz.api.rest.runner.events import SSEFormatter
        from datetime import datetime
        import threading

        # Create an active job
        job = Job(
            job_id="stream-active-job",
            status=JobStatus.RUNNING,
            start_time=datetime(2024, 6, 15, 10, 0, 0),
            cmd="kedro run -p my_pipeline",
        )
        runner_service.store.add_job(job)

        # Create an event queue and push events
        eq = runner_service.executor.create_event_queue("stream-active-job")

        # Push some events then sentinel (in a thread to simulate async)
        def push_events():
            import time
            time.sleep(0.1)
            eq.put(SSEFormatter.log_event("Running node: split_data", "stdout"))
            eq.put(SSEFormatter.progress_event(5, 1, "split_data"))
            eq.put(SSEFormatter.done_event("finished", 10.0, 0))
            eq.put(None)  # Sentinel

        t = threading.Thread(target=push_events, daemon=True)
        t.start()

        try:
            response = test_client.get("/api/run-stream/stream-active-job")
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")

            # Parse the streamed events
            import json
            events = []
            for line in response.text.strip().split("\n\n"):
                if line.startswith("data: "):
                    events.append(json.loads(line[len("data: "):]))

            # Should have: initial status + log + progress + done
            assert len(events) == 4
            assert events[0]["type"] == "status"
            assert events[0]["status"] == "running"
            assert events[1]["type"] == "log"
            assert events[2]["type"] == "progress"
            assert events[3]["type"] == "done"
        finally:
            t.join(timeout=2)
            runner_service.store._jobs.pop("stream-active-job", None)
            with runner_service.executor._lock:
                runner_service.executor._event_queues.pop("stream-active-job", None)


class TestDeprecatedEndpoint:
    """Tests for deprecation headers/body on POST /api/run-kedro-command."""

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_response_contains_deprecation_header(self, mock_popen, test_client):
        """The deprecated endpoint should include a Deprecation: true header."""
        response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        assert response.status_code == 202
        assert response.headers.get("Deprecation") == "true"

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_response_body_contains_deprecated_field(self, mock_popen, test_client):
        """The response body should include a _deprecated note."""
        response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        body = response.json()
        assert "_deprecated" in body
        assert body["_deprecated"] == "Use POST /api/run instead"

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_deprecated_endpoint_still_returns_job_id(self, mock_popen, test_client):
        """The deprecated endpoint should still return job_id and status."""
        response = test_client.post(
            "/api/run-kedro-command",
            params={"command": "kedro run --help"},
        )
        body = response.json()
        assert "job_id" in body
        assert body["status"] == "initialize"
