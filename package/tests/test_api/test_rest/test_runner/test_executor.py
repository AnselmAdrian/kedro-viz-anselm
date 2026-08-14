"""Tests for the PipelineExecutor class."""

from __future__ import annotations

import threading
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from kedro_viz.api.rest.runner.executor import PipelineExecutor
from kedro_viz.api.rest.runner.models import Job, JobStatus
from kedro_viz.api.rest.runner.store import JobStore


@pytest.fixture
def store():
    """Create a fresh JobStore for each test."""
    return JobStore()


@pytest.fixture
def executor(store):
    """Create a PipelineExecutor with a fresh store."""
    return PipelineExecutor(store)


def _make_job(store, job_id="test-job-1", status=JobStatus.INITIALIZE):
    """Helper to create and add a job to the store."""
    job = Job(
        job_id=job_id,
        status=status,
        start_time=datetime.now(),
        cmd="kedro run",
    )
    store.add_job(job)
    return job


def _mock_popen(*args, **kwargs):
    """Create a mock Popen that simulates a quick successful process."""
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.stdout = MagicMock()
    mock_proc.stderr = MagicMock()
    mock_proc.stdout.readline = MagicMock(return_value="")
    mock_proc.stderr.readline = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=0)
    mock_proc.communicate = MagicMock(return_value=("", ""))
    return mock_proc


def _mock_popen_failure(*args, **kwargs):
    """Create a mock Popen that simulates a failed process."""
    mock_proc = MagicMock()
    mock_proc.pid = 99999
    mock_proc.stdout = MagicMock()
    mock_proc.stderr = MagicMock()
    mock_proc.stdout.readline = MagicMock(return_value="")
    mock_proc.stderr.readline = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=1)
    mock_proc.communicate = MagicMock(return_value=("", "Error: pipeline failed\n"))
    return mock_proc


class TestPipelineExecutorStart:
    """Tests for PipelineExecutor.start method."""

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_start_updates_job_to_running(self, mock_popen, store, executor):
        """start() should update the job status to RUNNING with the process PID."""
        _make_job(store, "job-1")

        executor.start("job-1", ["kedro", "run"])

        job = store.get_job("job-1")
        assert job.pid == 12345

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_start_sets_finished_on_success(self, mock_popen, store, executor):
        """start() should set status to FINISHED when returncode is 0."""
        _make_job(store, "job-1")

        executor.start("job-1", ["kedro", "run"])

        job = store.get_job("job-1")
        assert job.status == JobStatus.FINISHED
        assert job.returncode == 0
        assert job.end_time is not None
        assert job.duration is not None
        assert job.duration >= 0

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen_failure)
    def test_start_sets_error_on_failure(self, mock_popen, store, executor):
        """start() should set status to ERROR when returncode is non-zero."""
        _make_job(store, "job-1")

        executor.start("job-1", ["kedro", "run", "--pipeline=bad"])

        job = store.get_job("job-1")
        assert job.status == JobStatus.ERROR
        assert job.returncode == 1
        assert job.end_time is not None

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_start_appends_remaining_output(self, mock_popen, store, executor):
        """start() should append any remaining output from communicate()."""
        mock_proc = MagicMock()
        mock_proc.pid = 111
        mock_proc.stdout = MagicMock()
        mock_proc.stderr = MagicMock()
        mock_proc.stdout.readline = MagicMock(return_value="")
        mock_proc.stderr.readline = MagicMock(return_value="")
        mock_proc.wait = MagicMock(return_value=0)
        mock_proc.communicate = MagicMock(return_value=("final output\n", "final err\n"))
        mock_popen.side_effect = lambda *a, **kw: mock_proc

        _make_job(store, "job-1")
        executor.start("job-1", ["kedro", "run"])

        job = store.get_job("job-1")
        assert "final output\n" in job.stdout
        assert "final err\n" in job.stderr

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_start_removes_process_reference_after_completion(self, mock_popen, store, executor):
        """After start() completes, the internal process reference should be cleaned up."""
        _make_job(store, "job-1")

        executor.start("job-1", ["kedro", "run"])

        assert "job-1" not in executor._processes


class TestPipelineExecutorTerminate:
    """Tests for PipelineExecutor.terminate method."""

    def test_terminate_nonexistent_job_returns_false(self, store, executor):
        """terminate() should return False for a job that doesn't exist."""
        result = executor.terminate("nonexistent")
        assert result is False

    def test_terminate_finished_job_returns_false(self, store, executor):
        """terminate() should return False for a job that already finished."""
        job = _make_job(store, "job-1", status=JobStatus.FINISHED)
        job.pid = 12345

        result = executor.terminate("job-1")
        assert result is False

    def test_terminate_job_without_pid_returns_false(self, store, executor):
        """terminate() should return False for a job with no PID."""
        _make_job(store, "job-1", status=JobStatus.RUNNING)
        # pid is None by default

        result = executor.terminate("job-1")
        assert result is False

    @patch("kedro_viz.api.rest.runner.executor.os.kill")
    def test_terminate_running_job_sends_sigterm(self, mock_kill, store, executor):
        """terminate() should send SIGTERM and update status to TERMINATED."""
        import signal

        job = _make_job(store, "job-1", status=JobStatus.RUNNING)
        store.update_job("job-1", pid=12345)

        result = executor.terminate("job-1")

        assert result is True
        mock_kill.assert_called_once_with(12345, signal.SIGTERM)
        job = store.get_job("job-1")
        assert job.status == JobStatus.TERMINATED
        assert job.end_time is not None
        assert job.duration is not None

    @patch("kedro_viz.api.rest.runner.executor.os.kill")
    def test_terminate_initialize_job(self, mock_kill, store, executor):
        """terminate() should work for jobs in INITIALIZE state too."""
        import signal

        _make_job(store, "job-1", status=JobStatus.INITIALIZE)
        store.update_job("job-1", pid=55555)

        result = executor.terminate("job-1")

        assert result is True
        mock_kill.assert_called_once_with(55555, signal.SIGTERM)

    @patch("kedro_viz.api.rest.runner.executor.os.kill", side_effect=OSError("No such process"))
    def test_terminate_handles_os_error(self, mock_kill, store, executor):
        """terminate() should return False if os.kill raises an exception."""
        _make_job(store, "job-1", status=JobStatus.RUNNING)
        store.update_job("job-1", pid=12345)

        result = executor.terminate("job-1")
        assert result is False


class TestPipelineExecutorStreamReader:
    """Tests for PipelineExecutor._stream_reader method."""

    def test_stream_reader_appends_stdout(self, store, executor):
        """_stream_reader should append lines to job stdout."""
        _make_job(store, "job-1")

        pipe = MagicMock()
        pipe.readline = MagicMock(side_effect=["line 1\n", "line 2\n", ""])

        executor._stream_reader(pipe, "job-1", "stdout")

        job = store.get_job("job-1")
        assert "line 1\n" in job.stdout
        assert "line 2\n" in job.stdout

    def test_stream_reader_appends_stderr(self, store, executor):
        """_stream_reader should append lines to job stderr."""
        _make_job(store, "job-1")

        pipe = MagicMock()
        pipe.readline = MagicMock(side_effect=["error 1\n", "error 2\n", ""])

        executor._stream_reader(pipe, "job-1", "stderr")

        job = store.get_job("job-1")
        assert "error 1\n" in job.stderr
        assert "error 2\n" in job.stderr


class TestQuoteIfNeeded:
    """Tests for PipelineExecutor.quote_if_needed static method."""

    def test_no_spaces_unchanged(self):
        """Strings without spaces should be returned as-is."""
        assert PipelineExecutor.quote_if_needed("hello") == "hello"

    def test_with_spaces_quoted(self):
        """Strings with spaces should be wrapped in double quotes."""
        assert PipelineExecutor.quote_if_needed("hello world") == '"hello world"'

    def test_empty_string(self):
        """Empty string has no spaces, should be returned as-is."""
        assert PipelineExecutor.quote_if_needed("") == ""

    def test_single_space(self):
        """A single space should be quoted."""
        assert PipelineExecutor.quote_if_needed(" ") == '" "'

    def test_path_with_spaces(self):
        """A file path with spaces should be quoted."""
        assert PipelineExecutor.quote_if_needed("C:/My Projects/foo") == '"C:/My Projects/foo"'


class TestExtractErrorSummary:
    """Tests for the extract_error_summary utility function."""

    def test_typical_python_traceback(self):
        """Should extract the error message from a Python traceback."""
        from kedro_viz.api.rest.runner.executor import extract_error_summary

        stderr = (
            "Traceback (most recent call last):\n"
            "  File \"pipeline.py\", line 42, in run\n"
            "    result = transform(data)\n"
            "ValueError: could not convert string to float: 'abc'\n"
        )
        result = extract_error_summary(stderr)
        assert result == "could not convert string to float: 'abc'"

    def test_kedro_error_pattern(self):
        """Should extract message from Kedro-style [ERROR] output."""
        from kedro_viz.api.rest.runner.executor import extract_error_summary

        stderr = (
            "INFO: Starting pipeline...\n"
            "[ERROR] Something went wrong with configuration\n"
        )
        result = extract_error_summary(stderr)
        assert result == "Something went wrong with configuration"

    def test_empty_stderr_returns_none(self):
        """Should return None when stderr is empty."""
        from kedro_viz.api.rest.runner.executor import extract_error_summary

        assert extract_error_summary("") is None
        assert extract_error_summary(None) is None

    def test_long_error_truncated(self):
        """Should truncate error summary to max_length characters."""
        from kedro_viz.api.rest.runner.executor import extract_error_summary

        long_message = "x" * 300
        stderr = f"RuntimeError: {long_message}\n"
        result = extract_error_summary(stderr)
        assert len(result) == 200

    def test_no_recognizable_pattern_falls_back_to_last_line(self):
        """Should return the last non-empty line when no error pattern matches."""
        from kedro_viz.api.rest.runner.executor import extract_error_summary

        stderr = (
            "some debug output\n"
            "another line of info\n"
            "final process message\n"
        )
        result = extract_error_summary(stderr)
        assert result == "final process message"

    def test_exception_pattern(self):
        """Should match 'Exception:' pattern in stderr."""
        from kedro_viz.api.rest.runner.executor import extract_error_summary

        stderr = "kedro.exceptions.KedroContextException: Unable to load context\n"
        result = extract_error_summary(stderr)
        assert result == "Unable to load context"

    def test_whitespace_only_stderr_returns_none(self):
        """Should return None when stderr contains only whitespace."""
        from kedro_viz.api.rest.runner.executor import extract_error_summary

        assert extract_error_summary("   \n   \n  ") is None


class TestExecutorErrorSummaryIntegration:
    """Integration test: executor extracts error_summary on failure."""

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen")
    def test_error_summary_stored_on_failure(self, mock_popen_cls, store, executor):
        """When a process fails, error_summary should be extracted from stderr."""
        mock_proc = MagicMock()
        mock_proc.pid = 777
        mock_proc.stdout = MagicMock()
        mock_proc.stderr = MagicMock()
        # Simulate stderr output with an error pattern
        mock_proc.stdout.readline = MagicMock(return_value="")
        mock_proc.stderr.readline = MagicMock(
            side_effect=["ValueError: invalid input data\n", ""]
        )
        mock_proc.wait = MagicMock(return_value=1)
        mock_proc.communicate = MagicMock(return_value=("", ""))
        mock_popen_cls.return_value = mock_proc

        _make_job(store, "job-err")
        executor.start("job-err", ["kedro", "run"])

        job = store.get_job("job-err")
        assert job.status == JobStatus.ERROR
        assert job.error_summary == "invalid input data"

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_no_error_summary_on_success(self, mock_popen, store, executor):
        """When a process succeeds, error_summary should be None."""
        _make_job(store, "job-ok")
        executor.start("job-ok", ["kedro", "run"])

        job = store.get_job("job-ok")
        assert job.status == JobStatus.FINISHED
        assert job.error_summary is None


class TestExecutorProgressTracking:
    """Tests for PipelineExecutor integration with ProgressTracker."""

    def test_stream_reader_parses_node_start(self, store):
        """_stream_reader should detect 'Running node:' and call node_started."""
        from kedro_viz.api.rest.runner.progress import ProgressTracker

        tracker = ProgressTracker()
        exec_with_progress = PipelineExecutor(store, progress_tracker=tracker)
        tracker.init_run("job-1")
        _make_job(store, "job-1")

        pipe = MagicMock()
        pipe.readline = MagicMock(
            side_effect=[
                "2024-06-15 10:30:00 INFO  Running node: split_data_node\n",
                "",
            ]
        )

        exec_with_progress._stream_reader(pipe, "job-1", "stdout")

        progress = tracker.get_progress("job-1")
        assert progress.current_node == "split_data_node"
        assert len(progress.node_events) == 1
        assert progress.node_events[0].status == "running"
        assert progress.node_events[0].node_name == "split_data_node"

    def test_stream_reader_parses_node_complete(self, store):
        """_stream_reader should detect 'Completed.*node:' and call node_completed."""
        from kedro_viz.api.rest.runner.progress import ProgressTracker

        tracker = ProgressTracker()
        exec_with_progress = PipelineExecutor(store, progress_tracker=tracker)
        tracker.init_run("job-1", total_nodes=2)
        _make_job(store, "job-1")

        # First simulate a start, then a complete
        pipe = MagicMock()
        pipe.readline = MagicMock(
            side_effect=[
                "2024-06-15 Running node: train_model\n",
                "2024-06-15 Completed 1 out of 2 tasks. Completed node: train_model\n",
                "",
            ]
        )

        exec_with_progress._stream_reader(pipe, "job-1", "stdout")

        progress = tracker.get_progress("job-1")
        assert progress.nodes_completed == 1
        assert len(progress.node_events) == 2
        assert progress.node_events[1].status == "success"
        assert progress.node_events[1].node_name == "train_model"

    def test_stream_reader_parses_node_error(self, store):
        """_stream_reader should detect node error pattern."""
        from kedro_viz.api.rest.runner.progress import ProgressTracker

        tracker = ProgressTracker()
        exec_with_progress = PipelineExecutor(store, progress_tracker=tracker)
        tracker.init_run("job-1")
        _make_job(store, "job-1")

        pipe = MagicMock()
        pipe.readline = MagicMock(
            side_effect=[
                "Running node: bad_node\n",
                "Node bad_node failed with error\n",
                "",
            ]
        )

        exec_with_progress._stream_reader(pipe, "job-1", "stdout")

        progress = tracker.get_progress("job-1")
        assert len(progress.node_events) == 2
        assert progress.node_events[0].status == "running"
        assert progress.node_events[1].status == "failed"
        assert progress.node_events[1].node_name == "bad_node"

    def test_stream_reader_no_progress_without_tracker(self, store, executor):
        """Without a progress tracker, _stream_reader should still work normally."""
        _make_job(store, "job-1")

        pipe = MagicMock()
        pipe.readline = MagicMock(
            side_effect=["Running node: my_node\n", ""]
        )

        # Should not raise even without progress tracker
        executor._stream_reader(pipe, "job-1", "stdout")

        job = store.get_job("job-1")
        assert "Running node: my_node\n" in job.stdout

    def test_stream_reader_does_not_parse_stderr(self, store):
        """Progress parsing should only happen for stdout, not stderr."""
        from kedro_viz.api.rest.runner.progress import ProgressTracker

        tracker = ProgressTracker()
        exec_with_progress = PipelineExecutor(store, progress_tracker=tracker)
        tracker.init_run("job-1")
        _make_job(store, "job-1")

        pipe = MagicMock()
        pipe.readline = MagicMock(
            side_effect=["Running node: some_node\n", ""]
        )

        exec_with_progress._stream_reader(pipe, "job-1", "stderr")

        progress = tracker.get_progress("job-1")
        assert progress.current_node is None
        assert len(progress.node_events) == 0

    @patch("kedro_viz.api.rest.runner.executor.subprocess.Popen", side_effect=_mock_popen)
    def test_start_initializes_progress(self, mock_popen, store):
        """start() should call init_run on the progress tracker."""
        from kedro_viz.api.rest.runner.progress import ProgressTracker

        tracker = ProgressTracker()
        exec_with_progress = PipelineExecutor(store, progress_tracker=tracker)
        _make_job(store, "job-1")

        exec_with_progress.start("job-1", ["kedro", "run"])

        # Progress should have been initialized
        progress = tracker.get_progress("job-1")
        assert progress is not None
        assert progress.nodes_total == 0

    def test_full_kedro_log_sequence(self, store):
        """End-to-end: simulate a full Kedro run log and verify progress."""
        from kedro_viz.api.rest.runner.progress import ProgressTracker

        tracker = ProgressTracker()
        exec_with_progress = PipelineExecutor(store, progress_tracker=tracker)
        tracker.init_run("job-1", total_nodes=3)
        _make_job(store, "job-1")

        log_lines = [
            "2024-06-15 10:30:00 INFO  Running node: split_data_node\n",
            "2024-06-15 10:30:02 INFO  Completed 1 out of 3 tasks. Completed node: split_data_node\n",
            "2024-06-15 10:30:02 INFO  Running node: train_model_node\n",
            "2024-06-15 10:30:10 INFO  Completed 2 out of 3 tasks. Completed node: train_model_node\n",
            "2024-06-15 10:30:10 INFO  Running node: evaluate_model_node\n",
            "2024-06-15 10:30:12 INFO  Completed 3 out of 3 tasks. Completed node: evaluate_model_node\n",
            "",
        ]

        pipe = MagicMock()
        pipe.readline = MagicMock(side_effect=log_lines)

        exec_with_progress._stream_reader(pipe, "job-1", "stdout")

        progress = tracker.get_progress("job-1")
        assert progress.nodes_completed == 3
        assert progress.current_node is None  # last node completed
        # 3 starts + 3 completes = 6 events
        assert len(progress.node_events) == 6
