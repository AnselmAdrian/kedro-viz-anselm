"""Tests for the ProgressTracker class."""

from __future__ import annotations

import threading

import pytest

from kedro_viz.api.rest.runner.progress import ProgressTracker


@pytest.fixture
def tracker():
    """Create a fresh ProgressTracker for each test."""
    return ProgressTracker()


class TestProgressTrackerInitRun:
    """Tests for ProgressTracker.init_run."""

    def test_init_run_creates_progress_entry(self, tracker):
        """init_run should create a RunProgress entry for the job."""
        tracker.init_run("job-1", total_nodes=5)
        progress = tracker.get_progress("job-1")
        assert progress is not None
        assert progress.nodes_total == 5
        assert progress.nodes_completed == 0
        assert progress.current_node is None
        assert progress.node_events == []

    def test_init_run_default_total_nodes(self, tracker):
        """init_run without total_nodes should default to 0."""
        tracker.init_run("job-2")
        progress = tracker.get_progress("job-2")
        assert progress.nodes_total == 0

    def test_init_run_overwrites_existing(self, tracker):
        """init_run on an existing job should reset progress."""
        tracker.init_run("job-1", total_nodes=3)
        tracker.node_started("job-1", "n1", "node_one")
        tracker.init_run("job-1", total_nodes=10)
        progress = tracker.get_progress("job-1")
        assert progress.nodes_total == 10
        assert progress.node_events == []
        assert progress.current_node is None


class TestProgressTrackerNodeStarted:
    """Tests for ProgressTracker.node_started."""

    def test_node_started_sets_current_node(self, tracker):
        """node_started should update current_node."""
        tracker.init_run("job-1")
        tracker.node_started("job-1", "n1", "split_data_node")
        progress = tracker.get_progress("job-1")
        assert progress.current_node == "split_data_node"

    def test_node_started_appends_running_event(self, tracker):
        """node_started should add a 'running' event."""
        tracker.init_run("job-1")
        tracker.node_started("job-1", "n1", "split_data_node")
        progress = tracker.get_progress("job-1")
        assert len(progress.node_events) == 1
        event = progress.node_events[0]
        assert event.node_id == "n1"
        assert event.node_name == "split_data_node"
        assert event.status == "running"
        assert event.timestamp is not None

    def test_node_started_no_op_if_job_not_initialized(self, tracker):
        """node_started on a non-existent job should be a no-op."""
        tracker.node_started("no-such-job", "n1", "node")
        assert tracker.get_progress("no-such-job") is None


class TestProgressTrackerNodeCompleted:
    """Tests for ProgressTracker.node_completed."""

    def test_node_completed_increments_count(self, tracker):
        """node_completed should increment nodes_completed."""
        tracker.init_run("job-1", total_nodes=3)
        tracker.node_started("job-1", "n1", "node_one")
        tracker.node_completed("job-1", "n1", "node_one")
        progress = tracker.get_progress("job-1")
        assert progress.nodes_completed == 1

    def test_node_completed_clears_current_node(self, tracker):
        """node_completed should clear current_node when it matches."""
        tracker.init_run("job-1")
        tracker.node_started("job-1", "n1", "node_one")
        assert tracker.get_progress("job-1").current_node == "node_one"
        tracker.node_completed("job-1", "n1", "node_one")
        assert tracker.get_progress("job-1").current_node is None

    def test_node_completed_does_not_clear_different_current_node(self, tracker):
        """node_completed should not clear current_node if names don't match."""
        tracker.init_run("job-1")
        tracker.node_started("job-1", "n1", "node_one")
        tracker.node_started("job-1", "n2", "node_two")
        # current_node is now "node_two"
        tracker.node_completed("job-1", "n1", "node_one")
        assert tracker.get_progress("job-1").current_node == "node_two"

    def test_node_completed_appends_success_event_with_duration(self, tracker):
        """node_completed should add a 'success' event with duration."""
        tracker.init_run("job-1")
        tracker.node_started("job-1", "n1", "node_one")
        tracker.node_completed("job-1", "n1", "node_one")
        progress = tracker.get_progress("job-1")
        assert len(progress.node_events) == 2
        event = progress.node_events[1]
        assert event.status == "success"
        assert event.duration is not None
        assert event.duration >= 0

    def test_node_completed_no_op_if_job_not_initialized(self, tracker):
        """node_completed on a non-existent job should be a no-op."""
        tracker.node_completed("no-such-job", "n1", "node")
        assert tracker.get_progress("no-such-job") is None


class TestProgressTrackerNodeFailed:
    """Tests for ProgressTracker.node_failed."""

    def test_node_failed_appends_failed_event(self, tracker):
        """node_failed should add a 'failed' event with error."""
        tracker.init_run("job-1")
        tracker.node_started("job-1", "n1", "node_one")
        tracker.node_failed("job-1", "n1", "node_one", error="ValueError: bad input")
        progress = tracker.get_progress("job-1")
        assert len(progress.node_events) == 2
        event = progress.node_events[1]
        assert event.status == "failed"
        assert event.error == "ValueError: bad input"
        assert event.duration is not None

    def test_node_failed_clears_current_node(self, tracker):
        """node_failed should clear current_node when it matches."""
        tracker.init_run("job-1")
        tracker.node_started("job-1", "n1", "node_one")
        tracker.node_failed("job-1", "n1", "node_one")
        assert tracker.get_progress("job-1").current_node is None

    def test_node_failed_does_not_increment_completed(self, tracker):
        """node_failed should NOT increment nodes_completed."""
        tracker.init_run("job-1", total_nodes=3)
        tracker.node_started("job-1", "n1", "node_one")
        tracker.node_failed("job-1", "n1", "node_one")
        assert tracker.get_progress("job-1").nodes_completed == 0

    def test_node_failed_no_op_if_job_not_initialized(self, tracker):
        """node_failed on a non-existent job should be a no-op."""
        tracker.node_failed("no-such-job", "n1", "node")
        assert tracker.get_progress("no-such-job") is None


class TestProgressTrackerGetProgress:
    """Tests for ProgressTracker.get_progress."""

    def test_get_progress_returns_none_for_unknown_job(self, tracker):
        """get_progress for unknown job should return None."""
        assert tracker.get_progress("unknown") is None

    def test_get_progress_returns_current_state(self, tracker):
        """get_progress should reflect the current state of the run."""
        tracker.init_run("job-1", total_nodes=3)
        tracker.node_started("job-1", "n1", "node_one")
        tracker.node_completed("job-1", "n1", "node_one")
        tracker.node_started("job-1", "n2", "node_two")

        progress = tracker.get_progress("job-1")
        assert progress.nodes_total == 3
        assert progress.nodes_completed == 1
        assert progress.current_node == "node_two"
        assert len(progress.node_events) == 3


class TestProgressTrackerThreadSafety:
    """Tests for ProgressTracker thread safety."""

    def test_concurrent_updates_do_not_corrupt_state(self, tracker):
        """Multiple threads updating progress should not corrupt state."""
        tracker.init_run("job-1", total_nodes=100)

        def update_nodes(start, count):
            for i in range(start, start + count):
                node_id = f"n{i}"
                tracker.node_started("job-1", node_id, f"node_{i}")
                tracker.node_completed("job-1", node_id, f"node_{i}")

        threads = [
            threading.Thread(target=update_nodes, args=(i * 10, 10))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        progress = tracker.get_progress("job-1")
        # All 100 nodes should be completed
        assert progress.nodes_completed == 100
        # Each node produces 2 events (started + completed)
        assert len(progress.node_events) == 200
