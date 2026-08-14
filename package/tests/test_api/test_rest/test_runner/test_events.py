"""Tests for SSE event formatting."""

from __future__ import annotations

import json

from kedro_viz.api.rest.runner.events import SSEFormatter


class TestSSEFormatter:
    """Tests that SSEFormatter methods produce valid SSE-formatted strings."""

    def test_log_event_stdout(self):
        """log_event should produce a valid SSE data line for stdout."""
        result = SSEFormatter.log_event("Hello, world!", "stdout")
        assert result.startswith("data: ")
        assert result.endswith("\n\n")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["type"] == "log"
        assert payload["stream"] == "stdout"
        assert payload["line"] == "Hello, world!"

    def test_log_event_stderr(self):
        """log_event should produce a valid SSE data line for stderr."""
        result = SSEFormatter.log_event("Error occurred", "stderr")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["type"] == "log"
        assert payload["stream"] == "stderr"
        assert payload["line"] == "Error occurred"

    def test_log_event_default_stream(self):
        """log_event defaults to stdout."""
        result = SSEFormatter.log_event("some output")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["stream"] == "stdout"

    def test_log_event_empty_line(self):
        """log_event handles empty line."""
        result = SSEFormatter.log_event("")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["line"] == ""

    def test_progress_event(self):
        """progress_event should include node counts and current node."""
        result = SSEFormatter.progress_event(10, 3, "train_model")
        assert result.startswith("data: ")
        assert result.endswith("\n\n")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["type"] == "progress"
        assert payload["nodes_total"] == 10
        assert payload["nodes_completed"] == 3
        assert payload["current_node"] == "train_model"

    def test_progress_event_no_current_node(self):
        """progress_event with None current_node."""
        result = SSEFormatter.progress_event(5, 5, None)
        payload = json.loads(result[len("data: ") : -2])
        assert payload["current_node"] is None

    def test_status_event(self):
        """status_event should include status and optional error_summary."""
        result = SSEFormatter.status_event("running")
        assert result.startswith("data: ")
        assert result.endswith("\n\n")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["type"] == "status"
        assert payload["status"] == "running"
        assert payload["error_summary"] is None

    def test_status_event_with_error(self):
        """status_event with an error summary."""
        result = SSEFormatter.status_event("error", "ValueError: bad input")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["status"] == "error"
        assert payload["error_summary"] == "ValueError: bad input"

    def test_done_event(self):
        """done_event should include all completion fields."""
        result = SSEFormatter.done_event("finished", 42.5, 0, None)
        assert result.startswith("data: ")
        assert result.endswith("\n\n")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["type"] == "done"
        assert payload["status"] == "finished"
        assert payload["duration"] == 42.5
        assert payload["returncode"] == 0
        assert payload["error_summary"] is None

    def test_done_event_with_error(self):
        """done_event for a failed run."""
        result = SSEFormatter.done_event("error", 10.0, 1, "ModuleNotFoundError: No module named 'foo'")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["type"] == "done"
        assert payload["status"] == "error"
        assert payload["duration"] == 10.0
        assert payload["returncode"] == 1
        assert payload["error_summary"] == "ModuleNotFoundError: No module named 'foo'"

    def test_done_event_defaults(self):
        """done_event with only status provided."""
        result = SSEFormatter.done_event("finished")
        payload = json.loads(result[len("data: ") : -2])
        assert payload["duration"] is None
        assert payload["returncode"] is None
        assert payload["error_summary"] is None

    def test_all_events_are_valid_json(self):
        """All event types should produce parseable JSON."""
        events = [
            SSEFormatter.log_event("test line", "stdout"),
            SSEFormatter.progress_event(5, 2, "node_a"),
            SSEFormatter.status_event("running"),
            SSEFormatter.done_event("finished", 1.0, 0),
        ]
        for event in events:
            assert event.startswith("data: ")
            assert event.endswith("\n\n")
            # Should be valid JSON between "data: " and the double newline
            json_str = event[len("data: ") : -2]
            parsed = json.loads(json_str)
            assert "type" in parsed
