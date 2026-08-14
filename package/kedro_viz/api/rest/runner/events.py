"""SSE event formatting for the streaming endpoint."""

from __future__ import annotations

import json
from typing import Optional


class SSEFormatter:
    """Formats Server-Sent Events for the streaming endpoint.

    Each method returns a properly formatted SSE string:
    `data: {json_payload}\n\n`
    """

    @staticmethod
    def log_event(line: str, stream: str = "stdout") -> str:
        """Format a log line as an SSE event.

        Args:
            line: The log line text.
            stream: Either "stdout" or "stderr".

        Returns:
            A formatted SSE data string.
        """
        data = json.dumps({"type": "log", "stream": stream, "line": line})
        return f"data: {data}\n\n"

    @staticmethod
    def progress_event(
        nodes_total: int, nodes_completed: int, current_node: Optional[str]
    ) -> str:
        """Format a progress update as an SSE event.

        Args:
            nodes_total: Total number of nodes in the pipeline.
            nodes_completed: Number of completed nodes.
            current_node: Name of the currently executing node, or None.

        Returns:
            A formatted SSE data string.
        """
        data = json.dumps(
            {
                "type": "progress",
                "nodes_total": nodes_total,
                "nodes_completed": nodes_completed,
                "current_node": current_node,
            }
        )
        return f"data: {data}\n\n"

    @staticmethod
    def status_event(status: str, error_summary: Optional[str] = None) -> str:
        """Format a status change as an SSE event.

        Args:
            status: The new job status value.
            error_summary: Optional error summary (for error/terminated states).

        Returns:
            A formatted SSE data string.
        """
        data = json.dumps(
            {"type": "status", "status": status, "error_summary": error_summary}
        )
        return f"data: {data}\n\n"

    @staticmethod
    def done_event(
        status: str,
        duration: Optional[float] = None,
        returncode: Optional[int] = None,
        error_summary: Optional[str] = None,
    ) -> str:
        """Format a completion event as an SSE event.

        Args:
            status: The final job status.
            duration: Total execution duration in seconds.
            returncode: Process exit code.
            error_summary: Optional error summary for failed runs.

        Returns:
            A formatted SSE data string.
        """
        data = json.dumps(
            {
                "type": "done",
                "status": status,
                "duration": duration,
                "returncode": returncode,
                "error_summary": error_summary,
            }
        )
        return f"data: {data}\n\n"
