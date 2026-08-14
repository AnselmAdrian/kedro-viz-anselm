"""Pipeline executor for subprocess management."""

from __future__ import annotations

import logging
import os
import queue
import re
import signal
import subprocess
import threading
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from kedro_viz.api.rest.runner.events import SSEFormatter
from kedro_viz.api.rest.runner.models import JobStatus
from kedro_viz.api.rest.runner.store import JobStore

if TYPE_CHECKING:
    from kedro_viz.api.rest.runner.progress import ProgressTracker

logger = logging.getLogger(__name__)

# Patterns for parsing Kedro log output for progress tracking
NODE_START_PATTERN = re.compile(r"Running node:\s+(.+)")
NODE_COMPLETE_PATTERN = re.compile(r"Completed.*node:\s+(.+?)(?:\s|$)")
NODE_ERROR_PATTERN = re.compile(r"Node (.+?) failed with error")


def extract_error_summary(stderr: str, max_length: int = 200) -> Optional[str]:
    """Extract the first meaningful error line from stderr.

    Scans stderr line-by-line for common error patterns (Exception, Error,
    Failed, etc.) and returns the matched message. Falls back to the last
    non-empty line if no pattern matches.

    Args:
        stderr: The full stderr output from the subprocess.
        max_length: Maximum length of the returned summary string.

    Returns:
        A short error description, or None if stderr is empty.
    """
    if not stderr:
        return None

    # Common error patterns
    patterns = [
        r"(?:Error|Exception|Failed).*?:\s*(.+)",
        r"raise\s+\w+\((.+)\)",
        r"\[ERROR\]\s*(.+)",
    ]

    for line in stderr.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        for pattern in patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                summary = match.group(1).strip()
                return summary[:max_length] if len(summary) > max_length else summary

    # Fallback: last non-empty line of stderr
    lines = [l.strip() for l in stderr.strip().split("\n") if l.strip()]
    if lines:
        return lines[-1][:max_length]

    return None


class PipelineExecutor:
    """Manages subprocess execution for Kedro pipeline runs.

    Encapsulates spawning, monitoring, and terminating subprocesses.
    Uses a JobStore to persist job state updates.
    """

    def __init__(
        self, store: JobStore, progress_tracker: Optional["ProgressTracker"] = None
    ) -> None:
        self._store = store
        self._progress = progress_tracker
        self._processes: dict[str, subprocess.Popen] = {}
        self._event_queues: dict[str, queue.Queue] = {}
        self._lock = threading.Lock()

    def create_event_queue(self, job_id: str) -> queue.Queue:
        """Create and return an event queue for SSE streaming.

        Args:
            job_id: The job ID to create a queue for.

        Returns:
            The newly created event queue.
        """
        q: queue.Queue = queue.Queue()
        with self._lock:
            self._event_queues[job_id] = q
        return q

    def get_event_queue(self, job_id: str) -> Optional[queue.Queue]:
        """Get the event queue for a job, or None if not found.

        Args:
            job_id: The job ID to look up.

        Returns:
            The event queue if it exists, otherwise None.
        """
        with self._lock:
            return self._event_queues.get(job_id)

    def start(self, job_id: str, cmd: list[str]) -> None:
        """Spawn a subprocess and monitor it until completion.

        This method blocks until the subprocess exits — it is intended
        to be called from a background task, not from the request thread.

        Args:
            job_id: The ID of the job to execute.
            cmd: The command to run as a list of arguments.
        """
        # Create an event queue for SSE streaming
        self.create_event_queue(job_id)

        logger.info("Running Kedro command: %s", cmd)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        logger.info("Started Kedro command with PID: %s", process.pid)

        # Track the active process
        with self._lock:
            self._processes[job_id] = process

        # Initialize progress tracking for this run
        if self._progress:
            self._progress.init_run(job_id)

        # Store pid & running status
        self._store.update_job(job_id, pid=process.pid, status=JobStatus.RUNNING)

        # Start reader threads to update job logs while the process runs
        t_out = threading.Thread(
            target=self._stream_reader,
            args=(process.stdout, job_id, "stdout"),
            daemon=True,
        )
        t_err = threading.Thread(
            target=self._stream_reader,
            args=(process.stderr, job_id, "stderr"),
            daemon=True,
        )
        t_out.start()
        t_err.start()

        # Wait for the process to finish
        returncode = process.wait()

        # Join reader threads briefly to collect remaining output
        t_out.join(timeout=1)
        t_err.join(timeout=1)

        # Final collect (communicate to ensure no left-over data)
        try:
            rem_out, rem_err = process.communicate(timeout=0.1)
        except Exception:
            rem_out, rem_err = "", ""

        if rem_out:
            self._store.append_logs(job_id, stdout=rem_out)
        if rem_err:
            self._store.append_logs(job_id, stderr=rem_err)

        end_time = datetime.now()
        job = self._store.get_job(job_id)
        duration = None
        if job and job.start_time:
            duration = (end_time - job.start_time).total_seconds()

        final_status = JobStatus.FINISHED if returncode == 0 else JobStatus.ERROR

        # Extract error summary from stderr on failure
        error_summary = None
        if final_status == JobStatus.ERROR and job:
            error_summary = extract_error_summary(job.stderr)

        self._store.update_job(
            job_id,
            returncode=returncode,
            status=final_status,
            end_time=end_time,
            duration=duration,
            error_summary=error_summary,
        )

        # Push done event to the SSE queue and close it with sentinel
        event_queue = self.get_event_queue(job_id)
        if event_queue is not None:
            event_queue.put(
                SSEFormatter.done_event(
                    final_status.value, duration, returncode, error_summary
                )
            )
            event_queue.put(None)  # Sentinel: signal stream end

        # Remove process reference
        with self._lock:
            self._processes.pop(job_id, None)

        logger.info("Kedro job %s finished with return code %d", job_id, returncode)

    def terminate(self, job_id: str) -> bool:
        """Terminate a running subprocess by job ID.

        Sends SIGTERM to the process and updates the job status
        to TERMINATED.

        Args:
            job_id: The ID of the job to terminate.

        Returns:
            True if the process was terminated, False otherwise.
        """
        job = self._store.get_job(job_id)
        if not job:
            return False

        pid = job.pid
        status = job.status

        if not pid or status not in {JobStatus.INITIALIZE, JobStatus.RUNNING}:
            return False

        try:
            os.kill(pid, signal.SIGTERM)
            end_time = datetime.now()
            duration = None
            if job.start_time:
                duration = (end_time - job.start_time).total_seconds()
            self._store.update_job(
                job_id,
                status=JobStatus.TERMINATED,
                end_time=end_time,
                duration=duration,
            )
            # Remove process reference
            with self._lock:
                self._processes.pop(job_id, None)
            return True
        except Exception as exc:
            logger.exception("Failed to terminate Kedro job %s: %s", job_id, exc)
            return False

    def _stream_reader(self, pipe, job_id: str, key: str) -> None:
        """Read lines from a subprocess pipe and append them to job logs.

        Also parses Kedro log output for progress tracking when reading
        stdout and a ProgressTracker is configured. Pushes SSE log events
        to the job's event queue.

        Args:
            pipe: The stdout or stderr pipe from a Popen instance.
            job_id: The job ID to append logs to.
            key: Either "stdout" or "stderr".
        """
        event_queue = self.get_event_queue(job_id)
        try:
            for line in iter(pipe.readline, ""):
                if not line:
                    break
                if key == "stdout":
                    self._store.append_logs(job_id, stdout=line)
                    # Parse progress from stdout
                    if self._progress:
                        self._parse_progress(job_id, line)
                else:
                    self._store.append_logs(job_id, stderr=line)

                # Push log event to SSE queue
                if event_queue is not None:
                    event_queue.put(SSEFormatter.log_event(line.rstrip("\n"), key))
        finally:
            pass

    def _parse_progress(self, job_id: str, line: str) -> None:
        """Parse a single stdout line for Kedro node lifecycle patterns.

        Also pushes progress events to the SSE event queue.

        Args:
            job_id: The job ID to update progress for.
            line: A single line of stdout output.
        """
        start_match = NODE_START_PATTERN.search(line)
        if start_match:
            node_name = start_match.group(1).strip()
            self._progress.node_started(job_id, node_name, node_name)
            self._push_progress_event(job_id)
            return

        complete_match = NODE_COMPLETE_PATTERN.search(line)
        if complete_match:
            node_name = complete_match.group(1).strip()
            self._progress.node_completed(job_id, node_name, node_name)
            self._push_progress_event(job_id)
            return

        error_match = NODE_ERROR_PATTERN.search(line)
        if error_match:
            node_name = error_match.group(1).strip()
            self._progress.node_failed(job_id, node_name, node_name, error=line.strip())
            self._push_progress_event(job_id)

    def _push_progress_event(self, job_id: str) -> None:
        """Push the current progress state as an SSE event to the queue.

        Args:
            job_id: The job ID to push progress for.
        """
        event_queue = self.get_event_queue(job_id)
        if event_queue is None:
            return
        progress = self._progress.get_progress(job_id)
        if progress is not None:
            event_queue.put(
                SSEFormatter.progress_event(
                    progress.nodes_total,
                    progress.nodes_completed,
                    progress.current_node,
                )
            )

    @staticmethod
    def quote_if_needed(text: str) -> str:
        """Wrap text in double quotes if it contains spaces.

        Args:
            text: The string to potentially quote.

        Returns:
            The original string, or the string wrapped in double quotes
            if it contains spaces.
        """
        if " " in text:
            return f'"{text}"'
        return text
