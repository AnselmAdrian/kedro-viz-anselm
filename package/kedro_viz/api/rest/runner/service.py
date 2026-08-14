"""Runner service orchestrating execution.

RunnerService wraps JobStore + PipelineExecutor and provides the
orchestration logic that router endpoints delegate to. The router
handles HTTP concerns (status codes, response formatting); this class
handles business logic (command parsing, job creation, cancellation).
"""

from __future__ import annotations

import shlex
import uuid
from datetime import datetime
from typing import Optional

from kedro_viz.api.rest.runner.executor import PipelineExecutor
from kedro_viz.api.rest.runner.models import Job, JobStatus, RunConfig
from kedro_viz.api.rest.runner.store import JobStore
from kedro_viz.api.rest.runner.validator import validate_raw_command


class ActiveJobError(Exception):
    """Raised when attempting to start a run while another is active."""

    def __init__(self, active_job_id: str, started_at: datetime):
        self.active_job_id = active_job_id
        self.started_at = started_at
        super().__init__(f"A run is already active: {active_job_id}")


class RunnerService:
    """Orchestrates pipeline execution through Store and Executor.

    Attributes:
        store: The JobStore used to persist job state.
        executor: The PipelineExecutor used to run subprocesses.
    """

    def __init__(self, store: JobStore, executor: PipelineExecutor) -> None:
        self.store = store
        self.executor = executor

    def start_run(self, config: RunConfig) -> tuple[Job, list[str]]:
        """Start a run from a structured RunConfig.

        Validates config, checks mutex, builds command, creates job.

        Args:
            config: The structured run configuration.

        Returns:
            A tuple of (Job, cmd_list) for background task scheduling.

        Raises:
            ActiveJobError: If a run is already active.
            ValueError: If config validation fails.
        """
        # Check mutex
        active_job = self.store.get_active_job()
        if active_job is not None:
            raise ActiveJobError(active_job.job_id, active_job.start_time)

        # Build command from config
        cmd = self._build_command(config)

        # Create job
        job_id = str(uuid.uuid4())
        job = Job(
            job_id=job_id,
            status=JobStatus.INITIALIZE,
            start_time=datetime.now(),
            config=config,
            cmd=" ".join([PipelineExecutor.quote_if_needed(c) for c in cmd]),
        )
        self.store.add_job(job)
        return job, cmd

    def _build_command(self, config: RunConfig) -> list[str]:
        """Construct kedro run command from structured config.

        Args:
            config: The RunConfig with pipeline, env, tags, params, etc.

        Returns:
            A list of command-line arguments for subprocess execution.
        """
        cmd = ["kedro", "run"]
        if config.pipeline:
            cmd.extend(["-p", config.pipeline])
        if config.env:
            cmd.extend(["-e", config.env])
        if config.tags:
            for tag in config.tags:
                cmd.extend(["--tags", tag])
        if config.from_nodes:
            for node in config.from_nodes:
                cmd.extend(["--from-nodes", node])
        if config.to_nodes:
            for node in config.to_nodes:
                cmd.extend(["--to-nodes", node])
        if config.params:
            # Format params as key=value pairs
            params_str = ",".join(f"{k}={v}" for k, v in config.params.items())
            cmd.extend(["--params", params_str])
        return cmd

    def start_raw_run(self, command: str) -> tuple[Job, list[str]]:
        """Start a run from a raw command string.

        Parses the command, creates a job, and adds it to the store.
        Returns the Job instance and the parsed command list so the
        caller (router) can schedule the actual subprocess execution
        as a background task.

        Args:
            command: The raw command string (e.g. "run --pipeline=my_pipeline"
                     or "kedro run --pipeline=my_pipeline").

        Returns:
            A tuple of (Job, cmd_list) where cmd_list is the parsed
            command ready for subprocess execution.

        Raises:
            ActiveJobError: If a run is already active.
            ValueError: If the command fails validation (e.g. disallowed
                verb or shell metacharacters detected). The exception
                message contains the validation error details.
        """
        # Enforce single-process execution constraint
        active_job = self.store.get_active_job()
        if active_job is not None:
            raise ActiveJobError(active_job.job_id, active_job.start_time)

        cmd = shlex.split(command)
        if not cmd[0] == "kedro":
            cmd = ["kedro"] + cmd

        # Validate the command before proceeding
        validation = validate_raw_command(cmd)
        if not validation.valid:
            raise ValueError("; ".join(validation.errors))

        job_id = str(uuid.uuid4())
        job = Job(
            job_id=job_id,
            status=JobStatus.INITIALIZE,
            start_time=datetime.now(),
            cmd=" ".join([PipelineExecutor.quote_if_needed(c) for c in cmd]),
        )
        self.store.add_job(job)

        return job, cmd

    def cancel_run(self, job_id: str) -> Optional[bool]:
        """Cancel an active run.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            True if the process was terminated, False if the process
            could not be terminated (e.g. already finished), or None
            if the job was not found.
        """
        job = self.store.get_job(job_id)
        if job is None:
            return None
        return self.executor.terminate(job_id)

    def get_status(self, job_id: str) -> Optional[Job]:
        """Get job status by ID.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            The Job if found, otherwise None.
        """
        return self.store.get_job(job_id)

    def get_history(self, limit: int = 50) -> list[Job]:
        """Get run history.

        Args:
            limit: Maximum number of jobs to return. Defaults to 50.

        Returns:
            A list of Job instances, most recent first.
        """
        return self.store.get_history(limit)
