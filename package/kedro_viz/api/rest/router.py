"""`kedro_viz.api.rest.router` defines REST routes and handling logic."""

import asyncio
import logging
import queue

from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse

from kedro_viz.api.rest.requests import (
    DeployerConfiguration,
)
from kedro_viz.api.rest.responses.base import APINotFoundResponse
from kedro_viz.api.rest.responses.metadata import (
    MetadataAPIResponse,
    get_metadata_response,
)
from kedro_viz.api.rest.responses.nodes import (
    NodeMetadataAPIResponse,
    get_node_metadata_response,
)
from kedro_viz.api.rest.responses.pipelines import (
    GraphAPIResponse,
    get_pipeline_response,
)
from kedro_viz.api.rest.responses.run_events import (
    RunStatusAPIResponse,
    get_run_status_response,
)
from kedro_viz.api.rest.responses.version import (
    VersionAPIResponse,
    get_version_response,
)
from kedro_viz.api.rest.responses.env import (
    EnvironmentAPIResponse,
    get_env_response,
)
from pathlib import Path

from kedro_viz.api.rest.runner.models import RunConfig
from kedro_viz.api.rest.runner.store import JobStore
from kedro_viz.api.rest.runner.executor import PipelineExecutor
from kedro_viz.api.rest.runner.progress import ProgressTracker
from kedro_viz.api.rest.runner.service import ActiveJobError, RunnerService
from kedro_viz.api.rest.runner.events import SSEFormatter

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api",
    responses={404: {"model": APINotFoundResponse}},
)


@router.get("/main", response_model=GraphAPIResponse)
async def main():
    return get_pipeline_response()


@router.get("/env", response_model=EnvironmentAPIResponse)
async def get_env():
    return get_env_response()

@router.get(
    "/nodes/{node_id}",
    response_model=NodeMetadataAPIResponse,
    response_model_exclude_none=True,
)
async def get_single_node_metadata(node_id: str):
    return get_node_metadata_response(node_id)


@router.get(
    "/pipelines/{registered_pipeline_id}",
    response_model=GraphAPIResponse,
)
async def get_single_pipeline_data(registered_pipeline_id: str):
    return get_pipeline_response(registered_pipeline_id)


@router.get(
    "/version",
    response_model=VersionAPIResponse,
)
async def get_version():
    return get_version_response()


@router.get("/run-status", response_model=RunStatusAPIResponse)
async def get_last_run_status():
    """Get run status data for pipeline visualization.

    This endpoint provides access to Kedro pipeline run status in structured format.

    Returns:
        JSON response containing run status data in structured format

    Example structured format:
    ```
    {
        "nodes": {
            "node_id": {
                "status": "success",
                "duration": 0.123,
                "error": null
            }
        },
        "datasets": {
            "dataset_id": {
                "name": "dataset.name",
                "size": 1024,
                "error": null
            }
        },
        "pipeline": {
            "run_id": "unique-id",
            "start_time": "2023-05-14T10:15:30Z",
            "end_time": "2023-05-14T10:20:45Z",
            "duration": 315.25,
            "status": "completed"
            "error": null
        }
    }
    ```
    """
    try:
        return get_run_status_response()
    except Exception as exc:
        logger.exception("An exception occurred while getting run status: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"message": "Failed to get run status data"},
        )


@router.post("/deploy")
async def deploy_kedro_viz(input_values: DeployerConfiguration):
    from kedro_viz.integrations.deployment.deployer_factory import DeployerFactory

    try:
        from azure.core.exceptions import ServiceRequestError
    except ImportError:  # pragma: no cover
        ServiceRequestError = None  # type: ignore

    try:
        deployer = DeployerFactory.create_deployer(
            input_values.platform, input_values.endpoint, input_values.bucket_name
        )
        deployer.deploy(input_values.is_all_previews_enabled)
        response = {
            "message": "Website deployed on "
            f"{input_values.platform and input_values.platform.upper()}",
            "url": input_values.endpoint,
        }
        return JSONResponse(status_code=200, content=response)
    except PermissionError as exc:  # pragma: no cover
        logger.exception("Permission error in deploying Kedro Viz : %s ", exc)
        return JSONResponse(
            status_code=401, content={"message": "Please provide valid credentials"}
        )
    except (
        (FileNotFoundError, ServiceRequestError)
        if ServiceRequestError is not None
        else FileNotFoundError
    ) as exc:  # pragma: no cover
        logger.exception("FileNotFoundError while deploying Kedro Viz : %s ", exc)
        return JSONResponse(
            status_code=400, content={"message": "The specified bucket does not exist"}
        )
    except Exception as exc:  # pragma: no cover
        logger.exception("Deploying Kedro Viz failed: %s ", exc)
        return JSONResponse(status_code=500, content={"message": f"{exc}"})


job_store = JobStore(storage_dir=Path(".viz/runner_jobs"))
progress_tracker = ProgressTracker()
executor = PipelineExecutor(job_store, progress_tracker=progress_tracker)
runner_service = RunnerService(store=job_store, executor=executor)


# TODO: Remove this endpoint after frontend migration to POST /api/run is complete.
# The old endpoint is retained temporarily for backward compatibility.
# When removing, also delete `validate_raw_command` from validator.py and
# `start_raw_run` from service.py.
@router.post("/run-kedro-command")
async def run_kedro_command(command: str, background_tasks: BackgroundTasks):
    """
    Run a Kedro command provided as a string in a subprocess and return the output.
    Example request body: {"command": "run --pipeline=my_pipeline"}

    .. deprecated::
        Use POST /api/run with a structured RunConfig body instead.
    """
    logger.warning(
        "Deprecated endpoint called: POST /api/run-kedro-command. "
        "Use POST /api/run instead."
    )
    try:
        job, cmd = runner_service.start_raw_run(command)
    except ActiveJobError as exc:
        return JSONResponse(
            status_code=409,
            content={
                "message": "A run is already active",
                "active_job_id": exc.active_job_id,
                "started_at": exc.started_at.isoformat(),
            },
        )
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"message": "Command validation failed", "errors": str(exc).split("; ")},
        )

    background_tasks.add_task(runner_service.executor.start, job.job_id, cmd)
    response = JSONResponse(
        status_code=202,
        content={
            "job_id": job.job_id,
            "status": "initialize",
            "_deprecated": "Use POST /api/run instead",
        },
    )
    response.headers["Deprecation"] = "true"
    return response


@router.get("/kedro-command-status/{job_id}")
async def get_kedro_command_status(job_id: str):
    """
    Get the status of a previously run Kedro command.
    """
    job = runner_service.get_status(job_id)
    if not job:
        return JSONResponse(
            status_code=404,
            content={"message": "Job not found"},
        )

    # Use full logs from disk for completed jobs (in-memory may be capped)
    stdout, stderr = runner_service.store.get_full_logs(job_id)

    return JSONResponse(
        status_code=200,
        content={
            "start_time": job.start_time.isoformat(),
            "cmd": job.cmd,
            "duration": job.duration,
            "end_time": job.end_time.isoformat() if job.end_time else None,
            "status": job.status.value,
            "stdout": stdout,
            "stderr": stderr,
            "returncode": job.returncode,
            "error_summary": job.error_summary,
        },
    )


@router.post("/kedro-command-cancel/{job_id}")
async def cancel_kedro_command(job_id: str):
    """Attempt to terminate a running Kedro command."""
    result = runner_service.cancel_run(job_id)
    if result is None:
        return JSONResponse(status_code=404, content={"message": "Job not found"})

    return JSONResponse(status_code=200, content={"terminated": result})


@router.post("/run")
async def start_run(config: RunConfig, background_tasks: BackgroundTasks):
    """Start a validated pipeline run from a structured config.

    This is the primary endpoint for starting pipeline runs. It accepts
    a structured RunConfig JSON body and returns the created job.

    Frontend contract:
        Request:  POST /api/run  { pipeline?, env?, tags?, from_nodes?, to_nodes?, params? }
        Response: 202 { "job_id": "<uuid>", "status": "initialize" }
        Error:    400 { "message": "...", "errors": [...] }
        Conflict: 409 { "message": "A run is already active", "active_job_id": "...", "started_at": "..." }

    The response shape (job_id + status) is compatible with the frontend's
    runner-api.js expectations.
    """
    try:
        job, cmd = runner_service.start_run(config)
    except ActiveJobError as exc:
        return JSONResponse(
            status_code=409,
            content={
                "message": "A run is already active",
                "active_job_id": exc.active_job_id,
                "started_at": exc.started_at.isoformat(),
            },
        )
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"message": "Validation failed", "errors": str(exc).split("; ")},
        )

    background_tasks.add_task(runner_service.executor.start, job.job_id, cmd)
    return JSONResponse(
        status_code=202,
        content={"job_id": job.job_id, "status": "initialize"},
    )


@router.get("/run-history")
async def get_run_history(limit: int = 50):
    """Get metadata for recent pipeline runs."""
    jobs = runner_service.get_history(limit)
    return JSONResponse(
        status_code=200,
        content=[
            {
                "job_id": job.job_id,
                "status": job.status.value,
                "start_time": job.start_time.isoformat(),
                "end_time": job.end_time.isoformat() if job.end_time else None,
                "duration": job.duration,
                "cmd": job.cmd,
                "returncode": job.returncode,
                "error_summary": job.error_summary,
            }
            for job in jobs
        ],
    )


@router.get("/run-progress/{job_id}")
async def get_run_progress(job_id: str):
    """Get node-level progress for a running job."""
    progress = progress_tracker.get_progress(job_id)
    if progress is None:
        return JSONResponse(
            status_code=404,
            content={"message": "Job not found or no progress available"},
        )

    result = {
        "nodes_total": progress.nodes_total,
        "nodes_completed": progress.nodes_completed,
        "current_node": progress.current_node,
        "node_events": [
            {
                "node_id": e.node_id,
                "node_name": e.node_name,
                "status": e.status,
                "timestamp": e.timestamp.isoformat(),
                "duration": e.duration,
                "error": e.error,
            }
            for e in progress.node_events
        ],
    }
    return JSONResponse(status_code=200, content=result)


@router.get("/run-stream/{job_id}")
async def stream_run_events(job_id: str):
    """Stream live updates for a running job via Server-Sent Events.

    Returns a StreamingResponse with media_type text/event-stream.
    On connect, sends current state snapshot so reconnecting clients
    are caught up. Pushes log, progress, and done events as they occur.
    Terminates when the job reaches a terminal state.
    """
    job = runner_service.get_status(job_id)
    if not job:
        return JSONResponse(
            status_code=404,
            content={"message": "Job not found"},
        )

    event_queue = runner_service.executor.get_event_queue(job_id)

    if event_queue is None:
        # Job already completed — send snapshot and done
        async def completed_stream():
            yield SSEFormatter.status_event(job.status.value, job.error_summary)
            yield SSEFormatter.done_event(
                job.status.value, job.duration, job.returncode, job.error_summary
            )

        return StreamingResponse(completed_stream(), media_type="text/event-stream")

    async def event_generator():
        """Async generator that reads from the event queue."""
        # Send initial status snapshot
        yield SSEFormatter.status_event(job.status.value, job.error_summary)

        try:
            while True:
                try:
                    # Use asyncio-friendly polling to avoid blocking the event loop
                    event = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: event_queue.get(timeout=1)
                    )
                    if event is None:  # Sentinel: stream closed
                        break
                    yield event
                except queue.Empty:
                    continue
        except asyncio.CancelledError:
            pass  # Client disconnected

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get(
    "/metadata",
    response_model=MetadataAPIResponse,
)
async def get_metadata():
    try:
        return get_metadata_response()
    except Exception as exc:
        logger.exception("An exception occurred while getting app metadata: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"message": "Failed to get app metadata"},
        )
