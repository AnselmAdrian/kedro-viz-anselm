"""Tests for runner data models (JobStatus, Job, RunConfig, RunProgress, etc.)."""

import json
from datetime import datetime

import pytest

from kedro_viz.api.rest.runner.models import (
    Job,
    JobStatus,
    NodeEvent,
    RunConfig,
    RunProgress,
    ValidationResult,
)


class TestJobStatus:
    """Tests for JobStatus enum."""

    def test_values(self):
        assert JobStatus.INITIALIZE == "initialize"
        assert JobStatus.RUNNING == "running"
        assert JobStatus.FINISHED == "finished"
        assert JobStatus.ERROR == "error"
        assert JobStatus.TERMINATED == "terminated"
        assert JobStatus.INTERRUPTED == "interrupted"

    def test_is_string_enum(self):
        assert isinstance(JobStatus.RUNNING, str)
        assert JobStatus.RUNNING == "running"

    def test_all_members(self):
        expected = {
            "INITIALIZE",
            "RUNNING",
            "FINISHED",
            "ERROR",
            "TERMINATED",
            "INTERRUPTED",
        }
        assert set(JobStatus.__members__.keys()) == expected


class TestRunConfig:
    """Tests for RunConfig Pydantic model."""

    def test_defaults_all_none(self):
        config = RunConfig()
        assert config.pipeline is None
        assert config.env is None
        assert config.tags is None
        assert config.params is None
        assert config.from_nodes is None
        assert config.to_nodes is None

    def test_with_all_fields(self):
        config = RunConfig(
            pipeline="data_engineering",
            env="local",
            tags=["train", "evaluate"],
            params={"model.learning_rate": 0.01},
            from_nodes=["split_data"],
            to_nodes=["train_model"],
        )
        assert config.pipeline == "data_engineering"
        assert config.env == "local"
        assert config.tags == ["train", "evaluate"]
        assert config.params == {"model.learning_rate": 0.01}
        assert config.from_nodes == ["split_data"]
        assert config.to_nodes == ["train_model"]

    def test_json_serialization(self):
        config = RunConfig(pipeline="my_pipeline", tags=["train"])
        data = config.model_dump()
        assert data["pipeline"] == "my_pipeline"
        assert data["tags"] == ["train"]
        assert data["env"] is None

    def test_from_dict(self):
        data = {"pipeline": "de", "params": {"lr": 0.1}}
        config = RunConfig(**data)
        assert config.pipeline == "de"
        assert config.params == {"lr": 0.1}


class TestNodeEvent:
    """Tests for NodeEvent dataclass."""

    def test_creation(self):
        ts = datetime(2025, 1, 15, 10, 30, 0)
        event = NodeEvent(
            node_id="split_data_node",
            node_name="split_data",
            status="running",
            timestamp=ts,
        )
        assert event.node_id == "split_data_node"
        assert event.node_name == "split_data"
        assert event.status == "running"
        assert event.timestamp == ts
        assert event.duration is None
        assert event.error is None

    def test_with_duration_and_error(self):
        ts = datetime(2025, 1, 15, 10, 30, 0)
        event = NodeEvent(
            node_id="train_node",
            node_name="train_model",
            status="failed",
            timestamp=ts,
            duration=12.5,
            error="OutOfMemoryError",
        )
        assert event.duration == 12.5
        assert event.error == "OutOfMemoryError"


class TestRunProgress:
    """Tests for RunProgress dataclass."""

    def test_defaults(self):
        progress = RunProgress()
        assert progress.nodes_total == 0
        assert progress.nodes_completed == 0
        assert progress.current_node is None
        assert progress.node_events == []

    def test_with_events(self):
        ts = datetime(2025, 1, 15, 10, 30, 0)
        event = NodeEvent(
            node_id="n1", node_name="node_1", status="success", timestamp=ts
        )
        progress = RunProgress(
            nodes_total=5,
            nodes_completed=1,
            current_node="node_2",
            node_events=[event],
        )
        assert progress.nodes_total == 5
        assert progress.nodes_completed == 1
        assert progress.current_node == "node_2"
        assert len(progress.node_events) == 1

    def test_node_events_default_factory_isolation(self):
        """Each instance gets its own list."""
        p1 = RunProgress()
        p2 = RunProgress()
        ts = datetime(2025, 1, 15, 10, 30, 0)
        p1.node_events.append(
            NodeEvent(node_id="x", node_name="x", status="running", timestamp=ts)
        )
        assert len(p2.node_events) == 0


class TestJob:
    """Tests for Job dataclass."""

    def test_minimal_creation(self):
        now = datetime(2025, 1, 15, 10, 0, 0)
        job = Job(
            job_id="abc-123",
            status=JobStatus.INITIALIZE,
            start_time=now,
        )
        assert job.job_id == "abc-123"
        assert job.status == JobStatus.INITIALIZE
        assert job.start_time == now
        assert job.config is None
        assert job.cmd == ""
        assert job.end_time is None
        assert job.duration is None
        assert job.returncode is None
        assert job.pid is None
        assert job.stdout == ""
        assert job.stderr == ""
        assert job.error_summary is None
        assert job.progress is None

    def test_full_creation(self):
        now = datetime(2025, 1, 15, 10, 0, 0)
        end = datetime(2025, 1, 15, 10, 5, 0)
        config = RunConfig(pipeline="de")
        progress = RunProgress(nodes_total=3, nodes_completed=3)

        job = Job(
            job_id="def-456",
            status=JobStatus.FINISHED,
            start_time=now,
            config=config,
            cmd="kedro run -p de",
            end_time=end,
            duration=300.0,
            returncode=0,
            pid=12345,
            stdout="output logs",
            stderr="",
            error_summary=None,
            progress=progress,
        )
        assert job.status == JobStatus.FINISHED
        assert job.config.pipeline == "de"
        assert job.duration == 300.0
        assert job.returncode == 0
        assert job.pid == 12345
        assert job.progress.nodes_total == 3


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_valid_result(self):
        result = ValidationResult(valid=True)
        assert result.valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_invalid_result(self):
        result = ValidationResult(
            valid=False,
            errors=["Pipeline 'foo' not found"],
            warnings=["Tags matched 0 nodes"],
        )
        assert result.valid is False
        assert len(result.errors) == 1
        assert len(result.warnings) == 1

    def test_lists_default_factory_isolation(self):
        """Each instance gets its own lists."""
        r1 = ValidationResult(valid=True)
        r2 = ValidationResult(valid=True)
        r1.errors.append("something")
        assert len(r2.errors) == 0


class TestJobToDict:
    """Tests for Job.to_dict() serialisation."""

    def test_minimal_job(self):
        now = datetime(2025, 1, 15, 10, 0, 0)
        job = Job(job_id="abc-123", status=JobStatus.INITIALIZE, start_time=now)
        result = job.to_dict()

        assert result["job_id"] == "abc-123"
        assert result["status"] == "initialize"
        assert result["start_time"] == "2025-01-15T10:00:00"
        assert result["cmd"] == ""
        assert result["end_time"] is None
        assert result["duration"] is None
        assert result["returncode"] is None
        assert result["pid"] is None
        assert result["error_summary"] is None
        assert result["config"] is None
        assert result["progress"] is None

    def test_excludes_stdout_stderr(self):
        now = datetime(2025, 1, 15, 10, 0, 0)
        job = Job(
            job_id="abc-123",
            status=JobStatus.RUNNING,
            start_time=now,
            stdout="some output",
            stderr="some error",
        )
        result = job.to_dict()
        assert "stdout" not in result
        assert "stderr" not in result

    def test_full_job_with_config_and_progress(self):
        start = datetime(2025, 1, 15, 10, 0, 0)
        end = datetime(2025, 1, 15, 10, 5, 0)
        ts = datetime(2025, 1, 15, 10, 1, 0)

        config = RunConfig(pipeline="de", env="local", tags=["train"])
        event = NodeEvent(
            node_id="n1",
            node_name="split_data",
            status="success",
            timestamp=ts,
            duration=3.5,
        )
        progress = RunProgress(
            nodes_total=5, nodes_completed=1, current_node="train", node_events=[event]
        )

        job = Job(
            job_id="def-456",
            status=JobStatus.FINISHED,
            start_time=start,
            config=config,
            cmd="kedro run -p de",
            end_time=end,
            duration=300.0,
            returncode=0,
            pid=12345,
            stdout="logs",
            stderr="",
            error_summary=None,
            progress=progress,
        )
        result = job.to_dict()

        assert result["status"] == "finished"
        assert result["start_time"] == "2025-01-15T10:00:00"
        assert result["end_time"] == "2025-01-15T10:05:00"
        assert result["duration"] == 300.0
        assert result["returncode"] == 0
        assert result["pid"] == 12345
        assert result["cmd"] == "kedro run -p de"

        # Config serialised as dict
        assert result["config"]["pipeline"] == "de"
        assert result["config"]["env"] == "local"
        assert result["config"]["tags"] == ["train"]

        # Progress serialised with ISO timestamps
        assert result["progress"]["nodes_total"] == 5
        assert result["progress"]["nodes_completed"] == 1
        assert result["progress"]["current_node"] == "train"
        assert result["progress"]["node_events"][0]["node_id"] == "n1"
        assert result["progress"]["node_events"][0]["timestamp"] == "2025-01-15T10:01:00"
        assert result["progress"]["node_events"][0]["duration"] == 3.5

    def test_status_enum_serialised_as_string(self):
        now = datetime(2025, 1, 15, 10, 0, 0)
        for status in JobStatus:
            job = Job(job_id="x", status=status, start_time=now)
            result = job.to_dict()
            assert result["status"] == status.value
            assert isinstance(result["status"], str)


class TestJobFromDict:
    """Tests for Job.from_dict() deserialisation."""

    def test_minimal_dict(self):
        data = {
            "job_id": "abc-123",
            "status": "initialize",
            "start_time": "2025-01-15T10:00:00",
        }
        job = Job.from_dict(data)
        assert job.job_id == "abc-123"
        assert job.status == JobStatus.INITIALIZE
        assert job.start_time == datetime(2025, 1, 15, 10, 0, 0)
        assert job.config is None
        assert job.progress is None
        assert job.cmd == ""

    def test_full_dict(self):
        data = {
            "job_id": "def-456",
            "status": "finished",
            "start_time": "2025-01-15T10:00:00",
            "end_time": "2025-01-15T10:05:00",
            "cmd": "kedro run -p de",
            "duration": 300.0,
            "returncode": 0,
            "pid": 12345,
            "error_summary": None,
            "config": {
                "pipeline": "de",
                "env": "local",
                "tags": ["train"],
                "params": None,
                "from_nodes": None,
                "to_nodes": None,
            },
            "progress": {
                "nodes_total": 5,
                "nodes_completed": 3,
                "current_node": "train_model",
                "node_events": [
                    {
                        "node_id": "n1",
                        "node_name": "split_data",
                        "status": "success",
                        "timestamp": "2025-01-15T10:01:00",
                        "duration": 3.5,
                        "error": None,
                    }
                ],
            },
        }
        job = Job.from_dict(data)

        assert job.status == JobStatus.FINISHED
        assert job.start_time == datetime(2025, 1, 15, 10, 0, 0)
        assert job.end_time == datetime(2025, 1, 15, 10, 5, 0)
        assert job.duration == 300.0
        assert job.returncode == 0
        assert job.pid == 12345
        assert job.cmd == "kedro run -p de"

        # RunConfig reconstructed
        assert job.config is not None
        assert job.config.pipeline == "de"
        assert job.config.env == "local"
        assert job.config.tags == ["train"]

        # RunProgress reconstructed
        assert job.progress is not None
        assert job.progress.nodes_total == 5
        assert job.progress.nodes_completed == 3
        assert job.progress.current_node == "train_model"
        assert len(job.progress.node_events) == 1
        assert job.progress.node_events[0].node_id == "n1"
        assert job.progress.node_events[0].timestamp == datetime(2025, 1, 15, 10, 1, 0)
        assert job.progress.node_events[0].duration == 3.5

    def test_from_dict_missing_optional_fields(self):
        """from_dict handles missing optional keys gracefully."""
        data = {
            "job_id": "x",
            "status": "running",
            "start_time": "2025-06-01T12:00:00",
        }
        job = Job.from_dict(data)
        assert job.end_time is None
        assert job.duration is None
        assert job.returncode is None
        assert job.pid is None
        assert job.error_summary is None
        assert job.config is None
        assert job.progress is None


class TestJobToJson:
    """Tests for Job.to_json() serialisation."""

    def test_produces_valid_json(self):
        now = datetime(2025, 1, 15, 10, 0, 0)
        job = Job(job_id="abc-123", status=JobStatus.RUNNING, start_time=now)
        json_str = job.to_json()

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed["job_id"] == "abc-123"
        assert parsed["status"] == "running"
        assert parsed["start_time"] == "2025-01-15T10:00:00"

    def test_json_excludes_stdout_stderr(self):
        now = datetime(2025, 1, 15, 10, 0, 0)
        job = Job(
            job_id="x",
            status=JobStatus.FINISHED,
            start_time=now,
            stdout="big output",
            stderr="errors",
        )
        json_str = job.to_json()
        parsed = json.loads(json_str)
        assert "stdout" not in parsed
        assert "stderr" not in parsed


class TestJobFromJson:
    """Tests for Job.from_json() deserialisation."""

    def test_from_json_minimal(self):
        json_str = json.dumps(
            {
                "job_id": "abc-123",
                "status": "error",
                "start_time": "2025-01-15T10:00:00",
            }
        )
        job = Job.from_json(json_str)
        assert job.job_id == "abc-123"
        assert job.status == JobStatus.ERROR
        assert job.start_time == datetime(2025, 1, 15, 10, 0, 0)


class TestJobSerialisationRoundTrip:
    """Tests that to_dict/from_dict and to_json/from_json are round-trip safe."""

    def test_dict_round_trip_minimal(self):
        now = datetime(2025, 1, 15, 10, 0, 0)
        original = Job(job_id="rt-1", status=JobStatus.INITIALIZE, start_time=now)
        restored = Job.from_dict(original.to_dict())

        assert restored.job_id == original.job_id
        assert restored.status == original.status
        assert restored.start_time == original.start_time

    def test_dict_round_trip_full(self):
        start = datetime(2025, 1, 15, 10, 0, 0)
        end = datetime(2025, 1, 15, 10, 5, 0)
        ts = datetime(2025, 1, 15, 10, 1, 30)

        config = RunConfig(
            pipeline="data_engineering",
            env="local",
            tags=["train", "evaluate"],
            params={"model.lr": 0.01},
            from_nodes=["split"],
            to_nodes=["train"],
        )
        events = [
            NodeEvent(
                node_id="n1",
                node_name="split_data",
                status="success",
                timestamp=ts,
                duration=2.5,
            ),
            NodeEvent(
                node_id="n2",
                node_name="train_model",
                status="failed",
                timestamp=ts,
                duration=10.0,
                error="OOM",
            ),
        ]
        progress = RunProgress(
            nodes_total=5,
            nodes_completed=2,
            current_node="evaluate",
            node_events=events,
        )

        original = Job(
            job_id="rt-2",
            status=JobStatus.ERROR,
            start_time=start,
            config=config,
            cmd="kedro run -p data_engineering",
            end_time=end,
            duration=300.0,
            returncode=1,
            pid=9999,
            stdout="should not persist",
            stderr="should not persist",
            error_summary="OOM in train_model",
            progress=progress,
        )

        restored = Job.from_dict(original.to_dict())

        assert restored.job_id == original.job_id
        assert restored.status == original.status
        assert restored.start_time == original.start_time
        assert restored.end_time == original.end_time
        assert restored.duration == original.duration
        assert restored.returncode == original.returncode
        assert restored.pid == original.pid
        assert restored.cmd == original.cmd
        assert restored.error_summary == original.error_summary

        # Config round-trip
        assert restored.config is not None
        assert restored.config.pipeline == original.config.pipeline
        assert restored.config.env == original.config.env
        assert restored.config.tags == original.config.tags
        assert restored.config.params == original.config.params
        assert restored.config.from_nodes == original.config.from_nodes
        assert restored.config.to_nodes == original.config.to_nodes

        # Progress round-trip
        assert restored.progress is not None
        assert restored.progress.nodes_total == original.progress.nodes_total
        assert restored.progress.nodes_completed == original.progress.nodes_completed
        assert restored.progress.current_node == original.progress.current_node
        assert len(restored.progress.node_events) == 2
        assert restored.progress.node_events[0].node_id == "n1"
        assert restored.progress.node_events[0].timestamp == ts
        assert restored.progress.node_events[1].error == "OOM"

        # stdout/stderr not round-tripped (by design)
        assert restored.stdout == ""
        assert restored.stderr == ""

    def test_json_round_trip(self):
        start = datetime(2025, 6, 1, 12, 0, 0)
        config = RunConfig(pipeline="ml", params={"lr": 0.1})
        original = Job(
            job_id="json-rt",
            status=JobStatus.FINISHED,
            start_time=start,
            config=config,
            cmd="kedro run -p ml",
            duration=45.0,
            returncode=0,
        )

        json_str = original.to_json()
        restored = Job.from_json(json_str)

        assert restored.job_id == original.job_id
        assert restored.status == original.status
        assert restored.start_time == original.start_time
        assert restored.config.pipeline == "ml"
        assert restored.config.params == {"lr": 0.1}
        assert restored.duration == 45.0
        assert restored.returncode == 0
