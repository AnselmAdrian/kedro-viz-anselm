"""Tests for the JobStore class."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta

import pytest

from kedro_viz.api.rest.runner.models import Job, JobStatus
from kedro_viz.api.rest.runner.store import JobStore


def _make_job(
    job_id: str = "job-1",
    status: JobStatus = JobStatus.INITIALIZE,
    start_time: datetime | None = None,
) -> Job:
    """Helper to create a Job with sensible defaults."""
    return Job(
        job_id=job_id,
        status=status,
        start_time=start_time or datetime.now(),
    )


class TestAddJob:
    def test_add_and_retrieve(self):
        store = JobStore()
        job = _make_job("abc")
        store.add_job(job)
        assert store.get_job("abc") is job

    def test_add_multiple_jobs(self):
        store = JobStore()
        job1 = _make_job("j1")
        job2 = _make_job("j2")
        store.add_job(job1)
        store.add_job(job2)
        assert store.get_job("j1") is job1
        assert store.get_job("j2") is job2


class TestUpdateJob:
    def test_update_status(self):
        store = JobStore()
        job = _make_job("j1", status=JobStatus.INITIALIZE)
        store.add_job(job)
        store.update_job("j1", status=JobStatus.RUNNING)
        assert store.get_job("j1").status == JobStatus.RUNNING

    def test_update_multiple_fields(self):
        store = JobStore()
        job = _make_job("j1")
        store.add_job(job)
        end = datetime.now()
        store.update_job("j1", status=JobStatus.FINISHED, end_time=end, returncode=0)
        updated = store.get_job("j1")
        assert updated.status == JobStatus.FINISHED
        assert updated.end_time == end
        assert updated.returncode == 0

    def test_update_nonexistent_raises_key_error(self):
        store = JobStore()
        with pytest.raises(KeyError, match="Job not found"):
            store.update_job("no-such-id", status=JobStatus.ERROR)

    def test_update_invalid_attribute_raises(self):
        store = JobStore()
        job = _make_job("j1")
        store.add_job(job)
        with pytest.raises(AttributeError, match="no attribute"):
            store.update_job("j1", nonexistent_field="value")


class TestGetJob:
    def test_get_existing_job(self):
        store = JobStore()
        job = _make_job("x")
        store.add_job(job)
        assert store.get_job("x") is job

    def test_get_missing_returns_none(self):
        store = JobStore()
        assert store.get_job("missing") is None


class TestGetActiveJob:
    def test_no_active_job(self):
        store = JobStore()
        store.add_job(_make_job("j1", status=JobStatus.FINISHED))
        store.add_job(_make_job("j2", status=JobStatus.ERROR))
        assert store.get_active_job() is None

    def test_initialize_is_active(self):
        store = JobStore()
        job = _make_job("j1", status=JobStatus.INITIALIZE)
        store.add_job(job)
        assert store.get_active_job() is job

    def test_running_is_active(self):
        store = JobStore()
        job = _make_job("j1", status=JobStatus.RUNNING)
        store.add_job(job)
        assert store.get_active_job() is job

    def test_terminated_is_not_active(self):
        store = JobStore()
        store.add_job(_make_job("j1", status=JobStatus.TERMINATED))
        assert store.get_active_job() is None

    def test_interrupted_is_not_active(self):
        store = JobStore()
        store.add_job(_make_job("j1", status=JobStatus.INTERRUPTED))
        assert store.get_active_job() is None

    def test_returns_first_active_among_many(self):
        store = JobStore()
        store.add_job(_make_job("j1", status=JobStatus.FINISHED))
        active = _make_job("j2", status=JobStatus.RUNNING)
        store.add_job(active)
        store.add_job(_make_job("j3", status=JobStatus.ERROR))
        assert store.get_active_job() is active


class TestGetHistory:
    def test_empty_store(self):
        store = JobStore()
        assert store.get_history() == []

    def test_sorted_by_start_time_descending(self):
        store = JobStore()
        now = datetime.now()
        j1 = _make_job("j1", start_time=now - timedelta(minutes=10))
        j2 = _make_job("j2", start_time=now - timedelta(minutes=5))
        j3 = _make_job("j3", start_time=now)
        # Add in non-chronological order
        store.add_job(j2)
        store.add_job(j1)
        store.add_job(j3)
        history = store.get_history()
        assert [j.job_id for j in history] == ["j3", "j2", "j1"]

    def test_limit_restricts_results(self):
        store = JobStore()
        now = datetime.now()
        for i in range(10):
            store.add_job(
                _make_job(f"j{i}", start_time=now - timedelta(minutes=i))
            )
        history = store.get_history(limit=3)
        assert len(history) == 3
        # Most recent first
        assert history[0].job_id == "j0"

    def test_limit_larger_than_store_returns_all(self):
        store = JobStore()
        store.add_job(_make_job("j1"))
        store.add_job(_make_job("j2"))
        assert len(store.get_history(limit=100)) == 2


class TestAppendLogs:
    def test_append_stdout(self):
        store = JobStore()
        job = _make_job("j1")
        store.add_job(job)
        store.append_logs("j1", stdout="line 1\n")
        store.append_logs("j1", stdout="line 2\n")
        assert store.get_job("j1").stdout == "line 1\nline 2\n"

    def test_append_stderr(self):
        store = JobStore()
        job = _make_job("j1")
        store.add_job(job)
        store.append_logs("j1", stderr="error 1\n")
        store.append_logs("j1", stderr="error 2\n")
        assert store.get_job("j1").stderr == "error 1\nerror 2\n"

    def test_append_both(self):
        store = JobStore()
        job = _make_job("j1")
        store.add_job(job)
        store.append_logs("j1", stdout="out\n", stderr="err\n")
        assert store.get_job("j1").stdout == "out\n"
        assert store.get_job("j1").stderr == "err\n"

    def test_append_to_nonexistent_job_is_noop(self):
        store = JobStore()
        # Should not raise
        store.append_logs("missing", stdout="data")

    def test_append_empty_strings_no_change(self):
        store = JobStore()
        job = _make_job("j1")
        store.add_job(job)
        store.append_logs("j1", stdout="", stderr="")
        assert store.get_job("j1").stdout == ""
        assert store.get_job("j1").stderr == ""


class TestThreadSafety:
    def test_concurrent_add_and_get(self):
        """Verify no exceptions under concurrent access."""
        store = JobStore()
        errors: list[Exception] = []

        def add_jobs(start_id: int):
            try:
                for i in range(50):
                    store.add_job(_make_job(f"t-{start_id}-{i}"))
            except Exception as e:
                errors.append(e)

        def read_jobs():
            try:
                for _ in range(100):
                    store.get_history()
                    store.get_active_job()
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=add_jobs, args=(0,)),
            threading.Thread(target=add_jobs, args=(1,)),
            threading.Thread(target=read_jobs),
            threading.Thread(target=read_jobs),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []

    def test_concurrent_append_logs(self):
        """Verify append_logs is safe under concurrent access."""
        store = JobStore()
        job = _make_job("j1")
        store.add_job(job)
        errors: list[Exception] = []

        def append_stdout():
            try:
                for i in range(100):
                    store.append_logs("j1", stdout=f"out-{i}\n")
            except Exception as e:
                errors.append(e)

        def append_stderr():
            try:
                for i in range(100):
                    store.append_logs("j1", stderr=f"err-{i}\n")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=append_stdout),
            threading.Thread(target=append_stdout),
            threading.Thread(target=append_stderr),
            threading.Thread(target=append_stderr),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        # Each thread appended 100 lines; two threads for each stream
        result = store.get_job("j1")
        assert result.stdout.count("\n") == 200
        assert result.stderr.count("\n") == 200


class TestHydrateFromDisk:
    def test_loads_jobs_from_disk(self, tmp_path):
        """Jobs persisted to disk should be loaded on store construction."""
        job_dir = tmp_path / "test-job-1"
        job_dir.mkdir()
        meta = {
            "job_id": "test-job-1",
            "status": "finished",
            "start_time": "2025-01-15T10:00:00",
            "cmd": "kedro run",
            "end_time": "2025-01-15T10:05:00",
            "duration": 300.0,
            "returncode": 0,
        }
        (job_dir / "meta.json").write_text(json.dumps(meta))

        store = JobStore(storage_dir=tmp_path)
        job = store.get_job("test-job-1")
        assert job is not None
        assert job.status == JobStatus.FINISHED

    def test_marks_running_jobs_as_interrupted(self, tmp_path):
        """Jobs with 'running' status should be marked 'interrupted' on hydration."""
        job_dir = tmp_path / "running-job"
        job_dir.mkdir()
        meta = {
            "job_id": "running-job",
            "status": "running",
            "start_time": "2025-01-15T10:00:00",
            "cmd": "kedro run",
        }
        (job_dir / "meta.json").write_text(json.dumps(meta))

        store = JobStore(storage_dir=tmp_path)
        job = store.get_job("running-job")
        assert job.status == JobStatus.INTERRUPTED

    def test_marks_initialize_jobs_as_interrupted(self, tmp_path):
        """Jobs with 'initialize' status should be marked 'interrupted'."""
        job_dir = tmp_path / "init-job"
        job_dir.mkdir()
        meta = {
            "job_id": "init-job",
            "status": "initialize",
            "start_time": "2025-01-15T10:00:00",
            "cmd": "kedro run",
        }
        (job_dir / "meta.json").write_text(json.dumps(meta))

        store = JobStore(storage_dir=tmp_path)
        job = store.get_job("init-job")
        assert job.status == JobStatus.INTERRUPTED

    def test_no_storage_dir_skips_hydration(self):
        """Without storage_dir, hydration is a no-op."""
        store = JobStore()  # No storage_dir
        assert store.get_history() == []

    def test_skips_invalid_meta_json(self, tmp_path):
        """Invalid meta.json files should be skipped with a warning."""
        job_dir = tmp_path / "bad-job"
        job_dir.mkdir()
        (job_dir / "meta.json").write_text("not valid json {{{")

        # Should not raise - invalid files are skipped
        store = JobStore(storage_dir=tmp_path)
        assert store.get_job("bad-job") is None
        assert store.get_history() == []


class TestJobCap:
    def test_add_jobs_beyond_max_oldest_removed(self, tmp_path):
        """When more jobs than max_jobs are added, the oldest is evicted."""
        store = JobStore(storage_dir=tmp_path, max_jobs=3)
        # Add 4 jobs with increasing start times
        for i in range(4):
            job = Job(
                job_id=f"j-{i}",
                status=JobStatus.FINISHED,
                start_time=datetime(2025, 1, 1, i, 0, 0),
                cmd="kedro run",
            )
            store.add_job(job)
        # Oldest (j-0) should be removed from memory
        assert store.get_job("j-0") is None
        # Others still exist
        assert store.get_job("j-1") is not None
        assert store.get_job("j-2") is not None
        assert store.get_job("j-3") is not None

    def test_evicted_job_disk_directory_deleted(self, tmp_path):
        """Evicted job's directory on disk should be removed."""
        store = JobStore(storage_dir=tmp_path, max_jobs=2)
        j1 = Job(
            job_id="old-job",
            status=JobStatus.FINISHED,
            start_time=datetime(2025, 1, 1, 0, 0, 0),
            cmd="kedro run",
        )
        j2 = Job(
            job_id="mid-job",
            status=JobStatus.FINISHED,
            start_time=datetime(2025, 1, 1, 1, 0, 0),
            cmd="kedro run",
        )
        j3 = Job(
            job_id="new-job",
            status=JobStatus.FINISHED,
            start_time=datetime(2025, 1, 1, 2, 0, 0),
            cmd="kedro run",
        )
        store.add_job(j1)
        store.add_job(j2)
        store.add_job(j3)
        # old-job's directory should be deleted from disk
        assert not (tmp_path / "old-job").exists()
        # Others still on disk
        assert (tmp_path / "mid-job").exists()
        assert (tmp_path / "new-job").exists()

    def test_cap_default_is_50(self):
        """Default max_jobs should be 50."""
        store = JobStore()
        assert store._max_jobs == 50


class TestLogCapping:
    def test_stdout_capped_at_1mb(self, tmp_path):
        """In-memory stdout is truncated when it exceeds 1MB."""
        store = JobStore(storage_dir=tmp_path)
        job = Job(
            job_id="cap-test",
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
            cmd="kedro run",
        )
        store.add_job(job)
        # Append 2MB of data
        big_data = "x" * (2 * 1024 * 1024)
        store.append_logs("cap-test", stdout=big_data)
        # In-memory should be capped to ~500KB
        in_memory_job = store.get_job("cap-test")
        assert len(in_memory_job.stdout) == 524_288
        # Disk should have full content
        stdout, _ = store.get_full_logs("cap-test")
        assert len(stdout) == 2 * 1024 * 1024

    def test_stderr_capped_at_1mb(self, tmp_path):
        """In-memory stderr is truncated when it exceeds 1MB."""
        store = JobStore(storage_dir=tmp_path)
        job = Job(
            job_id="cap-test-err",
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
            cmd="kedro run",
        )
        store.add_job(job)
        big_data = "e" * (2 * 1024 * 1024)
        store.append_logs("cap-test-err", stderr=big_data)
        in_memory_job = store.get_job("cap-test-err")
        assert len(in_memory_job.stderr) == 524_288
        _, stderr = store.get_full_logs("cap-test-err")
        assert len(stderr) == 2 * 1024 * 1024

    def test_keeps_tail_on_truncation(self, tmp_path):
        """Truncation keeps the tail (most recent) content."""
        store = JobStore(storage_dir=tmp_path)
        job = Job(
            job_id="tail-test",
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
            cmd="kedro run",
        )
        store.add_job(job)
        # Write data where we can identify the tail
        prefix = "A" * (1024 * 1024)  # 1MB of A's
        suffix = "B" * (524_288)  # 500KB of B's
        store.append_logs("tail-test", stdout=prefix + suffix)
        in_memory_job = store.get_job("tail-test")
        # The in-memory version should be the tail (all B's)
        assert in_memory_job.stdout == suffix

    def test_no_cap_under_1mb(self, tmp_path):
        """Logs under 1MB should not be truncated."""
        store = JobStore(storage_dir=tmp_path)
        job = Job(
            job_id="small-test",
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
            cmd="kedro run",
        )
        store.add_job(job)
        small_data = "x" * (512 * 1024)  # 512KB
        store.append_logs("small-test", stdout=small_data)
        assert len(store.get_job("small-test").stdout) == 512 * 1024


class TestGetFullLogs:
    def test_reads_from_disk(self, tmp_path):
        """get_full_logs reads log files from disk."""
        store = JobStore(storage_dir=tmp_path)
        job = Job(
            job_id="log-test",
            status=JobStatus.FINISHED,
            start_time=datetime.now(),
            cmd="kedro run",
        )
        store.add_job(job)
        store.append_logs("log-test", stdout="hello\n", stderr="world\n")
        stdout, stderr = store.get_full_logs("log-test")
        assert stdout == "hello\n"
        assert stderr == "world\n"

    def test_fallback_to_memory_without_storage_dir(self):
        """Without storage_dir, get_full_logs returns in-memory logs."""
        store = JobStore()
        job = Job(
            job_id="mem-test",
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
            cmd="kedro run",
        )
        store.add_job(job)
        store.append_logs("mem-test", stdout="out", stderr="err")
        stdout, stderr = store.get_full_logs("mem-test")
        assert stdout == "out"
        assert stderr == "err"

    def test_missing_job_returns_empty(self, tmp_path):
        """get_full_logs for non-existent job returns empty strings."""
        store = JobStore(storage_dir=tmp_path)
        stdout, stderr = store.get_full_logs("nonexistent")
        assert stdout == ""
        assert stderr == ""


class TestReadFromDisk:
    def test_get_job_loads_from_disk_when_not_in_memory(self, tmp_path):
        """get_job falls back to disk when job is not in memory."""
        # Create a job on disk manually
        job_dir = tmp_path / "disk-job"
        job_dir.mkdir()
        meta = {
            "job_id": "disk-job",
            "status": "finished",
            "start_time": "2025-01-15T10:00:00",
            "cmd": "kedro run",
            "end_time": "2025-01-15T10:05:00",
            "duration": 300.0,
            "returncode": 0,
        }
        (job_dir / "meta.json").write_text(json.dumps(meta))

        store = JobStore(storage_dir=tmp_path)
        # Remove from memory to simulate eviction
        with store._lock:
            if "disk-job" in store._jobs:
                del store._jobs["disk-job"]

        # Should still find it via disk
        job = store.get_job("disk-job")
        assert job is not None
        assert job.job_id == "disk-job"
        assert job.status == JobStatus.FINISHED

    def test_evicted_job_status_endpoint_returns_logs(self, tmp_path):
        """After eviction, the status endpoint can still get full logs from disk."""
        store = JobStore(storage_dir=tmp_path, max_jobs=2)

        # Add 3 jobs - first will be evicted
        j1 = Job(
            job_id="evict-me",
            status=JobStatus.FINISHED,
            start_time=datetime(2025, 1, 1, 0, 0, 0),
            cmd="kedro run",
        )
        store.add_job(j1)
        store.append_logs("evict-me", stdout="j1-output\n", stderr="j1-error\n")

        j2 = Job(
            job_id="keep-1",
            status=JobStatus.FINISHED,
            start_time=datetime(2025, 1, 1, 1, 0, 0),
            cmd="kedro run",
        )
        store.add_job(j2)
        j3 = Job(
            job_id="keep-2",
            status=JobStatus.FINISHED,
            start_time=datetime(2025, 1, 1, 2, 0, 0),
            cmd="kedro run",
        )
        store.add_job(j3)

        # j1 evicted from memory but disk directory still removed by cap enforcement
        # So for this test, we use max_jobs=3 and manually evict
        store2 = JobStore(storage_dir=tmp_path, max_jobs=50)
        # Add job and logs, then manually evict from memory
        j_manual = Job(
            job_id="manual-evict",
            status=JobStatus.FINISHED,
            start_time=datetime(2025, 6, 1, 0, 0, 0),
            cmd="kedro run",
        )
        store2.add_job(j_manual)
        store2.append_logs("manual-evict", stdout="full-output\n", stderr="full-error\n")

        # Manually evict from memory
        with store2._lock:
            del store2._jobs["manual-evict"]

        # get_job should load from disk
        job = store2.get_job("manual-evict")
        assert job is not None
        assert job.status == JobStatus.FINISHED

        # get_full_logs should read from disk files
        stdout, stderr = store2.get_full_logs("manual-evict")
        assert stdout == "full-output\n"
        assert stderr == "full-error\n"
