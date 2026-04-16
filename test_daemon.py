import importlib.machinery
import importlib.util
import hashlib
import json
import os
import re
import time
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

SCRIPT_DIR = Path(__file__).resolve().parent


def load_script_module(name: str, path: Path):
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def daemon():
    return load_script_module("codex_s3_archive_daemon", SCRIPT_DIR / "codex-s3-archive-daemon")


@pytest.fixture(scope="module")
def hook():
    return load_script_module("codex_s3_archive_hook_stop", SCRIPT_DIR / "codex-s3-archive-hook-stop")


def make_config(state_root: Path, **overrides):
    config = {
        "bucket": "mach-zero-codex",
        "prefix": "raw",
        "user_id": "tanl",
        "machine_id": "m3",
        "state_root": str(state_root),
        "max_raw_chunk_bytes": 1048576,
    }
    config.update(overrides)
    return config


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_credentials(state_root: Path, access_key="AKIA_TEST", secret_key="secret-test"):
    creds_path = state_root / "credentials.json"
    creds_path.write_text(
        json.dumps(
            {
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key,
            }
        ),
        encoding="utf-8",
    )
    return creds_path


def enqueue_job(hook, state_root: Path, transcript: Path, *, session_id="session-1", turn_id="turn-1", created_at="2026-04-16T00:00:00.000Z"):
    return hook.enqueue_job(
        state_root,
        {
            "session_id": session_id,
            "turn_id": turn_id,
            "transcript_path": str(transcript),
            "cwd": "/tmp/project",
            "model": "gpt-5",
            "hook_event_name": "Stop",
        },
        created_at=created_at,
    )


def set_old_mtime(path: Path, age_seconds: int):
    old_time = time.time() - age_seconds
    os.utime(path, (old_time, old_time))


def make_client_error(status_code: int, code: str | None = None, message="boom"):
    return ClientError(
        {
            "Error": {
                "Code": code or str(status_code),
                "Message": message,
            },
            "ResponseMetadata": {
                "HTTPStatusCode": status_code,
            },
        },
        "PutObject",
    )


def test_create_s3_client_supports_aws_and_r2_configs(tmp_path, daemon, monkeypatch):
    sessions = []

    class FakeSession:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.client_calls = []

        def client(self, service_name, **kwargs):
            self.client_calls.append((service_name, kwargs))
            return {
                "service_name": service_name,
                "session_kwargs": self.kwargs,
                "client_kwargs": kwargs,
            }

    def fake_session(**kwargs):
        session = FakeSession(**kwargs)
        sessions.append(session)
        return session

    monkeypatch.setattr(daemon.boto3, "Session", fake_session)
    write_credentials(tmp_path)

    aws_client = daemon.create_s3_client(
        make_config(tmp_path, region="us-west-2"),
        tmp_path,
    )
    r2_client = daemon.create_s3_client(
        make_config(
            tmp_path,
            endpoint_url="https://acct.r2.cloudflarestorage.com",
            region_name="auto",
        ),
        tmp_path,
    )

    assert sessions[0].kwargs == {
        "aws_access_key_id": "AKIA_TEST",
        "aws_secret_access_key": "secret-test",
        "region_name": "us-west-2",
    }
    assert sessions[0].client_calls == [("s3", {})]
    assert aws_client["client_kwargs"] == {}

    assert sessions[1].kwargs == {
        "aws_access_key_id": "AKIA_TEST",
        "aws_secret_access_key": "secret-test",
        "region_name": "auto",
    }
    assert sessions[1].client_calls == [
        ("s3", {"endpoint_url": "https://acct.r2.cloudflarestorage.com"})
    ]
    assert r2_client["client_kwargs"] == {"endpoint_url": "https://acct.r2.cloudflarestorage.com"}


def test_create_s3_client_missing_credentials_raises_keyerror(tmp_path, daemon):
    with pytest.raises(KeyError):
        daemon.create_s3_client(make_config(tmp_path, region="us-west-2"), tmp_path)


def test_hook_build_job_initializes_retry_count(tmp_path, hook):
    job = hook.build_job(
        {
            "session_id": "session-1",
            "turn_id": "turn-1",
            "transcript_path": str(tmp_path / "transcript.jsonl"),
        },
        created_at="2026-04-16T00:00:00.000Z",
    )

    assert job["retry_count"] == 0


def test_hook_build_job_rejects_missing_transcript_path(hook):
    with pytest.raises(ValueError, match="transcript_path missing"):
        hook.build_job(
            {
                "session_id": "session-1",
                "turn_id": "turn-1",
                "transcript_path": None,
            },
            created_at="2026-04-16T00:00:00.000Z",
        )


def test_hook_main_logs_and_skips_enqueue_when_transcript_path_missing(tmp_path, hook):
    state_root = tmp_path / "state"
    result = hook.main(
        ["--state-root", str(state_root)],
        stdin_text=json.dumps(
            {
                "session_id": "session-1",
                "turn_id": "turn-1",
                "transcript_path": None,
            }
        ),
    )

    assert result == 0
    assert list((state_root / "queue").glob("*.json")) == []
    assert "transcript_path missing" in (state_root / "logs" / "hook-stop.log").read_text(encoding="utf-8")


def test_read_incremental_bytes_from_offset_and_checkpoint_advances(tmp_path, daemon, hook):
    transcript = tmp_path / "transcript.jsonl"
    initial_bytes = b'{"a":1}\n{"b":2}\n'
    transcript.write_bytes(initial_bytes)

    raw_bytes, byte_end = daemon.read_incremental_bytes(transcript, start_offset=0)
    assert raw_bytes == initial_bytes
    assert byte_end == len(initial_bytes)

    skip_source = tmp_path / "skip-source.jsonl"
    tail = b'{"tail":1}\n'
    skip_source.write_bytes((b"x" * 500) + tail)
    skipped_bytes, skipped_end = daemon.read_incremental_bytes(skip_source, start_offset=500)
    assert skipped_bytes == tail
    assert skipped_end == 500 + len(tail)

    config = make_config(tmp_path)
    enqueue_job(hook, tmp_path, transcript, created_at="2026-04-16T00:00:00.000Z")
    result1 = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
    )
    cp_path = daemon.checkpoint_path(tmp_path, "session-1", str(transcript))
    assert result1["status"] == "uploaded"
    assert read_json(cp_path)["last_uploaded_byte_offset"] == len(initial_bytes)

    appended = b'{"c":3}\n'
    transcript.write_bytes(initial_bytes + appended)
    enqueue_job(
        hook,
        tmp_path,
        transcript,
        turn_id="turn-2",
        created_at="2026-04-16T00:00:02.000Z",
    )
    result2 = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:03.000Z",
    )
    cp_after = read_json(cp_path)
    assert result2["status"] == "uploaded"
    assert f"/{len(initial_bytes)}-{len(initial_bytes) + len(appended)}.jsonl" in result2["s3_key"]
    assert cp_after["last_uploaded_byte_offset"] == len(initial_bytes) + len(appended)


def test_read_incremental_bytes_respects_max_chunk_and_preserves_remainder(tmp_path, daemon):
    transcript = tmp_path / "large.jsonl"
    data = b"".join(
        '{{"line":{},"pad":"{}"}}\n'.format(i, "x" * 64).encode("utf-8")
        for i in range(200)
    )
    transcript.write_bytes(data)

    first_chunk, first_end = daemon.read_incremental_bytes(transcript, start_offset=0, max_bytes=4096)
    assert 0 < len(first_chunk) <= 4096 + 8192
    assert first_chunk.endswith(b"\n")

    collected = [first_chunk]
    offset = first_end
    while offset < len(data):
        chunk, next_offset = daemon.read_incremental_bytes(transcript, start_offset=offset, max_bytes=4096)
        assert chunk
        assert next_offset > offset
        collected.append(chunk)
        offset = next_offset

    assert b"".join(collected) == data


@pytest.mark.parametrize(
    ("content", "expected", "expected_end"),
    [
        (b'{"a":1}\n{"b":2', b'{"a":1}\n', len(b'{"a":1}\n')),
        (b'{"a":1}\n', b'{"a":1}\n', len(b'{"a":1}\n')),
        (b'{"a":1', b"", 0),
    ],
)
def test_read_incremental_bytes_handles_partial_tail_lines(tmp_path, daemon, content, expected, expected_end):
    transcript = tmp_path / "partial.jsonl"
    transcript.write_bytes(content)

    raw_bytes, byte_end = daemon.read_incremental_bytes(transcript, start_offset=0)
    assert raw_bytes == expected
    assert byte_end == expected_end


def test_checkpoint_is_scoped_to_transcript_path(tmp_path, daemon, hook):
    transcript_one = tmp_path / "one.jsonl"
    transcript_two = tmp_path / "two.jsonl"
    transcript_one.write_bytes(b'{"path":1}\n')
    transcript_two.write_bytes(b'{"path":2}\n')
    config = make_config(tmp_path)

    enqueue_job(
        hook,
        tmp_path,
        transcript_one,
        session_id="shared-session",
        turn_id="turn-1",
        created_at="2026-04-16T00:00:00.000Z",
    )
    daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
    )
    cp_one_path = daemon.checkpoint_path(tmp_path, "shared-session", str(transcript_one))
    cp_one = read_json(cp_one_path)

    enqueue_job(
        hook,
        tmp_path,
        transcript_two,
        session_id="shared-session",
        turn_id="turn-2",
        created_at="2026-04-16T00:00:02.000Z",
    )
    daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:03.000Z",
    )
    cp_two_path = daemon.checkpoint_path(tmp_path, "shared-session", str(transcript_two))

    assert cp_one_path != cp_two_path
    assert cp_one_path.exists()
    assert cp_two_path.exists()
    assert read_json(cp_one_path) == cp_one
    assert read_json(cp_two_path)["last_uploaded_byte_offset"] == len(transcript_two.read_bytes())


def test_truncated_transcript_resets_checkpoint_and_keeps_job(tmp_path, daemon, hook, caplog):
    transcript = tmp_path / "truncate.jsonl"
    transcript.write_bytes(b'{"a":1}\n{"b":2}\n')
    config = make_config(tmp_path)

    enqueue_job(hook, tmp_path, transcript, created_at="2026-04-16T00:00:00.000Z")
    daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
    )

    transcript.write_bytes(b'{"a":1}\n')
    job_path = enqueue_job(
        hook,
        tmp_path,
        transcript,
        turn_id="turn-2",
        created_at="2026-04-16T00:00:02.000Z",
    )
    caplog.set_level("WARNING")
    result = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:03.000Z",
    )
    cp_path = daemon.checkpoint_path(tmp_path, "session-1", str(transcript))

    assert result["status"] == "truncated_reset"
    assert "archive_truncated" in caplog.text
    assert read_json(cp_path)["last_uploaded_byte_offset"] == 0
    assert job_path.exists()


def test_sweep_orphaned_staging_respects_ttl_and_ready_files(tmp_path, daemon, caplog):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    valid = staging_dir / "valid"
    daemon.write_bytes_atomic(valid.with_suffix(".jsonl"), b'{"ok":1}\n')
    daemon.write_json_atomic(valid.with_suffix(".meta.json"), {"byte_end": 10})
    daemon.write_bytes_atomic(valid.with_suffix(".ready"), b"")
    for path in (valid.with_suffix(".jsonl"), valid.with_suffix(".meta.json"), valid.with_suffix(".ready")):
        set_old_mtime(path, 1200)

    stale = staging_dir / "stale"
    daemon.write_bytes_atomic(stale.with_suffix(".jsonl"), b'{"stale":1}\n')
    daemon.write_json_atomic(stale.with_suffix(".meta.json"), {"byte_end": 12})
    for path in (stale.with_suffix(".jsonl"), stale.with_suffix(".meta.json")):
        set_old_mtime(path, 1200)

    broken_ready = staging_dir / "broken-ready"
    daemon.write_json_atomic(broken_ready.with_suffix(".meta.json"), {"byte_end": 9})
    daemon.write_bytes_atomic(broken_ready.with_suffix(".ready"), b"")
    for path in (broken_ready.with_suffix(".meta.json"), broken_ready.with_suffix(".ready")):
        set_old_mtime(path, 1200)

    young = staging_dir / "young"
    daemon.write_bytes_atomic(young.with_suffix(".jsonl"), b'{"young":1}\n')
    daemon.write_json_atomic(young.with_suffix(".meta.json"), {"byte_end": 12})

    caplog.set_level("WARNING")
    daemon.sweep_orphaned_staging(staging_dir, ttl_seconds=600)

    assert valid.with_suffix(".jsonl").exists()
    assert valid.with_suffix(".meta.json").exists()
    assert valid.with_suffix(".ready").exists()
    assert not stale.with_suffix(".jsonl").exists()
    assert not stale.with_suffix(".meta.json").exists()
    assert not broken_ready.with_suffix(".meta.json").exists()
    assert not broken_ready.with_suffix(".ready").exists()
    assert young.with_suffix(".jsonl").exists()
    assert young.with_suffix(".meta.json").exists()
    assert caplog.text.count("archive_orphaned_staging") == 2


def test_run_daemon_calls_startup_sweep_once(tmp_path, daemon, monkeypatch):
    calls = []
    fake_s3 = object()
    fake_jitter = object()

    def fake_sweep(staging_dir, ttl_seconds=600):
        calls.append((staging_dir, ttl_seconds))

    def fake_process_next_job(state_root, config, *, sleep_fn, now_fn, s3=None, jitter_fn=None):
        assert s3 is fake_s3
        assert jitter_fn is fake_jitter
        return {"status": "noop", "loop_sleep": None}

    monkeypatch.setattr(daemon, "sweep_orphaned_staging", fake_sweep)
    monkeypatch.setattr(daemon, "create_s3_client", lambda config, state_root: fake_s3)
    monkeypatch.setattr(daemon, "process_next_job", fake_process_next_job)

    result = daemon.run_daemon(
        tmp_path,
        make_config(tmp_path),
        once=True,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:00.000Z",
        jitter_fn=fake_jitter,
    )

    assert result["status"] == "noop"
    assert calls == [(tmp_path / "staging", daemon.ORPHAN_TTL_SECONDS)]


def test_empty_read_after_prior_success_deletes_job_without_stall(tmp_path, daemon, hook):
    transcript = tmp_path / "partial-only.jsonl"
    transcript.write_bytes(b'{"dangling":1')
    config = make_config(tmp_path)
    job_path = enqueue_job(
        hook,
        tmp_path,
        transcript,
        created_at="2026-04-16T00:00:01.000Z",
    )
    cp_path = daemon.checkpoint_path(tmp_path, "session-1", str(transcript))
    daemon.write_json_atomic(
        cp_path,
        {
            "session_id": "session-1",
            "transcript_path": str(transcript),
            "last_uploaded_byte_offset": 0,
            "last_success_at": "2026-04-16T00:00:02.000Z",
        },
    )
    sleep_calls = []

    result = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=sleep_calls.append,
        now_fn=lambda: "2026-04-16T00:00:03.000Z",
    )

    assert result["status"] == "already_consumed"
    assert sleep_calls == []
    assert not job_path.exists()
    assert not (tmp_path / "queue" / "dead" / job_path.name).exists()


def test_process_next_job_dead_letters_invalid_job_without_crashing(tmp_path, daemon, caplog):
    config = make_config(tmp_path)
    job_path = tmp_path / "queue" / "bad.json"
    daemon.write_json_atomic(
        job_path,
        {
            "job_type": "codex_stop_archive",
            "created_at": "2026-04-16T00:00:00.000Z",
            "session_id": "session-1",
            "turn_id": "turn-1",
            "transcript_path": None,
        },
    )

    caplog.set_level("WARNING")
    result = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
    )

    dead_path = tmp_path / "queue" / "dead" / job_path.name
    heartbeat = read_json(tmp_path / "heartbeat.json")
    assert result["status"] == "invalid_job"
    assert dead_path.exists()
    assert not job_path.exists()
    assert heartbeat["last_error"] == "archive_invalid_job: transcript_path missing"
    assert "archive_invalid_job" in caplog.text


def test_stall_count_increments_resets_on_success_and_dead_letters_after_max(tmp_path, daemon, hook, monkeypatch):
    transcript = tmp_path / "stall.jsonl"
    transcript.write_bytes(b'{"a":1')
    config = make_config(tmp_path)
    job_path = enqueue_job(
        hook,
        tmp_path,
        transcript,
        created_at="2026-04-16T00:00:00.000Z",
    )

    sleep_calls = []
    first = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=sleep_calls.append,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
    )
    assert first["status"] == "stalled"
    assert first["stall_count"] == 1
    assert sleep_calls == [daemon.STALL_BACKOFF_BASE]
    assert read_json(job_path)["_stall_count"] == 1

    captured_job_writes = []
    original_write_json_atomic = daemon.write_json_atomic

    def tracking_write_json_atomic(path, obj):
        if path == job_path:
            captured_job_writes.append(dict(obj))
        original_write_json_atomic(path, obj)

    monkeypatch.setattr(daemon, "write_json_atomic", tracking_write_json_atomic)
    transcript.write_bytes(b'{"a":1}\n')
    second = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:02.000Z",
    )

    assert second["status"] == "uploaded"
    assert any(obj.get("_stall_count") == 0 for obj in captured_job_writes)
    assert not job_path.exists()

    transcript_dead = tmp_path / "dead-letter.jsonl"
    transcript_dead.write_bytes(b'{"b":2')
    dead_job_path = enqueue_job(
        hook,
        tmp_path,
        transcript_dead,
        session_id="session-dead",
        turn_id="turn-dead",
        created_at="2026-04-16T00:01:00.000Z",
    )
    dead_sleep_calls = []

    for attempt in range(1, daemon.MAX_STALL_COUNT + 1):
        result = daemon.process_next_job(
            tmp_path,
            config,
            sleep_fn=dead_sleep_calls.append,
            now_fn=lambda: "2026-04-16T00:01:30.000Z",
        )
        if attempt < daemon.MAX_STALL_COUNT:
            assert result["status"] == "stalled"
            assert read_json(dead_job_path)["_stall_count"] == attempt
        else:
            assert result["status"] == "dead_letter"

    assert dead_sleep_calls == [10, 20, 30, 40]
    assert not dead_job_path.exists()
    assert (tmp_path / "queue" / "dead" / dead_job_path.name).exists()


def test_process_next_job_uploads_to_s3_and_cleans_staging(tmp_path, daemon, hook):
    transcript = tmp_path / "upload.jsonl"
    transcript.write_bytes(b'{"alpha":1}\n{"beta":2}\n')
    config = make_config(tmp_path)
    fake_s3 = Mock()

    enqueue_job(hook, tmp_path, transcript, created_at="2026-04-16T00:00:00.000Z")
    result = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
        s3=fake_s3,
    )

    assert result["status"] == "uploaded"
    cp_path = daemon.checkpoint_path(tmp_path, "session-1", str(transcript))
    assert read_json(cp_path)["last_uploaded_byte_offset"] == len(transcript.read_bytes())
    assert list((tmp_path / "staging").iterdir()) == []

    kwargs = fake_s3.put_object.call_args.kwargs
    expected_hash = hashlib.sha256(transcript.read_bytes()).hexdigest()
    assert kwargs["Bucket"] == "mach-zero-codex"
    assert kwargs["Key"] == result["s3_key"]
    assert kwargs["Body"] == transcript.read_bytes()
    assert kwargs["Metadata"] == {
        "triggering-turn-id": "turn-1",
        "machine-id": "m3",
        "raw-sha256": expected_hash,
    }


def test_idle_heartbeat_preserves_last_success_after_upload(tmp_path, daemon, hook):
    transcript = tmp_path / "idle-heartbeat.jsonl"
    transcript.write_bytes(b'{"alpha":1}\n')
    config = make_config(tmp_path)
    fake_s3 = Mock()

    enqueue_job(hook, tmp_path, transcript, created_at="2026-04-16T00:00:00.000Z")
    uploaded = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
        s3=fake_s3,
    )

    idle = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:02.000Z",
        s3=fake_s3,
    )

    heartbeat = read_json(tmp_path / "heartbeat.json")
    assert uploaded["status"] == "uploaded"
    assert idle["status"] == "idle"
    assert heartbeat == {
        "last_cycle_at": "2026-04-16T00:00:02.000Z",
        "queue_len": 0,
        "last_success_s3_key": uploaded["s3_key"],
        "last_error": None,
    }


def test_process_next_job_passes_jitter_fn_to_upload(tmp_path, daemon, hook, monkeypatch):
    transcript = tmp_path / "jitter.jsonl"
    transcript.write_bytes(b'{"alpha":1}\n')
    config = make_config(tmp_path)
    fake_jitter = object()
    captured = {}

    def fake_put_chunk_to_s3(raw_bytes, job, start, end, partition_date, config, tp_hash, s3=None, *, sleep_fn, jitter_fn):
        captured["jitter_fn"] = jitter_fn
        return "raw/2026-04-16/tanl/session-1/deadbeef/0-12.jsonl"

    monkeypatch.setattr(daemon, "put_chunk_to_s3", fake_put_chunk_to_s3)
    enqueue_job(hook, tmp_path, transcript, created_at="2026-04-16T00:00:00.000Z")

    result = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=lambda _: None,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
        jitter_fn=fake_jitter,
        s3=object(),
    )

    assert result["status"] == "uploaded"
    assert captured["jitter_fn"] is fake_jitter


def test_put_chunk_to_s3_retries_retryable_server_errors(tmp_path, daemon, monkeypatch):
    job = {
        "session_id": "session-1",
        "turn_id": "turn-1",
    }
    config = make_config(tmp_path)
    fake_s3 = Mock()
    fake_s3.put_object.side_effect = [
        make_client_error(429),
        make_client_error(503),
        make_client_error(500),
        {},
    ]
    sleep_calls = []
    monkeypatch.setattr(daemon.random, "uniform", lambda a, b: 0.0)

    s3_key = daemon.put_chunk_to_s3(
        b'{"a":1}\n',
        job,
        0,
        8,
        "2026-04-16",
        config,
        "123456789abc",
        s3=fake_s3,
        sleep_fn=sleep_calls.append,
    )

    assert fake_s3.put_object.call_count == 4
    assert sleep_calls == [2.0, 4.0, 8.0]
    assert s3_key.endswith("/12345678/0-8.jsonl")


def test_process_next_job_dead_letters_after_exhausting_server_retries(tmp_path, daemon, hook, monkeypatch, caplog):
    transcript = tmp_path / "server-error.jsonl"
    transcript.write_bytes(b'{"alpha":1}\n')
    config = make_config(tmp_path)
    fake_s3 = Mock()
    fake_s3.put_object.side_effect = [make_client_error(429) for _ in range(daemon.MAX_S3_PUT_ATTEMPTS)]
    sleep_calls = []
    monkeypatch.setattr(daemon.random, "uniform", lambda a, b: 0.0)
    caplog.set_level("ERROR")

    job_path = enqueue_job(hook, tmp_path, transcript, created_at="2026-04-16T00:00:00.000Z")
    result = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=sleep_calls.append,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
        s3=fake_s3,
    )

    dead_path = tmp_path / "queue" / "dead" / job_path.name
    assert result["status"] == "upload_failed"
    assert dead_path.exists()
    assert not job_path.exists()
    assert sleep_calls == [2.0, 4.0, 8.0, 16.0, 32.0]
    assert any(path.name.endswith(".ready") for path in (tmp_path / "staging").iterdir())
    assert "archive_upload_failed" in caplog.text


def test_put_chunk_to_s3_retries_network_errors_without_consuming_server_budget(tmp_path, daemon, monkeypatch):
    job = {
        "session_id": "session-1",
        "turn_id": "turn-1",
    }
    config = make_config(tmp_path)
    fake_s3 = Mock()
    fake_s3.put_object.side_effect = [
        EndpointConnectionError(endpoint_url="https://example.invalid"),
        EndpointConnectionError(endpoint_url="https://example.invalid"),
        EndpointConnectionError(endpoint_url="https://example.invalid"),
        make_client_error(500),
        make_client_error(500),
        make_client_error(500),
        make_client_error(500),
        make_client_error(500),
        {},
    ]
    sleep_calls = []
    monkeypatch.setattr(daemon.random, "uniform", lambda a, b: 0.0)

    s3_key = daemon.put_chunk_to_s3(
        b'{"a":1}\n',
        job,
        0,
        8,
        "2026-04-16",
        config,
        "123456789abc",
        s3=fake_s3,
        sleep_fn=sleep_calls.append,
    )

    assert fake_s3.put_object.call_count == 9
    assert sleep_calls == [2.0, 4.0, 8.0, 2.0, 4.0, 8.0, 16.0, 32.0]
    assert s3_key.endswith("/12345678/0-8.jsonl")


def test_put_chunk_to_s3_caps_network_backoff(tmp_path, daemon, monkeypatch):
    job = {
        "session_id": "session-1",
        "turn_id": "turn-1",
    }
    config = make_config(tmp_path)
    fake_s3 = Mock()
    fake_s3.put_object.side_effect = [
        EndpointConnectionError(endpoint_url="https://example.invalid")
        for _ in range(9)
    ] + [{}]
    sleep_calls = []
    monkeypatch.setattr(daemon.random, "uniform", lambda a, b: 0.0)

    daemon.put_chunk_to_s3(
        b'{"a":1}\n',
        job,
        0,
        8,
        "2026-04-16",
        config,
        "123456789abc",
        s3=fake_s3,
        sleep_fn=sleep_calls.append,
    )

    assert sleep_calls[-1] == float(daemon.S3_NETWORK_RETRY_CAP_SECS)


def test_process_next_job_dead_letters_on_non_retryable_403_and_preserves_staging(tmp_path, daemon, hook, monkeypatch, caplog):
    transcript = tmp_path / "forbidden.jsonl"
    transcript.write_bytes(b'{"alpha":1}\n')
    config = make_config(tmp_path)
    fake_s3 = Mock()
    fake_s3.put_object.side_effect = make_client_error(403, code="AccessDenied")
    sleep_calls = []
    monkeypatch.setattr(daemon.random, "uniform", lambda a, b: 0.0)
    caplog.set_level("ERROR")

    job_path = enqueue_job(hook, tmp_path, transcript, created_at="2026-04-16T00:00:00.000Z")
    result = daemon.process_next_job(
        tmp_path,
        config,
        sleep_fn=sleep_calls.append,
        now_fn=lambda: "2026-04-16T00:00:01.000Z",
        s3=fake_s3,
    )

    dead_path = tmp_path / "queue" / "dead" / job_path.name
    assert result["status"] == "upload_failed"
    assert dead_path.exists()
    assert sleep_calls == []
    staging_names = sorted(path.name for path in (tmp_path / "staging").iterdir())
    assert any(name.endswith(".jsonl") for name in staging_names)
    assert any(name.endswith(".meta.json") for name in staging_names)
    assert any(name.endswith(".ready") for name in staging_names)
    assert "archive_upload_failed" in caplog.text


def test_s3_key_matches_expected_structure(tmp_path, daemon):
    transcript = tmp_path / "path-based.jsonl"
    config = make_config(tmp_path)
    job = {"session_id": "session-1"}
    tp_hash = hashlib.sha256(str(transcript).encode("utf-8")).hexdigest()[:12]

    s3_key = daemon.build_s3_key(job, 0, 8, "2026-04-16", config, tp_hash)

    assert re.fullmatch(
        r"raw/2026-04-16/tanl/session-1/[0-9a-f]{8}/0-8\.jsonl",
        s3_key,
    )
    assert f"/{tp_hash[:8]}/" in s3_key


@pytest.mark.parametrize(
    ("status", "expected_exit_code"),
    [
        ("uploaded", 0),
        ("idle", 0),
        ("invalid_job", 1),
        ("upload_failed", 1),
    ],
)
def test_main_once_maps_result_status_to_exit_code(tmp_path, daemon, monkeypatch, status, expected_exit_code):
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")

    def fake_run_daemon(state_root, config, *, once=False):
        assert once is True
        return {"status": status}

    monkeypatch.setattr(daemon, "run_daemon", fake_run_daemon)

    exit_code = daemon.main(["--state-root", str(tmp_path), "--config", str(config_path), "--once"])

    assert exit_code == expected_exit_code
