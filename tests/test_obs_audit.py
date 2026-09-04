import json
import socket

import pytest

from pgllens.caller import Caller, set_caller
from pgllens.config import Settings
from pgllens.database import pool
from pgllens.obs import audit
from pgllens.tools._util import tool_errors

DSN = "postgresql://u:p@localhost:5432/flux"


def make_settings(**kw):
    base = {"database_url": DSN, "exposed_schemas": "public"}
    base.update(kw)
    return Settings(_env_file=None, **base)


@pytest.fixture
def audit_file(tmp_path):
    path = tmp_path / "audit.jsonl"
    audit.configure_audit(make_settings(audit_log_file=str(path)))
    yield path
    audit.configure_audit(make_settings(audit_stdout=False))


def records(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


async def test_tool_call_audit_carries_the_authenticated_identity(audit_file):
    set_caller(Caller(client_id="0oa1client", sub="00u1user", ip="203.0.113.7"))

    @tool_errors
    async def query(sql: str) -> str:
        return "ok"

    await query(sql="SELECT 1")
    record = records(audit_file)[-1]
    assert record["sub"] == "00u1user"
    assert record["client_id"] == "0oa1client"
    assert record["ip"] == "203.0.113.7"
    assert record["tool"] == "query"
    assert record["outcome"] == "ok"


async def test_audit_records_an_args_hash_and_never_the_arguments(audit_file):
    # The SQL is the client's data. The hash answers "did they run this again?"
    # without putting the query text in the audit trail.
    set_caller(Caller())

    @tool_errors
    async def query(sql: str) -> str:
        return "ok"

    await query(sql="SELECT secret FROM vault")
    record = records(audit_file)[-1]
    assert len(record["args_hash"]) == 16
    assert "vault" not in json.dumps(record)


async def test_the_same_arguments_hash_the_same_and_different_ones_differ(audit_file):
    set_caller(Caller())

    @tool_errors
    async def query(sql: str) -> str:
        return "ok"

    await query(sql="SELECT 1")
    await query(sql="SELECT 1")
    await query(sql="SELECT 2")
    hashes = [r["args_hash"] for r in records(audit_file)]
    assert hashes[0] == hashes[1] != hashes[2]


async def test_audit_records_the_row_count(audit_file):
    set_caller(Caller())

    @tool_errors
    async def query(sql: str) -> str:
        pool.record_rows(42)
        return "ok"

    await query(sql="SELECT 1")
    assert records(audit_file)[-1]["rows"] == 42


async def test_the_row_count_does_not_leak_between_calls(audit_file):
    set_caller(Caller())

    @tool_errors
    async def big(sql: str) -> str:
        pool.record_rows(42)
        return "ok"

    @tool_errors
    async def small(sql: str) -> str:
        return "ok"

    await big(sql="x")
    await small(sql="y")
    assert records(audit_file)[-1]["rows"] == 0


async def test_a_failing_tool_is_still_audited_with_its_identity(audit_file):
    set_caller(Caller(client_id="c1", sub="s1"))

    @tool_errors
    async def query(sql: str) -> str:
        raise RuntimeError("boom")

    await query(sql="SELECT 1")
    record = records(audit_file)[-1]
    assert record["outcome"] == "db_error"
    assert record["client_id"] == "c1"


async def test_get_erd_widget_audit_carries_the_same_identity_fields_as_every_other_tool(audit_file):
    # get_erd_widget returns CallToolResult (not tool_errors' plain str), but it
    # still routes through @tool_errors like every other tool -- tool_errors
    # detects the CallToolResult return annotation and wraps errors accordingly.
    # It must still carry the same identity/args_hash/rows shape every other
    # tool's audit line does.
    from unittest.mock import AsyncMock, MagicMock

    from mcp.server.apps import Apps

    from pgllens.tools import erd as mod

    set_caller(Caller(client_id="0oa1client", sub="00u1user", ip="203.0.113.7"))

    intro = MagicMock()
    intro.tables = AsyncMock(return_value=[])
    intro.foreign_keys = AsyncMock(return_value=[])

    apps = Apps()
    db, settings = MagicMock(), MagicMock()
    mod.register_apps(apps, db, settings, intro)
    get_erd_widget = apps.tools()[0].fn

    await get_erd_widget()

    record = records(audit_file)[-1]
    assert record["tool"] == "get_erd_widget"
    assert record["sub"] == "00u1user"
    assert record["client_id"] == "0oa1client"
    assert record["ip"] == "203.0.113.7"
    assert record["outcome"] == "ok"
    assert len(record["args_hash"]) == 16
    assert record["rows"] == 0


def test_syslog_sink_receives_the_record():
    # Durable sink without the compose stack: any collector speaking syslog.
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind(("127.0.0.1", 0))
    receiver.settimeout(2.0)
    host, port = receiver.getsockname()
    try:
        audit.configure_audit(make_settings(audit_syslog=f"{host}:{port}"))
        audit.audit("tool_call", tool="query", outcome="ok")
        payload = receiver.recv(4096).decode("utf-8", "replace")
        assert "tool_call" in payload
    finally:
        audit.configure_audit(make_settings())
        receiver.close()


def test_an_unreachable_syslog_target_does_not_break_startup():
    # Audit must never be the reason the server fails to start or a tool call
    # raises -- configure_audit already has this contract for the file sink.
    audit.configure_audit(make_settings(audit_syslog="203.0.113.255:9"))
    audit.audit("tool_call", tool="query", outcome="ok")
    audit.configure_audit(make_settings())


def test_both_sinks_can_be_active_at_once(tmp_path):
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind(("127.0.0.1", 0))
    receiver.settimeout(2.0)
    host, port = receiver.getsockname()
    path = tmp_path / "audit.jsonl"
    try:
        audit.configure_audit(
            make_settings(audit_log_file=str(path), audit_syslog=f"{host}:{port}")
        )
        audit.audit("tool_call", tool="query", outcome="ok")
        assert "tool_call" in receiver.recv(4096).decode("utf-8", "replace")
        assert "tool_call" in path.read_text(encoding="utf-8")
    finally:
        audit.configure_audit(make_settings(audit_stdout=False))
        receiver.close()


def test_audit_goes_to_stdout_when_nothing_else_is_configured(capsys):
    audit.configure_audit(make_settings())
    try:
        audit.audit("tool_call", tool="query", outcome="ok")
        line = capsys.readouterr().out.strip().splitlines()[-1]
        record = json.loads(line)
        assert record["event"] == "tool_call" and record["tool"] == "query"
    finally:
        audit.configure_audit(make_settings(audit_stdout=False))


def test_audit_stdout_off_when_a_file_is_configured(tmp_path, capsys):
    path = tmp_path / "audit.jsonl"
    audit.configure_audit(make_settings(audit_log_file=str(path)))
    try:
        audit.audit("tool_call", tool="query", outcome="ok")
        assert capsys.readouterr().out == ""
        assert len(records(path)) == 1
    finally:
        audit.configure_audit(make_settings(audit_stdout=False))


def test_audit_stdout_can_be_forced_alongside_the_file(tmp_path, capsys):
    path = tmp_path / "audit.jsonl"
    audit.configure_audit(make_settings(audit_log_file=str(path), audit_stdout=True))
    try:
        audit.audit("tool_call", tool="query", outcome="ok")
        assert json.loads(capsys.readouterr().out.strip())["tool"] == "query"
        assert len(records(path)) == 1
    finally:
        audit.configure_audit(make_settings(audit_stdout=False))


def test_audit_stdout_false_disables_the_fallback(capsys):
    audit.configure_audit(make_settings(audit_stdout=False))
    audit.audit("tool_call", tool="query", outcome="ok")
    assert capsys.readouterr().out == ""


def test_audit_file_rotates_at_the_configured_size(tmp_path):
    path = tmp_path / "audit.jsonl"
    audit.configure_audit(make_settings(audit_log_file=str(path), audit_log_max_bytes=500, audit_log_backups=2))
    try:
        for i in range(40):
            audit.audit("tool_call", tool="query", outcome="ok", i=i)
        names = sorted(p.name for p in tmp_path.iterdir())
        assert names == ["audit.jsonl", "audit.jsonl.1", "audit.jsonl.2"]
        assert path.stat().st_size <= 500
    finally:
        audit.configure_audit(make_settings(audit_stdout=False))


def test_audit_rotation_disabled_with_zero(tmp_path):
    path = tmp_path / "audit.jsonl"
    audit.configure_audit(make_settings(audit_log_file=str(path), audit_log_max_bytes=0))
    try:
        for i in range(40):
            audit.audit("tool_call", tool="query", outcome="ok", i=i)
        assert [p.name for p in tmp_path.iterdir()] == ["audit.jsonl"]
    finally:
        audit.configure_audit(make_settings(audit_stdout=False))


def test_audit_rotation_defaults():
    s = make_settings()
    assert s.audit_log_max_bytes == 100_000_000 and s.audit_log_backups == 10


async def test_audit_line_carries_trace_id_only_when_tracing(audit_file, monkeypatch):
    from pgllens.obs import telemetry

    monkeypatch.setattr(telemetry, "current_trace_id", lambda: "0af7651916cd43dd8448eb211c80319c")

    @tool_errors
    async def query(sql: str) -> str:
        return "ok"

    await query(sql="SELECT 1")
    assert records(audit_file)[-1]["trace_id"] == "0af7651916cd43dd8448eb211c80319c"

    monkeypatch.setattr(telemetry, "current_trace_id", lambda: None)
    await query(sql="SELECT 1")
    assert "trace_id" not in records(audit_file)[-1]
