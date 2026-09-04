"""Tests for structured JSON logging, correlation ids, and the audit sink."""

from __future__ import annotations

import asyncio
import json
import logging

from pgllens.config import Settings
from pgllens.obs import audit as audit_mod
from pgllens.obs import correlation as corr
from pgllens.obs.logconfig import configure_logging

DSN = "postgresql://u:p@localhost:5432/flux"


def make(**kw) -> Settings:
    base = {"database_url": DSN, "exposed_schemas": "public"}
    base.update(kw)
    return Settings(_env_file=None, **base)


def test_json_logging_emits_parseable_records(capsys):
    configure_logging(make(log_level="INFO"))
    logging.getLogger("pgllens").info("hello")
    out = capsys.readouterr()
    line = [ln for ln in (out.err + out.out).splitlines() if "hello" in ln][-1]
    rec = json.loads(line)
    assert rec["message"] == "hello"
    assert rec["level"] == "INFO"
    assert rec["logger"] == "pgllens"


def test_correlation_id_is_included_when_set(capsys):
    configure_logging(make())
    corr.set_correlation_id("abc-123")
    logging.getLogger("pgllens").info("with-id")
    out = capsys.readouterr()
    line = [ln for ln in (out.err + out.out).splitlines() if "with-id" in ln][-1]
    assert json.loads(line)["correlation_id"] == "abc-123"


async def test_correlation_ids_are_isolated_per_context():
    # NOTE: brief's version used asyncio.gather over to_thread(asyncio.run, ...),
    # which is needlessly roundabout for proving contextvar isolation. asyncio.gather
    # already runs each coroutine as its own Task with its own copied Context, so a
    # plain gather of three coroutines that each set-then-read their own id is a more
    # direct proof of per-context isolation.
    async def worker(value: str) -> str | None:
        corr.set_correlation_id(value)
        await asyncio.sleep(0)
        return corr.correlation_id()

    results = await asyncio.gather(worker("id-0"), worker("id-1"), worker("id-2"))
    assert sorted(results) == ["id-0", "id-1", "id-2"]


def test_audit_writes_one_json_object_per_line(tmp_path):
    path = tmp_path / "audit.jsonl"
    audit_mod.configure_audit(make(audit_log_file=str(path)))
    audit_mod.audit("tool_call", tool="query", schema="public")
    audit_mod.audit("tool_call", tool="list_tables", schema="public")
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["event"] == "tool_call" and first["tool"] == "query"
    assert "timestamp" in first


def test_audit_falls_back_to_stdout_without_configuration(capsys):
    try:
        audit_mod.configure_audit(make())          # no file, no syslog: Play tier
        audit_mod.audit("tool_call", tool="query")
        out = capsys.readouterr()
        line = [ln for ln in (out.out + out.err).splitlines() if "tool_call" in ln][-1]
        rec = json.loads(line)
        assert rec["event"] == "tool_call" and rec["tool"] == "query"
    finally:
        # Otherwise the stdout handler leaks into every later test's capsys.
        audit_mod.configure_audit(make(audit_stdout=False))


def test_configure_audit_releases_previous_file_handle_when_disabled(tmp_path):
    path = tmp_path / "audit.jsonl"
    audit_mod.configure_audit(make(audit_log_file=str(path)))
    audit_mod.audit("tool_call", tool="query")

    audit_mod.configure_audit(make(audit_stdout=False))  # disable: no file/syslog/stdout

    # No handler left attached to the audit logger...
    assert logging.getLogger("pgllens.audit").handlers == []
    # ...and the file handle was actually released (observable on Windows as a
    # PermissionError if the old FileHandler is still open).
    path.unlink()


def test_json_logging_redacts_denied_extra_keys(capsys):
    configure_logging(make())
    logging.getLogger("pgllens").info(
        "login", extra={"password": "hunter2", "username": "alice"}
    )
    out = capsys.readouterr()
    line = [ln for ln in (out.err + out.out).splitlines() if "login" in ln][-1]
    rec = json.loads(line)
    assert rec["password"] == "***"
    assert rec["username"] == "alice"
