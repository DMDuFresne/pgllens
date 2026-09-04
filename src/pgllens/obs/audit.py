"""Append-only JSONL audit sink -- distinct from operational logging.

Records that an action occurred (tool calls, etc.) -- never a credential, connection
string, or row of query results. Only the explicit kwargs a caller passes are
serialized, so nothing leaks by accident.

One JSON object per line, written via a dedicated ``logging.getLogger("pgllens.audit")``
with its own file handler. ``propagate=False`` keeps audit out of operational logs.
"""

from __future__ import annotations

import contextlib
import json
import logging
import logging.handlers
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pgllens.config import Settings

_LOGGER_NAME = "pgllens.audit"

# Module state: set once by configure_audit(). When disabled/un-configured, audit() no-ops.
_enabled: bool = False
_logger: logging.Logger | None = None


def configure_audit(settings: Settings) -> None:
    """Configure the audit sink from ``settings``. Idempotent; never raises.

    When ``settings.audit_log_file`` is unset, disables the sink (``audit`` no-ops).
    Otherwise attaches a single file handler at that path, creating parent dirs, with a
    message-only formatter (the message is already a JSON line) and ``propagate=False``.
    When nothing is configured, audit lines go to stdout (Play tier).
    """
    global _enabled, _logger

    # Idempotent: tear down any handler we previously attached before doing anything
    # else, so disabling audit (or reconfiguring it) always releases the old file
    # handle -- otherwise a configure-then-disable sequence leaks an open FileHandler
    # (and locks the file on Windows).
    logger = logging.getLogger(_LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    use_stdout = (
        settings.audit_stdout
        if settings.audit_stdout is not None
        else not (settings.audit_log_file or settings.audit_syslog)
    )

    if not settings.audit_log_file and not settings.audit_syslog and not use_stdout:
        _enabled = False
        _logger = None
        return

    _enabled = False
    logger.setLevel(logging.INFO)
    logger.propagate = False  # audit must never leak into operational logs

    if settings.audit_log_file:
        try:
            path = Path(settings.audit_log_file)
            path.parent.mkdir(parents=True, exist_ok=True)

            file_handler: logging.Handler
            if settings.audit_log_max_bytes > 0:
                file_handler = logging.handlers.RotatingFileHandler(
                    str(path),
                    maxBytes=settings.audit_log_max_bytes,
                    backupCount=settings.audit_log_backups,
                    encoding="utf-8",
                )
            else:
                file_handler = logging.FileHandler(str(path), encoding="utf-8")
            file_handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(file_handler)

            _logger = logger
            _enabled = True
        except Exception:
            logging.getLogger(__name__).warning(
                "audit log setup failed; file sink disabled", exc_info=True
            )

    if settings.audit_syslog:
        try:
            host, _, port = settings.audit_syslog.rpartition(":")
            syslog_handler = logging.handlers.SysLogHandler(address=(host, int(port)))
            syslog_handler.setFormatter(logging.Formatter("pgllens-audit: %(message)s"))
            logger.addHandler(syslog_handler)
            _logger = logger
            _enabled = True
        except Exception:
            # Same contract as the file sink: a broken audit destination must
            # never stop the server from starting.
            logging.getLogger(__name__).warning(
                "audit syslog setup failed; syslog sink disabled", exc_info=True
            )

    if use_stdout:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(stdout_handler)
        _logger = logger
        _enabled = True

    if not _enabled:
        _logger = None


def audit(event: str, **fields: Any) -> None:
    """Write one audit record. Safe no-op when audit is disabled/unconfigured; never raises.

    Emits ``{"timestamp": <ISO-8601 UTC>, "event": event, **fields}`` as a single
    ``json.dumps`` line. Only the explicit ``fields`` passed are serialized -- pass no
    credentials, connection strings, or row data.
    """
    if not _enabled or _logger is None:
        return
    try:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "event": event,
            **fields,
        }
        _logger.info(json.dumps(payload, sort_keys=True, default=str))
    except Exception:
        with contextlib.suppress(Exception):
            logging.getLogger(__name__).warning(
                "audit() dropped a record (serialization error)", exc_info=True
            )
