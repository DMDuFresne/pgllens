"""Structured (JSON) operational logging.

Emits one JSON object per line on stderr, carrying ``timestamp``, ``level``, ``logger``,
``message``, and the correlation id when set (see ``obs/correlation.py``). Distinct from
the JSONL *audit* sink (``obs/audit.py``), which records discrete actions rather than the
general operational log stream.

SECRETS NEVER LOGGED: nothing here ever serializes a ``Settings`` object or connection
string; callers must not pass them via ``extra=``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pgllens.obs.correlation import correlation_id

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pgllens.config import Settings

# LogRecord attributes that are framework-internal -- everything else a caller attaches via
# ``logger.x(..., extra={...})`` is serialized as a top-level structured field.
_RESERVED: frozenset[str] = frozenset(
    vars(logging.makeLogRecord({})).keys()
) | {"message", "asctime", "taskName"}

# SECRETS NEVER LOGGED: extra= fields with one of these names are redacted rather than
# emitted, so a future `logger.info(..., extra={"password": ...})` can't leak silently.
_DENIED_KEYS: frozenset[str] = frozenset(
    {"password", "secret", "token", "authorization", "connection_string", "pwd"}
)


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        cid = correlation_id()
        if cid is not None:
            payload["correlation_id"] = cid
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = "***" if key.lower() in _DENIED_KEYS else value
        return json.dumps(payload, default=str, sort_keys=True)


def configure_logging(settings: Settings) -> None:
    """Install a single handler on the root logger at ``settings.log_level``.

    Idempotent: replaces any handler this function previously installed rather than
    stacking, so calling it twice (e.g. from ``__main__`` and again in tests) never
    doubles log output. ``settings.log_format`` selects ``"json"`` (default) or plain
    ``"text"`` formatting.
    """
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    root = logging.getLogger()
    for handler in list(root.handlers):
        if handler.get_name() == "pgllens":
            root.removeHandler(handler)
    handler = logging.StreamHandler()
    handler.set_name("pgllens")
    if settings.log_format == "text":
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    else:
        handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
