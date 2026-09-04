"""Who is making this call -- a contextvar, set once per request.

The tool bodies (`tools/_util.py`'s audit line, `tools/query.py`'s cost budget)
need the authenticated identity, but they are called through the MCP layer and
never see the ASGI scope. Same pattern as obs/correlation.py: contextvars give
each asyncio task its own copy, so concurrent requests never read each other's
identity.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Caller:
    """The authenticated caller. Defaults describe an unauthenticated request
    in `none` mode -- never a fabricated identity."""

    client_id: str = "anonymous"
    sub: str | None = None
    ip: str = "unknown"
    scopes: frozenset[str] = field(default_factory=frozenset)


# noqa-worthy in general (B039 warns about mutable ContextVar defaults), but
# Caller is a frozen dataclass -- this single shared instance can never be
# mutated in place, so sharing it across contexts is safe.
_caller: ContextVar[Caller] = ContextVar("pgllens_caller", default=Caller())  # noqa: B039


def caller() -> Caller:
    return _caller.get()


def set_caller(value: Caller) -> Token[Caller]:
    """Set the caller for the current context and return a reset token.

    `CallerContextMiddleware` uses the token to restore the previous value in
    a `finally`, so identity never leaks past the request that set it into
    whatever else later runs on the same pooled task/thread. Callers that
    don't need to restore anything (tests, `reset_caller`'s absence in a
    one-shot script) can simply ignore the return value -- `_caller.set`
    always returns one regardless.
    """
    return _caller.set(value)


def reset_caller(token: Token[Caller]) -> None:
    """Undo a `set_caller` call, restoring whatever was set before it."""
    _caller.reset(token)
