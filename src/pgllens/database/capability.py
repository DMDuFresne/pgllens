"""Extension and version probes, cached for the process lifetime."""

from __future__ import annotations

import functools
import re
from collections.abc import Awaitable, Callable

from pgllens.database.pool import Db

# Version and schema alongside the name: an extension's objects live in
# whatever schema CREATE EXTENSION put them in (public by default, but a DBA
# can choose), and its views/columns arrive with extension versions, not
# server versions -- pg_upgrade leaves extversion where it was.
_EXTENSIONS_SQL = """
    SELECT e.extname, e.extversion, n.nspname
    FROM pg_extension e
    JOIN pg_namespace n ON n.oid = e.extnamespace
"""


class Capabilities:
    def __init__(self, db: Db) -> None:
        self._db = db
        self._extensions: dict[str, tuple[str, str]] | None = None   # name -> (version, schema)
        self._server_version: tuple[int, int] | None = None

    async def refresh(self) -> None:
        result = await self._db.run_system(_EXTENSIONS_SQL, max_rows=500)
        self._extensions = {str(r[0]): (str(r[1]), str(r[2])) for r in result.rows}

    async def _installed(self) -> dict[str, tuple[str, str]]:
        if self._extensions is None:
            await self.refresh()
        assert self._extensions is not None
        return self._extensions

    async def has_extension(self, name: str) -> bool:
        return name in await self._installed()

    async def extension_schema(self, name: str) -> str | None:
        """Schema the extension's objects were created in; None if not installed."""
        entry = (await self._installed()).get(name)
        return entry[1] if entry else None

    async def extension_version(self, name: str) -> tuple[int, ...]:
        """`extversion` as a comparable tuple ((1, 10) > (1, 9)); () if not
        installed or unparseable."""
        entry = (await self._installed()).get(name)
        if entry is None:
            return ()
        # Leading dotted-numeric run only: "1.11-devel" is (1, 11). Parsing
        # STOPS at the first non-numeric part rather than skipping it, so a
        # suffixed version never compares as if the suffixed part were absent.
        m = re.match(r"\d+(?:\.\d+)*", entry[0])
        return tuple(int(p) for p in m.group().split(".")) if m else ()

    async def server_version(self) -> tuple[int, int]:
        # Cached like _extensions: the server version cannot change under a
        # live connection, so one SHOW per process is enough.
        if self._server_version is None:
            result = await self._db.run_system("SHOW server_version_num")
            num = int(str(result.rows[0][0]))
            self._server_version = (num // 10000, (num % 10000) // 100)
        return self._server_version


class ExtensionMissingError(Exception):
    """Raised by a `requires_extension`-gated tool when the extension is absent.
    tools/_util.tool_errors renders it as an EXTENSION_MISSING error envelope;
    the tool stays registered and visible (a capability gap is an error envelope, never a missing tool)."""

    def __init__(self, extension: str) -> None:
        super().__init__(f"extension `{extension}` is not installed on this database")
        self.extension = extension


def requires_extension(
    name: str, caps: Capabilities
) -> Callable[[Callable[..., Awaitable[str]]], Callable[..., Awaitable[str]]]:
    """Gate a tool on an extension. The tool stays REGISTERED and visible in
    tools/list -- it raises ExtensionMissingError, which tool_errors turns into
    the fixed error shape naming the CREATE EXTENSION statement."""

    def decorate(fn: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
        @functools.wraps(fn)
        async def wrapper(*args: object, **kwargs: object) -> str:
            if not await caps.has_extension(name):
                raise ExtensionMissingError(name)
            return await fn(*args, **kwargs)

        return wrapper

    return decorate
