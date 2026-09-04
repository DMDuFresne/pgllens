import pytest

from pgllens.database.capability import Capabilities, ExtensionMissingError, requires_extension
from pgllens.database.format import QueryResult


class FakeDb:
    def __init__(self, installed):
        self.installed = installed
        self.calls = 0

    async def run_system(self, sql, params=(), max_rows=None):
        self.calls += 1
        return QueryResult(["extname", "extversion", "nspname"],
                           [(n, "1.10", "public") for n in self.installed], False)


async def test_extension_version_and_schema():
    class Db(FakeDb):
        async def run_system(self, sql, params=(), max_rows=None):
            self.calls += 1
            return QueryResult(["extname", "extversion", "nspname"],
                               [("pg_stat_statements", "1.10", "stats")], False)

    caps = Capabilities(Db([]))
    assert await caps.extension_schema("pg_stat_statements") == "stats"
    # (1, 10) > (1, 9): compared as ints, not as the string "1.10" < "1.9".
    assert await caps.extension_version("pg_stat_statements") == (1, 10)
    assert await caps.extension_version("pg_stat_statements") > (1, 9)
    assert await caps.extension_schema("timescaledb") is None
    assert await caps.extension_version("timescaledb") == ()


async def test_extension_present():
    assert await Capabilities(FakeDb(["timescaledb"])).has_extension("timescaledb")


async def test_extension_absent():
    assert not await Capabilities(FakeDb([])).has_extension("timescaledb")


async def test_extension_list_is_cached():
    db = FakeDb(["timescaledb"])
    caps = Capabilities(db)
    await caps.has_extension("timescaledb")
    await caps.has_extension("pg_stat_statements")
    assert db.calls == 1


async def test_gated_tool_raises_extension_missing_when_absent():
    caps = Capabilities(FakeDb([]))

    @requires_extension("pg_stat_statements", caps)
    async def get_query_store() -> str:
        raise AssertionError("body must not run when the extension is absent")

    with pytest.raises(ExtensionMissingError) as ei:
        await get_query_store()
    assert ei.value.extension == "pg_stat_statements"


async def test_gated_tool_runs_when_the_extension_is_present():
    caps = Capabilities(FakeDb(["pg_stat_statements"]))

    @requires_extension("pg_stat_statements", caps)
    async def get_query_store() -> str:
        return "rows"

    assert await get_query_store() == "rows"


async def test_server_version_is_cached():
    class VersionDb(FakeDb):
        async def run_system(self, sql, params=(), max_rows=None):
            self.calls += 1
            return QueryResult(["server_version_num"], [("170004",)], False)

    db = VersionDb([])
    caps = Capabilities(db)
    assert await caps.server_version() == (17, 0)
    assert await caps.server_version() == (17, 0)
    assert db.calls == 1


async def test_extension_version_stops_at_a_non_numeric_part():
    class Db(FakeDb):
        async def run_system(self, sql, params=(), max_rows=None):
            return QueryResult(["extname", "extversion", "nspname"],
                               [("pg_stat_statements", "1.11-devel", "public")], False)

    assert await Capabilities(Db([])).extension_version("pg_stat_statements") == (1, 11)
