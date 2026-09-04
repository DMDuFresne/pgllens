"""Server instructions sent to the client at initialize."""

from __future__ import annotations

from pgllens.config import Settings

_BASE = """PgLLens is a read-only lens onto one PostgreSQL database.

Exposed schemas: {schemas}. The default when a tool's `schema` argument is
omitted is `{default}`.

Start with `search_columns` or `schema_overview` rather than guessing a table
name. Read a view's definition with `get_view_definition` before hand-writing
SQL that duplicates it. Every tool is read-only: the connection runs with
default_transaction_read_only=on and writes are rejected before they are sent.
"""


def build_instructions(settings: Settings) -> str:
    text = _BASE.format(schemas=", ".join(settings.exposed_schemas),
                        default=settings.default_schema)
    context = settings.domain_context_text
    if context:
        text += f"\n## Domain context\n\n{context}\n"
    return text
