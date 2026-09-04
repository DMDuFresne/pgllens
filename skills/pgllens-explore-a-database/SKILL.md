---
name: pgllens-explore-a-database
description: Use when getting oriented in an unfamiliar PostgreSQL database — "what tables does this database have", "explain this schema to me", "how are these tables related", "what does this column mean", "show me the data model", "find where X is stored" — before writing any query against a schema you haven't looked at yet. Triggers on schema, database, tables, ERD, relationships, data model, sample data.
---

# pgllens-explore-a-database

## Overview

Walking an unfamiliar database with pgllens, a **read-only** lens: no query here can change
anything, so explore freely. The goal is to understand what the schema *means*, not just what
it's shaped like, before writing SQL against it.

**REQUIRED BACKGROUND:** read pgllens-using first for the `schema` argument, the exposed-schema
allowlist, the house rules (view-first, convention-aware filtering, unit-aware aggregation), and the
structure-vs-meaning distinction between `describe_table` and `get_ontology`.

## Workflow

1. **Overview.** `schema_overview` — per-schema table/view counts and total row estimate. Tells
   you the size and shape of what you're dealing with before drilling in.
2. **Meaning first.** `get_ontology` (optionally scoped with `schema`) — which tables are hubs
   (most referenced), how tables connect, and naming conventions in use (soft delete, audit
   columns, lookup tables, junction tables), plus any operator-supplied domain context. **Read
   this before guessing at semantics.** A convention the ontology reports across several
   tables (a soft-delete marker, say) holds throughout the schema, not just on the one table
   you happened to look at.
   Guessing from a column name alone (`flag`, `type`, `status`) is how wrong answers happen —
   the ontology exists precisely to stop that.
3. **Structure.** `describe_table` for the specific tables that matter — columns, types,
   nullability, primary key, default, and a `values` column carrying an enum's labels or a
   low-cardinality column's most common values. `list_tables` for row estimates and comments
   across a whole schema. `get_constraints` for the `CHECK`/`UNIQUE`/`EXCLUDE` rules the
   database actually enforces, and `get_triggers` for what fires on insert/update/delete —
   both are business rules `describe_table` doesn't show.
4. **Relationships.** `get_erd` for a visual data model — a Mermaid `erDiagram` code block by
   default (`format="text"` for a plain listing), readable on any host. `get_erd_widget` is the
   same diagram as an interactive MCP Apps widget (pan/zoom, drag, search, drill-down) for a
   host that renders them, with a plain markdown summary either way. Scope either with `schema`
   or `tables` on a large database rather than dumping everything (`max_nodes`, default 60,
   truncates by FK degree beyond that; `depth`, 1-3, widens the FK neighbourhood pulled in
   around `tables`). Use `get_relationships` for a plain foreign-key list when a diagram is
   overkill; pass `table` to scope to one table's incoming and outgoing relationships, and
   `find_path` when you need the join route between two tables that aren't directly related.

   On a host that renders the widget, the diagram is interactive: the user can click a table
   card themselves to open a drawer with sample rows, columns, and stats fetched live (via
   `get_sample_data`/`describe_table`/`get_table_stats`, the only tools the widget is allowed to
   call). If the user is already looking at the ERD and clicking into a table, don't re-fetch
   that same sample/describe/stats data yourself — they're seeing it live in the drawer. Only
   call those tools yourself when you need the data for your own reasoning (writing a query,
   answering a question) rather than the user's own browsing.
5. **Sample data.** `get_sample_data` (up to 1000 unfiltered rows, default 10) — confirms your
   read of the ontology against what's actually stored. If the ontology reports a soft-delete
   convention, confirm with sample data that the marker column actually carries non-null values.
6. **Stats.** `get_table_stats` — row count plus per-column null count/percentage and distinct
   count, batched into one query where possible (falls back to one query per column for a
   json/xml column that can't support `COUNT(DISTINCT)`). Useful for judging cardinality (is
   this column effectively unique? mostly null?) before using it in a join or filter.

## Finding a concept across the whole database

Don't guess which table holds "email" or "customer ID" — `search_columns` with a
case-insensitive substring pattern (e.g. `email`) searches column names across every exposed
schema at once. Faster and more reliable than paging through `list_tables` output by eye.

## Programmable objects

If the schema exposes business logic through views, read their definitions before
reimplementing the join: `get_view_definition` shows the view's full SQL and column listing,
often the fastest way to learn how a set of tables is meant to be joined, and a view may
already apply the filters and conversions the ontology describes. `list_functions`
lists stored functions with parameters, return type, volatility, and comment; `get_function_source`
shows the full definition, including all overloads.

## If the schema just changed

`refresh_schema` forces a re-read of the database catalog, replacing the cached schema metadata
(`list_tables`, `get_ontology`, and friends cache results for `SCHEMA_REFRESH_INTERVAL_MS`). Use
it if a DDL change was just made elsewhere and pgllens is still showing the old shape.
