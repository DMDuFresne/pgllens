---
name: pgllens-document-a-schema
description: Use when asked to produce a written reference document for a PostgreSQL database from the live catalog -- "document this schema", "write a data dictionary", "generate schema docs", "describe every table", "make an ERD and table reference", "onboarding doc for the database" -- where the output is a markdown file for other people, not a conversational tour. Triggers on data dictionary, schema docs, documentation, reference, onboarding doc, ERD, table reference.
---

# pgllens-document-a-schema

## Overview

Producing a schema reference document (markdown) from a live database through pgllens, a
**read-only** lens. The document is for readers who were not in the conversation: every claim
in it must come from a tool result, every quoted comment must be verbatim, and every estimate
or heuristic must be labelled as such. For getting oriented yourself, use
pgllens-explore-a-database instead; this skill assumes you already know how to walk the
database and turns that walk into a document with a fixed section order. It carries no
knowledge of any particular schema; everything below applies to whatever the server exposes.

**REQUIRED BACKGROUND:** read pgllens-using first for the `schema` argument, the exposed-schema
allowlist, the structure-vs-meaning distinction between `describe_table` and `get_ontology`,
and the rule that a missing extension is a reported gap, not a clean result.

## Workflow

Gather in this order, then write the document in the output order below.

1. **Environment.** `server_info` (PostgreSQL version) and `list_extensions` (installed
   extensions, e.g. `timescaledb`, `pg_stat_statements`). If `timescaledb` is present, also
   `list_hypertables`: hypertable, time column, chunk interval, compression, jobs, chunk count
   and range.
2. **Inventory.** `schema_overview` for per-schema table/view counts and row estimates, then
   `list_tables` per schema for kind (table / view / materialized view), row estimate, and the
   table `COMMENT`. Operator comments are the best source of meaning the database has; quote
   them verbatim in the document rather than paraphrasing.
3. **Meaning.** `get_ontology` per schema: hubs (most referenced tables), naming conventions
   detected (audit columns, lookup tables), inferred roles (audit, time-series), and
   relationships. It is heuristic ("Name and shape heuristics; no data sampled"), so it feeds
   the overview paragraph and the hub-first ordering, labelled as inferred. A table with no
   FKs in or out that the ontology calls audit or time-series is still documented in full.
4. **Diagrams.** `get_erd(format="mermaid", schema=<schema>)` once per schema. For a busy hub
   use `tables=<hub_table>` with `depth` (1-3) for a focused sub-diagram around it. The Mermaid
   uses `}|--||` / `}o--||` cardinality and PK/FK markers; paste the code block as returned.
   `format="text"` gives the same tables and relationships as markdown tables if a diagram is
   not wanted.
5. **Tables.** For each table, hub-first then alphabetical: `describe_table` for columns with
   type, nullability, default (e.g. `identity (always)`, `now()`), pk marker, column comment,
   and `values`; `get_constraints(table=<table>)` because `describe_table` shows only the PK,
   and this is where `CHECK`, `UNIQUE`, `FOREIGN KEY` (with `references` as `schema.table`) and
   the `validated` column live (a `CHECK` can be `NOT VALID`; the `validated` column shows it);
   `get_relationships(table=<table>)` for outgoing and incoming FKs, including cross-schema
   ones; `get_triggers(table=<table>)` for trigger name, enabled state, full `CREATE TRIGGER`
   text and the function called. When two tables that look related return `0 paths` from
   `find_path`, say so and repeat the tool's note that they may be linked only through views or
   application logic.
6. **Views and functions.** `get_view_definition` for every view and materialized view:
   identity, columns, full `SELECT` source. `list_functions` per schema (name, arguments,
   return type, volatility, comment), then `get_function_source` for each: full
   `CREATE OR REPLACE` text, language, security (invoker/definer), strict flag, all overloads.
   A function named by a trigger in step 5 belongs here too; cross-reference the two.

## The `values` column

`describe_table` fills `values` two ways, and the document must say which is which. An enum
column always shows its labels from the catalog; collect these into the Enums section. A
non-enum column with at most 20 distinct values shows a sample of most-common values from
planner statistics, which is approximate and absent for a never-analyzed table; present those
as "observed values (planner sample)", never as an exhaustive list. A column matching
`REDACT_COLUMNS` shows no sampled values (enum labels still show): never put sample data for a
masked column in a document, and note the column as masked.

## Row estimates

`list_tables` and `describe_table` report planner estimates. A TimescaleDB hypertable parent
shows 0 because rows live in chunks; if an exact number matters, use `query` with `count(*)`
and say in the document that it was counted, not estimated.

## Output order

1. **Environment**: PostgreSQL version, extensions, date generated.
2. **Overview**: ontology paragraph, hubs, conventions, each labelled inferred.
3. **ERD per schema**: one Mermaid block per `<schema>`; optional `<hub_table>` sub-diagrams.
4. **Tables**: hub-first then alphabetical; per `<table>`: comment, columns table,
   constraints, relationships in/out, triggers.
5. **Views and materialized views**: identity, columns, source.
6. **Functions**: signature, volatility, security, strict, source.
7. **Enums**: labels collected from `describe_table` `values`.
8. **Extensions and hypertables**: `list_extensions`, `list_hypertables` detail.
9. **Caveats**: estimates, heuristics, masked columns, extensions absent, anything not visible
   through the exposed schemas.

## Contract

The document contains nothing that did not come from a tool result in this session. Comments
are quoted verbatim. Every row count is marked estimate or counted. Ontology content is marked
inferred. Planner value samples are marked approximate. Masked columns carry no sample data.
Missing extensions are reported as gaps, not omitted. Sections appear in the output order
above, even when a section is empty (write "none" rather than dropping it).
