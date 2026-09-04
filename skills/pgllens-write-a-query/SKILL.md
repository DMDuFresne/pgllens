---
name: pgllens-write-a-query
description: Use when writing an analytical SQL query against a PostgreSQL database through pgllens, as opposed to tuning a slow one or exploring the schema. "write a query for", "how many X per Y", "give me the SQL that", "pull the rows where", "report on", "count/sum/average of", "top N by" -- the discover/validate/run/page loop for getting a correct answer out of a read-only lens. Triggers on SQL, query, report, count, sum, average, rows, join, group by, aggregate.
---

# pgllens-write-a-query

## Overview

Writing a new query with pgllens, a **read-only** lens. Nothing you send can change the
database, so the risk is not damage, it is a wrong answer: a query against the wrong table, a
join that fans out, a result cut off at the row cap and mistaken for the whole. The loop below
is how you get the right answer and know that you have it. For a query that is already correct
but slow, use pgllens-tune-a-query; for a database you have not seen yet, start with
pgllens-explore-a-database.

**REQUIRED BACKGROUND:** read pgllens-using first.

## The loop

1. **Discover before writing.** `list_tables` for what exists (optional `schema`; an unknown
   schema returns `SCHEMA_UNKNOWN` naming the exposed ones), `describe_table` for columns,
   types, and an enum column's labels (so a `WHERE <status_column> IN (...)` names real values
   instead of guessed ones), `search_columns` when you know a column name but not which table
   holds it. If a view or a materialized view already covers the question (`list_tables` shows
   kind view / materialized view), `get_view_definition` first and prefer it: it usually already
   encodes the join and the business rule you would otherwise rebuild by hand.
2. **Validate.** `validate_query(sql)` checks the read-only gate and plans the statement without
   running it. A rejection is an error envelope with code `QUERY_REJECTED` and a plain message,
   e.g. "query must start with SELECT, WITH, TABLE or VALUES." or "pg_sleep is not permitted
   (read-only lens)." Fix the SQL; do not try to route around the gate.
3. **Size it.** For any large or unbounded table (row estimates come from `list_tables`), or any
   join you have not run before, pass `explain_first=true` to see the planner's estimated rows
   and cost prefixed to the result, or `max_estimated_rows=N` to have the server refuse before
   touching data when the estimate exceeds N. The refusal reads like "Planner estimates 9,099
   rows, above `max_estimated_rows`=100." with a hint to narrow with `WHERE`, add `LIMIT`, or
   raise it. A plan the server cannot parse never refuses. `explain_query(sql, analyze=false)`
   shows the plan on its own; `analyze=true` really executes the query (still read-only), so
   treat it as a run, not a look.
4. **Run and page.** `query(sql, limit, page)`. The server caps each page at 200 rows by default;
   `limit` is 1 to the server maximum and an out-of-range value is rejected, not clamped. When
   the footer says "more rows exist", fetch `page=2`, `page=3`, ... (1 to 10000); the SQL must
   contain an `ORDER BY` and you must pass the same `limit` on every page or the windows will not
   line up. For a report, aggregate in SQL so the answer fits in one page instead of paging raw
   rows and summing them yourself.
5. **Follow the footer.** Every response ends with a "Next:" line naming sensible follow-up
   calls. When unsure what to do next, do that.

## Worked example

"How many <fact rows> in a given state per <dimension>, and the total of a measure?" A fact
table joined through one dimension to a second, grouped by the outer dimension's label column,
filtered on the fact's enum column (if the schema has one; otherwise drop the `WHERE`):

```sql
SELECT d2.<label_column> AS group_label,
       count(*) AS matching_rows,
       coalesce(sum(f.<measure_column>), 0) AS total_measure
FROM <schema>.<fact_table> AS f
JOIN <schema>.<dimension_table> AS d1 ON d1.<d1_id> = f.<d1_id>
JOIN <schema>.<outer_dimension>  AS d2 ON d2.<d2_id> = d1.<d2_id>
WHERE f.<status_column> IN ('<label_1>', '<label_2>')
GROUP BY d2.<d2_id>, d2.<label_column>
ORDER BY matching_rows DESC, d2.<label_column>;
```

`describe_table` on the fact table supplies the enum labels for the `WHERE`, `get_relationships`
or `find_path` supplies the join keys, and the `ORDER BY` makes the result pageable.

## Interpreting the result

- **Masked cells.** A column whose output name matches the operator's `REDACT_COLUMNS` patterns
  renders as `[masked]`. This is display masking by output-column name, not a security boundary.
  Do not alias the column or wrap it in a function to get the value out; that would bypass the
  mask, and it is exactly what not to do. Report the value as masked.
- **Truncated cells.** A cell longer than 2,000 characters is cut with a marker stating its full
  length. Read it in slices: `substr(col::text, 1, 2000)`, then `substr(col::text, 2001, 2000)`.
- **Duplicate column names.** `SELECT a.<col>, b.<col>` (two tables sharing a column name, such
  as a timestamp both carry) returns both values (the row shape is positional). Alias them anyway
  so the reader can tell which is which.
- **Permission denied on `SELECT *`.** Some tables grant `SELECT` per column, not at table level.
  On such a table, `SELECT *` and `get_sample_data` fail with a `DB_ERROR` "permission denied for
  table <name>", while an explicit column list works. On that error, retry with the columns from
  `describe_table` before concluding the table is unreadable.

## The contract

pgllens runs exactly the single `SELECT`/`WITH`/`TABLE`/`VALUES` statement you send, inside a
`default_transaction_read_only=on` session, and returns at most one page of rows per call. It
will not run a write, will not run a second statement, will not unmask a column, and will not
hand back more rows than the cap. Your side of the contract: look at the schema before writing,
validate before running, size a large-table query before running it, page with `ORDER BY` and a
fixed `limit`, and say "more rows exist" or "masked" out loud when the response does.
