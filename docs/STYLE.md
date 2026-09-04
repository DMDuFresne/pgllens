# LLens Markdown Style Guide

*How pgllens writes tool output.* The same style is shared by other LLens servers, and
`src/pgllens/llens_style/` imports nothing from `pgllens.*` so it can be copied verbatim into one.
Companion to LLENS-RS-1. The standard says what is required; this guide shows what it looks like.

---

## The shape of every response

Three parts, always in this order, always separated the same way.

```
## server · tool · scope              ← header, one line
*plane · timestamp · status*          ← metadata, one line

(body)                                ← facts, tables, code

---                                   ← rule
tally line                            ← counts
*Next: tool(args) · tool(args)*       ← follow-ups
```

A reader, model or human, learns this once and then knows where to look in every pgllens result. Do not deviate to "make a special case clearer." The consistency is the clarity.

---

## Header

**One H2. Three parts. Middle dots between them.**

```
## pgllens · describe_table · app_core.asset
## pgllens · get_table_health · app_core
## pgllens · server_info
```

- Server slug is lowercase, no spaces: `pgllens`.
- Tool name exactly as registered.
- Scope is the thing the caller selected: a `schema.table`, a schema name, a function name. Instance-wide tools drop it.
- Separator is ` · ` (space, U+00B7, space). Not `-`, not `|`, not `/`.

**Don't**

```
# Table                               ← H1 is reserved; says nothing about server or scope
## Results for app_core.asset         ← prose, and no tool name
## PgLLens — Describe Table           ← wrong case, wrong separator, wrong name
```

---

## Metadata line

**Italic. Directly under the header. Fields separated by middle dots.**

```
*catalog · 2026-09-03T15:49:03Z*
*catalog+stats · 2026-09-03T15:49:03Z (cached 4m)*
*catalog+stats · 2026-09-03T15:47:12Z · stats stale (last analyze 3d)*
*query · 2026-09-03T15:49:03Z · more rows exist*
```

Three slots:

1. **plane**: where the answer came from: `catalog`, `stats`, `catalog+stats`, or `query`.
2. **timestamp**: ISO-8601 UTC to the second. If cached, append `(cached Nm)`.
3. **status**: *only when something is wrong.* Healthy responses have two fields, not three. Never write `status: ok`.

The metadata line is the only place relative time (`4m`, `22s`) is allowed.

---

## Body

### Pick the shape by the data

| data | shape |
|---|---|
| a handful of named facts | definition bullets |
| many records of the same kind | table |
| a tree up to three deep | nested bullets |
| a tree deeper than three | table with a `path` column |
| code, SQL, logic, DDL | fenced block with a language tag |

Don't mix. A body is bullets *or* a table *or* a code block per section, not a table with a stray bullet under it.

### Definition bullets

```
- kind: `function`
- language: `plpgsql`
- volatility: `stable`
- rows: `~9.2K` (estimate) — reltuples `9187`
```

- Key, colon, space, value. Keys lowercase.
- Value in inline code when it is an identifier, number, address, path, or enum. Plain when it is a sentence.
- Qualifiers in parentheses after the value. Related raw value after an em dash.

**Don't**

```
- **Kind:** function                    ← bold keys, Title Case, no code
- kind = function                       ← wrong delimiter
- The object is a plpgsql function.     ← prose where a fact belongs
```

### Tables

```
| schema | tables | views | rows (estimate) |
|---|---|---|---|
| `app_core` | 6 | 2 | ~9.2K |
| `app_audit` | 2 | 0 | 82 |
```

- Header row always. Lowercase column names. Units or qualifiers in the header (`rows (estimate)`, `size (MB)`), never repeated per cell.
- Identifiers in inline code so they copy-paste into the next call.
- Right-aligning numbers is optional; consistency within a table is not.
- Never more rows than the page size. If there are more, the footer says so.
- Sort deliberately and predictably: by size descending, by name ascending, by scan order. Never in the order the catalog happened to return.

**Don't**

```
| Schema | Tables/Views | Row Estimate |    ← Title Case, compound column
| app_core | 8 | 9187 |                    ← no code, raw number
```

### Section headings inside the body

**H3 only.** Lowercase, one or two words, no punctuation.

```
### identity
### columns
### needs attention
```

Use them when the body has two or more distinct groups of facts. A body with a single table or a single bullet group has no H3 at all. Never H2 (that's the header), never H4.

### Code and logic

```sql
SELECT sensor_id, recorded_at, value FROM app_core.reading WHERE quality = 'suspect' ORDER BY recorded_at DESC LIMIT 20
```

```sql
CREATE TRIGGER work_order_stamp_closed_at BEFORE UPDATE ON app_core.work_order FOR EACH ROW EXECUTE FUNCTION app_core.fn_stamp_closed_at()
```

- Always a language tag: `sql`, `text`. Never a bare fence.
- One statement, one object per block unless the caller asked for a listing.
- Do not annotate inside the fence. Put the explanation in a bullet or blockquote next to it.

### Caveats

**Blockquote. One sentence. Immediately under the thing it qualifies.**

```
- rows: `~9.2K` (estimate) — reltuples `9187`

> Estimate from pg_class.reltuples; last analyze 2026-09-01T02:00:00Z.
```

A caveat is not a footnote. It goes where the reader's eye is when they need it. Never collect caveats at the bottom; never omit them because they clutter.

### Marking what's derived

Anything the server computed rather than read gets a one-word tag in parentheses, right after the value:

```
(decoded)   (estimate)   (inferred)   (heuristic)   (cached)
```

Readers should never wonder whether `~9.2K` was counted or guessed.

---

## Values

### Numbers

| write | not |
|---|---|
| `~9.2K rows` | `9187 rows` when it's an estimate |
| `4 GB` | `524288 × 8kB` |
| `28m` / `3d 4h` | `0:28:02.318735` |
| `5 req/s` | `5 rps` / `5/s` |
| `128,000 rows` | `128000 rows` |
| `82` | `~82` when it's exact |

Abbreviate with `~` and one decimal above 1,000 *when it's an estimate*. Exact counts stay exact. Human units first; the raw value belongs in `response_format: "json"` or a trailing parenthetical when precision matters.

### Timestamps

ISO-8601 UTC, seconds precision, `Z` suffix: `2026-09-03T15:49:03Z`. Everywhere in the body. No local time, no `9/3/2026`, no "today."

### Hex and bit fields

Always paired with meaning: `` `r` `` → `` `table` (decoded) — relkind `r` ``. A raw catalog code alone is a puzzle, not a fact.

### Identifiers

Inline code, exactly as the API accepts them. `` `app_core.asset` ``, `` `CMP-BR01-0001` ``, `` `fn_asset_reading_stats` ``. Never prettified, never title-cased, never split across lines.

---

## Vocabulary

One word per idea across the whole family.

| say | never |
|---|---|
| `target` (the schema, table, or database a call selects) | connection, device, source, DSN (as a selector) |
| `plane` | source, layer, mode |
| `cursor` | token, page token, offset |
| `governor` | throttle, rate limiter |
| `breaker` | circuit breaker (full), fuse, backoff state |
| `estimate` | approx, roughly, about |
| `truncated` | cut, clipped, limited |

If a new concept needs a word, add it here before it appears in output.

---

## Footer

**A horizontal rule, then a tally, then a `Next:` line.**

```
---
3 schemas · 13 objects · ~9.3K rows
*Next: list_tables(schema="app_core") · get_relationships(schema="app_core")*
```

### Tally line

Plain text, middle-dot separated. Counts of what the body shows and, if paged, what exists:

```
200 of 9,187 rows shown · page 1 of 46
3 schemas · 13 objects · ~9.3K rows
3 indexes · 2 unused · 0 invalid · 1 duplicate set · 0 uncovered fks
```

Required whenever the body contains a collection. If anything was truncated below the page size, say what and how: `3 comments truncated at 200 chars`.

### Next line

Italic. Starts with `Next:`. One to three literal tool calls using real values from this response, separated by middle dots.

```
*Next: describe_table(table="app_core.asset") · get_sample_data(table="app_core.asset", limit=5)*
*Next: get_function_source(function="fn_asset_reading_stats", schema="app_core")*
```

Every suggestion must be pasteable. `*Next: explore the tables*` is not a suggestion.

Omit the whole footer only when the body is a handful of scalars with no natural follow-up.

**Don't**

```
Full profile: describe_table(...)         ← no rule, no Next:, not italic
---
Showing some results. Use the cursor to see more.   ← no counts, no cursor
```

---

## Errors

Errors use the same header, with `error` in the scope slot, and a fixed four-bullet body.

```
## pgllens · list_tables · error

- code: `SCHEMA_UNKNOWN`
- message: Schema `public` is not exposed.
- hint: Pass one of the exposed schemas; call schema_overview() to list them.
- request_id: `01J8Q4V7…`
```

- **code**: UPPER_SNAKE from the registry in RS-1 §6. Inline code.
- **message**: one sentence, what went wrong, names the offending value in inline code.
- **hint**: one sentence, what to do instead. A corrected call, a different tool, or a precondition. Mandatory.
- **request_id**: inline code. Always present.

Add `- retry_after: \`22s\`` for `TIMEOUT` and any other retryable code.

No footer on errors. No apology, no "unfortunately," no prose paragraph.

### Partial results are not errors

If three of five tables answered, return a normal response for the three and add:

```
### unavailable
| table | code |
|---|---|
| `app_core.technician` | `DB_ERROR` (permission denied for column badge_pin) |
| `app_core.reading` | `TIMEOUT` (retry 22s) |
```

---

## `response_format` variants

The same tool, three renderings.

**`markdown`** (default): everything above.

**`concise`**: header line, body with no H3s and no blockquotes, tally line, no `Next:`.

```
## pgllens · schema_overview
*catalog+stats · 2026-09-03T15:47:12Z*

| schema | tables | views | rows (estimate) |
|---|---|---|---|
| `app_core` | 6 | 2 | ~9.2K |
| `app_audit` | 2 | 0 | 82 |
| `app_custom` | 2 | 1 | 22 |

---
3 schemas · 13 objects · ~9.3K rows
```

**`json`**: one-line text, data in `structuredContent`.

```
pgllens · schema_overview · 3 schemas · 13 objects (see structuredContent)
```

---

## Tone

- **No filler.** No "Here are the results," "Successfully retrieved," "Note that." The header already says what this is.
- **No first person.** The server does not say "I found." It presents.
- **No hedging adverbs.** Not "approximately 9.2K" but `~9.2K (estimate)`. Not "possibly stale" but `stats stale (last analyze 3d)`.
- **No exclamation marks, no emoji, no bold for emphasis.** Bold is not used anywhere in pgllens output.
- **Sentence case** for anything that is a sentence; **lowercase** for keys, headings, column names.
- **Present tense**, declarative. `kind: table`, not `the relation is currently a regular table`.

---

## Two full examples

### `describe_table`

```
## pgllens · describe_table · app_core.asset
*catalog+stats · 2026-09-03T15:52:40Z*

### identity
- kind: `table`
- primary key: `asset_id`
- rows: `12` (estimate)
- comment: Individual pieces of equipment at a site.

### columns
| column | type | null | default | pk | comment |
|---|---|---|---|---|---|
| `asset_id` | `integer` | no | `identity (always)` | ✓ |  |
| `site_id` | `integer` | no |  |  | FK → `app_core.site` |
| `name` | `text` | no |  |  |  |
| `kind` | `app_core.asset_kind` | no |  |  | Equipment class. Drives which sensor tags are expected. |
| `serial_number` | `text` | no |  |  |  |
| `installed_on` | `date` | no |  |  |  |
| `is_active` | `boolean` | no | `true` |  |  |
| `rated_kw` | `numeric(10,2)` | yes |  |  |  |
| `created_at` | `timestamptz` | no | `now()` |  |  |

### indexes
| name | definition | scans |
|---|---|---|
| `asset_pkey` | `btree (asset_id)` | 1,204 |
| `asset_serial_number_key` | `btree (serial_number)` | 388 |
| `asset_site_id_idx` | `btree (site_id)` | 41 |
| `asset_kind_idx` | `btree (kind) WHERE is_active` | 0 |

> Scan counts since last stats reset 2026-08-30T02:00:00Z.

---
9 columns · 4 indexes · 12 rows (estimate) · 64 kB
*Next: get_sample_data(table="app_core.asset", limit=5) · get_relationships(table="app_core.asset") · get_constraints(table="app_core.asset")*
```

### `list_hypertables`

```
## pgllens · list_hypertables
*catalog · 2026-09-03T15:53:11Z*

### hypertables
| hypertable | time column | chunk interval | compression |
|---|---|---|---|
| `app_core.reading` | `recorded_at` | 1 day | yes |

### jobs
| hypertable | job type | schedule | config | next run |
|---|---|---|---|---|
| `reading` | Compression Policy | 12 hours | `{"compress_after": "7 days"}` | 2026-09-04T02:00:00Z |

### chunks
| hypertable | chunks | range | total | compressed | ratio |
|---|---|---|---|---|---|
| `reading` | 15 | 2026-08-20 → 2026-09-03 | 2.1 MB | 0.6 MB | 3.5x |

> Row counts for the parent relation exclude the per-day child chunks under `_timescaledb_internal`.

---
1 hypertable · 1 job · 0 continuous aggregates
*Next: get_table_health(schema="app_core") · describe_table(table="app_core.reading")*
```

---

## Checklist before a tool ships

- [ ] H2 header: `server · tool · scope`
- [ ] Italic metadata line: plane · ISO timestamp · status only if degraded
- [ ] Body shape matches the data (bullets / table / code)
- [ ] Identifiers in inline code, copy-pasteable
- [ ] Derived values tagged `(decoded)` / `(estimate)` etc.
- [ ] Caveats in blockquotes next to what they qualify
- [ ] Human-readable units, ISO timestamps
- [ ] Vocabulary from the table, nothing else
- [ ] `---` then tally then `Next:` with literal calls
- [ ] Error path uses the four-bullet shape with a real hint
- [ ] `concise` and `json` variants render
- [ ] No bold, no filler, no first person

---

## Enforcement

`src/pgllens/llens_style/lint.py` checks every rule above that can be checked mechanically. Rule ids:
`H2_SHAPE`, `META_LINE`, `NO_H1_H4`, `H3_ONLY`, `NO_BOLD`, `NO_EMOJI`, `FENCE_LANG`, `FOOTER_SHAPE`,
`TABLE_NEEDS_TALLY`, `ERROR_SHAPE`, `NO_FILLER`. `tests/test_style_contract.py` runs it against every
registered tool and compares each output to `tests/golden/<case>.md`. `✓` (U+2713) is a text glyph, allowed
as a boolean mark in tables (for example in the `pk` column above) and is not affected by the emoji ban;
the ban is on emoji code points, not this glyph.
