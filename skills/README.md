# pgllens skills

Eight [Claude skills](https://docs.claude.com/en/docs/claude-code/skills) that teach a client how
to drive PgLLens. Start with `pgllens-using`: it sets the read-only posture and house rules and
routes to the other seven. Copy the directories into your project's `.claude/skills/` and Claude
Code picks the right one from each skill's trigger phrases. Every skill is database-agnostic and
read-only; none assumes a particular schema, column, or naming convention.

| Skill | Use when |
|---|---|
| `pgllens-using` | Working with a PostgreSQL database through the pgllens MCP server, at the start of any database task or when unsure which tool or skill fits. |
| `pgllens-explore-a-database` | Getting oriented in an unfamiliar PostgreSQL database before writing any query against a schema you haven't looked at yet. |
| `pgllens-write-a-query` | Writing an analytical SQL query against a PostgreSQL database, as opposed to tuning a slow one or exploring the schema. |
| `pgllens-tune-a-query` | A PostgreSQL query is slow and needs tuning: validate, explain, index, pg_stat_statements. |
| `pgllens-health-check` | A general PostgreSQL health sweep across vacuum/bloat, index health, blocking, waits, and space usage. |
| `pgllens-triage-an-incident` | A PostgreSQL database is misbehaving right now and someone needs to know why, ending in a named operator action. |
| `pgllens-document-a-schema` | Asked to produce a written reference document (data dictionary, schema docs, ERD and table reference) from the live catalog. |
| `pgllens-verify-a-deployment` | An operator has just deployed or upgraded pgllens, or changed its configuration, and wants proof through the MCP client that it works. |
