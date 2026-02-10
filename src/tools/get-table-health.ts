/**
 * Get Table Health tool - Vacuum/analyze stats, dead tuples, sizes, index usage
 */

import { rawQuery } from '../database/client.js';
import { getConfig, getDefaultSchema } from '../config.js';

interface GetTableHealthToolInput {
  schema?: string;
  table?: string;
}

export async function executeGetTableHealthTool(
  input: GetTableHealthToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    if (input.table) {
      return renderDetail(input.schema || getDefaultSchema(), input.table);
    }
    return renderSummary(input.schema);
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';
    return {
      content: [{ type: 'text', text: `Error getting table health: ${errorMessage}` }],
      isError: true,
    };
  }
}

// ─── Summary Mode ────────────────────────────────────────────────────────────

async function renderSummary(
  schemaFilter?: string
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  const config = getConfig();
  const schemas = schemaFilter ? [schemaFilter] : config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  const sql = `
    SELECT
      s.schemaname AS schema_name,
      s.relname AS table_name,
      s.n_live_tup AS live_tuples,
      s.n_dead_tup AS dead_tuples,
      CASE WHEN (s.n_live_tup + s.n_dead_tup) > 0
        THEN round(100.0 * s.n_dead_tup / (s.n_live_tup + s.n_dead_tup), 1)
        ELSE 0
      END AS dead_pct,
      s.last_vacuum,
      s.last_autovacuum,
      s.last_analyze,
      s.last_autoanalyze,
      pg_total_relation_size(s.relid) AS total_size,
      pg_relation_size(s.relid) AS table_size,
      pg_indexes_size(s.relid) AS index_size
    FROM pg_stat_user_tables s
    WHERE s.schemaname IN (${schemaPlaceholders})
    ORDER BY s.n_dead_tup DESC, s.n_live_tup DESC
  `;

  const result = await rawQuery<{
    schema_name: string;
    table_name: string;
    live_tuples: string;
    dead_tuples: string;
    dead_pct: string;
    last_vacuum: string | null;
    last_autovacuum: string | null;
    last_analyze: string | null;
    last_autoanalyze: string | null;
    total_size: string;
    table_size: string;
    index_size: string;
  }>(sql, schemas);

  if (result.rows.length === 0) {
    return {
      content: [{ type: 'text', text: 'No tables found in the specified schema(s).' }],
    };
  }

  const lines: string[] = [];
  lines.push('## Table Health Summary\n');
  lines.push('| Table | Live | Dead | Dead % | Total Size | Last Vacuum | Last Analyze |');
  lines.push('|-------|------|------|--------|------------|-------------|--------------|');

  const needsAttention: string[] = [];

  for (const r of result.rows) {
    const displayName = schemaFilter
      ? r.table_name
      : `${r.schema_name}.${r.table_name}`;
    const deadPct = parseFloat(r.dead_pct);
    const lastVac = r.last_autovacuum || r.last_vacuum;
    const lastAn = r.last_autoanalyze || r.last_analyze;

    lines.push(
      `| ${displayName} | ${fmtNum(r.live_tuples)} | ${fmtNum(r.dead_tuples)} | ${r.dead_pct}% | ${formatBytes(parseInt(r.total_size, 10))} | ${fmtDate(lastVac)} | ${fmtDate(lastAn)} |`
    );

    if (deadPct > 5) {
      needsAttention.push(`${displayName}: ${r.dead_pct}% dead tuples`);
    } else if (!lastVac) {
      needsAttention.push(`${displayName}: never vacuumed`);
    }
  }

  lines.push('');

  if (needsAttention.length > 0) {
    lines.push('### Tables Needing Attention\n');
    for (const msg of needsAttention) {
      lines.push(`- ${msg}`);
    }
    lines.push('');
  }

  lines.push(`*${result.rows.length} table(s) inspected. Use \`get_table_health\` with a specific table for detailed index usage.*`);

  return { content: [{ type: 'text', text: lines.join('\n') }] };
}

// ─── Detail Mode ─────────────────────────────────────────────────────────────

async function renderDetail(
  schemaName: string,
  tableName: string
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  // Table stats
  const statsSql = `
    SELECT
      s.n_live_tup AS live_tuples,
      s.n_dead_tup AS dead_tuples,
      CASE WHEN (s.n_live_tup + s.n_dead_tup) > 0
        THEN round(100.0 * s.n_dead_tup / (s.n_live_tup + s.n_dead_tup), 1)
        ELSE 0
      END AS dead_pct,
      s.last_vacuum,
      s.last_autovacuum,
      s.last_analyze,
      s.last_autoanalyze,
      s.vacuum_count,
      s.autovacuum_count,
      s.analyze_count,
      s.autoanalyze_count,
      s.n_tup_ins AS inserts_since_vacuum,
      s.n_tup_upd AS updates_since_vacuum,
      s.n_tup_del AS deletes_since_vacuum,
      pg_total_relation_size(s.relid) AS total_size,
      pg_relation_size(s.relid) AS table_size,
      pg_indexes_size(s.relid) AS index_size
    FROM pg_stat_user_tables s
    WHERE s.schemaname = $1
      AND s.relname = $2
  `;

  const statsResult = await rawQuery<{
    live_tuples: string;
    dead_tuples: string;
    dead_pct: string;
    last_vacuum: string | null;
    last_autovacuum: string | null;
    last_analyze: string | null;
    last_autoanalyze: string | null;
    vacuum_count: string;
    autovacuum_count: string;
    analyze_count: string;
    autoanalyze_count: string;
    inserts_since_vacuum: string;
    updates_since_vacuum: string;
    deletes_since_vacuum: string;
    total_size: string;
    table_size: string;
    index_size: string;
  }>(statsSql, [schemaName, tableName]);

  if (statsResult.rows.length === 0) {
    return {
      content: [
        { type: 'text', text: `Table ${schemaName}.${tableName} not found.` },
      ],
      isError: true,
    };
  }

  const s = statsResult.rows[0];
  const totalSize = parseInt(s.total_size, 10);
  const tableSize = parseInt(s.table_size, 10);
  const indexSize = parseInt(s.index_size, 10);
  const toastSize = totalSize - tableSize - indexSize;

  const lines: string[] = [];
  lines.push(`## Table Health: ${schemaName}.${tableName}\n`);

  // Size breakdown
  lines.push('### Size Breakdown');
  lines.push(`| Component | Size |`);
  lines.push(`|-----------|------|`);
  lines.push(`| Table data | ${formatBytes(tableSize)} |`);
  lines.push(`| Indexes | ${formatBytes(indexSize)} |`);
  if (toastSize > 0) lines.push(`| TOAST | ${formatBytes(toastSize)} |`);
  lines.push(`| **Total** | **${formatBytes(totalSize)}** |`);
  lines.push('');

  // Tuple stats
  lines.push('### Tuple Statistics');
  lines.push(`| Metric | Value |`);
  lines.push(`|--------|-------|`);
  lines.push(`| Live tuples | ${fmtNum(s.live_tuples)} |`);
  lines.push(`| Dead tuples | ${fmtNum(s.dead_tuples)} |`);
  lines.push(`| Dead tuple % | ${s.dead_pct}% |`);
  lines.push(`| Inserts (total) | ${fmtNum(s.inserts_since_vacuum)} |`);
  lines.push(`| Updates (total) | ${fmtNum(s.updates_since_vacuum)} |`);
  lines.push(`| Deletes (total) | ${fmtNum(s.deletes_since_vacuum)} |`);
  lines.push('');

  // Maintenance history
  lines.push('### Maintenance History');
  lines.push(`| Operation | Last Run | Count |`);
  lines.push(`|-----------|----------|-------|`);
  lines.push(`| Manual VACUUM | ${fmtDate(s.last_vacuum)} | ${fmtNum(s.vacuum_count)} |`);
  lines.push(`| Auto VACUUM | ${fmtDate(s.last_autovacuum)} | ${fmtNum(s.autovacuum_count)} |`);
  lines.push(`| Manual ANALYZE | ${fmtDate(s.last_analyze)} | ${fmtNum(s.analyze_count)} |`);
  lines.push(`| Auto ANALYZE | ${fmtDate(s.last_autoanalyze)} | ${fmtNum(s.autoanalyze_count)} |`);
  lines.push('');

  // Index usage stats
  const indexSql = `
    SELECT
      i.indexrelname AS index_name,
      i.idx_scan AS scans,
      i.idx_tup_read AS tuples_read,
      i.idx_tup_fetch AS tuples_fetched,
      pg_relation_size(i.indexrelid) AS index_size
    FROM pg_stat_user_indexes i
    WHERE i.schemaname = $1
      AND i.relname = $2
    ORDER BY i.idx_scan DESC
  `;

  const indexResult = await rawQuery<{
    index_name: string;
    scans: string;
    tuples_read: string;
    tuples_fetched: string;
    index_size: string;
  }>(indexSql, [schemaName, tableName]);

  if (indexResult.rows.length > 0) {
    lines.push('### Index Usage');
    lines.push('| Index | Scans | Tuples Read | Tuples Fetched | Size |');
    lines.push('|-------|-------|-------------|----------------|------|');

    const unused: string[] = [];

    for (const idx of indexResult.rows) {
      const scans = parseInt(idx.scans, 10);
      lines.push(
        `| ${idx.index_name} | ${fmtNum(idx.scans)} | ${fmtNum(idx.tuples_read)} | ${fmtNum(idx.tuples_fetched)} | ${formatBytes(parseInt(idx.index_size, 10))} |`
      );
      if (scans === 0) {
        unused.push(`${idx.index_name} (${formatBytes(parseInt(idx.index_size, 10))})`);
      }
    }

    lines.push('');

    if (unused.length > 0) {
      lines.push('### Unused Indexes (0 scans)\n');
      for (const u of unused) {
        lines.push(`- ${u}`);
      }
      lines.push('');
      lines.push(
        '*These indexes consume disk space but have never been used for lookups since the last stats reset. Consider dropping if not needed for constraints.*'
      );
      lines.push('');
    }
  }

  return { content: [{ type: 'text', text: lines.join('\n') }] };
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function formatBytes(n: number): string {
  if (n < 0) return '0 B';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function fmtNum(val: string | number): string {
  const n = typeof val === 'string' ? parseInt(val, 10) : val;
  if (isNaN(n)) return '0';
  return n.toLocaleString('en-US');
}

function fmtDate(val: string | null): string {
  if (!val) return 'Never';
  const d = new Date(val);
  return d.toISOString().replace('T', ' ').slice(0, 19);
}
