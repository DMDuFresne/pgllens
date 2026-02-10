/**
 * List Hypertables tool - TimescaleDB awareness: hypertables, policies, continuous aggregates, chunks
 */

import { rawQuery } from '../database/client.js';
import { getDefaultSchema } from '../config.js';

interface ListHypertablesToolInput {
  schema?: string;
  table?: string;
}

export async function executeListHypertablesTool(
  input: ListHypertablesToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schemaName = input.schema || getDefaultSchema();

    // Step 1: Detect TimescaleDB
    const extResult = await rawQuery<{ exists: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') AS exists`
    );

    if (!extResult.rows[0]?.exists) {
      return {
        content: [
          {
            type: 'text',
            text: `TimescaleDB extension is not installed in this database.\n\nTo enable TimescaleDB:\n\`\`\`sql\nCREATE EXTENSION IF NOT EXISTS timescaledb;\n\`\`\`\n\nUse \`list_extensions\` to see what extensions are currently installed.`,
          },
        ],
      };
    }

    const lines: string[] = [];
    lines.push('## TimescaleDB Hypertables\n');

    // Step 2: Hypertables + dimensions
    let hypertables: Array<{
      hypertable_schema: string;
      hypertable_name: string;
      compression_enabled: boolean;
      num_dimensions: string;
      time_column: string | null;
      time_interval: string | null;
    }> = [];

    try {
      const htParams: unknown[] = [schemaName];
      let htFilter = 'WHERE h.hypertable_schema = $1';
      if (input.table) {
        htParams.push(input.table);
        htFilter += ' AND h.hypertable_name = $2';
      }

      const htSql = `
        SELECT
          h.hypertable_schema,
          h.hypertable_name,
          h.compression_enabled,
          h.num_dimensions,
          d.column_name AS time_column,
          d.time_interval::text AS time_interval
        FROM timescaledb_information.hypertables h
        LEFT JOIN timescaledb_information.dimensions d
          ON h.hypertable_schema = d.hypertable_schema
          AND h.hypertable_name = d.hypertable_name
          AND d.dimension_number = 1
        ${htFilter}
        ORDER BY h.hypertable_schema, h.hypertable_name
      `;

      const htResult = await rawQuery<{
        hypertable_schema: string;
        hypertable_name: string;
        compression_enabled: boolean;
        num_dimensions: string;
        time_column: string | null;
        time_interval: string | null;
      }>(htSql, htParams);

      hypertables = htResult.rows;
    } catch (e) {
      lines.push(
        `*Warning: Could not query hypertable information. ${e instanceof Error ? e.message : ''}*\n`
      );
    }

    if (hypertables.length === 0) {
      const msg = input.table
        ? `No hypertable named "${input.table}" found in schema "${schemaName}".`
        : `No hypertables found in schema "${schemaName}".`;
      lines.push(msg);
      return { content: [{ type: 'text', text: lines.join('\n') }] };
    }

    // Summary table
    lines.push('| Hypertable | Time Column | Chunk Interval | Compression | Dimensions |');
    lines.push('|------------|-------------|----------------|-------------|------------|');
    for (const ht of hypertables) {
      lines.push(
        `| ${ht.hypertable_schema}.${ht.hypertable_name} | ${ht.time_column || '—'} | ${ht.time_interval || '—'} | ${ht.compression_enabled ? 'Enabled' : 'Disabled'} | ${ht.num_dimensions} |`
      );
    }
    lines.push('');

    // Step 3: Policies/Jobs
    try {
      const jobParams: unknown[] = [schemaName];
      let jobFilter = 'WHERE j.hypertable_schema = $1';
      if (input.table) {
        jobParams.push(input.table);
        jobFilter += ' AND j.hypertable_name = $2';
      }

      const jobSql = `
        SELECT
          j.hypertable_schema,
          j.hypertable_name,
          j.job_id,
          j.application_name AS job_type,
          j.schedule_interval::text AS schedule,
          j.config::text AS config,
          j.next_start::text AS next_start
        FROM timescaledb_information.jobs j
        ${jobFilter}
        ORDER BY j.hypertable_schema, j.hypertable_name, j.application_name
      `;

      const jobResult = await rawQuery<{
        hypertable_schema: string;
        hypertable_name: string;
        job_id: string;
        job_type: string;
        schedule: string;
        config: string | null;
        next_start: string | null;
      }>(jobSql, jobParams);

      if (jobResult.rows.length > 0) {
        lines.push('### Policies & Jobs\n');
        lines.push('| Hypertable | Job Type | Schedule | Config | Next Run |');
        lines.push('|------------|----------|----------|--------|----------|');

        for (const j of jobResult.rows) {
          const config = j.config
            ? j.config.length > 60
              ? j.config.slice(0, 57) + '...'
              : j.config
            : '—';
          lines.push(
            `| ${j.hypertable_name} | ${j.job_type} | ${j.schedule} | ${config} | ${j.next_start ? new Date(j.next_start).toISOString().slice(0, 19) : '—'} |`
          );
        }
        lines.push('');
      }
    } catch {
      // Jobs view may not exist in older versions
    }

    // Step 4: Continuous Aggregates
    try {
      const caSql = `
        SELECT
          ca.materialization_hypertable_schema,
          ca.materialization_hypertable_name,
          ca.view_schema,
          ca.view_name,
          ca.view_definition
        FROM timescaledb_information.continuous_aggregates ca
        WHERE ca.hypertable_schema = $1
        ${input.table ? 'AND ca.hypertable_name = $2' : ''}
        ORDER BY ca.view_schema, ca.view_name
      `;

      const caParams: unknown[] = [schemaName];
      if (input.table) caParams.push(input.table);

      const caResult = await rawQuery<{
        materialization_hypertable_schema: string;
        materialization_hypertable_name: string;
        view_schema: string;
        view_name: string;
        view_definition: string | null;
      }>(caSql, caParams);

      if (caResult.rows.length > 0) {
        lines.push('### Continuous Aggregates\n');

        for (const ca of caResult.rows) {
          lines.push(`**${ca.view_schema}.${ca.view_name}**`);
          lines.push(
            `Materialization: ${ca.materialization_hypertable_schema}.${ca.materialization_hypertable_name}`
          );
          if (ca.view_definition) {
            lines.push('```sql');
            lines.push(ca.view_definition);
            lines.push('```');
          }
          lines.push('');
        }
      }
    } catch {
      // Continuous aggregates view may differ across versions
    }

    // Step 5: Chunk Statistics
    try {
      const chunkParams: unknown[] = [schemaName];
      let chunkFilter = 'WHERE ch.hypertable_schema = $1';
      if (input.table) {
        chunkParams.push(input.table);
        chunkFilter += ' AND ch.hypertable_name = $2';
      }

      const chunkSql = `
        SELECT
          ch.hypertable_schema,
          ch.hypertable_name,
          count(*) AS chunk_count,
          min(ch.range_start)::text AS range_start,
          max(ch.range_end)::text AS range_end,
          sum(ch.chunk_size)::bigint AS total_bytes,
          sum(ch.compressed_chunk_size)::bigint AS compressed_bytes
        FROM (
          SELECT
            ch.hypertable_schema,
            ch.hypertable_name,
            ch.range_start,
            ch.range_end,
            COALESCE(chs.total_bytes, 0) AS chunk_size,
            COALESCE(chs.compressed_total_bytes, 0) AS compressed_chunk_size
          FROM timescaledb_information.chunks ch
          LEFT JOIN timescaledb_information.chunk_size chs
            ON ch.chunk_schema = chs.chunk_schema
            AND ch.chunk_name = chs.chunk_name
          ${chunkFilter}
        ) ch
        GROUP BY ch.hypertable_schema, ch.hypertable_name
        ORDER BY ch.hypertable_schema, ch.hypertable_name
      `;

      const chunkResult = await rawQuery<{
        hypertable_schema: string;
        hypertable_name: string;
        chunk_count: string;
        range_start: string | null;
        range_end: string | null;
        total_bytes: string | null;
        compressed_bytes: string | null;
      }>(chunkSql, chunkParams);

      if (chunkResult.rows.length > 0) {
        lines.push('### Chunk Statistics\n');
        lines.push('| Hypertable | Chunks | Date Range | Total Size | Compressed | Ratio |');
        lines.push('|------------|--------|------------|------------|------------|-------|');

        for (const c of chunkResult.rows) {
          const totalBytes = parseInt(c.total_bytes || '0', 10);
          const compressedBytes = parseInt(c.compressed_bytes || '0', 10);
          const ratio =
            totalBytes > 0 && compressedBytes > 0
              ? `${(totalBytes / compressedBytes).toFixed(1)}x`
              : '—';
          const range =
            c.range_start && c.range_end
              ? `${c.range_start.slice(0, 10)} → ${c.range_end.slice(0, 10)}`
              : '—';

          lines.push(
            `| ${c.hypertable_name} | ${c.chunk_count} | ${range} | ${formatBytes(totalBytes)} | ${compressedBytes > 0 ? formatBytes(compressedBytes) : '—'} | ${ratio} |`
          );
        }
        lines.push('');
      }
    } catch {
      // chunk_size view may not be available in all TimescaleDB versions;
      // try simpler fallback
      try {
        const simpleChunkParams: unknown[] = [schemaName];
        let simpleFilter = 'WHERE ch.hypertable_schema = $1';
        if (input.table) {
          simpleChunkParams.push(input.table);
          simpleFilter += ' AND ch.hypertable_name = $2';
        }

        const simpleChunkSql = `
          SELECT
            ch.hypertable_schema,
            ch.hypertable_name,
            count(*) AS chunk_count,
            min(ch.range_start)::text AS range_start,
            max(ch.range_end)::text AS range_end
          FROM timescaledb_information.chunks ch
          ${simpleFilter}
          GROUP BY ch.hypertable_schema, ch.hypertable_name
          ORDER BY ch.hypertable_schema, ch.hypertable_name
        `;

        const simpleResult = await rawQuery<{
          hypertable_schema: string;
          hypertable_name: string;
          chunk_count: string;
          range_start: string | null;
          range_end: string | null;
        }>(simpleChunkSql, simpleChunkParams);

        if (simpleResult.rows.length > 0) {
          lines.push('### Chunk Statistics\n');
          lines.push('| Hypertable | Chunks | Date Range |');
          lines.push('|------------|--------|------------|');

          for (const c of simpleResult.rows) {
            const range =
              c.range_start && c.range_end
                ? `${c.range_start.slice(0, 10)} → ${c.range_end.slice(0, 10)}`
                : '—';
            lines.push(`| ${c.hypertable_name} | ${c.chunk_count} | ${range} |`);
          }
          lines.push('');
        }
      } catch {
        // Silently skip chunk stats if not available
      }
    }

    return { content: [{ type: 'text', text: lines.join('\n') }] };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';
    return {
      content: [
        { type: 'text', text: `Error listing hypertables: ${errorMessage}` },
      ],
      isError: true,
    };
  }
}

function formatBytes(n: number): string {
  if (n <= 0) return '0 B';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}
