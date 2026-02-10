/**
 * Get Function Source tool - Full function/procedure definition with overload handling
 */

import { rawQuery } from '../database/client.js';
import { getDefaultSchema } from '../config.js';

const MAX_OUTPUT_CHARS = 100_000;

interface GetFunctionSourceToolInput {
  schema?: string;
  function_name: string;
}

export async function executeGetFunctionSourceTool(
  input: GetFunctionSourceToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schemaName = input.schema || getDefaultSchema();

    const sql = `
      SELECT
        p.oid,
        pg_get_functiondef(p.oid) AS full_definition,
        p.prosrc AS source,
        pg_get_function_result(p.oid) AS return_type,
        pg_get_function_arguments(p.oid) AS arguments,
        l.lanname AS language,
        p.provolatile AS volatility,
        p.prosecdef AS security_definer,
        p.proisstrict AS is_strict,
        p.prokind AS kind,
        obj_description(p.oid, 'pg_proc') AS comment
      FROM pg_proc p
      JOIN pg_namespace n ON p.pronamespace = n.oid
      JOIN pg_language l ON p.prolang = l.oid
      WHERE n.nspname = $1
        AND p.proname = $2
      ORDER BY pg_get_function_arguments(p.oid)
    `;

    const result = await rawQuery<{
      oid: number;
      full_definition: string | null;
      source: string;
      return_type: string;
      arguments: string;
      language: string;
      volatility: string;
      security_definer: boolean;
      is_strict: boolean;
      kind: string;
      comment: string | null;
    }>(sql, [schemaName, input.function_name]);

    if (result.rows.length === 0) {
      return {
        content: [
          {
            type: 'text',
            text: `Function ${schemaName}.${input.function_name} not found. Check the name and schema.`,
          },
        ],
        isError: true,
      };
    }

    const lines: string[] = [];
    const isOverloaded = result.rows.length > 1;

    lines.push(`## Function: ${schemaName}.${input.function_name}`);
    if (isOverloaded) {
      lines.push(`\n*${result.rows.length} overloads found.*\n`);
    }
    lines.push('');

    for (let i = 0; i < result.rows.length; i++) {
      const fn = result.rows[i];

      if (isOverloaded) {
        lines.push(`### Overload ${i + 1}`);
        lines.push('');
      }

      // Metadata
      const kindLabel: Record<string, string> = {
        f: 'Function',
        p: 'Procedure',
        a: 'Aggregate',
        w: 'Window Function',
      };
      const volatilityLabel: Record<string, string> = {
        v: 'VOLATILE',
        s: 'STABLE',
        i: 'IMMUTABLE',
      };

      lines.push(`**Kind**: ${kindLabel[fn.kind] || fn.kind}`);
      lines.push(`**Arguments**: ${fn.arguments || '(none)'}`);
      lines.push(`**Returns**: ${fn.return_type}`);
      lines.push(`**Language**: ${fn.language}`);
      lines.push(`**Volatility**: ${volatilityLabel[fn.volatility] || fn.volatility}`);

      if (fn.security_definer) lines.push('**Security**: DEFINER');
      if (fn.is_strict) lines.push('**Strict**: Yes (returns NULL on NULL input)');
      if (fn.comment) lines.push(`**Description**: ${fn.comment}`);
      lines.push('');

      // Full definition
      lines.push('```sql');
      lines.push(fn.full_definition || `-- Source body:\n${fn.source}`);
      lines.push('```');
      lines.push('');
    }

    let output = lines.join('\n');
    if (output.length > MAX_OUTPUT_CHARS) {
      output =
        output.slice(0, MAX_OUTPUT_CHARS) +
        '\n\n--- OUTPUT TRUNCATED ---\n' +
        'The function source is very large. Consider inspecting specific overloads.';
    }

    return { content: [{ type: 'text', text: output }] };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';
    return {
      content: [
        { type: 'text', text: `Error getting function source: ${errorMessage}` },
      ],
      isError: true,
    };
  }
}
