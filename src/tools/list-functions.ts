/**
 * List Functions tool - List all functions with parameters and descriptions
 */

import { getSchemaMetadata } from '../database/schema-loader.js';
import { getFunctionDescription } from '../descriptions/generator.js';
import { FunctionInfo } from '../types/index.js';

interface ListFunctionsToolInput {
  schema?: string;
}

/**
 * Format function signature for display
 */
function formatFunctionSignature(func: FunctionInfo): string {
  const params = func.parameters
    .map(p => {
      let param = '';
      if (p.mode !== 'IN') param += `${p.mode} `;
      if (p.name) param += `${p.name} `;
      param += p.dataType;
      if (p.defaultValue) param += ` DEFAULT ${p.defaultValue}`;
      return param.trim();
    })
    .join(', ');

  return `${func.functionName}(${params}) → ${func.returnType}`;
}

export async function executeListFunctionsTool(
  input: ListFunctionsToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schema = await getSchemaMetadata();
    let functions = schema.functions;

    if (input.schema) {
      functions = functions.filter(
        f => f.schemaName.toLowerCase() === input.schema!.toLowerCase()
      );
    }

    // Group by schema
    const grouped = new Map<string, FunctionInfo[]>();
    for (const func of functions) {
      if (!grouped.has(func.schemaName)) {
        grouped.set(func.schemaName, []);
      }
      grouped.get(func.schemaName)!.push(func);
    }

    const lines: string[] = [];

    for (const [schemaName, schemaFunctions] of grouped) {
      lines.push(`\n## Schema: ${schemaName}`);
      lines.push('');

      for (const func of schemaFunctions) {
        const signature = formatFunctionSignature(func);
        const description = getFunctionDescription(func);

        lines.push(`### ${func.functionName}`);
        lines.push(`\`\`\`sql`);
        lines.push(signature);
        lines.push(`\`\`\``);
        lines.push(description.split('\n')[0]); // First line of description
        if (func.isVolatile) {
          lines.push('*Note: This function may modify data (VOLATILE)*');
        }
        lines.push('');
      }
    }

    if (lines.length === 0) {
      lines.push('No functions found.');
    }

    return {
      content: [{ type: 'text', text: lines.join('\n') }],
    };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';

    return {
      content: [
        {
          type: 'text',
          text: `Error listing functions: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}
