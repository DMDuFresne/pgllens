/**
 * Describe Table tool - Get detailed table/view information
 */

import { generateTableDescription } from '../descriptions/generator.js';
import { getDefaultSchema } from '../config.js';

interface DescribeTableToolInput {
  schema?: string;
  table: string;
}

export async function executeDescribeTableTool(
  input: DescribeTableToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schemaName = input.schema || getDefaultSchema();
    const description = await generateTableDescription(schemaName, input.table);

    return {
      content: [{ type: 'text', text: description }],
    };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';

    return {
      content: [
        {
          type: 'text',
          text: `Error describing table: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}
