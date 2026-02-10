/**
 * List Tables tool - List all tables and views with descriptions
 */

import { generateTableListDescription } from '../descriptions/generator.js';

interface ListTablesToolInput {
  schema?: string;
}

export async function executeListTablesTool(
  input: ListTablesToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const description = await generateTableListDescription(input.schema);

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
          text: `Error listing tables: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}
