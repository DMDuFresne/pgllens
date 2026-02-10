/**
 * Get Ontology tool - Provides semantic understanding of the database
 *
 * Goes beyond structural schema discovery to extract meaning:
 * check constraints, enum types, unique keys, view definitions,
 * index patterns, triggers, and estimated row counts.
 *
 * All queries use pg_catalog (not information_schema) so they work
 * correctly with read-only database users.
 */

import { getOntologyMetadata } from '../database/schema-loader.js';
import { getDatabaseContext } from '../database/schema-loader.js';
import { getConfig } from '../config.js';
import {
  CheckConstraintInfo,
  EnumTypeInfo,
  UniqueConstraintInfo,
  ViewDefinitionInfo,
  IndexInfo,
  TriggerInfo,
  OntologyMetadata,
} from '../types/index.js';

const VALID_SECTIONS = [
  'overview',
  'constraints',
  'enums',
  'relationships',
  'views',
  'indexes',
  'triggers',
  'domain_context',
] as const;

type OntologySection = typeof VALID_SECTIONS[number];

const MAX_OUTPUT_CHARS = 100_000;
const MAX_VIEW_DEF_CHARS = 500;

interface GetOntologyToolInput {
  schema?: string;
  table?: string;
  sections?: OntologySection[];
}

export async function executeGetOntologyTool(
  input: GetOntologyToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const ontology = await getOntologyMetadata();
    const dbContext = await getDatabaseContext();
    const config = getConfig();

    const sections = new Set<OntologySection>(
      input.sections || [...VALID_SECTIONS]
    );

    // Apply schema filter
    const schemaFilter = input.schema?.toLowerCase();
    const filtered = schemaFilter
      ? filterBySchema(ontology, schemaFilter)
      : ontology;

    // Apply table focus filter (includes FK neighborhood)
    const focused = input.table
      ? filterByTable(filtered, input.table.toLowerCase())
      : filtered;

    // Build output
    const lines: string[] = [];

    lines.push('# Database Ontology');
    lines.push('');

    // Overview section
    if (sections.has('overview')) {
      lines.push(...renderOverview(dbContext, focused, schemaFilter));
    }

    // Domain context section
    if (sections.has('domain_context') && config.domainContext) {
      lines.push(...renderDomainContext(config.domainContext));
    }

    // Constraints section
    if (sections.has('constraints')) {
      lines.push(...renderCheckConstraints(focused.checkConstraints));
    }

    // Enums section
    if (sections.has('enums')) {
      lines.push(...renderEnumTypes(focused.enumTypes));
    }

    // Relationships (unique constraints / natural keys)
    if (sections.has('relationships')) {
      lines.push(...renderUniqueConstraints(focused.uniqueConstraints));
    }

    // Views section
    if (sections.has('views')) {
      lines.push(...renderViewDefinitions(focused.viewDefinitions));
    }

    // Indexes section
    if (sections.has('indexes')) {
      lines.push(...renderIndexes(focused.indexes));
    }

    // Triggers section
    if (sections.has('triggers')) {
      lines.push(...renderTriggers(focused.triggers));
    }

    // Metadata footer
    lines.push('---');
    lines.push(
      `*Ontology refreshed: ${focused.lastRefreshed.toISOString()}*`
    );

    let output = lines.join('\n');

    // Hard output limit
    if (output.length > MAX_OUTPUT_CHARS) {
      output =
        output.slice(0, MAX_OUTPUT_CHARS) +
        '\n\n--- OUTPUT TRUNCATED ---\n' +
        'Use `schema` or `table` params to narrow the scope, or `sections` to select specific parts.';
    }

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';

    return {
      content: [
        {
          type: 'text',
          text: `Error loading ontology: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}

// ─── Filters ────────────────────────────────────────────────────────────────

function filterBySchema(
  ontology: OntologyMetadata,
  schema: string
): OntologyMetadata {
  return {
    checkConstraints: ontology.checkConstraints.filter(
      c => c.schemaName.toLowerCase() === schema
    ),
    enumTypes: ontology.enumTypes.filter(
      e => e.schemaName.toLowerCase() === schema
    ),
    uniqueConstraints: ontology.uniqueConstraints.filter(
      u => u.schemaName.toLowerCase() === schema
    ),
    viewDefinitions: ontology.viewDefinitions.filter(
      v => v.schemaName.toLowerCase() === schema
    ),
    indexes: ontology.indexes.filter(
      i => i.schemaName.toLowerCase() === schema
    ),
    triggers: ontology.triggers.filter(
      t => t.schemaName.toLowerCase() === schema
    ),
    estimatedRowCounts: ontology.estimatedRowCounts.filter(
      r => r.schemaName.toLowerCase() === schema
    ),
    lastRefreshed: ontology.lastRefreshed,
  };
}

function filterByTable(
  ontology: OntologyMetadata,
  table: string
): OntologyMetadata {
  // Collect FK neighborhood: tables referenced by this table's indexes/constraints
  const neighborhood = new Set<string>([table]);

  // Add tables that share constraints or indexes with the target
  for (const cc of ontology.checkConstraints) {
    if (cc.tableName.toLowerCase() === table) {
      neighborhood.add(cc.tableName.toLowerCase());
    }
  }

  const matchesNeighborhood = (tableName: string) =>
    neighborhood.has(tableName.toLowerCase());

  return {
    checkConstraints: ontology.checkConstraints.filter(c =>
      matchesNeighborhood(c.tableName)
    ),
    enumTypes: ontology.enumTypes, // enums are type-level, always include
    uniqueConstraints: ontology.uniqueConstraints.filter(u =>
      matchesNeighborhood(u.tableName)
    ),
    viewDefinitions: ontology.viewDefinitions.filter(
      v =>
        matchesNeighborhood(v.viewName) ||
        v.definition.toLowerCase().includes(table)
    ),
    indexes: ontology.indexes.filter(i => matchesNeighborhood(i.tableName)),
    triggers: ontology.triggers.filter(t =>
      matchesNeighborhood(t.tableName)
    ),
    estimatedRowCounts: ontology.estimatedRowCounts.filter(r =>
      matchesNeighborhood(r.tableName)
    ),
    lastRefreshed: ontology.lastRefreshed,
  };
}

// ─── Renderers ──────────────────────────────────────────────────────────────

function renderOverview(
  dbContext: { databaseName: string; databaseComment: string | null; schemaComments: Record<string, string> },
  ontology: OntologyMetadata,
  schemaFilter?: string
): string[] {
  const lines: string[] = [];

  lines.push('## Database Overview');
  lines.push('');
  lines.push(`**Database**: ${dbContext.databaseName}`);
  if (dbContext.databaseComment) {
    lines.push(`**Description**: ${dbContext.databaseComment}`);
  }
  lines.push('');

  // Schema comments
  const schemaEntries = Object.entries(dbContext.schemaComments);
  const filteredSchemas = schemaFilter
    ? schemaEntries.filter(([name]) => name.toLowerCase() === schemaFilter)
    : schemaEntries;

  if (filteredSchemas.length > 0) {
    lines.push('### Schemas');
    for (const [name, comment] of filteredSchemas) {
      lines.push(`- **${name}**: ${comment}`);
    }
    lines.push('');
  }

  // Table row counts
  if (ontology.estimatedRowCounts.length > 0) {
    lines.push('### Table Scale (Estimated Rows)');
    lines.push('| Table | Est. Rows | Status |');
    lines.push('|-------|-----------|--------|');
    for (const rc of ontology.estimatedRowCounts) {
      const displayName = schemaFilter
        ? rc.tableName
        : `${rc.schemaName}.${rc.tableName}`;
      const status = rc.neverAnalyzed ? 'Never analyzed' : 'Estimated';
      lines.push(`| ${displayName} | ${formatNumber(rc.estimatedRows)} | ${status} |`);
    }
    lines.push('');
    lines.push(
      '*Estimates from pg_class.reltuples. Run ANALYZE or use `get_table_stats` for exact COUNT(\\*).*'
    );
    lines.push('');

    // Prominent warning for never-analyzed tables
    const neverAnalyzed = ontology.estimatedRowCounts.filter(rc => rc.neverAnalyzed);
    if (neverAnalyzed.length > 0) {
      lines.push('> **Warning:** The following tables have never been analyzed — row estimates may be wildly inaccurate:');
      for (const rc of neverAnalyzed) {
        const name = schemaFilter ? rc.tableName : `${rc.schemaName}.${rc.tableName}`;
        lines.push(`> - ${name}`);
      }
      lines.push('> Run `ANALYZE schema.table` or use `get_table_stats` for exact counts.');
      lines.push('');
    }
  }

  return lines;
}

function renderDomainContext(domainContext: string): string[] {
  const lines: string[] = [];
  lines.push('## Domain Context');
  lines.push('');
  lines.push(domainContext);
  lines.push('');
  return lines;
}

function renderCheckConstraints(
  constraints: CheckConstraintInfo[]
): string[] {
  const lines: string[] = [];

  if (constraints.length === 0) {
    return lines;
  }

  lines.push('## Value Constraints (CHECK)');
  lines.push('');
  lines.push('| Table | Constraint | Expression | Meaning |');
  lines.push('|-------|-----------|------------|---------|');

  for (const cc of constraints) {
    const meaning = interpretCheckExpression(cc.expression);
    lines.push(
      `| ${cc.tableName} | ${cc.constraintName} | \`${cc.expression}\` | ${meaning} |`
    );
  }

  lines.push('');
  return lines;
}

function renderEnumTypes(enums: EnumTypeInfo[]): string[] {
  const lines: string[] = [];

  if (enums.length === 0) {
    return lines;
  }

  lines.push('## Enum Types');
  lines.push('');

  for (const e of enums) {
    const label = e.comment ? `**${e.typeName}** — ${e.comment}` : `**${e.typeName}**`;
    lines.push(`### ${label}`);
    lines.push(`Schema: ${e.schemaName}`);
    lines.push('');
    lines.push(`Values: ${e.values.map(v => `\`${v}\``).join(', ')}`);
    lines.push('');
  }

  return lines;
}

function renderUniqueConstraints(
  constraints: UniqueConstraintInfo[]
): string[] {
  const lines: string[] = [];

  if (constraints.length === 0) {
    return lines;
  }

  lines.push('## Natural Keys (UNIQUE Constraints)');
  lines.push('');
  lines.push(
    'These column combinations are guaranteed unique and often represent natural business keys.'
  );
  lines.push('');
  lines.push('| Table | Constraint | Columns |');
  lines.push('|-------|-----------|---------|');

  for (const uc of constraints) {
    lines.push(
      `| ${uc.tableName} | ${uc.constraintName} | ${uc.columns.join(', ')} |`
    );
  }

  lines.push('');
  return lines;
}

function renderViewDefinitions(views: ViewDefinitionInfo[]): string[] {
  const lines: string[] = [];

  if (views.length === 0) {
    return lines;
  }

  lines.push('## View Definitions (Data Lineage)');
  lines.push('');

  for (const v of views) {
    const label = v.comment
      ? `**${v.schemaName}.${v.viewName}** — ${v.comment}`
      : `**${v.schemaName}.${v.viewName}**`;
    lines.push(`### ${label}`);
    lines.push('');

    const def = v.definition;
    if (def.length > MAX_VIEW_DEF_CHARS) {
      lines.push('```sql');
      lines.push(def.slice(0, MAX_VIEW_DEF_CHARS) + ' ...');
      lines.push('```');
      lines.push(
        `*Truncated (${def.length} chars). Use \`get_view_definition\` tool with view="${v.viewName}" for the full SQL and column listing.*`
      );
    } else {
      lines.push('```sql');
      lines.push(def);
      lines.push('```');
    }
    lines.push('');
  }

  return lines;
}

function renderIndexes(indexes: IndexInfo[]): string[] {
  const lines: string[] = [];

  // Filter out PK indexes (already implied by schema)
  const nonPkIndexes = indexes.filter(i => !i.isPrimary);

  if (nonPkIndexes.length === 0) {
    return lines;
  }

  lines.push('## Access Patterns (Indexes)');
  lines.push('');

  // Group by table
  const byTable = new Map<string, IndexInfo[]>();
  for (const idx of nonPkIndexes) {
    const key = `${idx.schemaName}.${idx.tableName}`;
    if (!byTable.has(key)) {
      byTable.set(key, []);
    }
    byTable.get(key)!.push(idx);
  }

  for (const [tableName, tableIndexes] of Array.from(byTable.entries()).sort()) {
    lines.push(`### ${tableName}`);
    lines.push('| Index | Columns | Type | Unique |');
    lines.push('|-------|---------|------|--------|');

    for (const idx of tableIndexes) {
      lines.push(
        `| ${idx.indexName} | ${idx.columns.join(', ')} | ${idx.indexType} | ${idx.isUnique ? 'Yes' : 'No'} |`
      );
    }
    lines.push('');
  }

  return lines;
}

function renderTriggers(triggers: TriggerInfo[]): string[] {
  const lines: string[] = [];

  if (triggers.length === 0) {
    return lines;
  }

  lines.push('## Automatic Behaviors (Triggers)');
  lines.push('');

  for (const t of triggers) {
    const eventsStr = t.events.join(' OR ');
    const desc = t.comment ? ` — ${t.comment}` : '';
    lines.push(
      `- **${t.tableName}.${t.triggerName}**: ${t.timing} ${eventsStr} (FOR EACH ${t.orientation}) -> \`${t.functionName}\`${desc}`
    );
  }

  lines.push('');
  return lines;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/**
 * Heuristic interpretation of CHECK constraint expressions into plain English
 */
function interpretCheckExpression(expr: string): string {
  // CHECK ((status = ANY (ARRAY['active'::text, 'inactive'::text])))
  const anyMatch = expr.match(
    /\((\w+)\s*=\s*ANY\s*\((?:ARRAY\[)?(.+?)(?:\])?\)\)/i
  );
  if (anyMatch) {
    const col = anyMatch[1];
    const vals = anyMatch[2]
      .replace(/::\w+/g, '')
      .replace(/'/g, '')
      .split(',')
      .map(v => v.trim());
    return `${col} must be one of: ${vals.join(', ')}`;
  }

  // CHECK ((col IN ('a', 'b', 'c')))
  const inMatch = expr.match(/\((\w+)\s+IN\s*\((.+?)\)\)/i);
  if (inMatch) {
    const col = inMatch[1];
    const vals = inMatch[2]
      .replace(/::\w+/g, '')
      .replace(/'/g, '')
      .split(',')
      .map(v => v.trim());
    return `${col} must be one of: ${vals.join(', ')}`;
  }

  // CHECK ((col >= 0))
  const rangeMatch = expr.match(
    /\((\w+)\s*(>=?|<=?|<>|!=)\s*(\S+)\)/i
  );
  if (rangeMatch) {
    const col = rangeMatch[1];
    const op = rangeMatch[2];
    const val = rangeMatch[3].replace(/::\w+/g, '');
    const opWord: Record<string, string> = {
      '>': 'greater than',
      '>=': 'at least',
      '<': 'less than',
      '<=': 'at most',
      '<>': 'not equal to',
      '!=': 'not equal to',
    };
    return `${col} must be ${opWord[op] || op} ${val}`;
  }

  // CHECK ((col IS NOT NULL)) or NOT NULL style
  if (/is\s+not\s+null/i.test(expr)) {
    return 'Value is required (NOT NULL)';
  }

  // CHECK ((length(col) > N))
  const lenMatch = expr.match(/length\((\w+)\)\s*(>=?|<=?)\s*(\d+)/i);
  if (lenMatch) {
    const col = lenMatch[1];
    const op = lenMatch[2];
    const val = lenMatch[3];
    if (op === '>=' || op === '>') {
      return `${col} must have at least ${val} characters`;
    }
    return `${col} must have at most ${val} characters`;
  }

  // Fallback: return the expression itself
  return expr.replace(/^CHECK\s*/i, '').replace(/^\(+|\)+$/g, '');
}

/**
 * Format large numbers with commas for readability
 */
function formatNumber(n: number): string {
  if (n < 0) return '0';
  return n.toLocaleString('en-US');
}
