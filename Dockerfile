# =============================================================================
# ProveIT MES MCP Server - Multi-stage Dockerfile
# =============================================================================
# Build: docker build -t proveit-mes-mcp-server .
# Run:   docker run -p 3000:3000 -e DATABASE_URL="..." proveit-mes-mcp-server
# OAuth: docker run -p 3000:3000 -e DATABASE_URL="..." proveit-mes-mcp-server --oauth
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder - Install dependencies and compile TypeScript
# -----------------------------------------------------------------------------
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files first for better layer caching
COPY package*.json ./

# Install all dependencies (including devDependencies for build)
RUN npm ci

# Copy source code
COPY tsconfig.json ./
COPY src/ ./src/

# Build TypeScript
RUN npm run build

# Prune dev dependencies
RUN npm prune --production

# -----------------------------------------------------------------------------
# Stage 2: Runtime - Minimal production image
# -----------------------------------------------------------------------------
FROM node:20-alpine AS runtime

# Security: Run as non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001 -G nodejs

WORKDIR /app

# Copy production dependencies
COPY --from=builder --chown=nodejs:nodejs /app/node_modules ./node_modules

# Copy compiled JavaScript
COPY --from=builder --chown=nodejs:nodejs /app/dist ./dist

# Copy package.json for version info
COPY --from=builder --chown=nodejs:nodejs /app/package.json ./

# Copy domain context file (optional, can be overridden via volume mount)
COPY --chown=nodejs:nodejs context.md ./

# Switch to non-root user
USER nodejs

# Environment variables (can be overridden at runtime)
ENV NODE_ENV=production
ENV DATABASE_URL=""
ENV MCP_PORT=3000
ENV SCHEMA_REFRESH_INTERVAL_MS=300000
ENV QUERY_TIMEOUT_MS=30000
ENV MAX_ROWS=1000
# Generic defaults - override for domain-specific deployments
ENV EXPOSED_SCHEMAS=public
ENV DOMAIN_CONTEXT_FILE=""

# Expose HTTP port
EXPOSE 3000

# Health check using the /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:3000/health || exit 1

# Run the server (pass --oauth as argument to enable OAuth)
CMD ["node", "dist/index.js"]
