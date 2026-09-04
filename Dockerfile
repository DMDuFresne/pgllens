# =============================================================================
# PgLLens - Dockerfile (Python / psycopg)
# =============================================================================
# Build: docker build -t pgllens .
# Run:   docker run -p 3000:3000 -e DATABASE_URL="postgresql://..." pgllens
#
# Three properties this build holds, all asserted in tests/test_supply_chain.py:
#   1. every dependency installed with a hash from uv.lock (--require-hashes)
#   2. the final image has no shell and no package manager (distroless)
#   3. it runs as uid 1001, never root
# =============================================================================

# Builder pins python3.11 to match the *runtime* interpreter baked into
# gcr.io/distroless/python3-debian12 below -- that image ships CPython 3.11, so
# wheels built here for 3.12 would leave every C extension (psycopg, pydantic-core,
# ...) an unloadable cp312 .so under a cp311 interpreter.
FROM ghcr.io/astral-sh/uv:0.9-python3.11-bookworm-slim AS builder

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# uv export writes a requirements file WITH hashes for every pinned dependency.
# --require-hashes then makes the install refuse any artifact the lock did not
# pin -- a compromised index cannot substitute a different wheel for a version.
RUN uv export --frozen --no-dev --no-emit-project --extra observability --extra redis \
      --format requirements-txt -o /tmp/requirements.txt \
 && uv venv /opt/venv \
 && VIRTUAL_ENV=/opt/venv uv pip install --require-hashes -r /tmp/requirements.txt \
 && VIRTUAL_ENV=/opt/venv uv pip install --no-deps . \
 && mkdir -p /tmp/empty-audit

# -----------------------------------------------------------------------------
# Final stage: distroless. No shell, no apt, no package manager -- an RCE in the
# app has no interpreter but Python itself to reach for.
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/python3-debian12:nonroot

LABEL org.opencontainers.image.source="https://github.com/DMDuFresne/pgllens" \
      org.opencontainers.image.description="PgLLens — give AI eyes on your PostgreSQL database" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /opt/venv /opt/venv
# /data/audit is where docker-compose mounts the pgllens-audit named volume
# (AUDIT_LOG_FILE=/data/audit/audit.jsonl by default). Docker seeds a NEW named
# volume's contents/ownership from whatever exists at that path in the image, so
# this directory must exist owned by the runtime uid BEFORE the volume is
# mounted -- otherwise the volume is root:root, the app cannot open the audit
# file, and the whole audit trail silently no-ops via configure_audit's guard.
COPY --from=builder --chown=1001:1001 /tmp/empty-audit /data/audit

# uid 1001's default cwd is /home/nonroot, which uid 1001 cannot stat/traverse
# in this image -- pydantic-settings' dotenv loader does Path('.env').is_file()
# relative to cwd on every Settings() construction and does not catch
# PermissionError (only missing-file paths), so the app crashes at startup
# with an unhandled PermissionError('.env'). WORKDIR here creates /app
# (root:root, mode 755, same as builder's) BEFORE `USER 1001` below, so it's
# world-traversable and uid 1001 can stat it -- no chown needed.
WORKDIR /app

# The venv's own bin/python is a symlink back to the *builder* image's system
# python and does not resolve here -- run the site-packages under distroless's
# own /usr/bin/python3 (also 3.11) instead of the venv's launcher.
ENV PYTHONPATH="/opt/venv/lib/python3.11/site-packages" \
    PYTHONUNBUFFERED=1

USER 1001
EXPOSE 3000
# Exec form, no shell: distroless has none. The healthcheck runs the same
# interpreter the app does.
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD ["/usr/bin/python3", "-c", \
       "import urllib.request;urllib.request.urlopen('http://localhost:3000/health')"]
ENTRYPOINT ["/usr/bin/python3", "-m", "pgllens"]
