#!/bin/sh
# ops/prometheus/entrypoint.sh
# Prometheus config has no env-var expansion. Copy the committed config to a
# writable tmpfs and append a remote_write block only when PROM_REMOTE_WRITE_URL
# is set, so "ship to our central Prometheus" is one .env line.
set -eu
cp /etc/prometheus/prometheus.yml /tmp/prometheus.yml
if [ -n "${PROM_REMOTE_WRITE_URL:-}" ]; then
  printf '\nremote_write:\n  - url: "%s"\n' "$PROM_REMOTE_WRITE_URL" >> /tmp/prometheus.yml
fi
exec /bin/prometheus --config.file=/tmp/prometheus.yml "$@"
