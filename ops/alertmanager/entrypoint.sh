#!/bin/sh
# ops/alertmanager/entrypoint.sh
# Alertmanager cannot conditionally define a receiver and does not expand env vars,
# so pick one of two committed configs. The webhook goes to a tmpfs file that only
# this container sees; it never lands in a config file or `docker inspect`.
set -eu
if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  umask 077
  printf '%s' "$SLACK_WEBHOOK_URL" > /tmp/slack_url
  CFG=/etc/alertmanager/alertmanager.slack.yml
else
  CFG=/etc/alertmanager/alertmanager.null.yml
fi
exec /bin/alertmanager --config.file="$CFG" --storage.path=/alertmanager "$@"
