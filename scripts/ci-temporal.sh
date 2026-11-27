#!/bin/sh
# CI: install the Temporal CLI, start a dev server in the background, and
# wait until it serves. The tests use TEST_TEMPORAL_ADDRESS=127.0.0.1:7233.
set -eu
if ! command -v temporal >/dev/null 2>&1; then
  curl -sSf https://temporal.download/cli.sh | sh >/dev/null
  echo "$HOME/.temporalio/bin" >> "${GITHUB_PATH:-/dev/null}"
  PATH="$HOME/.temporalio/bin:$PATH"
fi
temporal --version
nohup temporal server start-dev --headless --port 7233 --log-level warn > "${RUNNER_TEMP:-/tmp}/temporal.log" 2>&1 &
for i in $(seq 1 60); do
  if temporal operator cluster health --address 127.0.0.1:7233 >/dev/null 2>&1; then
    echo "Temporal serves."
    exit 0
  fi
  sleep 1
done
cat "${RUNNER_TEMP:-/tmp}/temporal.log"
echo "Temporal did not start." >&2
exit 1
