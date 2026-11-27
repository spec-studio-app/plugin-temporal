#!/bin/sh
# Writes the histories of the replay tests (internal/workflows/testdata/histories)
# from a session against a Temporal dev server. Run it only when a workflow
# changes its shape on purpose (with workflow.GetVersion), and commit the new
# files next to the old ones: the old histories must still replay.
#
#   temporal server start-dev --headless &
#   scripts/capture-histories.sh v2
#
# The argument names the new shape. The files are <name>-*.json; the script
# never replaces the files of an earlier shape.
set -eu
name="${1:?give a name for the new workflow shape, for example v2}"
cd "$(dirname "$0")/.."
TEST_TEMPORAL_ADDRESS="${TEST_TEMPORAL_ADDRESS:-127.0.0.1:7233}" CAPTURE_HISTORIES=1 CAPTURE_NAME="$name" \
  go test -count=1 -tags integration -run TestCaptureHistories ./internal/store/
ls -l internal/workflows/testdata/histories
