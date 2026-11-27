#!/bin/sh
# Runs the relay's Temporal store at RELAY_COMMIT, with the handoff step of
# testdata/relay/handoff.patch, against a Temporal server, and writes the
# relay run histories and the expected documents (testdata/relay).
#
#   scripts/handoff-fixtures.sh <out dir>      # for the end-to-end test
#   scripts/handoff-fixtures.sh --update       # rewrite testdata/relay/fixtures
#
# Needs git, Node 22 and a Temporal server at TEMPORAL_ADDRESS (default
# 127.0.0.1:7233). The keys and the deploy ID come from HANDOFF_KEYS and
# HANDOFF_DEPLOY, or from testdata/relay/fixtures/expected.json.
set -eu
RELAY_COMMIT=3f50e4a495af08f59518d6cc5d9c4e615636f1af
here="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:?give an out dir, or --update}"
if [ "$out" = "--update" ]; then out="$here/testdata/relay/fixtures"; fi
mkdir -p "$out"
out="$(cd "$out" && pwd)"

work="${RELAY_DIR:-$(mktemp -d)/relay}"
if [ ! -d "$work/.git" ]; then
  git clone -q https://github.com/spec-studio-app/relay.git "$work"
fi
cd "$work"
git checkout -q -f "$RELAY_COMMIT"
git clean -qfd src test
git apply "$here/testdata/relay/handoff.patch"
cp "$here/testdata/relay/handoff-fixtures.test.ts" test/
[ -d node_modules ] || npm ci --no-audit --no-fund >/dev/null
npx tsc -p tsconfig.json

if [ -z "${HANDOFF_KEYS:-}" ]; then
  HANDOFF_KEYS="$(node -e 'console.log(require(process.argv[1]).keys)' "$here/testdata/relay/fixtures/expected.json")"
fi
HANDOFF_DEPLOY="${HANDOFF_DEPLOY:-h$(node -e 'console.log(require("crypto").randomBytes(4).toString("hex"))')}"
HANDOFF_OUT="$out" HANDOFF_KEYS="$HANDOFF_KEYS" HANDOFF_DEPLOY="$HANDOFF_DEPLOY" \
  node --test --test-timeout=120000 dist/test/handoff-fixtures.test.js
echo "fixtures in $out"
