#!/bin/sh
# Builds the static release archives and their checksums:
#   scripts/build-release.sh <version> <out dir>
# Each archive is plugin-temporal_<version>_<os>_<arch>.tar.gz, with the
# binary and the README.
set -eu
version="${1:?give the version, for example 0.1.0}"
out="${2:?give the out dir}"
mkdir -p "$out"
for target in darwin/arm64 darwin/amd64 linux/arm64 linux/amd64; do
  os="${target%/*}"
  arch="${target#*/}"
  dir="$(mktemp -d)"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath \
    -ldflags "-s -w -X main.version=$version" -o "$dir/plugin-temporal" .
  cp README.md "$dir/"
  tar -czf "$out/plugin-temporal_${version}_${os}_${arch}.tar.gz" -C "$dir" plugin-temporal README.md
  rm -rf "$dir"
done
cd "$out"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum plugin-temporal_*.tar.gz > checksums.txt
else
  shasum -a 256 plugin-temporal_*.tar.gz > checksums.txt
fi
cat checksums.txt
