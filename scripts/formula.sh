#!/bin/sh
# Writes the Homebrew formula of a release to stdout:
#   scripts/formula.sh <version> <checksums.txt>
# The release workflow pushes it to spec-studio-app/homebrew-tap as
# Formula/plugin-temporal.rb.
set -eu
version="${1:?give the version}"
sums="${2:?give the checksums file}"
sha() {
  sum="$(grep " plugin-temporal_${version}_$1.tar.gz\$" "$sums" | cut -d' ' -f1)"
  [ -n "$sum" ] || { echo "No checksum for $1 in $sums" >&2; exit 1; }
  echo "$sum"
}
url="https://github.com/spec-studio-app/plugin-temporal/releases/download/v${version}"
cat <<RUBY
# The release workflow of spec-studio-app/plugin-temporal writes this file.
class PluginTemporal < Formula
  desc "Temporal backend plugin of Spec Studio: each spec in one Temporal workflow"
  homepage "https://github.com/spec-studio-app/plugin-temporal"
  version "${version}"

  on_macos do
    on_arm do
      url "${url}/plugin-temporal_${version}_darwin_arm64.tar.gz"
      sha256 "$(sha darwin_arm64)"
    end
    on_intel do
      url "${url}/plugin-temporal_${version}_darwin_amd64.tar.gz"
      sha256 "$(sha darwin_amd64)"
    end
  end

  on_linux do
    on_arm do
      url "${url}/plugin-temporal_${version}_linux_arm64.tar.gz"
      sha256 "$(sha linux_arm64)"
    end
    on_intel do
      url "${url}/plugin-temporal_${version}_linux_amd64.tar.gz"
      sha256 "$(sha linux_amd64)"
    end
  end

  def install
    bin.install "plugin-temporal"
  end

  test do
    assert_equal version.to_s, shell_output("#{bin}/plugin-temporal --version").strip
  end
end
RUBY
