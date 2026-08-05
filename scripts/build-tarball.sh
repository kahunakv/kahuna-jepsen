#!/usr/bin/env bash
# Builds a self-contained Kahuna.Server publish and packages it as the tarball
# that kahuna.db uploads to each Jepsen node.
#
# The tarball's root must contain:
#   Kahuna.Server      (executable)
#   certificate.pfx    (dev cert; Kahuna requires --https-certificate)
#   *.so / runtime deps
#
# Usage:
#   scripts/build-tarball.sh [/path/to/kahuna] [rid]
#
# `rid` must match the ARCHITECTURE OF THE JEPSEN NODE CONTAINERS, not your
# laptop: on Apple Silicon with default (arm64) containers use linux-arm64; if
# you run the nodes under amd64 emulation use linux-x64.

set -euo pipefail

KAHUNA_SRC="${1:-$HOME/kahuna}"
RID="${2:-linux-arm64}"
OUT_DIR="$(cd "$(dirname "$0")/.." && pwd)/target"
STAGE="$(mktemp -d)"

if [ ! -d "$KAHUNA_SRC/Kahuna.Server" ]; then
  echo "error: $KAHUNA_SRC does not look like the Kahuna repository" >&2
  exit 1
fi

echo ">> publishing Kahuna.Server for $RID"
dotnet publish "$KAHUNA_SRC/Kahuna.Server" \
  --configuration Release \
  --runtime "$RID" \
  --self-contained true \
  -p:RunAnalyzers=false \
  -o "$STAGE"

cp "$KAHUNA_SRC/certs/development-certificate.pfx" "$STAGE/certificate.pfx"

mkdir -p "$OUT_DIR"
tar -czf "$OUT_DIR/kahuna.tar.gz" -C "$STAGE" .
rm -rf "$STAGE"

echo ">> wrote $OUT_DIR/kahuna.tar.gz"
