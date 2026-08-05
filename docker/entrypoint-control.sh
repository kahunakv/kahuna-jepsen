#!/usr/bin/env bash
set -euo pipefail

# compose mounts docker/secret at /secret (read-only, host permissions). ssh
# rejects a key that is readable by group/other, so install a private copy.
if [ -f /secret/id_ed25519 ]; then
  mkdir -p /root/.ssh
  cp /secret/id_ed25519 /root/.ssh/id_ed25519
  chmod 700 /root/.ssh
  chmod 600 /root/.ssh/id_ed25519
fi

exec "$@"
