#!/usr/bin/env bash
# Brings up the 5-node cluster + control node and drops you into a shell on the
# control node, where `lein run test ...` works.
set -euo pipefail

cd "$(dirname "$0")"

# SSH key shared by the control node and all five nodes. Generated once and
# gitignored — these are disposable test containers on a private network.
if [ ! -f secret/id_ed25519 ]; then
  echo ">> generating SSH key pair in docker/secret"
  mkdir -p secret
  ssh-keygen -t ed25519 -N "" -C jepsen -f secret/id_ed25519 >/dev/null
fi

docker compose up -d --build
echo ">> waiting for sshd on nodes"
for n in n1 n2 n3 n4 n5; do
  until docker compose exec -T "$n" sh -c 'pgrep sshd >/dev/null' 2>/dev/null; do sleep 1; done
done

echo ">> control node shell (try: lein run test --help)"
exec docker compose exec control bash
