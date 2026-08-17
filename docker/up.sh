#!/usr/bin/env bash
# Brings up the 6-node cluster + control node and drops you into a shell on the
# control node, where `lein run test ...` works.
#
# Six nodes are started; which of them a test uses is `--nodes`. The stock jobs
# still run on n1..n5; the replication-factor profile wants all six.
set -euo pipefail

cd "$(dirname "$0")"

# SSH key shared by the control node and all six nodes. Generated once and
# gitignored — these are disposable test containers on a private network.
if [ ! -f secret/id_ed25519 ]; then
  echo ">> generating SSH key pair in docker/secret"
  mkdir -p secret
  ssh-keygen -t ed25519 -N "" -C jepsen -f secret/id_ed25519 >/dev/null
fi

docker compose up -d --build
echo ">> waiting for sshd on nodes"
for n in n1 n2 n3 n4 n5 n6; do
  until docker compose exec -T "$n" sh -c 'pgrep sshd >/dev/null' 2>/dev/null; do sleep 1; done
done

echo ">> control node shell (try: lein run test --help)"
exec docker compose exec control bash
