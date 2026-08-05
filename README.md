# kahuna-jepsen

Jepsen tests for [Kahuna](https://github.com/kahunakv/kahuna) — a distributed
lock manager, key/value store and sequencer built on Raft (Kommander) with
MVCC and 2PC transactions.

## What's here

| Path | Purpose |
|---|---|
| `src/kahuna/client.clj` | REST client + the response-type → ok/fail/info mapping |
| `src/kahuna/db.clj` | install / start / stop / kill / pause Kahuna on a node |
| `src/kahuna/workload/register.clj` | linearizable CAS-register over the KV store |
| `src/kahuna/core.clj` | test map, nemesis wiring, CLI |
| `docker/` | 5 Jepsen nodes + a control node |
| `scripts/build-tarball.sh` | self-contained `Kahuna.Server` publish → `target/kahuna.tar.gz` |

## Running

```bash
# 1. Build the server tarball. Second arg is the RID matching your node
#    containers' architecture (linux-arm64 on Apple Silicon by default).
scripts/build-tarball.sh ~/kahuna linux-arm64

# 2. Bring up the cluster and land on the control node.
docker/up.sh

# 3. From the control node shell:
# This exact invocation has been run green end-to-end (15 keys, all
# linearizable, under random-halves/majorities-ring partitions).
lein run test --workload register \
              --nodes n1,n2,n3,n4,n5 \
              --time-limit 60 \
              --concurrency 9 \
              --rate 15 \
              --ops-per-key 60 \
              --faults partition

# 4. Browse results
lein run serve   # http://localhost:8080
```

`docker/up.sh` generates an SSH key pair into `docker/secret/` (gitignored) and
bakes the public half into the node images. Key auth is mandatory rather than
cosmetic: Jepsen uploads the server tarball by shelling out to `scp`, which
cannot use a password.

## Design notes

**Client transport.** Tests speak plain HTTP on port 8081. Kahuna serves HTTPS
and Raft together on 8082, so using cleartext for clients keeps TLS handshake
noise out of the failure signal — and means a network partition of 8082 hits
replication without also killing the client connection.

**Error classification is the whole ballgame.** `kahuna.client/response-class`
decides whether a write outcome is definite or unknown. `MustRetry`,
`WaitingForReplication`, `Aborted`, `Errored`, and any socket timeout are
mapped to `:info` (indeterminate). Getting this wrong in either direction
invalidates the linearizability analysis: `:fail` on an operation that later
commits produces phantom violations, and `:ok` on one that never commits hides
real ones. Before trusting a red result, re-read this mapping.

**Durability.** Runs default to `Persistent`. `--ephemeral` switches to
`Ephemeral`, which should *not* be expected to survive kills — run it as a
separate, weaker-expectation test rather than mixing the two.

**WAL fsync.** Kahuna's own docker compose passes `--disable-wal-sync-writes`.
This suite does *not* by default, because a node that is SIGKILLed without an
fsynced WAL may legitimately lose acknowledged writes, which would be a
finding about the flag rather than about Kahuna. `--disable-wal-sync-writes`
re-enables it when that is what you want to test.

**Knossos memory is the practical limit, and it bites early.** Search cost is
driven by per-key concurrency and by the number of *indeterminate* (`:info`)
operations — not by wall-clock time. Observed on a 4 GB Docker VM:

| Config | Outcome |
|---|---|
| rate 50, 5 procs/key, 200 ops/key | `{:valid? :unknown, :cause :out-of-memory}` |
| rate 20, 5 procs/key, 100 ops/key | JVM OOM-killed by the kernel (`-Xmx` exceeded VM RAM — no stack trace, looks like a hang) |
| rate 15, 3 procs/key, 60 ops/key | analyzed cleanly, `:valid? true` |

So: turn `--concurrency-per-key` down before anything else, keep `-Xmx` below
the VM's actual RAM, and prefer a longer run at lower density over a dense one
that cannot be checked. An unanalyzable history proves nothing.

**Clock faults are off by default.** `settimeofday` inside a container moves
the shared kernel clock — on Docker Desktop that means the whole VM. Kahuna's
MVCC snapshots and lock leases ride on a hybrid logical clock, so clock skew is
likely the richest source of bugs here; run `--faults partition,clock` on a
disposable Linux host, not on your laptop.

## Continuous integration

`.github/workflows/jepsen.yml` runs the suite as a matrix over fault sets
(`partition`, `kill`, `partition,kill`).

It is currently **manual dispatch only** — the CI topology has not yet been
exercised on a real GitHub runner. Once a few manual runs come back clean,
uncomment the `schedule:` block at the top of the workflow to add a nightly.

It should not become a per-PR gate even then. Jepsen results are
nondeterministic,
and a slow, contended runner manufactures indeterminate operations a fast
machine would never produce. As a required check it would go red for reasons
unrelated to the change under review, and a check people learn to ignore is
worse than no check. A red nightly means "download the artifact and look at the
history", not "this PR is broken".

Differences from a local run:

* **Topology.** On a Linux runner the host reaches container IPs directly, so
  CI starts only `n1..n5` and runs Jepsen on the runner itself — no control
  container, no bind mount.
* **Heap.** `lein with-profile +ci` raises `-Xmx` to 8g, which a 16 GB runner
  supports and a 4 GB Docker Desktop VM does not.
* **Architecture.** The tarball is built for `linux-x64`, not `linux-arm64`.
* **Rate.** Defaults to 10 req/s: four vCPUs hosting five .NET servers plus the
  JVM cannot sustain laptop throughput, and pushing harder just converts into
  timeouts and an unanalyzable history.

`jepsen.cli` exits non-zero when the checker returns `:valid? false`, so the job
fails on its own. The whole `store/` directory — history, timeline HTML,
latency plots, per-node server logs — uploads as an artifact on every run.

## Roadmap

Built:
- [x] `register` — linearizable CAS register (Knossos)

Next, in rough order of value:
- [ ] `lock` — mutual exclusion via `/v1/locks/try-lock`, including fencing
      token monotonicity and lease-expiry behaviour under partition
- [ ] `append` — Elle list-append over transaction sessions
      (`start-tx-session` → `try-set`/`try-get` → `commit-tx-session`), checking
      serializability / snapshot isolation of the 2PC+MVCC layer
- [ ] `sequencer` — no duplicate or lost ids from `/v1/sequences/next` and
      `reserve`, across leader changes
- [ ] membership nemesis using `/v1/cluster/membership` (add/remove node)
- [ ] snapshot-read workload exercising `readTimestamp` + snapshot holds
