# kahuna-jepsen

[![jepsen](https://github.com/kahunakv/kahuna-jepsen/actions/workflows/jepsen.yml/badge.svg)](https://github.com/kahunakv/kahuna-jepsen/actions/workflows/jepsen.yml)

Jepsen tests for [Kahuna](https://github.com/kahunakv/kahuna) — a distributed
lock manager, key/value store and sequencer built on Raft (Kommander) with
MVCC and 2PC transactions.

Five workloads run against a 5-node cluster while a nemesis partitions,
kills and pauses nodes:

| Workload | What it checks |
|---|---|
| `register` | linearizability of a CAS register over the KV store (Knossos) |
| `lock` | mutual exclusion + fencing-token monotonicity, lease-aware |
| `append` | serializability of interactive transactions (Elle list-append) |
| `sequencer` | no id handed out twice, allocation-range integrity, idempotent replay |
| `snapshot` | a pinned MVCC snapshot never changes its answer |

- **[DESIGN.md](DESIGN.md)** — why the tests are shaped this way, and the limits
  you will hit. Read it before trusting or dismissing a red result.
- **[FINDINGS.md](FINDINGS.md)** — what the tests have found, and what is still
  unwritten.

## Requirements

- Docker with **≥2 CPUs** allocated to its VM — Elle's analysis deadlocks below
  that (`docker info` → `NCPU`; Docker Desktop → Settings → Resources)
- .NET 10 SDK, to build the Kahuna server tarball
- Leiningen and a JDK

## Running

```bash
# 1. Build the server tarball. Second arg is the RID matching your node
#    containers' architecture (linux-arm64 on Apple Silicon by default).
scripts/build-tarball.sh ~/kahuna linux-arm64

# 2. Bring up the cluster and land on the control node.
docker/up.sh

# 3. From the control node shell:
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

`lein run test --help` lists every option. The ones you are most likely to
reach for:

| Flag | Meaning |
|---|---|
| `--workload` | `register`, `lock`, `append`, `sequencer` or `snapshot` |
| `--faults` | comma-separated `partition,kill,pause,membership,clock`, or `all` |
| `--concurrency` | total client threads; **must** be an exact multiple of `--concurrency-per-key` |
| `--rate` | requests/sec per client |
| `--time-limit` | seconds of load |
| `--ephemeral` | use `Ephemeral` durability instead of `Persistent` |
| `--revision-retention` | persisted MVCC revisions kept per key. Leave it at the server default (keep forever) and the `snapshot` workload cannot fail: history a hold failed to protect would still be on disk |

Run `lein test` for the unit tests — negative controls proving the lock and
sequencer checkers actually reject real violations.

`docker/up.sh` generates an SSH key pair into `docker/secret/` (gitignored) and
bakes the public half into the node images. Key auth is mandatory rather than
cosmetic: Jepsen uploads the server tarball by shelling out to `scp`, which
cannot use a password.

## Reading a result

A run ends with a verdict map and either `Everything looks good!` or
`Analysis invalid!`. Two verdicts mean less than they appear to:

- `:valid? :unknown` — the checker could not finish, usually out of memory. It
  is not a pass. See [DESIGN.md](DESIGN.md#knossos-memory-is-the-practical-limit-and-it-bites-early).
- `:empty-transaction-graph` (append) — **nothing committed**, so there was
  nothing to analyze. Check the client's failure tally and the node logs before
  reading it as anything else; see [FINDINGS.md](FINDINGS.md).
- `:insufficient-data` (sequencer) — fewer than 25 allocations were observed, so
  "no duplicates" is vacuous. The checker returns `:valid? :unknown` rather than
  success; a run where every operation failed must not read as a clean run.
- `:insufficient-data` (snapshot) — either fewer than 25 reads landed inside a
  proven protection window, or every one of them was served by the key's
  current revision and so never touched retained history. Both are vacuous
  passes; check `:reads-below-head` and `:max-depth` before believing a green
  snapshot run.

Everything from a run lands in `store/<test>/<timestamp>/` — history, verdict,
timeline HTML, latency plots and per-node server logs.

**Reclaiming disk.** Kahuna logs at debug level, so each run drops ~70 MB of
server logs into `store/`; a few dozen runs fill a disk.

```bash
scripts/prune-logs.sh --dry-run   # show what would go
scripts/prune-logs.sh             # delete them
```

It deletes `n*/kahuna.log` only from **passing** runs, keeping failing runs
(they are the evidence behind every investigation), anything written in the last
hour, and any run with no verdict. History, `results.edn`, timeline and plots
are never touched, so a pruned run is still fully re-analyzable.

## What's here

| Path | Purpose |
|---|---|
| `src/kahuna/client.clj` | REST client + the response-type → ok/fail/info mapping |
| `src/kahuna/db.clj` | install / start / stop / kill / pause Kahuna on a node |
| `src/kahuna/workload/register.clj` | linearizable CAS-register over the KV store |
| `src/kahuna/workload/lock.clj` | mutual exclusion + fencing tokens over distributed locks |
| `src/kahuna/workload/append.clj` | Elle list-append over interactive transactions |
| `src/kahuna/workload/sequencer.clj` | duplicate-free id allocation across leader changes |
| `src/kahuna/workload/snapshot.clj` | MVCC snapshot reads pinned by snapshot holds |
| `src/kahuna/nemesis/membership.clj` | removes a node from the Raft roster and rejoins it |
| `src/kahuna/nemesis/health.clj` | samples `/v1/cluster/health` so recovery can be decomposed |
| `src/kahuna/checker/recovery.clj` | how long after a fault ends until a request succeeds, split into initialisation and consensus |
| `src/kahuna/core.clj` | test map, nemesis wiring, CLI |
| `test/` | negative controls proving the lock and sequencer checkers can actually fail |
| `docker/` | 5 Jepsen nodes + a control node |
| `scripts/build-tarball.sh` | self-contained `Kahuna.Server` publish → `target/kahuna.tar.gz` |

## Continuous integration

`.github/workflows/jepsen.yml` runs nightly (04:00 UTC) and on manual dispatch,
as a matrix over workloads and fault sets — `register` under `partition`,
`kill`, `partition,kill` and `pause`; `lock` under `partition`,
`partition,kill` and `pause`; `append` and `sequencer` under `partition` and
`partition,kill`; `snapshot` under `partition` and `partition,kill` with
revision retention turned down so reclamation actually happens; and `register`
under `membership`, which runs longer (900s) because a single leave/join cycle
costs a minute or two. It is deliberately not a per-PR gate
([why](DESIGN.md#why-ci-is-not-a-per-pr-gate)).

Differences from a local run:

* **Topology.** On a Linux runner the host reaches container IPs directly, so
  CI starts only `n1..n5` and runs Jepsen on the runner itself — no control
  container, no bind mount.
* **Heap.** `lein with-profile +ci` raises `-Xmx` to 11g, which a 16 GB runner
  supports and a 4 GB Docker Desktop VM does not.
* **Architecture.** The tarball is built for `linux-x64`, not `linux-arm64`.
* **Rate.** Defaults to 10 req/s: four vCPUs hosting five .NET servers plus the
  JVM cannot sustain laptop throughput, and pushing harder just converts into
  timeouts and an unanalyzable history.

`jepsen.cli` exits non-zero when the checker returns `:valid? false`, so the job
fails on its own. The whole `store/` directory uploads as an artifact on every
run.
