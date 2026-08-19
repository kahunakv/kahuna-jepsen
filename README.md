# kahuna-jepsen

[![jepsen](https://github.com/kahunakv/kahuna-jepsen/actions/workflows/jepsen.yml/badge.svg)](https://github.com/kahunakv/kahuna-jepsen/actions/workflows/jepsen.yml)

Jepsen tests for [Kahuna](https://github.com/kahunakv/kahuna) — a distributed
lock manager, key/value store and sequencer built on Raft (Kommander) with
MVCC and 2PC transactions.

Five workloads run against a 5-node cluster while a nemesis partitions,
kills and pauses nodes:

| Workload | What it checks |
|---|---|
| `register` | linearizability of a CAS register over the KV store (Knossos), plus a floor that refuses to certify a run whose reads observed nothing |
| `lock` | mutual exclusion + fencing-token monotonicity, lease-aware |
| `append` | serializability of interactive transactions (Elle list-append) |
| `sequencer` | no id handed out twice, allocation-range integrity, idempotent replay |
| `snapshot` | a pinned MVCC snapshot never changes its answer |

Any of them can also run against a **placed** cluster — six nodes, a
per-partition replication factor, and a nemesis that moves replicas while the
workload runs. See [the replication-factor profile](#the-replication-factor-profile).

`register`, `append` and `snapshot` can additionally run **key-range routed**,
with their keys served by ordered range descriptors that a nemesis splits and
merges underneath the workload. See
[the key-range profile](#the-key-range-profile).

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
| `--faults` | comma-separated `partition,kill,pause,membership,placement,range,clock`, `all`, or `none` for a fault-free control run |
| `--replication-factor` | voter replicas per range. 0 (default) is full replication and changes nothing; 3 is the standard placed profile, 1 the highest-signal cheap one |
| `--key-range` | route the workload's key space by key order instead of by hash, so ranges exist to be split. Off by default and inert for `lock` and `sequencer` |
| `--kill-targets` | what `kill` aims at: `one,minority,majority,all`. Drop `all` for the `snapshot` workload — a full-cluster kill destroys every live snapshot hold, so the run re-acquires instead of measuring |
| `--concurrency` | total client threads; **must** be an exact multiple of `--concurrency-per-key` |
| `--rate` | requests/sec per client |
| `--time-limit` | seconds of load |
| `--ephemeral` | use `Ephemeral` durability instead of `Persistent` |
| `--revision-retention` | persisted MVCC revisions kept per key. Leave it at the server default (keep forever) and the `snapshot` workload cannot fail: history a hold failed to protect would still be on disk |

Run `lein test` for the unit tests — negative controls proving the lock,
sequencer, register-observation and placement checkers actually reject real
violations.

### The replication-factor profile

By default every voter hosts every partition. With `--replication-factor` each
partition gets an explicit replica set, and the machinery that maintains it —
adding a Learner, seeding it, promoting it, dropping the outgoing replica and
purging the partition off the node that lost it — becomes reachable.

```bash
lein run test --workload register \
              --nodes n1,n2,n3,n4,n5,n6 \
              --partitions 8 \
              --replication-factor 3 \
              --force-compaction \
              --faults partition,kill,placement \
              --kill-targets one,majority \
              --time-limit 600 \
              --concurrency 8 --rate 10
```

| Flag | Why you need it |
|---|---|
| `--partitions 8` | at the default of 3, six nodes at RF 3 is barely distinguishable from full replication |
| `--force-compaction` | **not optional.** Without it the PITR floor protects 90 minutes of WAL, nothing is ever compacted inside a run, and every replica added by the nemesis catches up from the log — the snapshot-seeding path is never entered |
| `--faults …,placement` | the fault that moves replicas. `partition`, `kill` and `pause` never do |
| `--placement-nodes-out 3` | walks the roster down toward the replication factor and back, instead of one leave/rejoin at a time |
| `--placement-nodes-out 0` | the other end: never touch the roster, work the replication-factor overrides only. Still drives add, seed, promote and retire — what it drops is the decommission, which Kommander does not support at RF 1 |

The run's verdict gains a `:placement` key. It can return `:valid? :unknown` —
read that as "this run proved nothing", never as a pass:

```clojure
:placement {:valid? :unknown, :cause :vacuous, :missing [:seeding]}
```

means the planner moved replicas but no replica was ever seeded by snapshot,
so `PartitionStateTransfer` was not exercised — usually `--force-compaction`
missing, or a run too short to compact. `--require-placement-evidence` chooses
what a run must demonstrate; `none` switches the gate off.

### The key-range profile

By default a key is routed to a partition by hashing its key space, and the
range machinery — descriptors, splits, merges, cutovers — is never touched.
`--key-range` registers the workload's key space for **key-order** routing
before the run starts, so it is served by a set of half-open descriptors, one
Raft partition each, and a split becomes something a nemesis can force.

```bash
lein run test --workload register \
              --nodes n1,n2,n3,n4,n5,n6 \
              --partitions 8 \
              --replication-factor 3 \
              --force-compaction \
              --key-range \
              --faults partition,kill,placement,range \
              --time-limit 600 \
              --concurrency 8 --rate 10
```

| Flag | Why you need it |
|---|---|
| `--key-range` | without it the key space is hash-routed, has no descriptors, and every split answers `NoRange` |
| `--faults …,range` | the fault that splits and merges. Nothing else moves a boundary inside a run — auto-split fires at 1000 sampled keys on a 60 s cadence |
| `--range-interval 20` | a split creates a partition, quiesces the source, copies the upper half through the destination's log and commits a cutover. Faster than this just collects `ConcurrentSplit` |
| `--range-split-threshold 20 --range-collection-interval 5` | only if you want the *automatic* splitter instead of the forced one. 5 s is the floor — below the phase-two commit timeout the server refuses to start |

Registration is a **precondition, not a fault**: it happens once, before the
workload, and if it cannot be established on every node the DB setup fails
rather than letting the run proceed hash-routed under a name that says
otherwise. A cluster where some nodes range-route a space and others hash it
serves one key from two partitions, and the *workload's* checker would report
the result — a linearizability violation manufactured entirely by the harness.

The run's verdict gains a `:range` key, with the same three-way answer as
`:placement`:

```clojure
:range {:valid? :unknown, :cause :vacuous, :missing [:split]}
```

means the key space never left one descriptor, so no split transaction and no
cutover ever ran. A split counts as demonstrated if either the server
acknowledged one (`Succeeded`) or the sampler caught the space on more than one
descriptor — a merge pass can fold a range back between two samples, and an
auto-split can move a boundary with no nemesis operation behind it, so demanding
both would fail runs that did the work.

`:valid? false` means the descriptors stopped tiling the key space. The one to
read carefully is `:overlap`: a gap is loud — a key routes nowhere and the
workload notices at once — but an overlap looks exactly like the database
working, until two clients disagree about a value they both wrote successfully.

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
- `:placement {:valid? :unknown}` — the run never exercised placement (`:cause
  :vacuous`) or could not observe it (`:cause :unmeasured`). Not a pass; see
  above.
- `:range {:valid? :unknown}` — the key space never split (`:cause :vacuous`) or
  the run could not observe it (`:cause :unmeasured`). Not a pass; see above.
- `:observations {:valid? :unknown}` (register) — either nothing was ever
  acknowledged as written (`:cause :no-acked-writes`) or no read ever returned a
  value (`:cause :no-observed-reads`). Knossos will certify that second history
  happily — an empty read reaches `cas-register` as "unobserved" and matches any
  state — so this is the only thing standing between a broken read path and a
  green run. It has happened: 1543 blind reads across four jobs, all
  `:valid? true`. See [FINDINGS.md](FINDINGS.md).
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
| `src/kahuna/nemesis/placement.clj` | moves replicas: per-range factor overrides, decommission/rejoin, and the placement-table sampler |
| `src/kahuna/checker/placement.clj` | placement safety, and the refusal to call a run that moved nothing a pass |
| `src/kahuna/nemesis/range.clj` | splits and merges the workload's key space, and samples the range map |
| `src/kahuna/checker/range.clj` | range coverage — no gap, no overlap — and the refusal to call a run that split nothing a pass |
| `src/kahuna/nemesis/health.clj` | samples `/v1/cluster/health` so recovery can be decomposed |
| `src/kahuna/checker/recovery.clj` | how long after a fault ends until a request succeeds, split into initialisation and consensus |
| `src/kahuna/core.clj` | test map, nemesis wiring, CLI |
| `test/` | negative controls proving the lock, sequencer, register-observation, placement and range checkers can actually fail |
| `docker/` | 6 Jepsen nodes + a control node |
| `scripts/build-tarball.sh` | self-contained `Kahuna.Server` publish → `target/kahuna.tar.gz` |

## Continuous integration

`.github/workflows/jepsen.yml` runs nightly (04:00 UTC) and on manual dispatch,
as a matrix over workloads and fault sets — `register` under `partition`,
`kill`, `partition,kill` and `pause`; `lock` under `partition`,
`partition,kill` and `pause`; `append` and `sequencer` under `partition` and
`partition,kill`; `snapshot` under `partition` and `partition,kill` with
revision retention turned down so reclamation actually happens; `register`
under `membership`, which runs longer (900s) because a single leave/join cycle
costs a minute or two; and five replication-factor jobs on six nodes —
`register`, `append` and `lock` at RF 3, `register` at RF 1, and a scale-down
run that walks the roster down to the factor and back. It is deliberately not a
per-PR gate
([why](DESIGN.md#why-ci-is-not-a-per-pr-gate)).

Differences from a local run:

* **Topology.** On a Linux runner the host reaches container IPs directly, so
  CI runs Jepsen on the runner itself — no control container, no bind mount. It
  starts only the nodes a job names: `n1..n5` for the stock jobs, `n1..n6` for
  the replication-factor ones.
* **Heap.** `lein with-profile +ci` raises `-Xmx` to 11g, which a 16 GB runner
  supports and a 4 GB Docker Desktop VM does not.
* **Architecture.** The tarball is built for `linux-x64`, not `linux-arm64`.
* **Rate.** Defaults to 10 req/s: four vCPUs hosting five .NET servers plus the
  JVM cannot sustain laptop throughput, and pushing harder just converts into
  timeouts and an unanalyzable history.

`jepsen.cli` exits non-zero when the checker returns `:valid? false`, so the job
fails on its own. The whole `store/` directory uploads as an artifact on every
run.
