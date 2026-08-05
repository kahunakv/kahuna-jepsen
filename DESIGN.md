# Design notes

Why these tests are shaped the way they are, and the limits you will hit when
running them. Read this before trusting — or dismissing — a red result.

For what the tests have found, see [FINDINGS.md](FINDINGS.md).

## Client transport

Tests speak plain HTTP on port 8081. Kahuna serves HTTPS and Raft together on
8082, so using cleartext for clients keeps TLS handshake noise out of the
failure signal — and means a network partition of 8082 hits replication without
also killing the client connection.

## Error classification is the whole ballgame

`kahuna.client/response-class` decides whether a write outcome is definite or
unknown. `MustRetry`, `WaitingForReplication`, `Aborted`, `Errored`, and any
socket timeout map to `:info` (indeterminate).

Getting this wrong in either direction invalidates the analysis: `:fail` on an
operation that later commits produces phantom violations, and `:ok` on one that
never commits hides real ones. **Before trusting a red result, re-read this
mapping.** One open finding turns entirely on such a judgment — see the `:G1a`
entry in [FINDINGS.md](FINDINGS.md).

## Durability

Runs default to `Persistent`. `--ephemeral` switches to `Ephemeral`, which
should *not* be expected to survive kills — run it as a separate,
weaker-expectation test rather than mixing the two.

**WAL fsync.** Kahuna's own docker compose passes `--disable-wal-sync-writes`.
This suite does *not* by default, because a node that is SIGKILLed without an
fsynced WAL may legitimately lose acknowledged writes, which would be a finding
about the flag rather than about Kahuna. The flag re-enables that behaviour when
it is what you want to test.

## Locks are leased, so naive mutual exclusion is not the property

`LockActor` grants a lock until `now + expiresMs`; once that passes, another
owner may take it even though the previous holder never released and may still
believe it holds the lock. That is deliberate — it stops a crashed holder from
wedging the resource forever. A checker that flagged every overlapping holder
would report Kahuna's designed behaviour as a bug.

So the lock workload checks the two properties that survive expiry:

* **exclusion** — hold windows are trimmed to the earliest instant the lease
  could have lapsed (and shrunk further by `--lease-margin-ms` to absorb
  clock-rate differences), so an overlap it reports cannot be explained by
  expiry.
* **fencing** — tokens never go backwards and strictly increase when the lock
  changes hands. Only genuinely ordered pairs are compared: if B was invoked
  before A completed, the protocol promises nothing about their order.
  Re-acquisition by the current holder returning the *same* token is expected
  (`LockActor` returns `entry.FencingToken` unchanged) and is not a violation.

Run `lein test` for the negative controls that prove those checkers reject real
violations while accepting expiry-explained and concurrent ones.

## Transactions

**Every transaction-scoped request must carry both a coordinator key and a
non-zero operation id.** `KeyValuesManager.ClassifyRegistration` classifies a
request carrying exactly one of the pair as `Malformed` and answers
`InvalidInput` — it does not degrade to the unregistered path, because applying
it would mutate a participant outside the finalize fence. Omitting the id fails
*every* micro-op, so no transaction commits and Elle reports
`:empty-transaction-graph`. Read that verdict as "the client never spoke the
protocol", not as a finding.

The id doubles as the coordinator's dedup key: resubmitting one replays the
cached response instead of applying the operation twice, so a retry must reuse
its id rather than mint a fresh one.

Transactions are driven with `TrackAndValidate` read validation and pessimistic
locking — the combination Kahuna's docs credit for serializable behaviour — so
`--consistency-model` defaults to `serializable`. Pass `snapshot-isolation` to
check the weaker claim, or `--locking optimistic` to exercise the other path.

Kahuna has no native list-append, so an append is a read-modify-write inside the
session. The read half is deliberate: it puts the key in the transaction's read
set, which is exactly the conflict the isolation level is supposed to police.

## The append workload needs ≥2 CPUs, and fails fast without them

`elle.core/combine` launches `jepsen.history.task`s that await other tasks, and
that executor is sized from `availableProcessors`. With one worker a task blocks
forever on a subtask that can never be scheduled: the run reaches `Analyzing...`
and sits at 0 % CPU indefinitely, which reads as a slow check rather than a
deadlock.

Docker Desktop will happily hand its VM a single CPU on an 8-core host
(`docker info` → `NCPU`); raise it under Settings → Resources.
`kahuna.workload.append/check-cpus!` refuses to start rather than let you
discover this after a full run has already been collected.

## Knossos memory is the practical limit, and it bites early

Search cost is driven by per-key concurrency and by the number of
*indeterminate* (`:info`) operations — not by wall-clock time. Observed on a
4 GB Docker VM:

| Config | Outcome |
|---|---|
| rate 50, 5 procs/key, 200 ops/key | `{:valid? :unknown, :cause :out-of-memory}` |
| rate 20, 5 procs/key, 100 ops/key | JVM OOM-killed by the kernel (`-Xmx` exceeded VM RAM — no stack trace, looks like a hang) |
| rate 15, 3 procs/key, 60 ops/key | analyzed cleanly, `:valid? true` |

`--concurrency` must be an exact multiple of `--concurrency-per-key`;
`jepsen.independent` asserts at start-up otherwise (e.g. 10 threads cannot run
3 keys × 3 threads).

So: turn `--concurrency-per-key` down before anything else, keep `-Xmx` below
the VM's actual RAM, and prefer a longer run at lower density over a dense one
that cannot be checked. **An unanalyzable history proves nothing.**

## Clock faults are off by default

`settimeofday` inside a container moves the shared kernel clock — on Docker
Desktop that means the whole VM. Kahuna's MVCC snapshots and lock leases ride on
a hybrid logical clock, so clock skew is likely the richest source of bugs here;
run `--faults partition,clock` on a disposable Linux host, not on your laptop.

## Why CI is not a per-PR gate

Jepsen results are nondeterministic, and a slow, contended runner manufactures
indeterminate operations a fast machine would never produce. As a required check
it would go red for reasons unrelated to the change under review, and a check
people learn to ignore is worse than no check.

A red nightly means "download the artifact and look at the history", not "this
PR is broken".
