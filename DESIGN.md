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

## Sequences: gaps are legal, duplicates never are

The sequencer workload checks *less* than it could, on purpose.

Kahuna hands each node an exclusively reserved block of values and lets it drain
that block locally (`SequenceActor.TryPlanFromBlock`), touching storage only when
a block is exhausted. Two consequences fall straight out of that design:

- **Gaps are legal.** A leader change surrenders the unused tail of a block. So
  does an allocation whose acknowledgement was lost. Both burn values no client
  ever sees. A "no lost ids" check would fail nearly every run with a nemesis and
  would be testing an invariant Kahuna never claimed.
- **Out-of-order values are legal.** A node holding `[100,200)` can serve 105
  *after* another node served 205. There is no global order to violate, so a
  monotonicity check would manufacture failures rather than find them.

What remains is the property that actually matters: **no value is ever handed to
two callers.** That holds under any interleaving, on any node, at any time.

This asymmetry makes the workload unusually robust to indeterminacy. A `next`
that times out may or may not have consumed a value — but either way the result
is a gap, never a duplicate. Unlike `register`, no verdict here depends on
classifying an ambiguous response correctly; an `:info` op simply contributes
nothing. That is a rare luxury in this suite, and it is worth preserving: resist
adding checks that reintroduce a dependence on getting `:info` right.

The one thing indeterminacy *does* threaten is replay safety, which is why the
`:next-twice` operation exists. The documented way to retry a timed-out `next` is
to resend it with the same idempotency key; that must return the original
allocation rather than burning a second value. Under `partition,kill` the replay
may reach a different leader than the original — exactly where an idempotency
record could go missing.

**Allocation ranges are inclusive at both ends.** The server plans
`start = current + increment` and `end = current + increment * count`, so a range
covers exactly `count` values and `[1,5]` followed by `[6,10]` is adjacent, not
overlapping. An off-by-one here would report the most common shape in any history
as a duplicate.

**A run with too few allocations is `:unknown`, not clean.** An empty history
satisfies "no duplicates" perfectly, so without a floor a botched setup reads as
a pass — the same trap as `:empty-transaction-graph` in the append workload. The
sequencer's `setup!` also retries sequence creation, because it runs before the
cluster has finished electing and a single-shot create leaves every subsequent
operation answering `NotFound`.

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

## Replication factor: what the placed profile tests, and what it cannot

By default every voter hosts every partition, and none of the placement code
runs. With `--replication-factor` each partition gets an explicit replica set,
quorum is computed per range over its voters, and a large amount of machinery
becomes reachable that no other fault in this suite can reach.

### The profile, and why each part of it is load-bearing

**Six nodes, eight partitions, RF 3.** Five nodes at RF 3 is nearly full
replication: each range would live on three of five, most operations would land
on a hosting node, and the forwarding path would barely be exercised. At six
and eight, each node hosts about half the ranges and the Jepsen client — which
targets nodes directly, with no proxy — forwards most of its operations for
free.

**`--force-compaction`, or the snapshot path is never entered.** This is the
one knob that looks like tuning and is not. Kahuna's WAL compaction cannot
discard anything the PITR floor still protects, and that floor sits at `now −
pitr-window − base-snapshot-interval`: **90 minutes** at the defaults. No Jepsen
run reaches it, so nothing is ever compacted, so every replica the nemesis adds
catches up from the log and `PartitionStateTransfer` — whole-partition snapshot
seeding, the code the un-host purge has to interact correctly with — is never
called. A profile whose checkpoints never fire validates nothing about seeding
while looking exactly like one that does. The flag collapses the window to a
second and compacts every 200 operations, which destroys point-in-time recovery
and is why it belongs to a chaos profile and nowhere else.

**RF 1 as well as RF 3.** At one replica per range there is no second copy to
absorb a mistake, so a purge that ran too early or a seed that arrived
incomplete is data loss rather than degradation. It is the cheapest
configuration with the sharpest signal.

### A leave here goes through the API, not through SIGTERM

`kahuna.nemesis.membership` stops a node and waits for its shutdown hook to
commit `RemoveMember(self)`. It can only *infer* whether that worked, which is
why it refuses to act unless the cluster is fully formed — and that refusal is
what makes it close to a no-op when combined with any other fault.

The placement nemesis uses `POST /v1/cluster/leave`, which commits the removal
and answers with the consensus outcome. Being able to read *why* a removal did
not happen removes the need for a healthy-cluster precondition, so this fault
composes with `partition` and `kill` — which is the point, since "leadership
moves between the add and the promote" and "a node dies mid-purge" are the
scenarios worth hunting. The process is stopped only when the removal actually
committed; killing a node that is still in the roster would be a `kill` fault
wearing a leave's name.

### The checker's job is mostly to refuse

Everything else in this suite can fail. `kahuna.checker.placement` exists first
to stop a *pass* from being meaningless. Replica moves are the only thing that
runs the code under test, so a run whose planner never moved anything is not
evidence that moving is safe. That verdict is `:valid? :unknown`, and the
evidence it demands is read from two places: the sampled placement tables (did a
committed replica set change?) and the server logs (`Imported whole-partition
state` for seeding, `Stopped hosting` for the un-host purge). Jepsen captures
node logs before it checks, so reading them there is sound.

The safety properties it does assert are the ones Kommander states as
invariants — single mover per range, one committed map everywhere at a given
generation, at least one voter in a placed range, no replica on an unknown
endpoint. The tempting fifth, an upper bound on voters, is deliberately absent:
lowering a range's factor legitimately leaves it over-replicated until the
planner trims it, which is exactly what the nemesis's `:set-rf` asks for, and a
check that fires on the nemesis's own intended effect is a bug in the checker.

### What this profile cannot reach today

**In-place migration.** There is none: a cluster bootstrapped at RF 0 keeps
empty replica sets forever, and the planner never sees those ranges. So there
is no "turn RF on mid-run" fault to write, and a placed run means a cluster
bootstrapped placed.

**Combining `placement` with `membership`.** Two nemeses removing nodes
independently can walk a roster past quorum between them even though each
respects its own bound. The placement nemesis checks the *witnessed* roster
against its floor before every decommission, so it declines rather than
compounding, but the combination is still weaker than it looks and the CI matrix
does not use it.

## Key ranges: splitting under load, and why the split key is measured

Splits used to be listed here as unreachable. They were: `RangeSplitThreshold`
had no `Kahuna.Server` flag, the force-split primitive was an `internal` test
seam, the range map had no read surface, and ordinary keys were hash-routed with
no descriptors to split in the first place. All four are now exposed, so
`--key-range` turns the profile on.

### Registration is a precondition, and it fails the setup rather than the run

Key-range routing is opted into per key space. The seed descriptor is a single
replicated write; the per-node routing mode is derived from it, reconciled
inside the same call that applies a descriptor. So registering on one node does
converge everywhere — but only once the descriptor replicates, and the window
in between is a cluster where some nodes route a space by key order and the rest
hash it. One key, two partitions, two versions.

That is not a Kahuna bug to report, it is a harness bug that would look like
one: the divergence lands in the *workload's* history, and the linearizability
checker is what would announce it. So registration happens once, before any
client starts, on every node, and the run waits until every node reports the
space `KeyRange` with at least one descriptor. If it cannot, the DB setup fails.
Aborting is the opposite of how this suite treats a mid-run failure — a node
that does not come back is a result the nemesis records — and the difference is
that a run which never established routing would carry a `-kr` name while
testing hash routing.

### The split key comes from a live scan, not from a guess

The server refuses a split whose key falls outside the covering range
(`InvalidSplitKey`) and one that would leave either half empty
(`BelowMinRangeSize`). A nemesis that guessed at key names would spend a run
collecting refusals and then report a vacuous pass — the exact shape this suite
keeps rediscovering.

So the nemesis scans the chosen descriptor's own bounds for a page of live keys
and cuts at the page's median. The median of two or more distinct sorted keys is
strictly greater than the first and is itself a live key, so the left half
provably holds a key, the right half holds the median, and both of the server's
preconditions are satisfied by construction rather than by luck. A page with
fewer than two distinct keys is reported as a skip: the range really is too
small to split, which is a fact about the run and not a failure.

Bounds are compared **ordinally**, the same comparison the router uses.
`clojure.core/compare` on strings is `String/compareTo`, so this is free — but
it means `jepsen/register/10` sorts before `jepsen/register/2`, and a checker or
a nemesis that ordered keys numerically would read gaps that are not there and
aim splits outside the range it thought it was splitting.

### Not every failed split may be retried

`NotLeader` means the node did not attempt anything, so trying the next node is
free — and necessary, since only the meta-partition leader may split. Everything
else must stop where it is. `TransferFailed`, `QuiesceFailed`, `CutoverFailed`
and `ConcurrentSplit` come back with `determinate` false: the split transaction
was already under way and the map may still change after the call returns.
Issuing a second split against another node in that state means two cutovers
racing on one range, and the resulting map would be the harness's doing rather
than Kahuna's. The rule is: hunt for the leader, then take the first real
answer, whatever it is.

This is why the endpoint reports `determinate` separately from `success` at all,
and why the checker counts indeterminate outcomes apart from refusals. A run
that filed them together could report "nothing happened" while the boundaries
moved underneath it.

### A destination partition id is never handed out twice

A split allocates its destination one past every id Kommander has ever used, so
an id that was merged away or rolled back never comes back. That replaced an
allocator that read the *range map*, which forgets retired partitions — three
distinct failures came out of the same arithmetic: a split after a merge died on
an id Kommander still held as `Removed`; a split that failed after creating its
partition retired that id and wedged the auto-splitter for every key space
afterwards; and on a cluster with several initial partitions the id could land
inside the hash pool, where `CreatePartitionAsync` answers idempotent success and
ranged data silently joined hash-routed data on one partition.

The checker fails a run in which two acknowledged splits name the same
`newPartitionId`. It reads only the server's `Succeeded` answers, and the reason
matters: the obvious alternative — watching an id vanish from the sampled range
maps and return — cannot be made sound, because nodes lag, so an id can leave
every view and reappear from a node that had not caught up. On a run that churns
the roster that check would fire constantly and mean nothing. Two acknowledged
splits naming one id has no sampling exposure at all.

`PartitionCreationFailed` is reported apart from other refusals for the same
reason: it is not a violation on its own — leadership can move between the
leader gate and the call — but it is what a recycled id looks like from the
caller's side, so a run full of them is worth seeing without digging.

### Coverage is checked per view, and only views that have something to say

The descriptors of a key space must **tile** it: no gap, no overlap. A gap is
loud — a key routes nowhere and the workload sees it immediately. An overlap is
the dangerous one, because it looks exactly like the database working until two
clients disagree about a value they both wrote successfully. A split is the
moment the property is at risk, and the window in which one half of the cutover
is visible and the other is not is milliseconds wide, which is why the checker
reads sampled views rather than a final state.

Each single view is checked on its own, and that is sound because the map is
committed: a node that lags holds an *older complete* map, not a partial one.
What is **not** checked is a key space appearing to vanish. The placement
nemesis wipes and rejoins nodes, and a node that has not yet applied the meta
partition honestly reports the space as hash-routed and empty. Failing on that
would be reporting the harness's own teardown as a Kahuna bug, on every run that
churns the roster — so views with no descriptors are skipped entirely, and the
descriptor counts are reported instead. Generation monotonicity is left out for
the same reason.

### Why `split` is not in `--require-placement-evidence`

Two gates would be measuring one fact, and one of them badly. The placement
checker reads a log substring; the range checker reads the range map, which can
tell a cutover that committed from one that was attempted and can name the
partitions the space ended up on. So splits are gated by
`--require-range-evidence`, and `split` stays available in the placement
checker as a log-side cross-check for anyone who wants both. (The placement
marker did have to be tightened: `RangeSplitTrigger` logs `splitting …` before
it starts and `split … → P3` after the cutover, and the substring matched both.)

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
