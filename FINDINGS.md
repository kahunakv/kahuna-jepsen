# Findings and status


## Workload status

| Workload | Checks | Status |
|---|---|---|
| `register` | linearizable CAS register (Knossos) | green under `partition`, `kill`, `partition,kill` |
| `lock` | mutual exclusion + fencing-token monotonicity, lease-aware | green; previously found a real bug, since fixed and verified |
| `append` | Elle list-append over interactive transactions | found three real bugs; two fixed and verified, HTTP 500 all but fixed |

Not started:

- [ ] `sequencer` — no duplicate or lost ids from `/v1/sequences/next` and
      `reserve`, across leader changes
- [ ] membership nemesis using `/v1/cluster/membership` (add/remove node)
- [ ] snapshot-read workload exercising `readTimestamp` + snapshot holds
- [ ] `pause` fault in the CI matrix (supported by the harness, not yet wired in)

## Closed: fencing tokens regressed under partition + kill

**Found** 2026-08-04 by the `lock` workload. **Fixed and verified** 2026-08-05.

A later lock acquisition — strictly ordered in real time after an earlier one —
was granted a *lower* fencing token. Observed both in CI (token 4 → 0, 1, 0, 1)
and locally (23 → 19, with ~7 s of clean separation). Both acquisitions returned
`Locked`, so two owners were each told they held the lock, the second with the
older token. Needed *both* faults; neither alone ever reproduced it.

Root cause, from the Vorpal investigation: a follower's WAL replay floor (the
replicated `CommittedCheckpoint`) could advance past lock state the follower had
not yet flushed to its backend, so kill+restart silently lost committed lock
state on that node — and a partition then promoted exactly that node to leader,
which served `TryLock` from its stale backend.

Fixed in Kahuna `485daa3` (route lock writes through an unflushed overlay so
promoted leaders observe committed-but-not-yet-flushed state) and `39ec8e0`
(per-partition application durability floor, so restart replay and WAL
compaction respect committed-but-not-yet-flushed work).

Verified by six consecutive clean runs at `--faults partition,kill`, 120 s,
concurrency 10, rate 10:

| Run | Acquires | Max token | Holds |
|---|---|---|---|
| 1 | 49 | 48 | 28 |
| 2 | 44 | 43 | 35 |
| 3 | 8 | 8 | 4 |
| 4 | 28 | 27 | 14 |
| 5 | 35 | 34 | 20 |
| 6 | 57 | 56 | 28 |

Two things make that more than six green ticks. The investigation predicted that
*quiet* runs fail and *busy* runs pass, because a checkpoint is only proposed
when the leader's dirty queues are empty — and the original data matched exactly
(failures at 9 and 25 acquires, passes at 87 and 116). Run 3 completed just
**8** acquires, squarely inside the window that used to fail, and was clean.
And `max-token` now equals `acquire-count` or one less in every run, so tokens
advance exactly one per hand-off: the strong form of the property, not merely
"never decreased".

Six runs against a defect that previously failed roughly 2 in 5 would come up
clean about 5 % of the time by chance. Strong evidence, not proof.

## Transaction anomalies under partition + kill

**Found** 2026-08-05 by the `append` workload. Both were server bugs, both now
fixed and verified — the aborted read only after a false "fixed" verdict and a
recurrence, which is the part worth reading.

Originally seen in 6 runs at 90 s, concurrency 5, rate 5: one `:G1a`, one
`:incompatible-order`, one washout, three clean.

### Lost update (`:incompatible-order`)

Two reads of one key returned lists neither of which prefixes the other:

```clojure
{:key 14, :values [[17 18 19 22]
                   [17 18 19 10]]}
```

Two transactions read the same base `[17 18 19]`, each appended a different
element, and both committed — one silently discarded the other's append. Elle
ruled out every model down to and including `read-committed`.

Root-caused to three stacked defects. The one this workload's shape exposes:
a read-modify-write's base was never validated anywhere — the intent's
`BaseRevision` was nominal and folded only into the dedup digest, so once the
in-memory write-intent lease lapsed, an RMW committed blind over a moved base in
every locking and validation mode. Fixed by freezing each read-then-written
key's pre-write observation into its prepared intent as the validated base, and
running a staged-base compare-and-set before anything durable is proposed.

### Aborted read (`:G1a`)

A transaction whose `commit-tx-session` answered `Aborted` had its append read
by a later transaction that committed.

**The contract question this raised is settled: it was a server bug.** The
workload maps a commit-time `Aborted` to `:fail`, and that mapping is correct —
the server was the side making the untrue promise. A decision replication that
*times out* is indeterminate rather than failed, and three exception-free paths
reported a definite abort without first installing the durable Abort record that
fences the stalled commit out. `FinalizeAdmission.Rejected` now answers
`MustRetry`, and rollback and the session reaper install a durable Abort through
the record CAS before claiming `RolledBack` — reporting `Committed` if a stalled
commit already won.

No change to `kahuna.client/indeterminate-types` was needed.

Worth remembering for next time: the first hypothesis — the blanket
`catch (Exception) → Aborted` in `ExecuteCommit` — was **wrong**, and cheap to
disprove. Debug logging was on for the failing run (400 000+ lines) and
contained zero `KahunaAbortedException` / `TaskCanceledException` entries, so
none of those handlers had fired.

### Verification

11 informative runs at `--faults partition,kill`, **1 936 committed
transactions, zero anomalies of either kind** — commit counts 163, 274, 207,
146, 73, 138, 57, 176, 107, 248, 347. Fault-free baseline clean (201 committed).

Three further runs committed nothing and are excluded as uninformative: all
three were washed out by the still-open HTTP 500 below. A run with zero commits
can neither confirm nor refute an anomaly.

Before the fixes these reproduced in 2 of 6 informative runs. If a defect at
that rate survived, 11 consecutive clean runs would happen about 1 % of the
time. Strong evidence, not proof — and all of it on a 2-CPU VM at rate 5, which
is one point in the timing space.

### `:G1a` recurred, then was fixed properly

**Now clean at the load that actually reproduces it:** 22 runs at concurrency 10
/ rate 15, 21 informative, **6 713 committed transactions, zero anomalies**.

The final fix stops a discarded attempt's `Aborted` being preserved in
`context.Result`: the one-phase fast path's pre-propose validation sets a
conflict `Aborted` when its write-skew probe finds a concurrent intent, then
falls back to the standard flow — whose re-validation can pass and commit. The
transaction committed and its writes were visible, but the client was handed the
stale `Aborted`.

The history below is kept because the process mistakes are more instructive than
the result.

On the next build (carrying the HTTP-500 `TryWaitForLeader` fix), `:G1a` came
back in **2 of 9 informative runs**, now accompanied by `:dirty-update` — a
transaction updating a key based on an aborted transaction's write, which is the
same root condition carried one step further. Same signature as before: a writer
with `:error :aborted` whose append was read by a committed transaction.

Two readings, which this data cannot separate: the fix is incomplete, or the
11-run verification was luck. 0/11 followed by 2/9 is not statistically
distinguishable from a constant rate (Fisher's exact ≈ 0.13). **Treat "11 clean
runs" as weaker evidence than it looks for a defect at this rate** — that is the
lesson worth carrying forward from this file.

**Load level decides whether you see it.** Confirmed on a later build:

| Config | Runs | Commits (mean / max) | `:G1a` |
|---|---|---|---|
| concurrency 5, rate 5 | 10 | 104 / 197 | 0 |
| concurrency 10, rate 15 | 8 | 274 / 716 | 1 |

The three occurrences to date landed at 243, 247 and 716 commits; the quiet
configuration has never reproduced it in ~20 runs. So reproduce with:

```bash
lein run test --workload append --faults partition,kill \
  --nodes n1,n2,n3,n4,n5 --time-limit 90 --concurrency 10 --rate 15
```

That resolves the earlier ambiguity in favour of "the verification measured the
wrong thing": the original 11 clean runs were all at rate 5, a configuration now
known not to reach the failure window. Sustained traffic appears necessary but
not sufficient — one clean run committed 617.

High load also produces more `:kill :all` washouts (2 of 8 here). Those commit
nothing and must be discarded, not counted as passes.

`:incompatible-order` stayed clean across all 10 of those runs and remains
fixed; `:dirty-update` is in the aborted-read family, not the lost-update
family.

## Open: HTTP 500 from `start-tx-session`

`start-tx-session` can return a 500 with a non-JSON body when an unhandled
exception escapes `LocateAndStartTransaction`. The originally-reported cause
(`Kommander.RaftException: Invalid partition`) is **fixed** — `TryWaitForLeader`
now maps it to `MustRetry`, and it occurred zero times in 10 retest runs against
286/212/231 hits before.

A `RetryableExceptionMapping` middleware now maps `RaftException` and the
`Unavailable` / `DeadlineExceeded` / `Cancelled` gRPC statuses to `MustRetry`.
The originally-reported cause (`Invalid partition`) is **fixed** — zero
occurrences in 32 subsequent runs.

One gap remains, and it is precisely one status code: **every** surviving
escape is `RpcException` with `StatusCode="Internal"`, carrying an
`HttpProtocolException` / `HttpIOException` inner. gRPC reports an HTTP/2
connection torn down *mid-stream* as `Internal` rather than `Unavailable`, and
killing a node mid-forward produces exactly that. Rare now — one run in the last
22, with 6 occurrences. The fix should match `Internal` only when the inner
chain is an HTTP/2 transport failure; bare `Internal` is also what a genuine
server-side fault produces.

## Open: runs that commit nothing

Roughly 1 in 10 runs produces **zero** committed transactions and so yields
`:empty-transaction-graph` — which reads like a clean result and is not one.
**Discard these; never count them as passes.** That distinction has already
caused one incorrect "verified fixed" conclusion in this repo.

Most are `:kill :all` (every node down, nothing servable). The interesting case
is a run whose whole 90 s went:

```
761  [:start :must-retry 200]     ← nodes up and answering, no leader resolvable
559  :connection-refused
  0  successful transactions
```

with no `:kill :all` at all — just repeated `:kill :majority` plus partitions.
The cluster gets 11–16 s between a restart and the next majority kill, so the
open question is whether Kahuna can form a quorum inside that window or whether
recovery is simply slower than the fault schedule. Measuring
restart-to-first-successful-transaction directly would settle it.

```
Kommander.RaftException: Invalid partition: 3
   at Kommander.RaftManager.GetPartition(Int32 partitionId)
   at Kommander.RaftManager.WaitForLeader(Int32 partitionId, CancellationToken)
   at Kahuna.Server.KeyValues.KeyValueLocator.LocateAndStartTransaction(...)
```

`raft.Joined` was true and `AmILeader(3)` returned without throwing, yet
`WaitForLeader(3)` threw — the node considers itself joined while its
`RaftManager` has no partition 3.

**This is the trap to know about:** a run that hits it produces zero commits, and
Elle then reports `:empty-transaction-graph`. That verdict means "nothing to
analyze", never "no anomalies". Check the node logs before reading it as
anything else:

```bash
grep -c "Invalid partition" store/kahuna-append-*/latest/n*/kahuna.log
grep -oE "\[:start nil [0-9]+\]" store/kahuna-append-*/latest/jepsen.log | sort | uniq -c
```

The workload records the HTTP status in its `[:start …]` error for exactly this
reason — a nil response type is otherwise undiagnosable from the history.

## Harness bugs worth remembering

Both produced verdicts that looked like server findings.

**Missing operation ids** made every transaction fail with `InvalidInput`, so
nothing committed and Elle reported `:empty-transaction-graph`. Every
transaction-scoped request must carry *both* a coordinator key and a non-zero
operation id — see [DESIGN.md](DESIGN.md#transactions).

**A slingshot selector applied to non-maps.** `(contains? % :kahuna/abort)` was
evaluated against every thrown object, including the `SocketTimeoutException`s
that partitions produce; `contains?` throws on those, which escaped as an
unhandled `IllegalArgumentException` *and* skipped the rollback. Selectors that
inspect map contents need a `map?` guard first.
