# Findings and status

For *why* the tests are shaped the way they are, see [DESIGN.md](DESIGN.md).

## Workload status

| Workload | Checks | Status |
|---|---|---|
| `register` | linearizable CAS register (Knossos) | green; found a stale-read violation, since fixed and verified |
| `lock` | mutual exclusion + fencing-token monotonicity, lease-aware | green as of Kommander 1.0.10; found a fencing rollback (fixed, 16/16 verified) and a checker bug that faked exclusion violations |
| `append` | Elle list-append over interactive transactions | found four real bugs; three fixed (write skew closed on absence, 29 runs), HTTP 500 all but fixed |
| `sequencer` | no id handed out twice; allocation-range integrity; idempotent replay | new; found an HTTP 500 on the redirect path before a nemesis was even enabled. Now in the nightly matrix under `partition` and `partition,kill` |
| `snapshot` | a pinned snapshot never changes its answer | green again as of `65fcc70`. Its first nightly found the bug it was written for: with two or more concurrent holds, every hold but the oldest rewound to the oldest one's revision (64 violating reads locally, and it needed no fault at all). Also found two wire-contract bugs while being built — a routed read returning no commit timestamp, and a floor response that could not express a refusal. Five server findings, all fixed and verified — the fifth being 287 unclassifiable responses per run when an inter-node gRPC stream died |

Not started:

- [x] membership nemesis — a node leaves the roster and rejoins, verified
      against the committed roster on every operation. **Not** via
      `/v1/cluster/membership`, which is GET-only: membership is driven by
      `--join-existing` and `--graceful-leave-on-shutdown`. See
      `src/kahuna/nemesis/membership.clj`.
- [x] snapshot-read workload exercising `readTimestamp` + snapshot holds — see
      `src/kahuna/workload/snapshot.clj`. Two wire-contract findings came out of
      building it; the first nightly run then found a real snapshot violation
      (holds collapsing onto the floor boundary). All fixed and verified.
- [x] `pause` fault in the CI matrix — wired in for `lock` and `register`. A
      paused process keeps its connections and its leases but stops answering,
      which is the one failure mode `partition` and `kill` cannot produce.
- [x] measure restart-to-first-successful-transaction — `kahuna.checker.recovery`
      runs on every test and reports, per fault-free window, how long until an
      operation succeeds. Read the caveats with the numbers: it is an upper
      bound, not a clean measurement.
- [x] apply that measurement to a run that commits nothing. Caught one in a
      10-run hunt. The washouts are nodes that are listening but have not
      completed cluster initialization — mechanism below, filed against the
      server.
- [x] separate initialisation time from consensus recovery — unblocked by
      `GET /v1/cluster/health` on the server, sampled by
      `kahuna.nemesis.health` and split by the recovery checker into
      `:init-ms` / `:consensus-ms`.

## Closed: "mutual exclusion violated" was a bug in *this* checker

For a while this was recorded here as the most serious finding in the project:
two owners each told they held the same lock, in the worst case for **8.3 seconds
of a 10 s lease**, appearing 4 times in 8 runs and in no run before the Kahuna
lock fix. All of it was a false positive produced by `holds`.

**The mechanism.** All three violations have one history shape. The holder
invokes a release, which **times out** because the partition cut the ack path
(`:info`), and Jepsen renumbers the process; the same thread later retries the
release. `holds` handled `[:invoke :release]` with
`(assoc h :release-start (:time op))`, so each retry **overwrote** the recorded
release time. The hold was then closed using the *retry's* start, stretching the
"definite hold" window seconds past the *first* release attempt — across a period
in which that first release had already committed on the server.

That the release really did commit is provable from the histories: the next
grants mint cleanly ascending tokens, and `TryLock` only mints
`entry.FencingToken + 1` when the head record reads free. A leader minting *k+1*
had applied the unlock. The lock was genuinely free; the grants were correct.

**The server was exonerated**, including the `State == Locked` busy gate that
this file previously blamed. The gate was granting against unlock records that
had genuinely committed.

Why it only appeared after the lock fix: it needs a release to time out
mid-partition *and* the next grant to land inside the stale window. The lock fix
(correctly) stopped refusing grants against cold-loaded unlock records, so
post-release grants started completing promptly inside that window.

**Fix** (in `src/kahuna/workload/lock.clj`): keep the *earliest* `release-start`
(`(update h :release-start #(or % (:time op)))`), and end pending holds at
`min(lease-end, release-start)` instead of ignoring an in-flight release. Two
negative controls added in `test/kahuna/workload/lock_test.clj` — an
indeterminate-release-then-retry history must be **accepted**, and an overlap
occurring before any release attempt must still be **rejected**, so the fix
cannot have simply blinded the checker. `lein test kahuna.workload.lock-test`:
8 tests, 12 assertions, 0 failures.

**A caution about how this was nearly mis-closed.** The first write-up credited
Kommander 1.0.10 with fixing it, on the strength of an 8-of-8 clean sweep and a
"0.4 % by chance" argument. That was wrong: those runs already had the fixed
checker in the working tree, so two changes moved at once and the checker fix
alone explains the result. The statistic was also void — its 4-in-8 prior had
been measured with the *buggy* checker. When a test-suite change and a server
change land in the same window, a clean run set attributes to neither.

## Closed: fencing tokens roll back under partition alone

The lock partition's fencing counter **rolled back 36 grants and replayed them**,
with no process ever killed:

```
… 113(n3) 114(n3) 115(n5)   ← monotonic to 115
   79(n5)  80(n5)  81(n4)   ← restarts at 79
   82(n5)  83(n3)  84(n5) … ← monotonic again
```

Each replayed token went to a *different* owner than the first time, five
seconds after a single node was isolated — and on the **majority** side.

Not the previously-fixed fencing bug: that one needed `kill` to create a stale
node. Root-cause hypothesis (code-traced, not yet confirmed): a token is minted
from `entry.FencingToken + 1` where the entry is loaded from *locally applied*
state, and nothing makes a newly promoted leader finish applying the committed
log before it mints. Reads now gate on `ConfirmLeadershipAsync`, which carries a
promotion barrier; **writes do not**.

**Still failing after the first fix attempt** (2026-08-06). Now reproduced
locally at `--nemesis-interval 5`, with a *larger* rollback than CI's: **125 →
16**, 1 of 8 runs. At the default interval 15, 5 of 5 were clean — so shortening
the interval is what surfaces it, and a safety property should not depend on how
often leadership moves.

That fix corrected this file's earlier diagnosis: the **cold-load** path (backend
∪ overlay) was already right; the stale state was the **resident** cache entry on
a *former* leader, frozen at its last tenure and reused on re-promotion.

**A second fix attempt (Kommander 1.0.9) did not fix it**: fencing failed 2 of 8
runs. The rate is flat versus 1.0.8 (1 of 8) — not a distinguishable difference
at this sample size — but the *severity* is worse. Max token reached only 57 of
109 acquires in one run, and another produced a failure mode not seen before:
**`:token-reused-by-other-owner`**, the same token handed to two different
owners. That is worse than a monotonicity break, because a resource fencing on
token comparison cannot order two holders carrying an identical token at all.

The exclusion violations recorded alongside these were a checker bug, not a
consequence of this one — see the closed section above.

Diagnose by extracting the token sequence in grant order and looking for a
discontinuity, rather than trusting the violation list alone:

```bash
grep -oE ':token [0-9]+, :node "n[0-9]"' store/kahuna-lock-partition/latest/jepsen.log
```

### Fixed in Kommander 1.0.10 — verified 16 of 16

Root cause was in Kommander, not Kahuna: promotion drains read the WAL while
enqueued writes still sat in the write scheduler's queue, so the reads missed
them. Old code silently skipped them (the original 115 → 79); 1.0.9's new
gap-detection misclassified them as holes and orphaned everything above on
ordinary commits, which is why severity got worse rather than better. Server
logs show n5 advertising freshness 117 while its own drain reported "hole at 19",
then serving anyway. Kahuna's only change was the pin, 1.0.9 → 1.0.10.

**Two independent 8-run sets, all clean**, at the configuration that produced the
2-of-8 rate (`--faults partition`, 180 s, concurrency 10, rate 10,
`--nemesis-interval 5`):

| Set | Acquires (min–max) | Fencing |
|---|---|---|
| 1 | 85–128 | 8 of 8 clean |
| 2 | 66–155 | 8 of 8 clean |

No `:token-went-backwards`, no `:token-reused-by-other-owner`, and `max-token`
tracks `acquire-count` within a few in every run. At the 25 % prior, 16
consecutive clean runs is a **~1 %** outcome by chance; one 8-run set (~10 %)
was explicitly judged insufficient, which is why a second was run.

Unlike the exclusion result above, this one is **not** confounded by the
concurrent checker fix: `fencing-checker` builds its own `acquires` pass straight
from the history and never calls `holds`. The changed code feeds
`exclusion-checker` alone.

Verified at `--nemesis-interval 5` only; interval 15 is strictly less sensitive
to this bug, so it would add nothing.

## Closed: write skew (`:G2-item`) under partition + kill

An anti-dependency cycle — two transactions each read a key the other then
wrote, with no serial order explaining it. Elle rules out `:repeatable-read`
and `:serializable`:

```clojure
:steps ({:type :rw, :key 21, :value 12, :value' 13}
        {:type :rw, :key 24, :value 26, :value' 28}
        {:type :wr, :key 24, :value 28})
```

**1 of 15 informative runs (~7 %)** at concurrency 10 / rate 15. Both
transactions committed, so this depends on no classification judgment in the
harness — it comes from committed reads and writes alone. That matters because
the config under test is `Pessimistic` + `TrackAndValidate`, and read-set
validation is exactly the mechanism meant to prevent write skew.

A fix attempt on 2026-08-06 produced 6 clean runs — which **does not verify it**.
At a 7 % rate, six clean runs happen ~65 % of the time by chance. Verifying needs
~30 runs, or better, a deterministic test of the interleaving.

A third distinct class, not a recurrence of the aborted read or the lost update.
Whether it is *new* is undetermined: 0 in 22 runs on the previous build and 1 in
15 here is consistent with it having been there all along (at 7 %, 22 clean runs
happen ~20 % of the time), and it may simply have been invisible while louder
anomalies were failing the same runs.

### Closed on Kommander 1.0.10 — 29 runs, 29 clean

No `:G2-item`, no washouts, every run `:valid? true`. Commit counts 78–744, so
the runs sat squarely in the sustained-load window where the anomaly appeared.
At the 7 % rate, 29 clean runs is a **~12 %** outcome by chance — the ~30-run bar
set when the earlier 6-run attempt was judged worthless (~65 % by chance).

One run committed only 10 transactions and is discounted, making the honest count
**28 informative runs**. Recorded rather than silently included, since a
near-empty run padding the denominator is exactly how a 7 % bug hides.

**Closed on absence, not on a traced fix** — weaker than the fencing case, and
worth remembering as such. No code change was ever tied to this anomaly. The
plausible mechanism is the Kommander 1.0.10 fix: a leader serving on an
incomplete committed projection would let `TrackAndValidate` pass a read set it
should have rejected, since validation only sees what the node has applied. That
would make write skew a side effect of the same defect as the fencing rollback —
consistent with everything observed, but unconfirmed. A deterministic test of the
interleaving is what would actually settle it.

Reopen on any `:G2-item` at concurrency 10 / rate 15 on 1.0.10 or later; given the
~12 % residual, treat a recurrence as "it was always there", not a new regression.

## Closed: stale reads from a minority-partitioned node

**Fixed and verified 2026-08-06** — `register / partition` 8 of 8 clean, against
a prior 1-in-3 reproduction rate and a CI failure. The fix,
`ConfirmLeadershipForRead`, gates authoritative local reads on a
quorum-confirmed Raft read-index instead of the local `AmILeader` belief,
answering `MustRetry` when confirmation fails — applied across the KV, lock and
sequence locators.

The original report follows, because the diagnosis technique is reusable.

A node cut off from the majority keeps
answering reads with its last-known value, returned as a normal successful
`Get`. Knossos reports a linearizability violation; a client has no way to tell
the response apart from a current read.

The asymmetry is the tell: on the same isolated node, **writes correctly return
`MustRetry`** — it knows it cannot replicate. Only reads are served from local
state without a quorum check.

```
20:00:04.47  proc 244 (n2)  :info  :write [19 0]  :must-retry   ← writes refuse
20:00:06.44  proc  81 (n1)  :ok    :write [19 1]
20:00:06.51  proc 253 (n2)  :ok    :read  [19 0]  ← STALE, reported OK
20:00:09.46  proc 253 (n2)  :ok    :read  [19 0]  ← still stale, ~11 s in
```

n2's value froze at the value committed just before the partition, while the
rest of the cluster moved on. Reproduced locally in **1 of 3 runs** with a
different partition shape and key, so it is not a one-off.

`KeyValueLocator.LocateAndTryGetValue` serves a read locally when
`raft.AmILeader(...)` is true, or when the resolved leader equals the local
endpoint — both *local beliefs* rather than confirmed quorum. An isolated leader
holds that belief until it steps down. Fixing it needs a read index or a leader
lease, answering `MustRetry` when leadership cannot be confirmed.

Whether this was a regression is **still unresolved** — the job was green
throughout the project's history and 1-in-3 is high, which points that way, but
the `AmILeader`-gated read is not new code, and a bisect was blocked because
`39ec8e0` does not build standalone (`CS0246: IRaft`; it pins an older Kommander
than the current fixes need). Moot for remediation, open for release notes.

Read throughput under the new gate is **not** established by these runs: the
Jepsen generator is rate-limited, so it never saturates reads. Each confirmed
read now dispatches to the partition's single-writer thread, which is a
plausible serialization point at high QPS — that needs a throughput benchmark,
not a fault-injection run.

To diagnose a failing run, map the stale reader to its node —
`thread = process mod concurrency`, `node = thread mod 5 + 1` — and check it
against the `:start-partition` topology in `jepsen.log`.

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

## Mostly closed: runs that commit nothing

Roughly 1 in 10 runs produces **zero** committed transactions and so yields
`:empty-transaction-graph` — which reads like a clean result and is not one.
**Discard these; never count them as passes.** That distinction has already
caused one incorrect "verified fixed" conclusion in this repo.

The mechanism is now known — nodes that are listening but have not finished
initialising, refusing every request with `MustRetry` (see *"Answered"* below).
What is still open is why initialisation takes as long as it does.

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
recovery is simply slower than the fault schedule.

### Answered: the nodes are up, and not initialised

A hunt of 10 × 300 s `append` / `partition,kill` runs caught one of the
~1-in-10 washouts, and it settles the mechanism. Run
`store/kahuna-append-kill,partition/20260806T205010.154Z`: zero commits, and
every failure accounted for.

```
1614  [:start :must-retry 200]
1338  :connection-refused          ← client's node was killed
  15  :timeout
   8  :no-http-response
   0  successful transactions
```

All **1614** `MustRetry` responses match 1614 occurrences in the node logs of a
single message, character-identical every time:

```
KEYVALUE leader for partition 3 could not be resolved, returning MustRetry:
Cannot resolve leader for partition 3: node has not completed cluster initialization
```

with, from the same nodes:

```
JoinCluster: waiting for initialization (1005 ms elapsed): local=n1:8082 role=Voter
systemLeader=none systemState=Active p0MaxLog=1 p0Term=1 userPartitions=0/3 initialized=false
```

So the washouts are not a mystery and not primarily an availability problem
either: the nodes come back, open their HTTP port in about a second, and then
refuse everything until initialisation completes. The run contained a **22.9 s
window with no fault active at all** that committed nothing.

Filed against the server as *"No readiness signal: the API serves before
cluster initialization completes"*, and **since fixed**: `GET
/v1/cluster/health` now reports `{ready, initialized, localRole}` with 200/503,
and `initialized` was added to the membership response. Confirmed against a
live node mid-initialisation:

```
{"ready":false,"initialized":false,"localRole":"Voter"}  [HTTP 503]
```

Note the `role=Voter` in that state — the roster never was a usable readiness
check, which is why the flag has to gate first.

**Why the timing question is still open.** Knowing the refusal is "still
initialising" does not say how long initialising *should* take, or why
`systemLeader=none` persists. That remains the question at the top of this
section.

### Recovery latency, split into initialisation and consensus

Kahuna now exposes `GET /v1/cluster/health` — `{ready, initialized, localRole}`,
200 when the node can serve and 503 while it cannot. `kahuna.nemesis.health`
samples it on a timer and records `{node → ready?}` into the history;
`kahuna.checker.recovery` uses those samples to split each fault-free window at
the moment every node reported ready.

A 300 s `append` / `partition,kill` run at `--health-interval 0.5`:

```
:windows 12  :recovered 7  :never-recovered 4
:recovery-ms  {:min 26, :median 1428, :p95 3270, :max 3270}
:init-ms      {:median 17,   :max 377}     ← waiting for every node ready
:consensus-ms {:median 2546, :max 3253}    ← from all-ready to first success
```

Unlike `:port-open-ms`, these two **do** sum to `:recovery-ms` within a window,
and a test asserts it.

**In healthy windows, initialisation is not the cost — consensus is.** With
every node already back, readiness is essentially instant (17 ms median) and
the remaining ~2.5 s is the cluster resolving a leader before it will serve.
That does not contradict the washout finding above: those runs are the other
regime, where initialisation never completes at all.

#### Sample the readiness signal, never wait on it

Readiness is not node-local — `IsInitialized` needs the partition map from the
P0 leader, so a node cannot become ready while a partition or majority kill is
in force. Waiting for it inside a nemesis op therefore waits out unrelated
faults, and jepsen runs one nemesis process, so a nemesis that is waiting is a
nemesis that is not applying faults. Sampling observes the same transition
without touching the experiment; verified by comparing fault counts across
sampling rates (kill 22 vs 20, start 48 vs 38 at 0.5 s vs 2 s).

#### The sample interval bounds what you can conclude

At the 2 s default, only 1 of 3 recovered windows contained a sample, and that
one reported `init-ms 2194 / consensus-ms 2205` — a tidy 50/50 split that was
pure sampling artifact. At 0.5 s, 6 of 7 windows decomposed and initialisation
turned out to be two orders of magnitude smaller. A window shorter than the
interval cannot be split at all, and one barely longer is mostly quantisation
error. Lower `--health-interval` before quoting a number, and check how many
windows actually carry `:init-ms` before believing the median.

#### Three earlier versions of this measurement were wrong

Recorded because the failure mode repeats and is nearly invisible — each
version produced plausible numbers under an inaccurate label, and nothing went
red.

1. **Windows opened at every fault-ending op.** Under `partition,kill` that
   timed "recovery" from a partition heal while nodes were still killed. It
   reported a 164 ms median from one run, and a per-fault breakdown that looked
   meaningful and was not. Fixed: a window opens only when the *last*
   outstanding fault ends, pinned by
   `a-window-opens-only-when-the-last-fault-ends`.
2. **Readiness gated on HTTP 200.** The same `up?`-versus-`voter?` error as the
   membership nemesis: the port answers about a second after launch, so
   "startup" read as ~1 s while the node was still minutes from serving.
3. **Readiness gated on a real KV probe.** Correct-sounding and worse. Whether
   a KV request succeeds depends on the *cluster* being serviceable, not on
   this node being initialised, and the nemesis calls `start!` while other
   faults are still active — so each start blocked for the full 60 s timeout. A
   300 s run fell from ~20 nemesis windows to 5, none recovered, and the
   reported startup time was the timeout value itself. The harness was
   rewriting the fault schedule it was supposed to be observing.

The surviving rule: a nemesis that is waiting is a nemesis that is not applying
faults, and any wait inside a fault op is a change to the experiment.

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

## Closed: a routed read lost its commit timestamp

*Filed against the server; fixed in `2e006ad` and verified.*

**A `try-get` served by a node that does not own the key comes back with
`lastModified` of zero.** The value and revision are correct; the timestamp,
the last-used time and the state are all zeroed.

`Kahuna.Shared` documents `lastModified` as the field callers round-trip into a
later snapshot read:

> When the entry was last written. Callers round-trip this into a later
> snapshot read, so it must carry the real commit time rather than a
> placeholder.

**Why it is worse than a missing field.** Zero is not a neutral value in this
API — `HLCTimestamp.Zero` is the *read the latest committed value* sentinel for
`readTimestamp`. A client that follows the documented round-trip therefore does
not get an error, or an old value, or a rejection. It gets a **latest read
wearing the shape of a snapshot read**, on every request that happened to be
routed, with nothing anywhere to indicate the snapshot was ignored. Anything
built on as-of reads — point-in-time restore, consistent backup, repeatable
analytics scans — is silently reading the present instead of the past whenever
it talks to a non-owning node.

**The mechanism.** `KeyValuesService.TryGetKeyValueInternal` builds the gRPC
response for the inter-node hop and assigns only `Revision` and the three
`Expires*` fields:

```csharp
GrpcTryGetKeyValueResponse response = new()
{
    ServedFrom = "", Type = (GrpcKeyValueResponseType)type,
    Revision = keyValueContext.Revision,
    ExpiresNode = ..., ExpiresPhysical = ..., ExpiresCounter = ...,
    TimeElapsedMs = ...
};
```

`LastModified{Node,Physical,Counter}`, `LastUsed*` and `State` are declared in
`keyvalues.proto` and *are* read back by
`GrpcInterNodeCommunication.TryGetValue`, so they arrive as protobuf defaults —
zero, and `KeyValueState.Undefined`. The REST handler on the contacted node
then faithfully forwards those zeros. Local reads are unaffected, which is why
this survives a single-node test.

**Evidence.** In a 45 s five-node run, every successful pin came from one node
(n5, four different keys) and every `:no-timestamp` failure came from n1, n2 or
n4 — a per-node split, not a per-key one, which is the signature of routing
rather than of anything key-specific. The workload refuses to pin on a zero
timestamp, so this shows up as pin failures rather than as silently vacuous
snapshot reads; that guard is the only reason the workload does not quietly
verify nothing. See `:no-timestamp` in `src/kahuna/workload/snapshot.clj`.

### Fixed — the signature reversed

`TryGetKeyValueInternal` (and its `try-exists` sibling) now assign `LastUsed*`,
`LastModified*` and `State`. Verified on a 120 s `partition` run:

| | before | after |
|---|---|---|
| `:no-timestamp` pin failures | 28 of 55 | **0** |
| nodes serving successful pins | n5 only | n1 27, n2 23, n3 19, n4 15, n5 19 |
| holds / protected reads | 53 / 147 | **103 / 284** |

The node distribution is the part worth keeping. The bug's whole signature was
that pins concentrated on a single node; the fix's signature is that they spread
evenly across all five. That is a prediction the fix could have failed, rather
than an absence of errors that a dozen unrelated things could explain.

The property itself still holds afterwards — 183 of 284 protected reads served
from below head, max depth 14, no stability, leakage, provenance or floor
violations — and again under `partition,kill`.

## Closed: `/v1/kv/snapshot-floor` answered 200 with a body it could not express

*Filed against the server; fixed in `2e006ad` and verified.*

When the floor read fails retryably — typically on a node that is not the
meta-partition leader and cannot reach it — `RetryableExceptionMapping`
rewrites the response body to the key/value surface's generic
`{"type":101}` (MustRetry) and leaves the status at **200**.

`KahunaGetSnapshotFloorResponse` has no `type` field, so a client that reads
the documented shape sees `effectiveFloor` and `liveHolds` simply absent. The
obvious client-side reading of "no `liveHolds`" is *zero live holds* — the
exact opposite of the truth, which is that nothing was measured. A checker that
believed it would report the floor as empty while holds were live.

The harness treats a floor response without a numeric `liveHolds` as a failed
observation rather than a zero, and classifies a `type` of `:must-retry`
accordingly. The general shape is worth remembering: **a middleware that
normalises errors across a family of endpoints will produce bodies that some of
those endpoints' own contracts cannot represent.**

### Fixed

`KahunaGetSnapshotFloorResponse` now carries a `type` (`Get` on success), so a
substituted MustRetry deserializes into a legal instance and a refusal is
distinguishable from an empty success. The same audit added `type` to the other
`/v1/kv/` responses that lacked one — set-many, delete-many and many-values —
which is the more valuable half of the fix: those had the same latent trap and
none of them had been observed failing yet.

Verified on the same 120 s run: 131 floor reads ok, 8 `:info` (a genuine
MustRetry under partition, now correctly classified as indeterminate rather than
as a malformed response), none unreadable. Before the fix one node returned 9 of
9 unusable bodies.

The harness now checks the type *and* the count rather than inferring reachability
from whether `liveHolds` was present — the contract states it outright now, so
there is no reason to keep guessing from a field's absence.

## Closed: a held snapshot silently rewound to the floor-boundary revision

*Found by the first nightly `snapshot / partition` run (2026-08-07), reproduced
locally on the same commit. Filed against the server; fixed in `65fcc70` and
verified.*

**A read as of T, under a live hold at T, stops returning the value at T and
starts returning the oldest surviving revision instead — the one pinned by the
cluster-wide floor.** The answer changes mid-run, on every node at once, and
never changes back while the hold lives.

One hold from the nightly, in full:

```
05:31:52.305  :pin   key 4, t=L1786080708415, value "v960", revision 48   (n5)
05:31:53.761  :read  → "v960" rev 48                                     (n2)
05:32:02.161  :read  → "v960" rev 48                                     (n3)
05:32:05.423  :renew ok, lease-expiry L1786081325418
05:32:05.922  :read  → "v129" rev 5, lastModified L1786080640263         (n1)
05:32:07.971  :read  → "v129" rev 5                                      (n2)
... same answer from n1, n2, n3, n4 and n5 until the release at 05:32:26
```

The hold was renewed 500 ms before the flip and released 20 s after it, so no
lease lapsed; the read went backwards 43 revisions while the client held the
snapshot open and the server kept confirming the hold.

**The `lastModified` of the wrong answer is the tell.** In the local
reproduction it is `{:n 2, :l 1786114418220, :c 4}` — *bit-for-bit the effective
floor* the same run reported from `/v1/kv/snapshot-floor`:

```
:ok :floor {:floor {:n 2, :l 1786114418220, :c 4}, :live-holds 14, :node "n2"}
:ok :read  {... :value "v26", :revision 1, :last-modified {:n 2, :l 1786114418220, :c 4}}
```

The read did not return a random old version, or a version at the reader's own
timestamp. It returned **the floor-boundary revision**: the newest revision at
or before the *minimum* live hold in the cluster. Every hold above the minimum
collapses onto it.

**The mechanism.** `BaseHandler.RemoveExpiredRevisions` trims the in-memory
revision archive to the newest `RevisionRetention` entries, and exempts exactly
one older revision — the floor boundary — so that a read at the floor
timestamp still hits memory. The in-memory archive is therefore **not a
contiguous newest-N suffix**: it is `{boundary} ∪ {newest N}`, with a hole
between them.

`TryGetHandler` then does this:

```csharp
if (!entry.TryGetRevisionAtOrBefore(message.ReadTimestamp, out long snapRevision, out var snapshot))
{
    // In-memory archive trimmed the as-of revision; fall back to the persisted
    // revision history. This is correct because trimming drops the lowest revision
    // numbers, so an in-memory miss means the true as-of answer (if any) is older
    // and only on disk.
    ...GetKeyValueRevisionAtOrBefore(...)
}
```

The comment states the invariant the code depends on — *trimming drops the
lowest revision numbers* — and the boundary exemption is precisely what breaks
it. `TryGetRevisionAtOrBefore` returns the highest **in-memory** revision at or
before the snapshot, which for any T landing in the hole is the boundary. It
returns `true`, so the disk fallback never runs.

The disk was innocent throughout: `SqlitePersistenceBackend.PruneKeyRevisions`
takes the floor and deletes only `revision < floorRevision`, so the correct
revision 48 was sitting on disk the entire time. Nothing was lost. The read
simply stopped asking.

The same `TryGetRevisionAtOrBefore`-then-fall-back-on-false shape appears in
`TryExistsHandler`, `TryGetByRangeHandler` and `BucketScanContinuation`, so
point reads are unlikely to be the only affected path.

**Why the server's own guards stayed silent.** Two of them are pointed at the
adjacent question. `SnapshotFloorMetrics.MissingProtectedVersion` fires when a
trim *drops* the boundary; here the boundary is faithfully kept and then handed
out for timestamps it does not answer for. And
`TestSnapshotFloorPinEndToEnd` reads at `t1` — the floor timestamp itself,
where the boundary *is* the right answer — and asserts the disk fallback was
**not** called (`Assert.Equal(0, callsAfter - callsBefore)`), which is the exact
behaviour that makes the hole unreachable. No existing test reads at a
timestamp *above* the floor whose revision has been trimmed, which is the entire
bug.

**Scope.** A single-hold deployment cannot see this: with one hold the floor is
that hold, T is the boundary, and the boundary answer is correct. It needs two
or more concurrent holds at different timestamps — then every hold except the
oldest reads the oldest one's data. That is the ordinary case for anything that
takes overlapping consistent snapshots: concurrent backups, a long analytics
scan next to a PITR cut, two readers on different cursors.

**Evidence.**

| | nightly CI (`partition`, 300 s) | local repro (same config) | local, **no faults**, 180 s |
|---|---|---|---|
| fault windows | 10 | 10 | **0** |
| holds | 202 | 257 | 157 |
| protected reads | 578 | 681 | 387 |
| violating reads | ≥10 (checker caps the list) | **64**, over 16 holds and all 5 keys | **2**, over 2 holds |
| max read depth | 160 | 219 | 135 |
| leakage / provenance | clean | clean | clean |

**No fault is required.** The third column is a run with the nemesis producing
nothing at all (`:windows 0`), and it still fails. That follows from the
mechanism — a retention trim and a read are the only moving parts — and it makes
the bug far cheaper to reproduce than the nightly config suggests. Faults only
amplify it: a partition delays releases, so more holds sit open at once and the
gap between each hold and the cluster floor grows.

Leakage clean matters too: the wrong answers are all at or before the reader's
snapshot, so nothing here is a visibility violation the server would notice
internally. It is a *stale* snapshot, not a leaking one, which is why it can run
for a whole nightly without a single error in the server logs.

### Fixed — the hole is now declared rather than assumed away

`KeyValueEntry` gained two fields the trim path maintains: `FloorBoundaryRevision`
(which revision is pinned below the contiguous window) and
`FloorBoundaryCoverageEnd` (the earliest `LastModified` ever trimmed *above* the
boundary). `TryGetRevisionAtOrBefore` now reports a miss when the only candidate
is the boundary and the snapshot falls at or after that bound, so the caller
takes the disk path it was always supposed to take.

The shape of the fix is the part worth keeping. The old code carried the
invariant in a comment — *trimming drops the lowest revision numbers* — while a
different function quietly broke it. The new code makes the discontinuity a
value the entry carries, so the read path can ask instead of assume.

Verified on both configurations that failed:

| | `partition`, 300 s | no faults, 180 s |
|---|---|---|
| violating reads | 64 → **0** | 2 → **0** |
| protected reads | 681 → 652 | 387 → 486 |
| reads below head | — → 476 | — → 444 |
| max read depth | 219 → 44 | 135 → 55 |

Holds (215, 169) and protected reads are in line with the failing runs, so this
is not a pass bought by measuring less. **Max depth falling is the fix, not a
loss of coverage**: the enormous depths before were themselves the bug — reads
being answered with revision 0–5 while head sat near 220. What remains is
ordinary snapshot depth, still below head for 476 and 444 reads respectively.

A `partition,kill` run also passed, but thinly — 30 protected reads against a
threshold of 25, 9 below head. An earlier attempt at the same config washed out
entirely (15 holds, `:valid? :unknown`, pins failing on `:must-retry` and
connection-refused). Read the `partition` job as the one carrying the signal.

### Why `partition,kill` kept washing out: `:kill :all`

The 2026-08-08 nightly returned `:unknown` at 20 protected reads with
`:max-depth 1` — not one read in 300 s got deeper than a single revision below
head. The cause was the kill target mix, not the budget. `nemesis-packages`
drew from `[:one :majority :all]` and landed `all` four times and `majority`
four times in eleven kill events:

| | healthy nodes | share of run |
|---|---|---|
| all five | 5 | 44% |
| no quorum | 2 | 24% |
| nothing listening | 0 | 16% |

147 of 2547 operations succeeded. `:recovery` agreed — 13 windows, 8 never
recovered, `:starved-window-ms (0 0 0 55 211 974 4694 6952)`: the nemesis
re-fires before the cluster is usable, because a full restart costs ~2.4 s to
open the port and another ~2.2 s to reach consensus out of a 15 s interval.

A full-cluster kill is a legitimate fault for `register` or `lock`, whose state
is the client's. It is self-defeating here: it destroys every live hold, and
re-pinning costs a leader election plus a fresh write, so the run spends its
budget re-acquiring instead of reading retained history. `--kill-targets` now
exists for this; the CI job passes `one,majority`. On the same 300 s config that
took the run from `:unknown` (20 protected reads, max depth 1) to `:valid? true`
(30 protected reads, 26 holds, 14 below head, max depth 3), with recovery at
9 of 11 windows instead of 4 of 13.

Two honest caveats. One run is one run, and 30 against a threshold of 25 is
still thin — expect this job to return `:unknown` sometimes. And the throughput
difference between individual runs of this config is dominated by which faults
the nemesis happens to draw, not by the flag: an intermediate run with the same
`one,majority` targets managed only 22 acknowledged writes where the verified
one managed 363.

## Closed: `/v1/kv/snapshot-floor` reported zero live holds while holds were live

*Observed twice in the same nightly run; not reproduced locally. Filed against
the server; fixed in `65fcc70`.*

`GetSnapshotFloor` is documented to answer from the meta-partition leader —
`AmILeader` / `WaitForLeader`, otherwise route. Twice in the nightly, two nodes
gave contradictory answers a fraction of a second apart, with no fault active
and all five nodes reporting healthy:

```
05:30:37.465  :floor {:floor {:l 1786080635983}, :live-holds 2, :node "n1"}
05:30:37.574  :floor {:floor {:l 0},             :live-holds 0, :node "n4"}   ← 109 ms later
...
05:30:41.968  :floor {:floor {:l 0},             :live-holds 0, :node "n3"}
05:30:42.220  :floor {:floor {:l 1786080635983}, :live-holds 3, :node "n5"}   ← 252 ms later
```

The checker only asserts the zero case — "none at all, while we provably hold
one" — because the count is cluster-wide and "fewer than expected" has innocent
explanations. Here the server refutes itself: n1 named the floor and the count
while n4 said there was nothing, so no reading of the count makes both true.

Either a non-leader answered from its own replica, or a node briefly believed
itself meta-partition leader before its hold state was applied. Both come out
the same way at the API: **zero live holds and a floor of `HLCTimestamp.Zero` —
the value that means "reclaim anything"**. This fails open.

It is worth more than a wrong number, because the retention trim does not call
this endpoint at all: `RemoveExpiredRevisions` reads
`context.SnapshotFloorStore` *locally*, on every node, and starts with
`if (floorStore.Holds.Count > 0)`. A node whose hold set is empty for the same
reason prunes as though nothing were held anywhere. The endpoint is the only
externally visible symptom of a condition that has a much quieter internal
consequence.

Not reproduced in one 300 s local run — both zero-count reads there landed
before any hold existed, which is legitimate. The CI runner is slower and
elects later, which is consistent with a narrow window around meta-partition
leadership settling.

### Fixed — and the evidence is a refusal, not an absence

`GetSnapshotFloor` now answers locally only under read-index leadership
confirmation (`ConfirmLeadershipAsync`), never from local belief, and returns
`MustRetry` when leadership cannot be confirmed or the node has not joined. It
fails closed instead of reporting a floor of `Zero`.

An absence of zero-counts would be weak evidence here, since the race never
reproduced locally in the first place. The useful signal is the opposite one:
under `partition`, **23 floor reads came back `:info` (MustRetry)** against 218
successes — the new fail-closed path firing, on exactly the node states that
previously fabricated a zero. Fault-free, where leadership is never in doubt, it
fires once in 160. And every remaining zero-count read in both runs now lands
*before the first pin of the run*, where zero is the truth.

The harness needed no change: it already required `type == Get` **and** a numeric
`liveHolds` before recording an observation, so a MustRetry body — which now
carries a real `liveHolds: 0` — is still classified as "nothing was measured"
rather than "no holds". Requiring both, rather than inferring from the count
alone, is what makes that hold.

## Closed: unclassifiable HTTP 500s when an inter-node gRPC stream dies

*Found 2026-08-08 on `/v1/kv/snapshot-floor`; fixed in kahuna `c67ab1f`, and the
fix showed the blast radius was four times larger than the report.*

**This was under-reported when filed.** It went in as a floor-endpoint bug
because the floor endpoint is the only place the harness could see it: that
client branch records `(:status r)` when the response carries no recognizable
type, so it alone produced a legible `[:floor 500 "n1"]`. Every other endpoint
recorded the same condition as `[:write nil]`, `[:read nil]`,
`[:read-latest nil]` — a nil error type meaning "the server sent something this
client could not parse" — and those sat in the same error tally where the 500s
were found, read past as ordinary fault noise. Same 300 s `partition` config,
counted properly:

| | before `c67ab1f` | after |
|---|---|---|
| `[:floor 500 …]` | 18 | 0 |
| `[:write nil]` | 186 | 0 |
| `[:read nil]` | 42 | 0 |
| `[:read-latest nil]` | 35 | 0 |
| `[:acquire nil]` / `[:pin-lost-race nil]` | 6 | 0 |
| **total unclassifiable** | **287** | **0** |

It was never confined to the floor read; it hit the main KV data path.

`GetSnapshotFloor` answers from the meta-partition leader, routing to it when
this node is not it (`KeyValuesManager.cs:940`). Every other exit from that
method returns a typed `KeyValueResponseType`; the routed one is
`return await interNodeCommunication.GetSnapshotFloor(leader, ct)` with no
guard, so a gRPC transport failure escapes as an exception and the client gets
an HTTP 500 with a non-JSON body.

`RetryableExceptionMapping` exists precisely to stop that — its own doc comment
names "an inter-node gRPC stream dying mid-forward" as the case it catches. It
misses this one for two independent reasons:

```csharp
public static bool IsRetryable(Exception ex) => ex switch
{
    RaftException => true,
    RpcException rpc => rpc.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Cancelled,
    _ => false,
};
```

1. **The status is `Internal`, not `Unavailable`.** A dead HTTP/2 connection
   surfaces as `RpcException: Status(StatusCode="Internal", Detail="Error
   starting gRPC call. HttpRequestException: The HTTP/2 server didn't respond to
   a ping request within the configured KeepAlivePingDelay")`. That is exactly
   "no definitive answer was produced", which is the predicate the comment
   states, but it is not in the allowed status list.
2. **Only the outermost exception is inspected.** The same stack also throws
   `AggregateException: One or more errors occurred. (Status(StatusCode=...))`.
   No unwrapping of `InnerException`/`InnerExceptions`, so even a genuinely
   `Unavailable` RpcException escapes once something wraps it. The node logged
   22 `Unavailable` RpcExceptions in the same run.

**Why the 500s outlive the fault.** Seven of the eighteen landed in a window
with *no fault active* — the partition that broke the connection ended at
15:14:36 and the network was healed from 15:15:06 to 15:15:25, yet n1 kept
answering 500 until 15:15:33. An HTTP/2 keep-alive ping timeout poisons a
pooled connection; the pool keeps handing out the dead one until it is
discarded. So the blast radius of a partition on this endpoint extends well past
the partition, and a run that only samples during faults will miss it.

**Impact.** No checker was corrupted — these arrive as `:fail`/`:info` rather
than as observations, and the runs still passed every safety property. What
broke is the retry contract: a client loop cannot classify a 500 with a
non-JSON body, which is the same failure this endpoint already had once (see
the closed "answered 200 with a body it could not express" above). The refusal
path is where this endpoint's bugs live.

### Fixed — and the evidence is 267 refusals, not an absence

`c67ab1f` fixes it at both layers, and is stricter than the report suggested:
`InterNodeTransportFailure.IsRetryable` treats `Internal` as retryable **only**
when `Status.DebugException` or `InnerException` is an `HttpRequestException`,
`IOException` or `SocketException`, so a remote *application* error reporting
Internal still propagates. `RetryableExceptionMapping.IsRetryable` now recurses
through `AggregateException.InnerExceptions` and `InnerException`. The
forwarding site in `GrpcInterNodeCommunication.GetSnapshotFloor` also returns a
typed `MustRetry` directly rather than relying on the middleware — belt and
braces, and correct because a floor read is side-effect free.

Two 300 s runs (`partition`, and `partition,kill` at `--kill-targets
one,majority`): zero 500s, zero nil-type errors, both `:valid? true`. The
`partition` run was emphatically non-vacuous — 450 protected reads, 167 holds,
356 below head, max depth 32.

Absence would be weak evidence on its own, since the original 500s appeared in
only one run of four. The real proof is the **presence** of the new refusals in
the node logs:

| `Mapping unhandled retryable RpcException on …` | before | after |
|---|---|---|
| `/v1/kv/try-set` | 0 | 157 |
| `/v1/kv/try-get` | 0 | 83 |
| `/v1/kv/snapshot-hold/release` | 0 | 16 |
| `/v1/kv/snapshot-hold/renew` | 0 | 11 |

Zero before, 267 after, on the same workload and fault set. Every one of those
is an exception the old `IsRetryable` returned false for — that is, a 500 the
old build would have emitted. The forwarding guard in `GetSnapshotFloor` did not
fire in either run; the middleware backstop caught everything, which is the
layering working as designed.

Not a regression from `65fcc70` — the routed branch is what that commit added,
so the floor endpoint was newly *reachable* rather than newly broken. The
`try-set`/`try-get` exposure predates it.

## Harness bugs worth remembering

All of these produced verdicts that looked like server findings.

**`release-start` overwritten by release retries** invented mutual-exclusion
violations for months of runs — the largest false finding in the project. Written
up in full in the closed exclusion section above.

**Missing operation ids** made every transaction fail with `InvalidInput`, so
nothing committed and Elle reported `:empty-transaction-graph`. Every
transaction-scoped request must carry *both* a coordinator key and a non-zero
operation id — see [DESIGN.md](DESIGN.md#transactions).

**A membership nemesis that changed no membership.** Two independent causes,
both of which produced `Everything looks good!` on a register run while the
roster sat unchanged at version 1 the whole time:

* The first `:leave` fired 3 s into the test, while the target was still logging
  `JoinCluster: waiting for initialization ... systemLeader=none
  initialized=false`. Jepsen's readiness check was `up?`, which only asks
  whether HTTP answers — true within a second of boot, and long before the node
  is a committed Voter. With no leader there was no one to commit the
  `RemoveMember`, so the node simply died.
* Then, once that was gated, every leave still failed: the harness SIGKILLed the
  departing node 20 s after SIGTERM, but Kahuna sets no
  `HostOptions.ShutdownTimeout` so .NET's 30 s default applies, and
  `ReplicationService.StopAsync` spends up to 10 s of it inside `LeaveCluster`.
  The leave was being truncated every time. At 45 s the same code removed a node
  on the first attempt.

Both looked exactly like a server-side "graceful leave doesn't work" bug, and
the second one is the more instructive: the evidence *for* the wrong conclusion
was a node log that ended at `Application is shutting down...` with no leave
recorded — which is equally consistent with a SIGKILL discarding .NET's buffered
console output. Every `:leave` now reports the roster size before and after,
from a surviving node, as `:removed` in the history.

**A snapshot workload that could have verified nothing.** The first green
snapshot runs reported 69 protected reads and no violations. They were also
compatible with every one of those reads having been served by the key's
*current* revision — a read as of T needs no history at all while T is still
the newest version, so the hold, the floor and the retention policy would never
have been touched. The checker now computes how far below head each protected
read was served and returns `:unknown` when none of them reached retained
history. The distinction is visible in the numbers: with reclamation left at
its default (keep every revision forever) the property is nearly unfalsifiable,
because history the floor failed to protect would still be on disk; with
`--revision-retention 4` the same workload read at depths up to 12, which only
the floor could have kept alive.

**`(zero? nil)` crashed a checker on a malformed observation.** `get-in`'s
default only fires when a key is *absent*, and the floor client had assoc'd
`:live-holds nil` — present and nil — so the default never applied and the
checker died with a `NullPointerException` mid-analysis. The same
present-and-nil-versus-absent distinction had already cost this suite a
silently-disabled health sampler. Comparisons in a checker should use `= 0`
rather than `zero?` for exactly this reason: a checker that crashes on
malformed input tells you less than one that ignores it, and both are better
than one that reads it as a real observation.

**An indeterminate release the checker could not name.** The snapshot floor
checker returned ten violations against a server that had done nothing wrong.
`hold-windows` deliberately ends a hold's protection window on an *indeterminate*
release — a release that timed out may well have taken effect — but it can only
do that for a release op carrying a `:hold-id`. `with-errors` assoc'd its
`:type :info` onto the original *invocation*, whose value is `nil`, so every
release that failed at the transport layer arrived anonymous. The window then ran
to the lease bound (600 s), and each subsequent `live-holds 0` inside it was
reported as a hold vanishing from the registry.

The trace was unambiguous once found: hold `918a3fca…` pinned at 14:52:15.457,
floor reporting `live-holds 1` through 14:52:17.8, a
`:info :release nil :timeout` at 14:52:20.507, and the first `live-holds 0`
223 ms later at 14:52:20.730. The server had released the hold exactly as asked.

Two things made this survive review. First, it only bites under `kill` —
transport-level failures are rare under `partition` alone, which is why the
partition-only and no-fault verification runs were clean. Second, and worse, the
checker-side behaviour *was* tested:
`an-indeterminate-release-still-ends-protection` hand-writes a release op with a
`:hold-id` in its value and passes whether or not the client can produce one. The
bug lived in the seam between two individually-tested halves.
`a-release-that-throws-still-names-the-hold` now asserts that seam by driving the
real client with a throwing HTTP call, and fails against the old code.

**A nil error type that meant "unparseable server response".** When a response
carries no recognizable type, most client branches record `[:write nil]`,
`[:read nil]`, `[:read-latest nil]` — visually indistinguishable from ordinary
fault noise in an error tally, and easy to read straight past. They are not
noise: they mean the server sent a body the client could not classify. The floor
branch alone falls back to `(:status r)`, which is why a server-wide 500 bug was
first filed as a floor-endpoint bug and its true scope (287 responses per run,
mostly on `try-set`/`try-get`) only emerged once it was fixed. **Worth doing:**
record the HTTP status on every branch the way the floor branch does, so an
unclassifiable response names itself instead of arriving as `nil`.

**A slingshot selector applied to non-maps.** `(contains? % :kahuna/abort)` was
evaluated against every thrown object, including the `SocketTimeoutException`s
that partitions produce; `contains?` throws on those, which escaped as an
unhandled `IllegalArgumentException` *and* skipped the rollback. Selectors that
inspect map contents need a `map?` guard first.

## Running the tests: two traps that fake results

**`lein` and `java` live in the `jepsen-control` container, not on the host.**
Running `lein` on the host fails with `command not found`. Worse, `docker exec
jepsen-control bash -lc '…'` *also* fails: a **login** shell re-reads
`/etc/profile` and discards the image's `JAVA_HOME`/`PATH`, so `lein` is found
and `java` is not. Use `docker exec -w /jepsen jepsen-control bash -c '…'` — no
`-l`.

**Never derive a run's verdict from "the newest store directory" alone.** When
the run fails to start, that lookup silently walks back to a *previous* run and
reports its results as if they were current. This happened: eight failed
invocations produced a complete, plausible 8-run table copied from the prior
build's series, and it was caught only because the numbers were identical to
those already on record. Capture the newest directory *before* the run and assert
that a new one appeared; parse `results.edn` rather than scraping console text,
and treat a missing verdict as an error rather than defaulting it to a pass.
