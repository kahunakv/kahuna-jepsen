# Findings and status

For *why* the tests are shaped the way they are, see [DESIGN.md](DESIGN.md).

## Workload status

| Workload | Checks | Status |
|---|---|---|
| `register` | linearizable CAS register (Knossos) | green; found a stale-read violation, since fixed and verified |
| `lock` | mutual exclusion + fencing-token monotonicity, lease-aware | green as of Kommander 1.0.10; found a fencing rollback (fixed, 16/16 verified) and a checker bug that faked exclusion violations |
| `append` | Elle list-append over interactive transactions | found four real bugs; three fixed (write skew closed on absence, 29 runs), HTTP 500 all but fixed |
| `sequencer` | no id handed out twice; allocation-range integrity; idempotent replay | new; found an HTTP 500 on the redirect path before a nemesis was even enabled. Now in the nightly matrix under `partition` and `partition,kill` |

Not started:

- [x] membership nemesis — a node leaves the roster and rejoins, verified
      against the committed roster on every operation. **Not** via
      `/v1/cluster/membership`, which is GET-only: membership is driven by
      `--join-existing` and `--graceful-leave-on-shutdown`. See
      `src/kahuna/nemesis/membership.clj`.
- [ ] snapshot-read workload exercising `readTimestamp` + snapshot holds
- [x] `pause` fault in the CI matrix — wired in for `lock` and `register`. A
      paused process keeps its connections and its leases but stops answering,
      which is the one failure mode `partition` and `kill` cannot produce.
- [x] measure restart-to-first-successful-transaction — `kahuna.checker.recovery`
      runs on every test and reports recovery latency plus the windows where a
      fault landed before anything succeeded. Baseline below.
- [ ] apply that measurement to a run that commits nothing. Those are ~1 in 10,
      and the instrument has not yet caught one, so the original question is
      *not* settled — see below.

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
recovery is simply slower than the fault schedule.

### Recovery baseline (healthy runs)

`kahuna.checker.recovery` now measures this on every run. From a 300 s
`append` / `partition,kill` run at the default 15 s interval — a **healthy**
one, 1163 committed transactions:

```
:windows 24  :recovered 15  :never-recovered 6
:recovery-ms {:min 3, :median 164, :p95 10622, :max 10622}
:starved-window-ms (249 2377 2751 6623 7498 11299)
```

Two things follow, and a third does not.

* **Recovery is usually immediate.** A median of 164 ms says that when the
  cluster is left alone it is serving again almost at once. The blunt version of
  hypothesis (2) — "recovery is always slower than the fault schedule" — is not
  supported.
* **But the tail reaches the fault interval.** The slowest recovery was 10.6 s,
  against a 15 s interval. There is not much headroom, and one window went
  11.3 s with nothing succeeding.
* **This does not explain the commit-nothing runs.** Four of the six starved
  windows were shorter than 3 s — the nemesis simply hit again quickly, which
  says nothing about how fast the cluster *could* have recovered. And this run
  committed plenty, so it is a baseline for the healthy case, not a measurement
  of the pathological one.

**The original question is therefore still open.** What is needed is this
measurement on a run that commits nothing. Every run now carries it, so it is a
matter of collecting append runs until one of the ~1-in-10 appears and reading
its `:recovery` map. Do not treat the baseline above as the answer.

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
