# Diary

One entry per round. Each round is small (target ≤ ~300 changed lines), passes
the quality gates (`scripts/gates.sh`), and is reviewed before the next starts.

## Working protocol

1. Every major round gets an entry here: what, why, decisions, open questions,
   time spent.
2. Rounds are small and reviewed before moving on; each reviewed round is one
   commit.
3. Quality gates run at every round — see `scripts/gates.sh`.

## Plan

| Round | Scope | Gates |
|---|---|---|
| 0 | git, diary, gates script, driver-interaction analysis | static |
| 1 | Rungs 1–4 floor: execute DAG, overlap, refusal, fail hard on any driver error | static, test, live, failure |
| 1.1 | `failed_draining`, failure reasons on run and step, draining tests | all |
| 2 | Tests for refusal and send errors (scripted bus); liveness analysis | all |
| 2.5 | Liveness: fail a run that ends a scan with nothing in flight | all |
| 3 | Going further: bounded retries of retryable failures | all + manual fault runs |
| 4 | Going further: lost results (DROP) — reconcile loop, fail fast | all + manual drop run |
| 5 | One run at a time, enforced at `Start` | all + live two-start check |
| 6 | Review fixes: abandon on draining, unreachable driver, startup sweep, skipped steps, docs | all |
| last | NOTES.md, drafted from this diary | — |

---

## Round 0 — setup and design

**Done**
- `git init`; baseline commit of the untouched scaffold so every round is a
  reviewable diff.
- `scripts/gates.sh` — quality gates in stages (static / test / live / failure).
- `docs/driver-interaction.md` — walk-through of the executor↔driver protocol
  and its failure modes.

**Decisions**
- *One run at a time.* Assume a single workflow executing. Multiple runs would
  need device state tracked across runs and a result in one run to wake
  others.
- *Global in-process mutex*, held across decide → send → write outcome, in both
  `Start` and `HandleResult`. Simplest thing that makes the concurrency test
  (simultaneous results, dispatch-once) hold. Does not protect against
  multiple executor processes; upgrade path is a Postgres row lock on the run.
- *Full rescan on each result.* Read all steps of the run, dispatch every
  pending step whose deps are completed and whose device is free. O(steps) per
  result; fine for now, to be discussed in NOTES.md.
- *Step states: `pending / running / completed / failed`.* No `dispatched`:
  with the mutex spanning the send, nothing can observe it. It becomes
  necessary once the lock no longer spans the network call — see
  `docs/driver-interaction.md`.
- *Device occupancy from our own rows*, not from driver state: skip a device
  that has a `running` step. Avoids predictable refusals and avoids depending
  on a refusal being retried by a later rescan. Does not generalise to
  multiple runs without changing what triggers a rescan.
- *`dispatched_at` stamped only on acceptance*, because the timeline treats it
  as the step's start.
- *Errors fail the run.* A step result with an error fails the step and the
  run. A `SendCommand` error other than a refusal is ambiguous (the driver may
  have accepted) — v1 fails the run rather than risk double execution.
- *Idempotent updates.* Step transitions are conditional on the current status,
  so late or duplicate results after the run has ended are recorded (the work
  really happened) but schedule nothing.
- *`ErrNoResponders` fails the run too.* It is unambiguous "not executed"
  and could safely stay pending, but nothing would retry it yet. Fail hard for
  now; revisit with error handling.

**Gates**
- static: pass.
- test: runs against Postgres and fails as expected — the stub scheduler
  leaves every step `pending`. This is the baseline round 1 must turn green.
- live / failure: not run (nothing to check yet).

**Time:** ~

---

## Round 1 — the scheduler, floor rungs 1–4

**Done**
- `scheduler.go`: `Start` and `HandleResult` take the mutex, record what
  happened, then call `advance`, which rescans the run: fail it if any step
  failed, complete it if all completed, otherwise dispatch every ready step
  whose device is free.
- `store.go`: every transition is conditional on the current status —
  `StartRun` (pending→running, else `ErrRunNotPending`), `FinishRun` (only a
  running run), `RecordStepRunning` (pending→running, stamps `dispatched_at`),
  `RecordStepFinished` (running→completed/failed, scoped to the run, reports
  whether it applied), `RecordDispatchFailed` (pending→failed).
  `RecordStepDispatched` replaced by `RecordStepRunning`.
- `scheduler_test.go`: pure unit tests for readiness and priority.
- `gates.sh live` now starts the whole stack — the drivers are not
  dependencies of the executor in compose, so `up executor` left them down.

**Decisions**
- *Critical-path priority.* Among ready steps, prefer the one with the longest
  chain of steps still to come. In the default workflow the two liquid-handler
  fills become ready together; name order picks `fill_buffer_plate` and the
  run takes 12s, critical-path order picks `fill_reagent_plate` so its warm-up
  overlaps the buffer fill: 10s, the optimum. Assumes equal step durations;
  recomputed each scan (O(steps + deps)).
- *Rung 4 floor pulled into this round.* "Fail hard on any driver error" is a
  few lines once the rescan exists: a failed result marks the step failed and
  the next rescan fails the run. Deviation from the round plan, noted here.
- *Refusal despite our records saying the device is free* → log, leave
  pending, reconsider on the next result. Should not happen with one run.
- *Any `SendCommand` error* (including `ErrNoResponders`) → step `failed`
  with "dispatch failed, outcome unknown", run `failed`.
- *Scheduler cannot make progress* (DB error mid-handle, or a driver accepted
  but we could not record it) → `abandon`: log and try to fail the run so it
  does not sit `running` with nothing driving it.

**Gates**
- static: pass. Unit tests: pass.
- test (`go test -race`, simultaneous results): pass — was failing in round 0.
- live: all 5 checks pass on both workflows. Default 10s, Triple Assay 6s
  (both optimal), 0 refusals.
- failure: pass — run ends `failed`; `fill_buffer_plate` was mid-flight on
  the liquid handler and its late result was recorded `completed` afterwards
  with nothing new dispatched.

**Known gaps (for later rounds / NOTES.md)**
- A refused step is only retried on the next result. If no result is coming
  (dropped result, or the device busy with someone else's work) the run
  stalls.
- Steps that never ran stay `pending` on a failed run — indistinguishable from
  waiting. ~~Nothing records which step stopped the run beyond its `error`.~~
  (fixed in round 1.1)
- The mutex serialises all runs; per-result cost is a full read of the run's
  steps plus a DB round trip per dispatch, all under the lock.
- No crash recovery: a restart forgets nothing (state is in Postgres) but
  nothing resumes a `running` run.
- The unhappy paths (refusal, send error, late result) are covered only by
  manual runs, not tests.

**Time:** ~

---

## Round 1.1 — failure bookkeeping: draining, and why the run failed

Prompted by walking through a `check-failure.sh` run: the incubator failed at
~4.0s while the liquid handler had just been sent `fill_buffer_plate`. The run
said `failed` at 4.03s, but the liquid handler kept dispensing until 6.03s, and
nothing on the run said which step had stopped it.

**Done**
- New run status `failed_draining`: a step failed, nothing new is dispatched,
  but steps already on instruments have not reported. Instruments cannot be
  cancelled, so the run is not over until they do. When the last one reports,
  the run becomes `failed` and only then gets `finished_at`. With nothing in
  flight at the moment of failure, the run goes straight to `failed`.
- `runs.failed_step` and `runs.error`: the first failure that stopped the run
  (step name + the step's error). Steps keep their own `error` as before; a
  step that fails while the run drains records its error, the run keeps the
  first.
- Store: `FailRun` (running → failed / failed_draining, with reason; only from
  running, so the first failure wins) and `FinishRun(from, to)` (running →
  completed, failed_draining → failed).
- `abandon` (scheduler's own error) records `failed_step = NULL`,
  `error = "scheduler error: …"`.

**Decisions**
- "Pending work" for draining means steps *running on instruments*, not
  `pending` steps — those will never start.
- A dropped result for a draining step leaves the run `failed_draining`
  forever. Accepted for now: we genuinely do not know what that instrument
  did. Round 3 (drop detection) would resolve it.
- Schema changed in `db/init.sql`, which only runs on an empty volume. The
  local `postgres_data` volume was recreated (it held only runs created by our
  own gates). Anyone with an existing volume needs `docker compose down -v`.

- `scheduler_failure_test.go`: our own DB-backed tests, playing the drivers
  step by step with the `recordingBus` from the concurrency test. The workflow
  shape is fixed in the test, not read from `workflows.yaml`. Covers:
  drain then fail; a second failure while draining keeps the first as the
  run's reason; failure with nothing in flight fails immediately; a duplicate
  result after the run ended is ignored. Also asserts nothing new is
  dispatched once the run has failed (`warm_reagent_plate` stays pending).
  Checked by breaking draining on purpose: the three draining tests fail.

**Gates**
- static, unit tests, `go test -race` (8 tests): pass.
- live: 5/5 on both workflows, 10s and 6s, 0 refusals.
- failure: pass (6s now, was 4s — the run waits for the in-flight liquid
  handler step). A polled manual run shows the transition:
  ```
   0.03s  running          running=fill_sample_plate
   2.23s  running          running=fill_reagent_plate,incubate_samples
   4.19s  failed_draining  failed_step=incubate_samples  running=fill_buffer_plate
   6.15s  failed           failed_step=incubate_samples
  ```

**Time:** ~

---

## Round 2 — refusal and send-error paths under test

**Done**
- `harness_test.go`: the step-by-step run harness moved out of the failure
  tests, with a `scriptedBus` that accepts by default and can be scripted per
  step name to refuse or return an error. Records every offer (`tried`) and
  every acceptance (`sent`).
- `scheduler_dispatch_test.go`:
  - refused step stays `pending` with `dispatch_count` 0 and no
    `dispatched_at`, is not followed by another offer to the same device in
    the same scan, and goes out on the next result;
  - send error (timeout, and `ErrNoResponders`) fails the step as "dispatch
    failed, outcome unknown: …", run drains then fails, recording it;
  - a refusal plus a send error in the same scan, nothing else in flight →
    run goes straight to `failed`.
- `scheduler.go`: after a refusal, the device is skipped for the rest of the
  scan. Previously the next ready step for the same device was offered
  straight away, for a guaranteed second refusal.

**Decisions**
- *Refused devices are tracked separately from `busy`.* `busy` doubles as
  "our steps on instruments", which decides whether a failure drains. Folding
  a refused device into it would make a failure in the same scan wait in
  `failed_draining` for a result that never comes. The third test pins this.
- Checked the new tests by breaking the code on purpose: dropping the
  skip-after-refusal fails the refusal test; counting a refusal as busy fails
  the in-flight test.

**Liveness analysis** (`docs/liveness.md`)
- Invariant I: at every quiescent point, each non-terminal run has at least
  one step `running`. Assumption A: every `running` step eventually yields a
  recorded result. Variant: attempts are bounded. I ∧ A ⇒ every run
  terminates.
- A scan maintains I except when every ready step is refused and nothing is
  in flight — then the run stalls forever. Reproduced live: two runs started
  back to back; the second's `fill_sample_plate` was refused (liquid handler
  busy with the first run) and it sat `running` with every step pending.
  "One run at a time" is assumed, not enforced.
- A fails for dropped results, results lost while the executor is down, and
  DB errors while recording a result.
- Next: R1 — a wake-up timer when a scan ends with nothing in flight, and a
  no-progress deadline that fails the run.

**Gates**
- all: static, `go test -race` (11 tests + 2 subtests), live 5/5 on both
  workflows (10s / 6s, 0 refusals), failure pass.

**Time:** ~

---

## Round 2.5 — liveness: never end a scan with nothing in flight

**Done**
- End of `advance`: if the run is still non-terminal and nothing is
  `running`, no result will ever come to trigger another scan. Every refused
  step is marked `failed` with "refused by <device> (<reason>) with nothing in
  flight to wait for", and the run fails with the most urgent one as its
  reason. (If nothing was refused either — impossible for an acyclic workflow —
  the run fails with no step named.)
- Tests, single run, scripted bus: the only ready step refused at start;
  every ready step refused mid-run on two devices (each refused step records
  its reason, the run records the more urgent, un-offered steps stay pending).
- `docs/liveness.md` and `docs/driver-interaction.md` updated.

**Decisions**
- *Fail instead of a timer, for now.* A refusal we cannot explain is treated
  like any other driver error: something physical is holding the device, or
  the driver is misbehaving. Timers come with drop handling, which needs them
  anyway; then this becomes "retry with backoff until a deadline".
- *Refused steps are marked `failed`*, not left `pending`, so `failed_step`
  on the run points at a step that shows the failure.
- *Not enforcing one-run-at-a-time at `Start`.* It is an assumption of the
  exercise; a violation now fails the second run with a clear reason rather
  than hanging it.

**Gates**
- all pass: static, `go test -race` (13 tests + 2 subtests), live 5/5 on both
  workflows (10s / 6s, 0 refusals), failure.
- Live re-run of the two-runs reproduction: the second run now ends `failed`,
  `failed_step=fill_sample_plate`, `error=refused by liquid-handler-1 (busy
  with fill_sample_plate) with nothing in flight to wait for`.

**Time:** ~

---

## Round 3 — bounded retries

**Done**
- `HandleResult` → `record`: a failed result goes back to `pending` (and is
  re-dispatched by the same scan) when all hold: the driver says
  `retryable`, the run is still `running`, and the step has attempts left
  (`dispatch_count < maxAttempts`, `maxAttempts = 3`). Otherwise the step
  fails and its error says why it was not retried:
  - `<err> (attempt 1 of 3, retrying)` — while retrying
  - `<err> (attempt 3 of 3, giving up)` — bound reached
  - `<err> (retryable, not retried: run is failed_draining)`
  - non-retryable errors are recorded as reported.
- Store: `GetStep`, `RecordStepRetrying` (running → pending, keeps the error,
  leaves `finished_at` unset).
- Harness: `finish` now reports non-retryable failures (as the liquid
  handler does); `finishRetryable` for the others. The existing draining
  tests keep testing what they tested.
- `scheduler_retry_test.go`: retried and recovers; a retried step keeps its
  priority (fill_reagent_plate wins the liquid handler back from
  fill_buffer_plate); gives up after 3 and fails the run (draining); no retry
  while draining; a refused retry with nothing in flight fails the run.
  Checked by breaking the bound and the draining guard: each is caught.

**Decisions**
- *No new status.* A retrying step is `pending` with the last error kept, and
  immediately `running` again; `dispatch_count` is the attempt counter. On
  eventual success `error` is cleared, so the history survives only as
  `dispatch_count` > 1 — a choice; a step-attempts table would keep it.
- *Fixed `maxAttempts = 3`*, not per step. Per-step `max_attempts` in the
  workflow YAML would be the natural next step.
- *Retry immediately, no backoff.* The driver freed itself before reporting,
  so the device is available. For real instruments a failure often needs a
  human (jammed lid), and backoff or an operator gate would be better.
- *Only the driver's `retryable` flag grants a retry.* The liquid handler
  says `false` (part-dispensed plate). Send errors are never retried: the
  step may be executing.
- *Retrying deliberately breaks "every step runs exactly once"* for steps
  whose instrument said a repeat is safe. The acceptance script's
  duplicate-execution check flags them (see below).
- *A retry holds its device.* The retry goes out in the same scan the
  failure is recorded, so it keeps the device ahead of other ready steps of
  equal or lower priority. In the check-failure run, warm_reagent_plate never
  got the incubator while incubate_samples used its three attempts.

**Gates**
- all pass: static, `go test -race` (18 tests + 2 subtests), live 5/5 on
  both workflows (10s / 6s, 0 refusals), failure pass — now 8s:
  `incubate_samples` ran 3 times, run `failed` with
  `instrument error during incubate_samples (attempt 3 of 3, giving up)`.
- Manual, `INC_FAIL_PCT=50`, 4 acceptance runs: all 4 `completed` (13–15s)
  where each would have failed without retries. Each is flagged by check 5
  ("steps executed more than once") for the retried incubator step — the
  intended trade-off.

**Time:** ~

---

## Round 4 — lost results (DROP): reconcile loop, fail fast

Design discussed first, recorded in `docs/drops.md`.

**Done**
- `reconciler.go`: `RunReconciler` (started from `main`, every 1s) →
  `reconcile` → `reconcileSteps`. Snapshot in-flight steps, then query each
  device's `DriverState` (outside the mutex), then under the mutex: a step
  whose device is not busy with it is suspected; still so `lostGrace` (1s)
  later, it fails as "result lost: … outcome unknown" and the run is advanced
  (fails, or drains).
- `Scheduler` gains `now` (injectable clock) and `suspected`, keyed by
  `(step, attempt)`.
- Store: `ListInFlightSteps` (running steps of running / draining runs),
  `RecordStepLost` (conditional on status *and* `dispatch_count`).
- Harness: scripted `DriverState` per device, fake clock, `driverBusy` /
  `driverIdle` (Executed built from accepted commands, as a real driver),
  `reconcile` restricted to the test's own run.
- `reconciler_test.go`, 8 tests: dropped → fails after grace; late result
  within grace wins; busy step never lost; retry starts a fresh suspicion;
  dropped result on a draining run ends it; unreachable driver → no verdict;
  driver with no record → distinct message; stale snapshot cannot fail a new
  attempt. Checked by breaking the grace, the attempt key, and the
  attempt-conditional update: each caught.

**Decisions**
- Reconcile loop over per-step deadlines (durations unknown to the executor).
- Fail fast on a lost result (outcome unknown), consistent with send errors.
- "Not busy with this step" + grace is the whole rule; `Executed` only
  refines the message. No ID counting needed for correctness.
- Executor restarts out of scope (agreed).
- `reconcile` split into list + `reconcileSteps` so tests can scope it to
  their own run: the test DB is shared with other tests' runs and a live
  executor.

**Database errors — `abandon` kept, typed** (review of reconciler.go:105)
- Every DB error still abandons the run (fail closed); agreed as fine for
  this exercise. Analysis of the alternative (log + retry from the loop):
  reads and our own decisions are safe to redo, but a failed write of a
  driver's *acceptance* is not — a rescan would re-send a step the instrument
  is running. Proper fix is the write-ahead `dispatched` state; not built.
  Recorded in `docs/liveness.md` §2.
- The run's error is now always `<type>: <what we were doing>: <cause>`,
  e.g. `database error: recording result for fill_sample_plate: recording
  step completed: context canceled`. The type is passed explicitly at each
  call site (`errTypeDatabase` is the only one today); store errors inside
  `advance` / `record` are wrapped with the operation.
- `abandon` records the failure with `context.WithoutCancel`: with the
  caller's context it silently failed to record anything when that context
  was the thing that failed (a test proved it: run stayed `running`).
- `Start` detaches from the HTTP request's context: a client hanging up
  mid-start must not abandon the run, or have it misreported as a database
  error.
- Test: `TestDatabaseErrorAbandonsRunWithTypedReason` — a cancelled context
  makes the store fail for real; the run fails with the typed reason.
- *If `abandon` cannot record the failure either, the executor exits*
  (`log.Fatalf`, injectable as `Scheduler.fatal` for tests). Fail-stop: the
  table may not match the instruments, and scheduling on could send a step
  twice. No restart policy, so it stays down for an operator.
  Test: `TestAbandonExitsWhenFailureCannotBeRecorded` (scheduler over a
  closed DB). Live: stopped Postgres 0.5s into a run → the liquid handler's
  result could not be recorded or abandoned → executor exited 1 with the full
  error chain. After restarting Postgres and the executor, the reconcile loop
  failed the run within seconds: `result lost: liquid-handler-1 finished
  fill_sample_plate (attempt 1) but never reported; outcome unknown`.

**Found along the way**
- A first version of a test forgot to script the liquid handler; the default
  fake driver reports idle, and the reconciler (correctly) failed the step.
- The live executor, on start, failed ~50 runs that tests had left
  non-terminal — correct, and a reason tests should get their own database.

**Gates**
- all pass: static, `go test -race` (28 tests + 2 subtests), live 5/5 on
  both workflows (10s / 6s, 0 refusals, no reconcile activity on live runs),
  failure pass.
- Manual, `INC_DROP_PCT=100`: run failed at 6.98s with `result lost:
  incubator-1 finished incubate_samples (attempt 1) but never reported;
  outcome unknown` (incubator finished at ~4s); previously hung for ever.

**Time:** ~

---

## Round 5 — one run at a time, enforced

**Done**
- `StartRun` starts a run only if no other run is active (`running` or
  `failed_draining` — a draining run still has steps on instruments). One
  conditional `UPDATE … AND NOT EXISTS (…)`; if it does not apply, it says
  why: `ErrRunNotPending`, or `ErrAnotherRunActive: <active run id>`.
- `POST /runs/{id}/start` answers **409** for both (was 500).
- Harness: `createTestRun` aborts its run at cleanup if still active — tests
  leave runs active on purpose, which would now block the next test.
- `scheduler_start_test.go`: refused while another run is running (stays
  pending, nothing sent); refused while another is draining, starts once it
  has drained; a second start of the same run is "not pending". Checked by
  removing the `NOT EXISTS`: both clash tests fail.

**Decisions**
- *Enforced in the app, under the scheduler mutex.* READ COMMITTED does not
  make the check-and-update safe against a concurrent start by itself; the
  mutex serialises starts in this process. Across executor processes, a
  partial unique index on active runs would enforce it in the database.
- *Draining counts as active*: the instruments are still in use.

**Found along the way**
- Two runs in the local DB would have blocked every start for ever:
  `run-1b0258492cc7` (the round 2 two-run reproduction: `running`, all steps
  pending) and `run-b6b3d61a5981` (left `failed_draining` by a round 2
  mutation check). Nothing could finish either — nothing in flight, and the
  reconciler only looks at running steps. Marked `aborted` by hand.
- Consequence: a stuck run now blocks the whole system, not just itself. No
  abort endpoint exists (`aborted` is in the schema, unused).
- Acceptance reported 11s once; the run's recorded duration was 10.05s. The
  script polls once a second and counts whole seconds.

**Gates**
- all pass: static, `go test -race` (31 tests + 2 subtests), live 5/5 on
  both workflows (10.05s recorded / 6s), failure pass. Tests leave no active
  runs behind.
- Live: two runs started back to back → A `200`, B `409 {"error":"another run
  is active: run-514c550699dc"}`, B stays pending; after A completed, B
  started (`200`) and completed.

**Time:** ~

---

## Round 6 — review fixes

A review agent read the whole submission against README.md and our
principles (one run at a time; fail fast on system errors; fail fast on
environment errors except a busy device and retryable failures; simplicity;
the 2-hour brief). Findings taken: #1, #2, #4, #6, #8, #9. Kept as they were:
the explicit `errType` parameter (asked for), the retry path's reads.

**Done**
- **#1 (bug) `abandon` on a draining run.** `FailRun` only changes a
  `running` run and ignored rows affected, so `abandon` on a
  `failed_draining` run did nothing and reported success; with nothing in
  flight the run stayed draining for ever and, since round 5, blocked every
  start. New `Store.AbandonRun`: `running` or `failed_draining` → `failed`,
  keeping the first failure (`COALESCE`). Test:
  `TestAbandonEndsDrainingRunKeepingFirstFailure`.
- **#4 Unreachable driver.** A `DriverState` error now counts as "not busy
  with this step": the step fails after the same grace with `driver
  unreachable: <device> did not answer while running <step> (attempt n):
  <err>; outcome unknown`. Consistent with a send to that driver failing at
  once. The old test that pinned "no verdict" is now
  `TestUnreachableDriverFailsStepAfterGrace`.
- **#8** A failure to record a dispatch failure is now an error (→ abandon),
  as in the refusal path, instead of only a log line.
- **#2 Startup sweep.** `FailActiveRuns` runs in `main` before results are
  handled: every run left active fails with `executor restarted: run was
  active when the executor started; outcome unknown`; steps still on
  instruments drain as usual. Nothing is resumed — so a step whose acceptance
  was never recorded is never sent again. Tests: `TestFailActiveRunsAtStartup`,
  `TestFailActiveRunsFinishesStuckDrainingRun`.
- **#6 `skipped`.** When a run fails (`fail`, `abandon`, the sweep), its
  pending steps become `skipped`: never ran, never will. At the moment of
  failure, not after draining.
- **#9 Docs.** Stale "one run at a time is assumed"; liveness.md's
  "purely event-driven" and draining argument; driver-interaction.md's state
  diagram redrawn as built; drops.md out-of-scope list; plan table.
- Log line for a failure with no step no longer prints `: :`.

**Checks**
- Deliberate breakages: `AbandonRun` from `running` only → the draining
  abandon test fails; sweep without `advance` → the stuck-draining test fails.
- Live: restarted the executor 3s into a run. The new executor failed it
  (`failed_draining`, 2 steps in flight), subscribed, received both real
  results, drained to `failed`; the four steps that never ran are `skipped`.
- check-failure now shows `combine`, `read_plate`, `warm_reagent_plate` as
  `skipped`.

**Gates**
- all pass: static, `go test -race` (33 tests + 2 subtests), live 5/5 on
  both workflows (10s / 6s), failure pass.

**Time:** ~
