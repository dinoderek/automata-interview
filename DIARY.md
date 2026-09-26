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
| 2 | Tests for the unhappy paths (fake bus: refusal, send error, failed/late result) | + failure |
| 3 | Going further: bounded retries *or* dropped-result detection | + manual fault runs |
| 4 | NOTES.md, drafted from this diary | — |

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
