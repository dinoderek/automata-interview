# Lost results (`*_DROP_PCT`)

A driver that drops a result does the work, frees itself, decides success or
failure, and publishes nothing. The step stays `running` in our table, and the
run never moves again: the scheduler only acts on results. In terms of
[liveness.md](liveness.md), assumption A fails.

Implementation: `services/executor/reconciler.go`. Tests:
`reconciler_test.go`.

## What can be known

`DriverState` reports `Busy`, `CurrentStep` (a step **name**), `Executed`
(step **IDs** of every accepted command) and per-device `Dropped` / `Failed`
counts. So we can learn **that** a step finished on the instrument, never
**whether it succeeded** — the lost message was the only record of that.

## Decisions

1. **Detect with a reconcile loop**, not per-step deadlines. The executor
   does not know step durations (`STEP_DURATION` lives in the worker's
   environment). Every `reconcileEvery` (1s) the loop compares each running
   step with its driver. It is level-triggered: it needs no event to have
   happened. Started from `main`; tests call it directly.
2. **Fail fast.** A lost result means outcome unknown, so the step fails and
   the run fails (or drains). Same policy as a command that timed out.
   Rejected: assuming success (a failed step would feed a bad plate
   downstream, silently); retrying (the `retryable` flag was in the lost
   message, and "safe to repeat after a failure" is not "safe to repeat after
   a success" — double incubation); pausing for an operator (the realistic
   answer for a lab, but needs a new state and a way to resolve it).
3. **The rule:** a running step whose device is *not busy with it* for at
   least `lostGrace` (1s) has lost its result. Under fail-fast we do not need
   to tell "finished, unreported" from "driver has no record" (restarted) —
   both are outcome unknown — so `Executed` only shapes the error message
   (occurrences of the step ID vs `dispatch_count`). This also sidesteps the
   retry trap: while attempt 2 runs, the driver is busy with it.
4. **Driver I/O outside the mutex.** `DriverState` can take up to 3s;
   results must not queue behind it. Snapshot steps, then query drivers,
   then lock and act through conditional updates.

## Races

| Race | Guard |
|---|---|
| Result in transit when first seen idle (driver frees itself *before* publishing; our handler may be queued on the mutex) | `lostGrace`: first sighting only suspects; act only if still silent `lostGrace` later |
| Result recorded between snapshot and lock | `RecordStepLost` requires `status = running` → no-op |
| Retry dispatched between snapshot and lock | `RecordStepLost` requires `dispatch_count = attempt`; suspicion is keyed by `(step, attempt)` so a retry starts fresh (`TestStaleSnapshotCannotFailNewAttempt`, `TestRetryStartsAFreshSuspicion`) |
| Driver snapshot older than the step snapshot | steps are read first; a step read as running was already accepted |

Detection latency: 1–3s after the driver finishes (one tick to suspect, a
tick at least `lostGrace` later to act). Live, with `INC_DROP_PCT=100`, the
run failed ~3s after the incubator finished; before, it hung for ever.

## Cost

Per tick: one query for in-flight steps (index on status), plus one
`DriverState` request per device that has an in-flight step. Proportional to
in-flight work, not to run size or history. Idle system: one empty query.

## Out of scope

- **Executor restarts.** Nothing is resumed: `FailActiveRuns` fails every run
  left active at startup (round 6), and the loop drains what was in flight.
  In-memory suspicions start over.
- **Unreachable driver.** *Handled since round 6:* a `DriverState` error
  counts as "not busy with this step", so the step fails after the same
  grace with `driver unreachable: …; outcome unknown` — consistent with a
  command to that driver failing at once.
- **An instrument busy with our step for ever.** Believed indefinitely. A
  maximum duration per step would fail such a run (it cannot be cancelled).
- **Refusals** still fail the run rather than being retried with backoff
  (round 2.5), although the loop now provides the timer that would need.
- **Two runs and `CurrentStep`.** The driver reports the busy step by name;
  with two runs of the same workflow, "busy with fill_sample_plate" is
  ambiguous. Fine under one run at a time.

## Observed side effect

The test suite and the live stack share `executor_db`. Tests leave runs
non-terminal (e.g. a step deliberately left running); the live executor's
loop fails those as lost within seconds of starting — correct behaviour, but
a sign the tests should have their own database.
