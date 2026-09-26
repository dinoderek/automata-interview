# Liveness

Can a run get stuck? This states what has to be true for every run to finish,
and lists where the current code does not guarantee it.

## The property

**Every started run eventually reaches a terminal status** — `completed` or
`failed`. Ending as `failed` counts: the requirement is "fail rather than
hang".

## Invariant, assumption, variant

The scheduler is purely event-driven: it only acts in `Start` and
`HandleResult`. So a run makes progress only if something is guaranteed to
call it again.

> **I (invariant).** Whenever the scheduler is quiescent (not holding its
> mutex), every run in `running` or `failed_draining` has at least one step in
> `running`.

Qualifications:

- Only non-terminal runs. A terminal run has nothing in flight, correctly.
- Only at quiescent points. Mid-scan, zero steps may be running (a result was
  just recorded, the next dispatch not yet made); the scan in progress is
  itself the guarantee of a next step.
- `running` in our table is a *belief*. That a result actually arrives is a
  property of the environment:

> **A (assumption).** Every step in `running` eventually yields exactly one
> result, which is delivered to `HandleResult` and recorded.

> **V (variant).** Each recorded result ends one dispatch attempt of one step.
> Attempts are bounded by `maxAttempts` per step (retries, round 3), so only
> finitely many results can ever be recorded. Refusals do not consume
> attempts, but cannot loop either: a refused step either waits for a result
> already in flight (bounded, by the above) or fails the run (round 2.5).

**Argument.** By V, results eventually stop arriving. By A, at that point no
step is `running` — every one has reported. By I, a run with no step
`running` is terminal. ∎

## Why a scan maintains I (in the normal case)

At the end of a scan on a `running` run with no failed step and not every step
completed, suppose nothing is `running`. Then every non-completed step is
`pending`. The workflow is acyclic, so some pending step has every dependency
completed: a ready step exists. With nothing `running`, our records show every
device free, so the scan offers that step. Outcomes:

| Driver answer | Result | I |
|---|---|---|
| accepted | step `running` | holds |
| send error | run `failed` (nothing in flight) | holds — terminal |
| **refused** | step stays `pending`, nothing `running` | **violated** — unless handled, see §1 |

`failed_draining` maintains I: `fail()` chooses draining only if something is
`running`, and every scan of a draining run finishes it once nothing is.

## Where I does not hold

### 1. Every ready step refused, nothing in flight — run stalls forever

**Handled (round 2.5) by failing the run.** A scan that ends with the run
non-terminal and nothing `running` marks each refused step `failed` with
"refused by <device> (<reason>) with nothing in flight to wait for" and fails
the run with the most urgent one. This restores I (the run is terminal) at
the cost of failing runs a later retry might have saved. A wake-up timer
(R1) is the better answer, deferred until drop handling needs timers anyway.
Tests: `TestOnlyReadyStepRefusedFailsRun`, `TestAllReadyStepsRefusedFailsRun`.

A refusal leaves the step `pending` and waits for "the next result". If
nothing of this run is in flight, there is no next result.

A refusal means the device is busy with something our records do not show.
Our own steps never cause it: a driver frees itself *before* publishing its
result, so our records are always the more conservative. The foreign work
can be:

- **Another run.** "One run at a time" was an assumption, not enforced
  (enforced since round 5, see below).
  Reproduced on the live stack by starting two runs back to back:

  ```
  run-72be24dd654e  completed
  run-1b0258492cc7  running   every step pending
  scheduler: liquid-handler-1 refused fill_sample_plate: busy with fill_sample_plate
  ```

  Run B's only ready step was refused while run A held the liquid handler.
  A's results rescan only A, so B is never looked at again.
- **A step orphaned by an executor restart**, still executing on the
  instrument when a new run starts.
- **A step of an abandoned run** (see 2), or a step whose result was dropped,
  still occupying the device for the rest of its duration.
- **A human** using the instrument directly.

### 2. `abandon` cannot persist the failure

Every error that reaches `abandon` is a database error (driver errors are step
failures). `abandon` fails the run with `database error: <what we were
doing>: <cause>`, recorded even if the caller's context was cancelled.

If the database is unavailable, that write fails too. **The executor then
exits** (fail-stop). Its table may no longer match the instruments — if the
failed write was a driver's *acceptance* (`RecordStepRunning`), the step reads
`pending` while the instrument runs it, and the next scan would send it again,
a double execution. Stopping prevents that. Nothing restarts the executor
automatically (no restart policy in compose).

After a manual restart the reconcile loop resolves what it can: a step left
`running` whose driver is idle is failed as a lost result (observed live:
Postgres stopped mid-run → executor exited → restarted → run `failed`,
`result lost: … outcome unknown`). A step left `pending` while it actually
ran is not detectable, and a restart could send it again. The proper fix is
the write-ahead `dispatched` state (claim before sending, see
driver-interaction.md).

## Where A does not hold

| Cause | Effect |
|---|---|
| `*_DROP_PCT`: instrument does the work, never reports | step `running` forever. I holds vacuously; the run never moves. **Handled (round 4):** the reconcile loop fails the step as outcome unknown — see [drops.md](drops.md) |
| Executor down or NATS disconnected when a result is published | core NATS is at-most-once: the result is gone |
| DB error while recording a result | the message is already consumed; nothing redelivers it |
| Panic in a result handler | the process dies — the restart case above |

## Remedies

| # | Fix | Restores | Cost |
|---|---|---|---|
| R1 | **Enforce I as a post-condition of every scan** (done in its simplest form: fail the run). Better: if the run is non-terminal and nothing is `running`, arm a wake-up timer (backoff) that rescans. Past a no-progress deadline, fail the run with the reason (e.g. "fill_sample_plate refused by liquid-handler-1 for 30s"). | I, in all cases of §1 | small |
| R2 | **Enforce the single-run assumption** at `Start`: refuse to start while another run is non-terminal. | I, for the two-runs case only | tiny |
| R3 | **Per-step deadline + reconciliation.** A `running` step past its expected duration × k is checked with `DriverState`: still busy with it → wait; idle and `Executed` contains its ID → result lost. Outcome of the lost step is unknown (success or failure), so fail it, or retry if the device is known to be safe to repeat. | A, for drops | medium; the "going further" drop task |
| R4 | **Level-triggered reconcile loop.** Periodically, and at startup, scan every non-terminal run against the database and driver state, independent of events. Subsumes R1 and R3's trigger, and resumes runs after a restart. | I and A, including restarts | medium–large |

The general point: an edge-triggered scheduler loses progress permanently
whenever an edge is lost or never produced. Every remedy above adds a source
of wake-ups that does not depend on a driver reporting.

## Status

| Gap | Status |
|---|---|
| §1 refusal stall | handled — run fails immediately; timer (full R1) deferred to drop handling |
| §1 two concurrent runs | prevented (round 5, R2): `Start` refuses with 409 while another run is `running` or `failed_draining` |
| §2 abandon cannot persist | accepted — needs the database back; R4 would recover |
| A: drops | handled — reconcile loop (R3's check, R4's trigger), fail fast; see drops.md |
| A: restarts, lost results | accepted — R4 not planned; described in NOTES.md |

## Consequence of enforcing one run at a time

A run that never reaches a terminal status now blocks **every** later start,
not just itself. Two such runs were found in the local database when the rule
went in — both left over from before the fixes above (one `running` with
every step pending, one `failed_draining` with nothing in flight) — and were
marked `aborted` by hand. Anything that leaves a run non-terminal (a driver
that stays unreachable, an executor that exited, §2) now needs an operator.
There is no abort endpoint yet; `aborted` exists in the schema but nothing
sets it.
