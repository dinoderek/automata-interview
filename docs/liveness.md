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

> **V (variant).** Each recorded result moves one dispatch attempt of one step
> to a terminal state. The number of attempts is bounded (one per step today;
> `maxAttempts` per step once retries exist), so only finitely many results
> can ever be recorded.

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

- **Another run.** "One run at a time" is an assumption, not enforced.
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

If the database is unavailable, `abandon` logs and tries to fail the run; if
that write fails too, the run stays `running` with nothing driving it.

## Where A does not hold

| Cause | Effect |
|---|---|
| `*_DROP_PCT`: instrument does the work, never reports | step `running` forever. I holds vacuously; the run never moves |
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
| §1 two concurrent runs | handled the same way — the second run fails with the refusal as its reason. R2 not planned: one run at a time is an assumption of this exercise |
| §2 abandon cannot persist | accepted — needs the database back; R4 would recover |
| A: drops | open — R3 not planned; described in NOTES.md |
| A: restarts, lost results | accepted — R4 not planned; described in NOTES.md |
