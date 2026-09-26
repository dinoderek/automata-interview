# Liveness

Can a run get stuck? This states what has to be true for every run to finish,
and lists where the current code does not guarantee it.

## The property

**Every started run eventually reaches a terminal status** — `completed` or
`failed`. Ending as `failed` counts: the requirement is "fail rather than
hang".

## Invariant, assumption, variant

The scheduler was originally purely event-driven: it acted only in `Start`
and `HandleResult`, so a run made progress only if something was guaranteed
to call it again. Since round 4 the reconcile loop (every 1s) also acts, but
only on steps in `running`; this analysis is about the event-driven core, and
the loop is how assumption A is shored up.

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
> finitely many results can ever be recorded. A refusal fails its step
> (round 8), so it cannot loop either.

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
| send error | step `failed`, run `failed` (nothing in flight) | holds — terminal |
| refused | step `failed`, run `failed` (since round 8) | holds — terminal |
| **refused (rounds 1–2)** | step stayed `pending`, nothing `running` | **violated** — see §1 |

`failed_draining` maintains I: `fail()` chooses draining only if something is
`running`, and every scan of a draining run finishes it once nothing is. If
that scan hits a database error, `abandon` fails the draining run outright
(before round 6 it silently did nothing to a draining run, leaving it stuck).

## Where I does not hold

### 1. Every ready step refused, nothing in flight — run stalls forever

**History.** Rounds 1–2: a refusal left the step `pending` to wait for "the
next result"; with nothing of the run in flight there was none, and the run
hung. Round 2.5: a scan ending with nothing in flight failed the run. **Round
8: every refusal fails its step and the run.** Our own work cannot cause one
(occupancy comes from our own rows, drivers free themselves before reporting,
one run at a time with draining runs still active), so a refusal means the
device is not in the state we believe — the list below. The end-of-scan
check remains as a guard. Tests: `TestRefusalFailsStepAndRun`,
`TestRefusalWithWorkInFlightDrains`, `TestOnlyReadyStepRefusedFailsRun`,
`TestRetryRefusedFailsRun`.

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
failures). `abandon` fails the run — `running` or `failed_draining` — keeping
its first failure if it has one, otherwise recording `database error: <what we
were doing>: <cause>`, even if the caller's context was cancelled.

If the database is unavailable, that write fails too. **The executor then
exits** (fail-stop). Its table may no longer match the instruments — if the
failed write was a driver's *acceptance* (`RecordStepRunning`), the step reads
`pending` while the instrument runs it, and the next scan would send it again,
a double execution. Stopping prevents that. Nothing restarts the executor
automatically (no restart policy in compose).

After a manual restart, `FailActiveRuns` (round 6) fails every run left
active — `executor restarted: run was active when the executor started;
outcome unknown` — and nothing is resumed. Steps still `running` drain: their
results, or the reconcile loop, finish the run. (Before round 6 the loop alone
resolved what it could — observed live: Postgres stopped mid-run → executor
exited → restarted → run `failed`, `result lost: … outcome unknown`.) A step
left `pending` while it actually ran is not detectable; since the run is
failed rather than resumed, it is never sent again. The proper fix, needed to
*resume* runs, is the write-ahead `dispatched` state (claim before sending,
see driver-interaction.md).

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
| §1 refusal stall | cannot happen — every refusal fails its step and the run (round 8); a timer-based retry was not built |
| §1 two concurrent runs | prevented (round 5, R2): `Start` refuses with 409 while another run is `running` or `failed_draining` |
| §2 abandon cannot persist | executor exits (fail-stop); on restart `FailActiveRuns` fails the run |
| A: drops | handled — reconcile loop (R3's check, R4's trigger), fail fast; see drops.md |
| A: unreachable driver | handled (round 6) — treated like a lost result: failed after the grace, `driver unreachable: …` |
| A: restarts, lost results | runs left active are failed at startup (round 6), never resumed |

## Consequence of enforcing one run at a time

A run that never reaches a terminal status now blocks **every** later start,
not just itself. Two such runs were found in the local database when the rule
went in — both left over from before the fixes above (one `running` with
every step pending, one `failed_draining` with nothing in flight) — and were
marked `aborted` by hand.

Round 6 closed the known ways to get there: `abandon` now ends draining runs,
an unreachable driver's step fails after the grace, and a restart fails every
run left active. What remains needs an operator: an instrument that reports
itself busy with our step for ever, or a database that stays down. There is
no abort endpoint; `aborted` exists in the schema but nothing sets it.
