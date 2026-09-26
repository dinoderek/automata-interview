# Driver interaction model

See also [liveness.md](liveness.md): when a run is guaranteed to finish, and
where it is not.

How the executor and a driver talk, what can go wrong at each point, and what
that means for step state. Sources: `services/worker/main.go`,
`services/executor/bus.go`.

## The exchange

```
executor                         NATS                      driver
   │ SendCommand (request, 3s timeout)                        │
   │ ───────────────────────────────────────────────────────► │ handleCommand:
   │                                                          │   busy?  → reply {accepted:false, "busy with <name>"}
   │                                                          │   else busy=true, executed += stepID
   │ ◄─────────────────────────────────────── ack ─────────── │   reply {accepted:true}
   │                                                          │   go execute()
   │                                                          │     sleep(duration)
   │                                                          │     busy=false        ← freed BEFORE reporting
   │                                                          │     maybe Error (FAIL_PCT)
   │                                                          │     maybe return, no report (DROP_PCT)
   │ ◄──────────── steps.result (separate subject) ────────── │     publish StepResult
   │ go HandleResult(...)                                     │
```

## Facts the design leans on

- **The driver does not deduplicate.** It appends every accepted step ID to
  `executed` but never consults it. Sending the same step twice runs it twice.
  Exactly-once is entirely the executor's responsibility.
- **The driver frees itself before publishing the result.** When a result
  arrives, that device is idle, so rescanning on each result is well-timed.
  There is a short window where the driver is free but our state still says
  `running` — that makes us conservative, never wrong.
- **Ack and result travel on different subscriptions** (request inbox vs
  `steps.result`). NATS orders messages per subscription only, so a result can
  reach `HandleResult` before `SendCommand` has returned (short step
  durations make this likely).
- **Core NATS is at-most-once.** A result published while the executor is
  disconnected or restarting is lost — indistinguishable from `DROP_PCT`.
- **A refusal names the busy step by name, not by run or ID.** It cannot tell
  us whether the device is busy with our work or someone else's.
- **`DriverState().Executed` holds step IDs.** It is ground truth for "did
  this step ever start on this device", usable for reconciliation.

## Outcomes of one dispatch attempt

| # | What happened | Executor observes | Driver executed? | Step state we write |
|---|---|---|---|---|
| 1 | Accepted | `ack.Accepted=true` | yes | `running`, stamp `dispatched_at` |
| 2 | Refused | `ack.Accepted=false` | no | `failed`, and the run fails: the device is not in the state we believe (since round 7) |
| 3 | No driver subscribed | `ErrNoResponders` (immediate on NATS 2.10) | **no** | v1: `failed`, run fails (could safely stay `pending` once something retries it) |
| 4 | Ack lost / slow | timeout error | **unknown** | — |
| 5 | Executor crashes after deciding, before the outcome is written | nothing | **unknown** | — |
| 6 | Result arrives before the ack is processed | result for a step not yet `running` | yes | depends on locking |

Rows 4 and 5 are the only genuinely ambiguous ones.

- Row 4: v1 fails the run. The step is marked `failed` with error
  "dispatch outcome unknown" — note this is imprecise, since the step may in
  fact be executing.
- Row 5: the only real fix is reconciling on startup against
  `DriverState().Executed`. Crash recovery is out of scope for v1.
- Row 6: see locking below.

## `dispatched`: removed in v1, back since round 7

**Rounds 1–6** held the scheduler's mutex across *decide → send → record
outcome*. Nothing that makes decisions could observe a step between "chosen"
and "outcome recorded", so a claim state had no reader and was dropped. The
cost: a driver slow to answer held up every result and the reconcile loop for
up to the 3s command timeout.

**Round 7** sends outside the lock, which is what the original analysis said
would bring `dispatched` back:

1. under the lock: `pending → dispatched` (the claim: `dispatch_count+1`,
   `dispatched_at` stamped), for every step the scan chooses;
2. without the lock: `SendCommand`, one claim after another — all of a scan's
   claims go out, even if an earlier one fails, since they were chosen before
   any failure was known;
3. under the lock: `dispatched → running` (accepted), or `→ failed` (refused,
   or send error), conditional on the attempt.

Consequences:

- **Occupancy counts `dispatched`**, so no other scan can choose the step or
  its device while the command is in flight.
- **Row 6 (result before recorded ack)**: results apply to `dispatched` as
  well as `running`; the late ack then changes nothing. The ack is
  conditional on the attempt, so attempt 1's late ack cannot mark a retry's
  attempt 2 running.
- **Reconcile loop**: in-flight steps include `dispatched`; an in-memory set
  of claims being sent keeps it from taking a command in flight (driver idle,
  no record yet) for a lost result.
- **Rows 4 and 5 (ambiguous outcomes)** now leave the step `dispatched`, never
  `pending`: a step a driver may be running is never chosen again. At startup
  `FailActiveRuns` counts it as in flight and the loop resolves it.
- **A refusal fails the step.** Nothing of ours is on a device we send to
  (occupancy counts dispatched and running, drivers free themselves before
  reporting, one run at a time with draining runs still active). So a refusal
  means the device is not in the state we believe — an environment error.

Live: with the incubator frozen (`docker compose pause`), its command hung for
the 3s timeout; during it, the liquid handler's result was handled and the
next liquid-handler step sent. The run then failed as "dispatch failed,
outcome unknown" and drained.

State machine, as built:

```
pending ──claim──► dispatched ──ack accepted──► running ──result ok──────────────────► completed
                    │     │                        │
                    │     └──result (beat ack)─────┤──result error, not retryable─────► failed
                    │                              ├──result error, attempts used up──► failed
                    ├──refused─────────────────────┼──lost / unreachable (grace)──────► failed   (reconcile loop)
                    ├──send error──────────────────┘   (from dispatched too, if no send in flight)
                    │   (timeout, no responders)    └──result error, retryable, attempts left ──► pending
                    ▼
                  failed
pending ──run failed first──► skipped
```

Runs: `pending → running → completed`, or `running → failed_draining →
failed` when a step fails with others still on instruments (straight to
`failed` if none). `abandon` and `FailActiveRuns` also end runs; see
liveness.md.

## Device occupancy

The scheduler does not ask drivers whether they are busy. It derives
occupancy from its own rows: a device with a step in `dispatched` or
`running` is skipped. So our own work never causes a refusal, and a refusal
that does happen fails the step (since round 7; before, it waited for the next
result, or failed the run only if nothing else was in flight).

Limitation with multiple concurrent runs: the occupancy query itself
generalises if it is per-device across all running runs, but the trigger does
not — a result only rescans its own run, so a device freed by run A never
wakes run B's waiting step.
