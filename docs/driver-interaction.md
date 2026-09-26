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
| 2 | Refused | `ack.Accepted=false` | no | stays `pending` if other steps are in flight; otherwise `failed` and the run fails (see liveness.md) |
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

## Why v1 has no `dispatched` state

v1 holds a single in-process mutex across *decide → send → write outcome*, in
both `Start` and `HandleResult`. Consequences:

- Nothing that makes scheduling decisions can observe a step between "chosen"
  and "outcome recorded", so an intermediate claim state has no reader.
- Row 6 is serialised: the early `HandleResult` blocks on the mutex and, when
  it gets in, the step is already `running`.

So v1's state machine is (as built, through round 6):

```
pending ──ack accepted──► running ──result ok──────────────────────► completed
  │  ▲                    │   │
  │  │ refused, other     │   ├──result error, not retryable──────► failed
  │  │ work in flight     │   ├──result error, attempts used up───► failed
  │  └────────────────────┘   ├──lost / driver unreachable (grace)─► failed   (reconcile loop)
  │     (refusal stays        └──result error, retryable, attempts left ──► pending (sent again at once)
  │      pending; loops back)
  ├──send error (timeout, no responders)───────────────────────────► failed   ("dispatch failed, outcome unknown")
  ├──refused, nothing in flight────────────────────────────────────► failed   (docs/liveness.md §1)
  └──run failed first───────────────────────────────────────────────► skipped
```

Runs: `pending → running → completed`, or `running → failed_draining →
failed` when a step fails with others still on instruments (straight to
`failed` if none). `abandon` and `FailActiveRuns` also end runs; see
liveness.md.

`dispatched_at` is stamped only on acceptance, because the timeline uses it as
the step's start time; stamping a refused attempt would make steps appear to
start early and inflate overlap.

The `StepDispatched` constant in `models.go` is kept (the schema comment lists
it) but unused.

## When `dispatched` becomes necessary

As soon as the lock no longer spans the network call. The natural upgrade from
an in-process mutex is a Postgres row lock (`SELECT … FOR UPDATE` on the run),
which works across executor replicas and gives per-run locking — but you do
not hold a transaction open across a 3s network call. The sequence becomes:

1. tx: `pending → dispatched` (the claim; prevents a concurrent double-send)
2. send, outside any lock
3. tx: `dispatched → running` (accepted) or `→ pending` (refused)

Then `dispatched` is both the double-send guard and the crash-recovery marker
for rows 4 and 5, and `HandleResult` must accept a result from `dispatched` as
well as `running` (row 6).

## Device occupancy

The scheduler does not ask drivers whether they are busy. It derives
occupancy from its own rows: a device with a step in `running` is skipped.
This avoids predictable refusals and, more importantly, avoids relying on a
refusal being retried — a refused step is only reconsidered when some other
result triggers a rescan.

Limitation with multiple concurrent runs: the occupancy query itself
generalises if it is per-device across all running runs, but the trigger does
not — a result only rescans its own run, so a device freed by run A never
wakes run B's waiting step.
