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
| 1 | Rungs 1–3: execute DAG, overlap, cope with refusal | static, test, live |
| 2 | Rung 4: failing steps, late results, send errors | + failure |
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
