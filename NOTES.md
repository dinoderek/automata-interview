# The note

## How long

Between 2h30 and 3h including the note and the reading + discussion before the first commit. 

## The questions

### Did not build

- Did not support multiple workflows on the same executor. The brief did not ask for it, I was not sure it made any sense in a physical laboratory setting, and I was not sure it would fit into the 2 hours guideline given significant additional complexity.
- Did not support multiple instances of the executor process. Current solution relies on in memory mutex. To support multiple executor processes would need to use a database lock or similar. 
- Does not support resume after crash/restart. Main reason is complexity within 2 hours. 
- Did not attempt to optimise performance. In particular each result triggers a full rescan which is linear in number of steps, under one global lock, so quadratic over a run. The fix would be counting unmet dependencies per step and locking per run.

### Most likely to break

- The issues above (multiple workflows, multiple executors, crash/restart)
- One thing I did not investigate is whether there is pressure to respond to bus messages quickly
- The grace period of 1s in the reconciler is a guess
- Database outage breaks us

### Concurrent drivers

- Kept the bus's goroutine-per-result; every scheduling decision takes one global in-memory mutex. A serial bus alone would not be enough: `Start` and the reconcile loop change the same state, so the lock is needed anyway. Each handler rescans the whole run, so arrival order does not matter.
- Two simultaneous results: the second handler waits for the lock and sees what the first committed. Steps are claimed (`dispatched`) before any command is sent, and every update is conditional on the current state, so no step is sent twice. The provided race test forces this.

### Drivers drop

- Code polls drivers. If driver completed task we do not know outcome so fail the step. If driver is in progress, we continue polling. Code complexity would be similar if assuming success, but the driver decides a step failed before dropping the message, so assuming success could silently feed a bad plate to the next steps. Alternatives would be to improve the driver to store outcomes or to stop and ask the operator. 

### A few of my assumptions / decisions that are important 

- One workflow at a time
- Fail fast on database error
- Does not support resume
- Only "user" of the drivers. If a driver is busy when I expect ready then it's a failure. The brief calls a refusal "normal, not an error", but our own work can never cause one (we track device occupancy ourselves), so a refusal means something else holds the instrument.
- The code is made quite a bit more complex by my decision to not do SendCommand within the mutex. 

### What we built 

We went beyond the four floor requirements: 

- Retries of retryable failures, at most 3 attempts
- Drop detection
- Draining after failure
- Failure reasons and skipped steps
- Enforce one-run at a time
- Ordering ready steps by critical path: the default workflow takes 10s instead of 12s
- Failing runs left active at startup
- SendCommand outside of mutex loop allows for higher concurrency / less wait time between steps

### AI Conversation

- Kept a log in DIARY.md plus my own log in HUMAN_DIARY.md - not a line by line transcript though

## My thoughts on the exercise

### Brief clear

The brief was clear. The thing that I realised only now writing the note is that the database strongly hints at multi-process / resume that I did not handle but two hours is two hours. If multi-process or resume is of interest, I would put it explicitly in the brief. 


### Worth my time

This is always a tricky question with interviews... I think it is a good exercise, and good practice for future interviews, and for what are the bottlenecks when doing AI assisted coding interview - which means that AI writes the code :D. First time for me doing this in an interview setting and quite instructive.

### Anything in the way

Nope. The only thing that I would maybe have liked is for the exercise to be ready for multiple worktrees. This would've allowed me to run multiple agents in parallel. But in retrospect I did not need it.

### Anything I would change in the exercise

It's a nice exercise. I enjoyed it. I would perhaps provide clearer behavior definitions for retries and drops and provide scenarios to guide the implementation. For example, there are no real criteria to take to choose a retry policy for retriable errors; the behavior of the driver on drop is genuinely questionable. I do understand the choice of not providing acceptance tests, but more detailed scenarios would help inform the engineering choices. 