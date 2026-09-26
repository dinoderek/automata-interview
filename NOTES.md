# The note

## How long

Between 2h30 and 3h including the note and the reading + discussion before the first commit. 

## The questions

### Did not build

- Did not support multiple workflows on the same executor. The brief did not ask for it, I was not sure it made any sense in a physical laboratory setting, and I was not sure it would fit into the 2 hours guideline given significant additional complexity.
- Did not support multiple instances of the executor process. Current solution relies on in memory mutex. To support multiple executor processes would need to use a database lock or similar. 
- Does not support resume after crash/restart. Main reason is complexity within 2 hours. 
- Did not attempt to optimise performance. In paticular each result triggers a full rescan which is linear in number of steps. 

### Most likely to break

- The issues above (multiple workflows, multiple executors, crash/restart)
- One thing I did not investigate is whether there is pressure to respond to bus messages quickly
- The grace period of 1s in the reconciler is a guess
- Database outage breaks us

### Concurrent drivers

- Completions get serialised via in memory global mutex

### Drivers drop

- Code polls drivers. If driver completed task we do not know outcome so fail the step. If driver is in progress, we continue polling. Code complexity would be similar if assuming success. Alternatives would be to improve the driver to store outcomes or to stop and ask the operator. 

### A few of my assumptions / decisions that are important 

- One workflow at a time
- Fail fast on database error
- Does not support resume
- Only "user" of the drivers. If a driver is busy when I expect ready then its a failure.
- The code is made quite a bit more complex by my decision to not do SendCommand within the mutex. 

### What we built 

We went beyond the four floor requirements: 

- Bounded retries
- Drop detection
- Draining after failure
- Failure reasons and skipped steps
- Enforce one-run at a time
- SendCommand outside of mutex loop allows for higher concurrency / less wait time between steps

### AI Conversation

- Kept a log in DIARY.md plus my own log in HUMAN_DIARY.md - not a line by line transcript though

## My thoughts on the exercise

### Brief clear

The brief was clear. The thing that I realised only now writing the note is that the database strongly hints at multi-process / resume that I did not handle but two hours is two hours. If multi-process or resume is of interest, I would put it explicilty in the brief. 


### Worth my time

This is always a tricky question with interviews... I think it is a good exercise, and good practice for future interviews, and for what are the bottlenecks when doing AI assisted coding interview - which means that AI writes the code :D. First time for me doing this in an interview setting and quite instructive.

### Anything in the way

Nope. The only thing that I would maybe have liked is for the exercise to be ready for multiple worktrees. This would've allowed me to run multiple agents in parallel. But in retrospect I did not need it.

### Anything I would change in the exercise

It's a nice exercise. I enjoyed it. I would perhaps provide clearer beahvior definitions for retries and drops and provide scenarios to guide the implementation. For example, there is no real criteria to take to choose a retry policy for retriable errors; the beahvior of the driver on drop is genuinely questionable. I do understand the choice of not providing acceptance tests, but more detailed scenarios would help inform the engineering choices. 