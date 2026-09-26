# Principles / Limitations / Decisions

* One workflow running per executor
* Does not handle executor crashes
* Fail fast when environemnt does not match expectations


# Step 0

I'm using Claude Opus 5.5 off a 5x Max subscription via Claude Code MacOs App.

Step 0 is planning. Perfect Claude in Step 0.

# Step 1

Critical Path Priority: Claude decided to do "process based on longest chain" which is probably an unnecessary heuristic. It could be useful in the future if we ever have a good estimate of how long steps take

Failure with Driver running: added failure_draining to handle such a state. Added testing

# Step 2

Not convinced by refuesd handling. This is a bug or something unexpected in the enviornment. Fail fast?

Checking workflow liveness invariant. At least one step running at end of scan. We did have a scenario that broke liveness - added fail almost fast when there is not going to be progress. Added handling for that decision

Simplest approach would be to fail when a device is busy when we expect free. We relax this a bit and fail if progress for the workflow is not guaranteed, i.e. we end a scan with no Step waiting for a response. We will revisit when we handle DROP.