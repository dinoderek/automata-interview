# Principles / Limitations / Decisions

* One workflow running per executor
* Does not handle gracefully executor crashes, restarts with pending runs or busy drivers
* Fail fast when environemnt does not match expectations (one exception is device unexpected busy)
* Fail fast on database errors


# Step 0 - Setup

I'm using Claude Opus 5.5 off a 5x Max subscription via Claude Code MacOs App.

Step 0 is planning. Perfect Claude in Step 0.

# Step 1 - Scheduling Steps

Critical Path Priority: Claude decided to do "process based on longest chain" which is probably an unnecessary heuristic. It could be useful in the future if we ever have a good estimate of how long steps take

Failure with Driver running: added failure_draining to handle such a state. Added testing

# Step 2 - Handle refusals, liveness, more testing

Not convinced by refused handling. This is a bug or something unexpected in the enviornment. Fail fast?

Checking workflow liveness invariant. At least one step running at end of scan. We did have a scenario that broke liveness - added fail almost fast when there is not going to be progress. Added handling for that decision

Simplest approach would be to fail when a device is busy when we expect free. We relax this a bit and fail if progress for the workflow is not guaranteed, i.e. we end a scan with no Step waiting for a response. We will revisit when we handle DROP.

# Step 3 - Retries

OK with limitations (no retry logs, no wait before retry, retries not configurable)

# Step 4 - Drop

Devices are quite limited as designed - in particular it is not possible to know the outcome of the last operation (success/failure). Given this the best we can do is to determine whether the Step is still running on the device and if it is not we fail. I'm not sure this was aligned with the README, where it says "the device does its job". 

Did some refinement of the "system error" case, where we crash becu

# Step 5 - One workflow only running

Enforce the invariant above and ran a review

# Step 6 - Applied review fixes

Good. Then moved to the SendDispatch finding which was not acceptable in my mind. 

