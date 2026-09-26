# Step 1

Claude decided to do "process based on longest chain" which is probably an unnecessary heuristic. It could be useful in the future if we ever have a good estimate of how long steps take. I did not change it even though it is likely unnecessary - I would be tempted though as it is unnecessary complexity.

Failure with Driver running was not nice, so directed Claude to add failure_draining.