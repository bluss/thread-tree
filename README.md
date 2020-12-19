## Joinpool

Based on rayon-core by Niko Matsakis and Josh Stone.

Experimental simple thread pool used for spawning stack-bound scoped jobs with no work stealing.

This is good for:

- You want to split work recursively in jobs that use approximately the same time.

This is not good for:

- You need work stealing
- When you have jobs of uneven size



Ideas

Always have threads >= 1

Shrink threads by messaging Exit message - first thread to pick up, is free and
exits

Have a tree structure of thread pool subgroups? To mimic the branching split of
nested joins.
Thread Pool in 4 is a 2 -> 2 split; and 8 is a 2 -> 2 -> 2 split.

Maybe have a way to "reserve" a subbranch of this. Build a 16 => 2 -> 2 -> 2 ->
2 structure, and make it possible to reserve a subtree of that(!)

But: remember that a "2" group only needs one thread (current thread is
excluded). How does this translate to the thread tree? Maybe it doesn't.

Serve 8 jobs by having a 7-thread pool; split it and serve each half, 4 jobs, with a 3 thread pool.
This is done by integrating the "join(A, B)" operation with the split into sub-threadpools.
