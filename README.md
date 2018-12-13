## Joinpool

Based on rayon-core by Niko Matsakis and Josh Stone.

Experimental simple thread pool used for spawning stack-bound scoped jobs with no work stealing.

This is good for:

- You want to split work recursively in jobs that use approximately the same time.

This is not good for:

- You need work stealing
- When you have jobs of uneven size
