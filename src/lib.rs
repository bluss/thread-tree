//!
//! A hierarchical thread pool used for splitting work in a branching fashion.
//!
//! This thread pool is good for:
//!
//! - You want to split work recursively in jobs that use approximately the same time.
//! - You want thread pool overhead to be low
//!
//! This is not good for:
//!
//! - You need work stealing
//! - When you have jobs of uneven size
//!

// Stack jobs and job execution implementation based on rayon-core by Niko Matsakis and Josh Stone
//
use crossbeam_channel::{Sender, bounded};
#[cfg(feature="unstable-thread-sea")]
use std::sync::Arc;

#[cfg(feature="unstable-thread-sea")]
use crossbeam_channel::Receiver;
#[cfg(feature="unstable-thread-sea")]
use std::sync::atomic::AtomicUsize;
#[cfg(feature="unstable-thread-sea")]
use std::sync::atomic::Ordering;

use std::thread;

mod unwind;
mod job;

use crate::job::{JobRef, StackJob};

#[cfg(feature="unstable-thread-sea")]
type Message = JobRef;
#[cfg(feature="unstable-thread-sea")]
type GroupMessage = Receiver<Message>;

/// A macro thread pool that acts as a reservoir (a *Sea*) of threads,
/// from which you can checkout/reserve a thread pool of *n* threads.
///
/// Checking out threads from the thread pool has the advantage that contention between threads is
/// reduced when assigning work. The downside is that it is not always possible to reserve a
/// thread pool of the right size, if it is already in use.
#[cfg(feature="unstable-thread-sea")]
#[derive(Debug)]
pub struct ThreadSea {
    sender: Sender<GroupMessage>,
    receiver: Receiver<GroupMessage>,
    thread_count: AtomicUsize,
    threads_available: Arc<AtomicUsize>,
    //grow_lock: Mutex<()>,
    thread_id: AtomicUsize,
}

#[cfg(feature="unstable-thread-sea")]
#[derive(Debug)]
struct SeaLocalInfo {
    receiver: Receiver<GroupMessage>,
    threads_available: Arc<AtomicUsize>,
    thread_id: usize,
}


#[cfg(feature="unstable-thread-sea")]
impl ThreadSea {
    pub fn new(thread_count: usize) -> Self {
        let (sender, receiver) = bounded(thread_count); // unsure which kind of channel to use here
        let nthreads = thread_count;
        let thread_count = AtomicUsize::new(nthreads);
        let threads_available = Arc::new(AtomicUsize::new(nthreads));
        //let grow_lock = Mutex::default();
        let thread_id = AtomicUsize::new(0);
        let pool = ThreadSea { sender, receiver, threads_available, thread_count, thread_id };
        for _ in 0..nthreads {
            pool.add_thread();
        }
        pool
    }

    pub fn thread_count(&self) -> usize { self.thread_count.load(Ordering::Acquire) }

    pub fn reserve(&self, thread_count: usize) -> ThreadPool {
        /*
        let _guard = self.grow_lock.lock();
        let cur_threads = self.thread_count.load(Ordering::Acquire);
        let cur_available = self.threads_available.load(Ordering::Acquire);
        let used_available_threads;
        if false && thread_count > cur_available {
            let gap = thread_count - cur_available;
            //dbg!("Adding threads", gap);
            for _ in 0..gap {
                self.add_thread();
            }
            self.thread_count.fetch_add(gap, Ordering::Release);
            used_available_threads = cur_available;
        } else {
            used_available_threads = thread_count;
        }
        //self.threads_available.fetch_sub(used_available_threads, Ordering::Release);
        drop(_guard);
        */

        let (sender, receiver) = bounded(0); // rendezvous channel
        let mut nthreads = 0;
        for _ in 0..thread_count {
            // maybe try_send and only reserve the number of threads that is available?
            //self.sender.send(receiver.clone()).unwrap();
            let ret = self.sender.try_send(receiver.clone());
            if ret.is_ok() { nthreads += 1; }
        }
        //eprintln!("Reserved {} threads", nthreads);
        //assert!(nthreads != 0, "Failed to reserve any threads");
        ThreadPool {
            sender,
            thread_count: nthreads,
        }
    }

    fn local_info(&self) -> SeaLocalInfo {
        let receiver = self.receiver.clone();
        let threads_available = self.threads_available.clone();
        let thread_id = self.thread_id.fetch_add(1, Ordering::Relaxed);
        SeaLocalInfo { receiver, threads_available, thread_id }
    }


    fn add_thread(&self) {
        let local = self.local_info();
        std::thread::spawn(move || {
            let my_local = local;
            for channel in my_local.receiver {
                // We got reserved for a thread pool
                //eprintln!("Thread start {}", my_local.thread_id);
                for job in channel {
                    unsafe {
                        job.execute()
                    }
                }
                // sender dropped, so we leave the group
                //eprintln!("Thread idle {}", my_local.thread_id);
                //my_local.threads_available.fetch_sub(1, Ordering::Release);
            }
        });
    }

}

// ThreadTree message on the channel (is just a job ref)
type TTreeMessage = JobRef;

/// A hierarchical thread pool used for splitting work in a branching fashion.
///
/// See [`ThreadTree::new_with_level()`] to create a new thread tree,
/// and see [`ThreadTree::top()`] for a usage example.
///
/// The thread tree has the benefit that at each level, jobs can be sent directly to the thread
/// that is going to execute it - that means there is no contention between waiting threads. The
/// downside is that the structure of the thread tree is rather static.
#[derive(Debug)]
pub struct ThreadTree {
    sender: Option<Sender<TTreeMessage>>,
    child: Option<[Box<ThreadTree>; 2]>,
}

// Only three threads needed to have four leaves, see below.
//
//           (root)
//      (1)            2 
// (1.1)   1.2   (2.1)   2.2
//
// Leaves 1.1, 1.2, 2.1 and 2.2 but only 1.2, 2, and 2.2 are new threads - the others inherit the
// current thread from the parent.  That means we have a fanout of four (leaves 1.1 trough 2.2)
// using the current thread and three additional threads.
//
// The implementation is such that the root holds ownership of leaf 2, and the root contains a
// channel sender that passes jobs to the node 2.  Further nodes down continue the same way
// recursively.
//
// Idea for later: implement reservations of (parts of) the tree?
// So that a 2-2 tree can be used as two separate 1-2 trees simultaneously

impl ThreadTree {
    const BOTTOM: &'static Self = &ThreadTree::new_level0();

    /// Create a level 0 tree (with no parallelism)
    #[inline]
    pub const fn new_level0() -> Self {
        ThreadTree { sender: None, child: None }
    }

    /// Create an n-level thread tree with 2<sup>n</sup> leaves
    ///
    /// Level 0 has no parallelism
    /// Level 1 has two nodes
    /// Level 2 has four nodes (et.c.)
    ///
    /// Level must be <= 12; panics on invalid input
    pub fn new_with_level(level: usize) -> Box<Self> {
        assert!(level <= 12,
                "Input exceeds maximum level 12 (equivalent to 2**12 - 1 threads), got level='{}'",
                level);
        if level == 0 {
            Box::new(Self::new_level0())
        } else if level == 1 {
            Box::new(ThreadTree { sender: Some(Self::add_thread()), child: None })
        } else {
            let fork_2 = Self::new_with_level(level - 1);
            let fork_3 = Self::new_with_level(level - 1);
            Box::new(ThreadTree { sender: Some(Self::add_thread()), child: Some([fork_2, fork_3])})
        }
    }

    /// Return true if this is a non-dummy pool which will parallelize in join
    #[inline]
    pub fn is_parallel(&self) -> bool {
        self.sender.is_some()
    }

    /// Get the top thread tree context, where we can inject tasks with join.
    /// Each job gets a sub-context that can be used to inject tasks further down the corresponding
    /// branch of the tree.
    ///
    /// **Note** to avoid deadlocks, tasks should never be injected into a tree context that
    /// doesn't belong to the current level. To avoid this should be easy - only call .top() at the
    /// top level.
    ///
    /// The following example shows using a two-level tree and using context to spawn tasks.
    ///
    /// ```
    /// use thread_tree::{ThreadTree, ThreadTreeCtx};
    ///
    /// let tp = ThreadTree::new_with_level(2);
    ///
    /// fn f(index: i32, ctx: ThreadTreeCtx<'_>) -> i32 {
    ///     // do work in subtasks here
    ///     let (a, b) = ctx.join(move |_| index + 1, |_| index + 2);
    ///
    ///     return a + b;
    /// }
    ///
    /// let (r0, r1) = tp.top().join(|ctx| f(0, ctx), |ctx| f(1, ctx));
    ///
    /// assert_eq!(r0 + r1, (0 + 1) + (0 + 2) + (1 + 1) + (1 + 2));
    /// ```
    #[inline]
    pub fn top(&self) -> ThreadTreeCtx<'_> {
        ThreadTreeCtx::from(self)
    }

    // Create a new thread that executes jobs, and return the channel sender that feeds jobs to
    // this thread.
    //
    // Notice that jobs are executed with a panic guard, that makes the whole program abort if a
    // job panics. Jobs should not panic.
    fn add_thread() -> Sender<TTreeMessage> {
        let (sender, receiver) = bounded::<TTreeMessage>(1); // buffered, we know we have a connection
        std::thread::spawn(move || {
            for job in receiver {
                unsafe {
                    job.execute()
                }
            }
        });
        sender
    }
}

/// A level-specific handle to the thread tree, that can be used to inject jobs.
///
/// See [`ThreadTree::top()`] for more information.
#[derive(Debug, Copy, Clone)]
pub struct ThreadTreeCtx<'a> {
    tree: &'a ThreadTree,
    // This handle is marked as non-Send/Sync as a help - there is nothing safety critical about it
    // - but it helps the user to avoid deadlocks - see the top method.
    _not_send_sync: *const (),
}

impl ThreadTreeCtx<'_> {
    #[inline]
    pub(crate) fn get(&self) -> &ThreadTree { self.tree }

    #[inline]
    pub(crate) fn from(tree: &ThreadTree) -> ThreadTreeCtx<'_> {
        ThreadTreeCtx { tree, _not_send_sync: &() }
    }

    /// Return true if this level will parallelize in join (or if we are at the bottom of the tree)
    #[inline]
    pub fn is_parallel(&self) -> bool {
        self.get().is_parallel()
    }

    /// Branch out and run a and b simultaneously and return their results jointly.
    ///
    /// Job `a` runs on the current thread while `b` runs on the sibling thread; each is passed
    /// a lower level of the thread tree.
    /// If the bottom of the tree is reached, where no sibling threads are available, both `a` and
    /// `b` run on the current thread.
    ///
    /// If either `a` or `b` panics, the panic is propagated here. If both jobs are executing,
    /// the panic will not propagate until after both jobs have finished.
    /// 
    /// Warning: You must not .join() into the same tree from nested jobs. Nested jobs must
    /// be spawned using the context that each job receives as the first parameter.
    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
        where A: FnOnce(ThreadTreeCtx) -> RA + Send,
              B: FnOnce(ThreadTreeCtx) -> RB + Send,
              RA: Send,
              RB: Send,
    {
        let bottom_level = ThreadTree::BOTTOM;
        let self_ = self.get();
        let (fork_a, fork_b) = match &self_.child {
            None => (bottom_level, bottom_level),
            Some([fa, fb]) => (&**fa, &**fb),
        };
        //assert!(self_.sender.is_some());

        unsafe {
            let a = move || a(ThreadTreeCtx::from(fork_a));
            let b = move || b(ThreadTreeCtx::from(fork_b));

            // first send B to the sibling thread
            let b_job = StackJob::new(b); // plant this safely on the stack
            let b_job_ref = JobRef::new(&b_job);
            let b_runs_here = match self_.sender {
                Some(ref s) => { s.send(b_job_ref).unwrap(); None }
                None => Some(b_job_ref),
            };

            let a_result;
            {
                // Ensure that we will later wait for B, if it is running on
                // another thread. Both in the case of A panic or regular scope exit.
                //
                // If job A panics, we still cannot return until we are sure that job
                // B is complete. This is because it may contain references into the
                // enclosing stack frame(s).
                let _wait_for_b_guard = match b_runs_here {
                    None => Some(WaitForJobGuard::new(&b_job)),
                    Some(_) => None,
                };

                // Execute task A
                a_result = a();

                if let Some(b_job_ref) = b_runs_here {
                    b_job_ref.execute();
                }
                // wait for b here
            }
            (a_result, b_job.into_result())
        }
    }

    /// Branch out twice and join, running three different jobs
    ///
    /// Branches twice on the left side and once on the right.
    /// The closure is called with corresponding thread tree context and an index in 0..3 for the job.
    pub fn join3l<A, RA>(&self, a: &A) -> ((RA, RA), RA)
        where A: Fn(ThreadTreeCtx, usize) -> RA + Sync,
              RA: Send,
    {
        self.join(
            move |ctx| ctx.join(move |ctx| a(ctx, 0), move |ctx| a(ctx, 1)),
            move |ctx| a(ctx, 2))
    }

    /// Branch out twice and join, running three different jobs
    ///
    /// Branches once on the right side and twice on the right.
    /// The closure is called with corresponding thread tree context and an index in 0..3 for the job.
    pub fn join3r<A, RA>(&self, a: &A) -> (RA, (RA, RA))
        where A: Fn(ThreadTreeCtx, usize) -> RA + Sync,
              RA: Send,
    {
        self.join(
            move |ctx| a(ctx, 0),
            move |ctx| ctx.join(move |ctx| a(ctx, 1), move |ctx| a(ctx, 2)))
    }

    /// Branch out twice and join, running four different jobs.
    ///
    /// Branches twice on each side.
    /// The closure is called with corresponding thread tree context and an index in 0..4 for the job.
    pub fn join4<A, RA>(&self, a: &A) -> ((RA, RA), (RA, RA))
        where A: Fn(ThreadTreeCtx, usize) -> RA + Sync,
              RA: Send,
    {
        self.join(
            move |ctx| ctx.join(move |ctx| a(ctx, 0), move |ctx| a(ctx, 1)),
            move |ctx| ctx.join(move |ctx| a(ctx, 2), move |ctx| a(ctx, 3)))
    }
}


#[cfg(feature="unstable-thread-sea")]
/// A thread pool based on rendezvous channels
#[derive(Debug)]
pub struct ThreadPool {
    sender: Sender<JobRef>,
    thread_count: usize,
}

#[cfg(feature="unstable-thread-sea")]
#[derive(Debug)]
struct LocalInfo {
    //sender: Sender<JobRef>,
    receiver: Receiver<JobRef>,
}

#[cfg(feature="unstable-thread-sea")]
impl ThreadPool {
    /// Create a new thread pool with `thread_count` worker threads.
    pub fn new(thread_count: usize) -> Self {
        // A rendezvous channel is used, because to avoid deadlocks,
        // we need to know for sure that any job we send (in join) will eventually get
        // completed, while we are waiting.
        let (sender, receiver) = bounded(0); // rendezvous
        let pool = ThreadPool { sender, thread_count };
        for _ in 0..thread_count {
            pool.add_thread(&receiver);
        }
        pool
    }

    /// Get the current pool thread count
    pub fn thread_count(&self) -> usize { self.thread_count }

    fn add_thread(&self, receiver: &Receiver<JobRef>) {
        let local = LocalInfo { receiver: receiver.clone() };
        std::thread::spawn(move || {
            let my_local = local;
            for job in my_local.receiver {
                unsafe {
                    job.execute();
                }
            }
        });
    }

    /// Run a and b simultaneously (and return their results, if applicable).
    ///
    /// A runs on the current thread while b runs on the sibling thread (also on current thread, if
    /// no other thread is available).
    ///
    /// A join uses *one* thread from the worker pool at most, this is because the job is split
    /// between the current thread and another working thread. Of course, in recursive splits, it
    /// may be the case that the current thread is a thread from the pool, too.
    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
        where A: FnOnce() -> RA + Send,
              B: FnOnce() -> RB + Send,
              RA: Send,
              RB: Send,
    {
        unsafe {
            // first send B, if any thread is idle
            let b_job = StackJob::new(b); // plant this safely on the stack
            let b_job_ref = JobRef::new(&b_job);
            let b_runs_here = match self.sender.try_send(b_job_ref) {
                Ok(_) => None,
                Err(e) => Some(e.into_inner()),
            };
            let a_result;
            {
                // Ensure that we will later wait for B, if it is running on
                // another thread. Both in the case of A panic or regular scope exit.
                //
                // If job A panics, we still cannot return until we are sure that job
                // B is complete. This is because it may contain references into the
                // enclosing stack frame(s).
                let _wait_for_b_guard = match b_runs_here {
                    None => Some(WaitForJobGuard::new(&b_job)),
                    Some(_) => None,
                };

                // Execute task A
                a_result = a();

                if let Some(b_job_ref) = b_runs_here {
                    b_job_ref.execute();
                }
                // wait for b here
            }
            (a_result, b_job.into_result())
        }
    }

    /// Take the `seed`, split it recursively using the splitter (until it does not split it
    /// anymore); execute the parts as jobs on the thread pool, use function `combine` to combine
    /// the return values.
    pub fn recursive_fork_join<S, FS, FE, FC, R>(&self, seed: S, splitter: FS, for_each: FE, combine: FC) -> R
        where FS: Fn(S) -> (S, Option<S>) + Sync,
              FE: Fn(S) -> R + Sync,
              FC: Fn(R, R) -> R + Sync,
              R: Send,
              S: Send,
    {
        self.recursive_join_(seed, &splitter, &for_each, &combine)
    }

    fn recursive_join_<S, FS, FE, FC, R>(&self, seed: S, splitter: &FS, for_each: &FE, combine: &FC) -> R
        where FS: Fn(S) -> (S, Option<S>) + Sync,
              FE: Fn(S) -> R + Sync,
              FC: Fn(R, R) -> R + Sync,
              R: Send,
              S: Send,
    {
        match splitter(seed) {
            (single, None) => for_each(single),
            (first, Some(second)) => {
                let (a, b) = self.join(
                    move || self.recursive_join_(first, splitter, for_each, combine),
                    move || self.recursive_join_(second, splitter, for_each, combine));
                combine(a, b)
            }
        }
    }
}

fn wait_for_job<F, R>(job: &StackJob<F, R>) {
    while !job.probe() {
        //spin_loop_hint();
        thread::yield_now();
    }
}

struct WaitForJobGuard<'a, F, R> {
    job: &'a StackJob<F, R>,
}

impl<'a, F, R> WaitForJobGuard<'a, F, R>
{
    fn new(job: &'a StackJob<F, R>) -> Self {
        Self { job }
    }
}

impl<'a, F, R> Drop for WaitForJobGuard<'a, F, R> {
    fn drop(&mut self) {
        wait_for_job(self.job)
    }
}

#[cfg(feature="unstable-thread-sea")]
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    #[allow(deprecated)]
    fn sleep_ms(x: u32) {
        std::thread::sleep_ms(x)
    }
    #[test]
    fn it_works() {
        let pool = ThreadPool::new(10);
        pool.join(
            || {
                println!("I'm f!");
                sleep_ms(100);
                pool.join(|| {
                    println!("f.0");
                    pool.join(|| {
                        println!("f.0.0");
                        sleep_ms(500);
                    },
                    || {
                        println!("f.0.1");
                    });
                },
                || {
                    println!("f.1");
                    pool.join(|| {
                        println!("f.1.0");
                    },
                    || {
                        println!("f.1.1");
                    });
                });

            },
            || {
                println!("I'm g!"); sleep_ms(100)
            },
            );
        drop(pool);
        sleep_ms(100);
    }

    #[test]
    fn recursive() {
        let pool = ThreadPool::new(50);
        let ret = pool.recursive_fork_join(0..127, |x| {
            let len = x.end - x.start;
            let mid = x.start + len / 2;
            if len > 3 {
                (x.start..mid, Some(mid..x.end))
            } else {
                (x, None)
            }
            
        },
        |value| {
            println!("Thread: {:?}", value);
            value.sum::<i32>()
        },
        |a, b| a + b);
        assert_eq!(ret, (0..127).sum());
    }

    #[test]
    #[should_panic]
    fn panic_a() {
        let pool = ThreadPool::new(2);
        pool.join(|| panic!(), || 1 + 1);
    }

    #[test]
    #[should_panic]
    fn panic_b() {
        let pool = ThreadPool::new(2);
        pool.join(|| 1 + 1, || panic!());
    }

    #[test]
    #[should_panic]
    fn panic_both() {
        let pool = ThreadPool::new(2);
        pool.join(|| { sleep_ms(50); panic!("Panic in A") }, || panic!("Panic in B"));
    }

    #[test]
    fn on_panic_a_wait_for_b() {
        let pool = ThreadPool::new(2);
        for i in 0..3 {
            let start = AtomicUsize::new(0);
            let finish = AtomicUsize::new(0);
            let result = unwind::halt_unwinding(|| {
                pool.join(
                    || panic!("Panic in A"),
                    || {
                        start.fetch_add(1, Ordering::SeqCst);
                        sleep_ms(50);
                        finish.fetch_add(1, Ordering::SeqCst);
                    });
            });
            let start_count = start.load(Ordering::SeqCst);
            let finish_count = finish.load(Ordering::SeqCst);
            assert_eq!(start_count, finish_count);
            assert!(result.is_err());
            println!("Pass {} with start: {} == finish {}", i,
                     start_count, finish_count);
        }
    }
}

#[cfg(feature="unstable-thread-sea")]
#[cfg(test)]
mod sea_tests {
    use super::*;
    #[allow(deprecated)]

    #[test]
    fn thread_count_0() {
        let sea = ThreadSea::new(0);
        let pool1 = sea.reserve(0);
        //let pool2 = sea.reserve(1);
    }

    #[test]
    fn recursive() {
        let sea = ThreadSea::new(50);
        let pool1 = sea.reserve(25);

        pool1.recursive_fork_join(0..127, |x| {
            let len = x.end - x.start;
            let mid = x.start + len / 2;
            if len > 3 {
                (x.start..mid, Some(mid..x.end))
            } else {
                (x, None)
            }
            
        },
        |value| {
            println!("Thread: {:?}", value);
        },
        |_, _| ()
        );
        let pool2 = sea.reserve(50);
        //let pool2 = sea.reserve(50);
        drop(pool1);

        pool2.recursive_fork_join(0..127, |x| {
            let len = x.end - x.start;
            let mid = x.start + len / 2;
            if len > 3 {
                (x.start..mid, Some(mid..x.end))
            } else {
                (x, None)
            }
            
        },
        |value| {
            println!("Thread: {:?}", value);
        },
        |_, _| ()
        );
    }
}

#[cfg(test)]
mod thread_tree_tests {
    use super::*;
    #[allow(deprecated)]

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;
    use once_cell::sync::Lazy;
    use std::collections::HashSet;
    use std::thread;
    use std::thread::ThreadId;

    #[allow(deprecated)]
    fn sleep_ms(x: u32) {
        std::thread::sleep_ms(x)
    }

    #[test]
    fn stub() {
        let tp = ThreadTree::new_level0();
        let a = AtomicUsize::new(0);
        let b = AtomicUsize::new(0);

        tp.top().join(|_| a.fetch_add(1, Ordering::SeqCst),
                |_| b.fetch_add(1, Ordering::SeqCst));
        assert_eq!(a.load(Ordering::SeqCst), 1);
        assert_eq!(b.load(Ordering::SeqCst), 1);

        let f = || thread::current().id();
        let (aid, bid) = tp.top().join(|_| f(), |_| f());
        assert_eq!(aid, bid);
        assert!(!tp.top().is_parallel());
    }

    #[test]
    fn new_level_1() {
        let tp = ThreadTree::new_with_level(1);
        let a = AtomicUsize::new(0);
        let b = AtomicUsize::new(0);

        tp.top().join(|_| a.fetch_add(1, Ordering::SeqCst),
                |_| b.fetch_add(1, Ordering::SeqCst));
        assert_eq!(a.load(Ordering::SeqCst), 1);
        assert_eq!(b.load(Ordering::SeqCst), 1);

        let f = || thread::current().id();
        let (aid, bid) = tp.top().join(|_| f(), |_| f());
        assert_ne!(aid, bid);
        assert!(tp.top().is_parallel());
    }

    #[test]
    fn build_level_2() {
        let tp = ThreadTree::new_with_level(2);
        let a = AtomicUsize::new(0);
        let b = AtomicUsize::new(0);

        tp.top().join(|_| a.fetch_add(1, Ordering::SeqCst),
                |_| b.fetch_add(1, Ordering::SeqCst));
        assert_eq!(a.load(Ordering::SeqCst), 1);
        assert_eq!(b.load(Ordering::SeqCst), 1);

        let f = || thread::current().id();
        let ((aid, bid), (cid, did)) = tp.top().join(
            |tp1| tp1.join(|_| f(), |_| f()),
            |tp1| tp1.join(|_| f(), |_| f()));
        assert_ne!(aid, bid);
        assert_ne!(aid, cid);
        assert_ne!(aid, did);
        assert_ne!(bid, cid);
        assert_ne!(bid, did);
        assert_ne!(cid, did);
    }

    #[test]
    fn overload_2_2() {
        let global = ThreadTree::new_with_level(1);
        let tp = ThreadTree::new_with_level(2);
        let a = AtomicUsize::new(0);

        let range = 0..100;

        let work = |ctx: ThreadTreeCtx<'_>| {
            let subwork = || {
                for i in range.clone() {
                    a.fetch_add(i, Ordering::Relaxed);
                    sleep_ms(1);
                }
            };
            ctx.join(|_| subwork(), |_| subwork());
        };

        global.top().join(
            |_| tp.top().join(work, work),
            |_| tp.top().join(work, work));

        let sum = range.clone().sum::<usize>();

        assert_eq!(sum * 4 * 2, a.load(Ordering::SeqCst));

    }

    #[test]
    fn deep_tree() {
        static THREADS: Lazy<Mutex<HashSet<ThreadId>>> = Lazy::new(|| Mutex::default());
        const TREE_LEVEL: usize = 8;
        const MAX_DEPTH: usize = 12;

        static COUNT: AtomicUsize = AtomicUsize::new(0);

        let tp = ThreadTree::new_with_level(TREE_LEVEL);

        fn f(tp: ThreadTreeCtx<'_>, depth: usize) {
            COUNT.fetch_add(1, Ordering::SeqCst);
            THREADS.lock().unwrap().insert(thread::current().id());
            if depth >= MAX_DEPTH {
                return;
            }
            tp.join(
                |ctx| {
                    f(ctx, depth + 1);
                },
                |ctx| {
                    f(ctx, depth + 1);
                });
        }

        COUNT.fetch_add(2, Ordering::SeqCst); // for the two invocations below.
        tp.top().join(|ctx| f(ctx, 2), |ctx| f(ctx, 2));
        let visited_threads = THREADS.lock().unwrap().len();
        assert_eq!(visited_threads, 1 << TREE_LEVEL);
        assert_eq!(COUNT.load(Ordering::SeqCst), 1 << MAX_DEPTH);
    }

    #[test]
    #[should_panic]
    fn panic_a() {
        let pool = ThreadTree::new_with_level(1);
        pool.top().join(|_| panic!("Panic in A"), |_| 1 + 1);
    }

    #[test]
    #[should_panic]
    fn panic_b() {
        let pool = ThreadTree::new_with_level(1);
        pool.top().join(|_| 1 + 1, |_| panic!());
    }

    #[test]
    #[should_panic]
    fn panic_both_in_threads() {
        let pool = ThreadTree::new_with_level(1);
        pool.top().join(|_| { sleep_ms(50); panic!("Panic in A") }, |_| panic!("Panic in B"));
    }

    #[test]
    #[should_panic]
    fn panic_both_bottom() {
        let pool = ThreadTree::new_with_level(0);
        pool.top().join(|_| { sleep_ms(50); panic!("Panic in A") }, |_| panic!("Panic in B"));
    }

    #[test]
    fn on_panic_a_wait_for_b() {
        let pool = ThreadTree::new_with_level(1);
        for i in 0..3 {
            let start = AtomicUsize::new(0);
            let finish = AtomicUsize::new(0);
            let result = unwind::halt_unwinding(|| {
                pool.top().join(
                    |_| panic!("Panic in A"),
                    |_| {
                        start.fetch_add(1, Ordering::SeqCst);
                        sleep_ms(50);
                        finish.fetch_add(1, Ordering::SeqCst);
                    });
            });
            let start_count = start.load(Ordering::SeqCst);
            let finish_count = finish.load(Ordering::SeqCst);
            assert_eq!(start_count, finish_count);
            assert!(result.is_err());
            println!("Pass {} with start: {} == finish {}", i,
                     start_count, finish_count);
        }
    }
}
