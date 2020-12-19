

// Based on rayon-core by Niko Matsakis and Josh Stone
use crossbeam_channel::{Sender, Receiver, unbounded, bounded};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use parking_lot::Mutex;

use std::thread;

mod unwind;
mod job;

use crate::job::{JobRef, StackJob};

macro_rules! debug {
    ($($t:tt)*) => { $($t)* }
}

macro_rules! debug { ($($t:tt)*) => {} }

type Message = JobRef;
type GroupMessage = Receiver<Message>;

#[derive(Debug)]
pub struct ThreadSea {
    sender: Sender<GroupMessage>,
    receiver: Receiver<GroupMessage>,
    thread_count: AtomicUsize,
    threads_available: Arc<AtomicUsize>,
    grow_lock: Mutex<()>,
    thread_id: AtomicUsize,
}

#[derive(Debug)]
struct SeaLocalInfo {
    receiver: Receiver<GroupMessage>,
    threads_available: Arc<AtomicUsize>,
    thread_id: usize,
}


impl ThreadSea {
    pub fn new(thread_count: usize) -> Self {
        let (sender, receiver) = bounded(thread_count); // unsure which kind of channel to use here
        let nthreads = thread_count;
        let thread_count = AtomicUsize::new(nthreads);
        let threads_available = Arc::new(AtomicUsize::new(nthreads));
        let grow_lock = Mutex::default();
        let thread_id = AtomicUsize::new(0);
        let pool = ThreadSea { sender, receiver, threads_available, thread_count, grow_lock, thread_id };
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

    fn make_tree(&self) -> Fork
    {
        panic!()
    }

    // Build thread tree
    //
    // (1)
    // >> (a)
    // >> b
    // > 2
    // >> (c)
    // >> d
    //
    // Only three threads needed to have four leaves:
    //
    // leaves 1a, 1b, 2c, 2d but with threads 1a (main), b, 2, and 2d.
    //      (root)
    //   (1)      2 
    // (a)  b  (c)  d
    // 
    // 2: Fork with no children and 1 sender to d
    // 1: Fork with no children and 1 sender to b
    // root: Fork with children 1 and 2; sender to 2
    //
    //       1   <- node numbering?
    //      2 3
    //    4 5 6 7
    //   8 - 15 then 16 - 32 etc
}

type ForkMsg = JobRef;

#[derive(Debug)]
pub struct Fork {
    index: usize,
    sender: Option<Sender<ForkMsg>>,
    child: Option<[Arc<Fork>; 2]>,
}

impl Fork {

    pub fn build1(number: usize) -> Arc<Self>
    {
        Arc::new(Fork { index: number, sender: Some(Self::add_thread()), child: None })
    }

    pub fn build3() -> Arc<Self>
    {
        let fork_2 = Self::build1(2);
        let fork_3 = Self::build1(3);
        let fork_1 = Arc::new(Fork { index: 1, sender: Some(Self::add_thread()), child: Some([fork_2, fork_3])});
        fork_1
    }

    fn add_thread() -> Sender<ForkMsg>
    {
        let (sender, receiver) = bounded::<ForkMsg>(1); // buffered, we know we have a connection
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

#[derive(Debug)]
pub struct ForkCtx<'a> {
    pub fork: &'a Fork,
    _not_send_sync: *const (),
}

impl ForkCtx<'_> {
    //pub fn fork(&self) -> &Fork { self.fork }
}

impl Fork
{
    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
        where A: FnOnce(ForkCtx) -> RA + Send,
              B: FnOnce(ForkCtx) -> RB + Send,
              RA: Send,
              RB: Send,
    {
        let no_fork = Fork { index: 0, sender: None, child: None };
        let fork_a;
        let fork_b;
        match &self.child {
            None => {
                fork_a = &no_fork;
                fork_b = &no_fork;
            }
            Some([fa, fb]) => {
                fork_a = &*fa;
                fork_b = &*fb;
            }
        };
        assert!(self.sender.is_some());

        unsafe {
            let a = move || a(ForkCtx { fork: fork_a, _not_send_sync: &() });
            let b = move || b(ForkCtx { fork: fork_b, _not_send_sync: &() });

            // first send B, if any thread is idle
            let b_job = StackJob::new(b); // plant this safely on the stack
            let b_job_ref = JobRef::new(&b_job);
            let b_runs_here = match self.sender {
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
}


#[derive(Debug)]
pub struct ThreadPool {
    sender: Sender<JobRef>,
    thread_count: usize,
}

#[derive(Debug)]
struct LocalInfo {
    //sender: Sender<JobRef>,
    receiver: Receiver<JobRef>,
}

impl ThreadPool {
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

    pub fn recursive_join<S, FS, FA>(&self, seed: S, splitter: FS, for_each: FA)
        where FS: Fn(S) -> (S, Option<S>) + Sync,
              FA: Fn(S) + Sync,
              S: Send,
    {
        self.recursive_join_(seed, &splitter, &for_each);
    }

    fn recursive_join_<S, FS, FA>(&self, seed: S, splitter: &FS, for_each: &FA)
        where FS: Fn(S) -> (S, Option<S>) + Sync,
              FA: Fn(S) + Sync,
              S: Send
    {
        match splitter(seed) {
            (single, None) => for_each(single),
            (first, Some(second)) => {
                self.join(
                    move || self.recursive_join_(first, splitter, for_each),
                    move || self.recursive_join_(second, splitter, for_each));
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
        pool.recursive_join(0..127, |x| {
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
        });
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

#[cfg(test)]

mod sea_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    #[allow(deprecated)]
    fn sleep_ms(x: u32) {
        std::thread::sleep_ms(x)
    }

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

        pool1.recursive_join(0..127, |x| {
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
        });
        let pool2 = sea.reserve(50);
        //let pool2 = sea.reserve(50);
        drop(pool1);

        pool2.recursive_join(0..127, |x| {
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
        });
    }
}
