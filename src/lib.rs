

// Based on rayon-core by Niko Matsakis and Josh Stone
use crossbeam_channel::{Sender, Receiver, bounded};

use std::mem;
use std::thread;

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};


// from rayon
pub enum JobResult<T> {
    None,
    Ok(T),
    Panic(Box<Any + Send>),
}

// from rayon
struct StackJob<F, R> {
    func: UnsafeCell<Option<F>>,
    result: UnsafeCell<JobResult<R>>,
    latch: AtomicBool,
}

impl<F, R> StackJob<F, R> {
    fn new(f: F) -> Self
        where F: FnOnce() -> R + Send
    {
        Self {
            func: UnsafeCell::new(Some(f)),
            result: UnsafeCell::new(JobResult::None),
            latch: AtomicBool::new(false),
        }
    }

    fn into_result(self) -> R {
        unsafe {
            assert!((*self.func.get()).is_none());
            match mem::replace(&mut *self.result.get(), JobResult::None) {
                JobResult::None => unreachable!(),
                JobResult::Ok(r) => r,
                JobResult::Panic(x) => resume_unwinding(x),
            }
        }
    }
}

impl<F, R> Job for StackJob<F, R>
where
    //L: Latch + Sync,
    F: FnOnce() -> R + Send,
    R: Send,
{
    unsafe fn execute(this: *const Self) {
        let this = &*this;
        let abort = AbortIfPanic;
        let func = (*this.func.get()).take().unwrap();
        (*this.result.get()) = match halt_unwinding(|| func()) {
            Ok(x) => JobResult::Ok(x),
            Err(x) => JobResult::Panic(x),
        };
        this.latch.store(true, Ordering::SeqCst);
        //this.latch.set();
        mem::forget(abort);
    }
}


/// A `Job` is used to advertise work for other threads that they may
/// want to steal. In accordance with time honored tradition, jobs are
/// arranged in a deque, so that thieves can take from the top of the
/// deque while the main worker manages the bottom of the deque. This
/// deque is managed by the `thread_pool` module.
pub trait Job {
    /// Unsafe: this may be called from a different thread than the one
    /// which scheduled the job, so the implementer must ensure the
    /// appropriate traits are met, whether `Send`, `Sync`, or both.
    unsafe fn execute(this: *const Self);
}

/// Effectively a Job trait object. Each JobRef **must** be executed
/// exactly once, or else data may leak.
///
/// Internally, we store the job's data in a `*const ()` pointer.  The
/// true type is something like `*const StackJob<...>`, but we hide
/// it. We also carry the "execute fn" from the `Job` trait.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct JobRef {
    pointer: *const (),
    execute_fn: unsafe fn(*const ()),
}

unsafe impl Send for JobRef {}
unsafe impl Sync for JobRef {}

impl JobRef {
    /// Unsafe: caller asserts that `data` will remain valid until the
    /// job is executed.
    pub unsafe fn new<T>(data: *const T) -> JobRef
    where
        T: Job + Send,
    {
        let fn_ptr: unsafe fn(*const T) = <T as Job>::execute;

        // erase types:
        let fn_ptr: unsafe fn(*const ()) = mem::transmute(fn_ptr);
        let pointer = data as *const ();

        JobRef {
            pointer: pointer,
            execute_fn: fn_ptr,
        }
    }

    #[inline]
    pub unsafe fn execute(&self) {
        (self.execute_fn)(self.pointer)
    }
}


#[derive(Debug)]
pub struct ThreadPool {
    sender: Sender<JobRef>,
    receiver: Receiver<JobRef>,
    thread_count: usize,
}

#[derive(Debug)]
struct LocalInfo {
    //sender: Sender<JobRef>,
    receiver: Receiver<JobRef>,
}

impl ThreadPool {
    pub fn new(thread_count: usize) -> Self {
        let (sender, receiver) = bounded::<JobRef>(0); // rendezvous channel
        let pool = ThreadPool { sender, receiver, thread_count };
        for _ in 0..thread_count {
            pool.add_thread();
        }
        pool
    }

    pub fn thread_count(&self) -> usize { self.thread_count }

    fn local_info(&self) -> LocalInfo {
        let receiver = self.receiver.clone();
        LocalInfo { receiver }
    }


    fn add_thread(&self) {
        let local = self.local_info();
        std::thread::spawn(move || {
            let my_local = local;
            #[cfg(test)]
            let id = &my_local as *const _;
            #[cfg(test)]
            println!("{:p}: Starting with {:?}", id, my_local);
            for job in my_local.receiver {
                #[cfg(test)]
                println!("{:p}: got a job", id);
                unsafe {
                    job.execute();
                }
            }
            #[cfg(test)]
            println!("{:p}: disconnected .. exiting", id);
        });
    }

    pub fn join<A, B, RA, RB>(&self, f: A, g: B) -> (RA, RB)
        where A: FnOnce() -> RA + Send,
              B: FnOnce() -> RB + Send,
              RA: Send,
              RB: Send,
    {
        // first take care of g, if any thread is idle
        let g_job = StackJob::new(g); // plant this safely on the stack
        unsafe {
            let g_in_return = match self.sender.try_send(JobRef::new(&g_job)) {
                Ok(_) => None,
                Err(e) => Some(e.into_inner()),
            };
            let ra = f();
            if let Some(job_ref) = g_in_return {
                job_ref.execute();
            } else {
                // ensure job finishes
                while !g_job.latch.load(Ordering::SeqCst) {
                    //spin_loop_hint();
                    thread::yield_now();
                }
            }
            (ra, g_job.into_result())
        }
    }

    pub fn recursive_join<S, FS, FA>(&self, seed: S, splitter: FS, apply: FA)
        where FS: Fn(S) -> (S, Option<S>) + Sync,
              FA: Fn(S) + Sync,
              S: Send,
    {
        self.recursive_join_(seed, &splitter, &apply);
    }

    fn recursive_join_<S, FS, FA>(&self, seed: S, splitter: &FS, apply: &FA)
        where FS: Fn(S) -> (S, Option<S>) + Sync,
              FA: Fn(S) + Sync,
              S: Send
    {
        match splitter(seed) {
            (single, None) => apply(single),
            (first, Some(second)) => {
                self.join(
                    move || self.recursive_join_(first, splitter, apply),
                    move || self.recursive_join_(second, splitter, apply));
            }
        }
    }

}




#[cfg(test)]
mod tests {
    use super::*;
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
}


// Package up unwind recovery. Note that if you are in some sensitive
// place, you can use the `AbortIfPanic` helper to protect against
// accidental panics in the rayon code itself.

use std::any::Any;
use std::io::prelude::*;
use std::io::stderr;
use std::panic::{self, AssertUnwindSafe};

/// Executes `f` and captures any panic, translating that panic into a
/// `Err` result. The assumption is that any panic will be propagated
/// later with `resume_unwinding`, and hence `f` can be treated as
/// exception safe.
pub fn halt_unwinding<F, R>(func: F) -> thread::Result<R>
where
    F: FnOnce() -> R,
{
    panic::catch_unwind(AssertUnwindSafe(func))
}

pub fn resume_unwinding(payload: Box<Any + Send>) -> ! {
    panic::resume_unwind(payload)
}

pub struct AbortIfPanic;

fn aborting() {
    let _ = writeln!(&mut stderr(), "Rayon: detected unexpected panic; aborting");
}

impl Drop for AbortIfPanic {
    fn drop(&mut self) {
        aborting();
        ::std::process::abort(); // stable in rust 1.17
    }
}
