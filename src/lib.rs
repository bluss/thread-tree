

// Based on rayon-core by Niko Matsakis and Josh Stone
use crossbeam_channel::{Sender, Receiver, bounded};

use std::mem;
use std::thread;

mod unwind;
mod job;

use crate::job::{JobRef, StackJob};

macro_rules! debug {
    ($($t:tt)*) => { $($t)* }
}

macro_rules! debug { ($($t:tt)*) => {} }

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

    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
        where A: FnOnce() -> RA + Send,
              B: FnOnce() -> RB + Send,
              RA: Send,
              RB: Send,
    {
        unsafe {
            // first send B, if any thread is idle
            let b_job = StackJob::new(b); // plant this safely on the stack
            let b_runs_here = match self.sender.try_send(JobRef::new(&b_job)) {
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

