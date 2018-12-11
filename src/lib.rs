

// Based on rayon-core by Niko Matsakis and Josh Stone
use crossbeam_channel::{Sender, Receiver, bounded};

use std::thread;

mod unwind;
mod job;

use crate::job::{JobRef, StackJob};



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
                while !g_job.probe() {
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

