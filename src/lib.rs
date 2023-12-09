
//! # Notes
//! This library will spawn one thread that waits for io-uring completions and notifies the waker.
//! This is nesessary to remain compatible with every runtime.

use std::{fmt, error, io, thread, sync::{Arc, Mutex}, fs, os::fd::AsRawFd, task, future, pin::Pin, marker::PhantomData};
use private_io_state::IoState;

mod private_io_state {

    use std::sync::Mutex;

    pub struct IoState {
        io_uring: io_uring::IoUring,
        sq_lock: Mutex<()>,
        cq_lock: Mutex<()>,
    }

    impl IoState {
        pub fn new(io_uring: io_uring::IoUring) -> Self {
            Self {
                io_uring,
                sq_lock: Mutex::new(()),
                cq_lock: Mutex::new(())
            }
        }
        pub fn submitter(&self) -> io_uring::Submitter {
            self.io_uring.submitter()
        }
        pub fn with_submission<T>(&self, func: impl FnOnce(io_uring::SubmissionQueue) -> T) -> T {
            let guard = self.sq_lock.lock().expect("sq_lock poisoned");
            let sq = unsafe { self.io_uring.submission_shared() };
            let result = func(sq);
            drop(guard);
            result
        }
        pub fn with_completion<T>(&self, func: impl FnOnce(io_uring::CompletionQueue) -> T) -> T {
            let guard = self.cq_lock.lock().expect("sq_lock poisoned");
            let cq = unsafe { self.io_uring.completion_shared() };
            let result = func(cq);
            drop(guard);
            result
        }
    }

}

pub struct Completion<'a> {
    state: Arc<Mutex<CompletionState>>,
    marker: PhantomData<&'a ()>
}

impl<'a> future::Future for Completion<'a> {
    type Output = io::Result<usize>;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> task::Poll<Self::Output> {
        let mut guard = self.state.lock().expect("data lock poisoned");
        if let Some(result) = guard.result {
            if result.is_negative() {
                let err = io::Error::from_raw_os_error(-result);
                task::Poll::Ready(Err(err))
            } else {
                let val = result as usize;
                task::Poll::Ready(Ok(val))
            }
        } else {
            guard.waker = Some(cx.waker().clone());
            task::Poll::Pending
        }
    }
}

struct CompletionState {
    pub waker: Option<task::Waker>,
    pub result: Option<i32>,
}

pub struct QueueFull;
impl fmt::Debug for QueueFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "QueueFull: the io-uring submission queue is full") }
}
impl fmt::Display for QueueFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "QueueFull: the io-uring submission queue is full") }
}
impl error::Error for QueueFull {}

pub struct IoUring {
    shared: Arc<IoState>,
    reaper: Option<thread::JoinHandle<()>>,
}

impl IoUring {

    pub fn new() ->io::Result<Self> {
        Self::with_size(32)
    }

    pub fn with_size(size: u32) -> io::Result<Self> {

        let shared = Arc::new(IoState::new(io_uring::IoUring::new(size)?));
        let shared_clone = Arc::clone(&shared);

        let reaper = thread::spawn(move || {
            let submitter = shared_clone.submitter();
            loop {
                match submitter.submit_and_wait(1) {
                    Ok(..) => (),
                    Err(err) => panic!("reaper thread encountered an error: {}", err)
                }
                shared_clone.with_completion(|completion| {
                    for event in completion {
                        let data = unsafe { Arc::from_raw(event.user_data() as *mut Mutex<CompletionState>) };
                        let mut guard = data.lock().expect("data lock poisoned");
                        guard.result = Some(event.result());
                        drop(guard);
                    }
                })
            }
        });

        Ok(Self {
            shared,
            reaper: Some(reaper),
        })

    }

    pub fn read<'b>(&self, file: fs::File, buf: &'b mut [u8]) -> Result<Completion<'b>, QueueFull> {

        let state = Arc::new(Mutex::new(CompletionState {
            waker: None,
            result: None,
        }));
        let data = Arc::clone(&state);

        let op = io_uring::opcode::Read::new(
            io_uring::types::Fd(file.as_raw_fd()),
            buf.as_mut_ptr(),
            buf.len() as u32
        ).build().user_data(Arc::into_raw(data) as u64);

        self.shared.with_submission(|mut submission| {
            // SAFETY: `op` will be valid
            unsafe { submission.push(&op) }
        }).map_err(|_| QueueFull)?;

        Ok(Completion {
            state,
            marker: PhantomData
        })

    }

    fn join(&mut self) {
        self.reaper.take().unwrap().join().unwrap();
    }
    
}

impl Drop for IoUring {
    fn drop(&mut self) {
        self.join();
    }
}

#[cfg(test)]
mod tests {

    use std::fs;

    #[test]
    fn read() {
        let io = crate::IoUring::new().unwrap();
        let file = fs::File::open("src/foo.txt").unwrap();
        let mut buf = [0; 1024];
        io.read(file, &mut buf);
    }

}

