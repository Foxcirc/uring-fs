
//! # Notes
//! This library will spawn one thread that waits for io-uring completions and notifies the waker.
//! This is nesessary to remain compatible with every runtime.

use std::{fmt, error, io, thread, sync::{Arc, Mutex, atomic}, fs, os::{fd::{AsRawFd, FromRawFd}, unix::prelude::OsStrExt}, task, future, pin::Pin, marker::PhantomData, time, path};
use private_io_state::IoState;

mod private_io_state {

    use std::sync::{Mutex, atomic};

    pub struct IoState {
        io_uring: io_uring::IoUring,
        sq_lock: Mutex<()>,
        cq_lock: Mutex<()>,
        pub in_flight: atomic::AtomicUsize,
    }

    impl IoState {
        pub fn new(io_uring: io_uring::IoUring) -> Self {
            Self {
                io_uring,
                sq_lock: Mutex::new(()),
                cq_lock: Mutex::new(()),
                in_flight: atomic::AtomicUsize::new(0),
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

pub struct Completion<'a, T> {
    state: Arc<Mutex<CompletionState>>,
    func: fn(i32) -> T,
    marker: PhantomData<&'a ()>
}

impl<'a, T> future::Future for Completion<'a, T> {
    type Output = io::Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> task::Poll<Self::Output> {
        let mut guard = self.state.lock().expect("data lock poisoned");
        if let Some(result) = guard.result {
            if result.is_negative() {
                let err = io::Error::from_raw_os_error(-result);
                task::Poll::Ready(Err(err))
            } else {
                let val = (self.func)(result);
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
    pub exit_notification: bool,
}

pub struct QueueFull;
impl fmt::Debug for QueueFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "the io-uring submission queue is full") }
}
impl fmt::Display for QueueFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "the io-uring submission queue is full") }
}
impl error::Error for QueueFull {}

pub struct OpenOptions {
    pub flags: i32
}

impl OpenOptions {
    /// read only
    pub const READ: Self = Self { flags: libc::O_RDONLY };
    /// write only
    pub const WRITE: Self = Self { flags: libc::O_WRONLY };
    /// read & write
    pub const RDWR: Self = Self { flags: libc::O_RDWR };
    /// create
    pub const CREATE: Self = Self { flags: libc::O_CREAT };
    /// append
    pub const APPEND: Self = Self { flags: libc::O_APPEND };
}

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
                let exit = shared_clone.with_completion(|completion| {
                    let mut exit = false;
                    for event in completion {
                        shared_clone.in_flight.fetch_sub(1, atomic::Ordering::Relaxed);
                        let data = unsafe { Arc::from_raw(event.user_data() as *mut Mutex<CompletionState>) };
                        let mut guard = data.lock().expect("data lock poisoned");
                        guard.result = Some(event.result());
                        if let Some(waker) = guard.waker.take() { waker.wake() };
                        if guard.exit_notification { exit = true }
                        drop(guard);
                    }
                    exit
                });
                if exit { break }
            }
        });

        Ok(Self {
            shared,
            reaper: Some(reaper),
        })

    }

    fn submit<'b, T>(&self, entry: io_uring::squeue::Entry, func: fn(i32) -> T) -> io::Result<Completion<'b, T>> {

        let state = Arc::new(Mutex::new(CompletionState {
            waker: None,
            result: None,
            exit_notification: false,
        }));

        let data = Arc::clone(&state);
        let prepared_entry = entry.user_data(Arc::into_raw(data) as u64);

        self.shared.with_submission(|mut submission| {
            // SAFETY: `op` will be valid
            unsafe { submission.push(&prepared_entry) }
        }).map_err(|_| io::Error::new(io::ErrorKind::Other, QueueFull))?;

        self.shared.in_flight.fetch_add(1, atomic::Ordering::Relaxed);
        self.shared.submitter().submit()?;

        Ok(Completion {
            state,
            func,
            marker: PhantomData
        })

    }

    pub fn open<'b, P: AsRef<path::Path>>(&self, path: P, options: OpenOptions) -> io::Result<Completion<'b, fs::File>> {
        const CWD: io_uring::types::Fd = io_uring::types::Fd(-100); // represents the current working directory
        let op = io_uring::opcode::OpenAt::new(
            CWD,
            path.as_ref().as_os_str().as_bytes().as_ptr() as *const i8
        ).flags(options.flags).build();
        self.submit(op, |fd| {
            assert!(fd > 0);
            unsafe { fs::File::from_raw_fd(fd) }
        })
    }


    pub fn read<'b>(&self, file: fs::File, buf: &'b mut [u8]) -> io::Result<Completion<'b, usize>> {
        let op = io_uring::opcode::Read::new(
            io_uring::types::Fd(file.as_raw_fd()),
            buf.as_mut_ptr(),
            buf.len() as u32
        ).build();
        self.submit(op, |val| val as usize)
    }

    fn kill_reaper(&self) {

        let data = Arc::new(Mutex::new(CompletionState {
            waker: None,
            result: None,
            exit_notification: true,
        }));

        let op = io_uring::opcode::Timeout::new(
            &io_uring::types::Timespec::from(time::Duration::ZERO)
        ).build().user_data(Arc::into_raw(data) as u64);

        self.shared.with_submission(|mut submission| {
            // SAFETY: `op` will be valid
            unsafe { submission.push(&op) }
        }).unwrap();

        self.shared.in_flight.fetch_add(1, atomic::Ordering::Relaxed);

        self.shared.submitter().submit().expect("todo");

    }

    /// How many I/O operations are currently being processed.
    pub fn in_flight(&self) -> usize {
        self.shared.in_flight.load(atomic::Ordering::Relaxed)
    }
    
}

impl Drop for IoUring {
    fn drop(&mut self) {
        self.kill_reaper();
        self.reaper.take().unwrap().join().unwrap();
    }
}

/// Determines if this [`io::Error`] signals that the io-uring submission queue is full.
/// If the queue seems so be full regularely the queue size should be increased.
/// This is a possible error returned by any function that queues a new IO operation.
pub fn is_queue_full(error: io::Error) -> bool {
    error.get_ref().map(|inner| inner.downcast_ref::<QueueFull>().is_some()).unwrap_or(false)
}

#[cfg(test)]
mod tests {

    #[test]
    fn read() {

        extreme::run(async {

            let io = crate::IoUring::new().unwrap();
            let file = io.open("src/foo.txt", crate::OpenOptions::READ).unwrap().await.unwrap();

            let mut buf = [0; 1024];
            let bytes_read = io.read(file, &mut buf).unwrap().await.unwrap();
            println!("Bytes read: {}", bytes_read);

            let msg = String::from_utf8_lossy(&buf);
            println!("{}", msg);

        });
        
    }

}

