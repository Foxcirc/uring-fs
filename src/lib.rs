
//! # Features
//! - Truly asynchronous file operations using io-uring.
//! - Supports any async runtime.
//! - Linux only.
//! - Only depends on [io_uring](https://crates.io/crates/io_uring) and [libc](https://crates.io/crates/libc).
//!
//! # Example
//! ```no_run
//! # async fn foo() -> std::io::Result<()> {
//! let io = uring_fs::IoUring::new()?; // IoUring implements Send + Sync
//! let file = unsafe { io.open("foo.txt", uring_fs::OpenOptions::READ) }.await?;
//! //         ^^^^^^ see IoUring docs for why this is unsafe
//! let data = io.read_all(&file).await?; // awaiting returns io::Result
//! # Ok(())
//! # };
//! ```
//! See [`IoUring`] documentation for more important infos and examples.
//!
//! # Notes
//! This library will spawn a reaper thread that waits for io-uring completions and notifies the waker.
//! This is nesessary for compatibility across runtimes.
//!
//! Please also note that the code for this library is not tested very well and might
//! contain some subtle undefined behaviour. This library isn't very big and you can always
//! verify and fork it yourself.

use std::{io, thread, sync::{Arc, Mutex, atomic}, os::{fd::{AsRawFd, FromRawFd, RawFd}, unix::prelude::OsStrExt}, task, future, pin::Pin, marker::PhantomData, time, path, mem};
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

const CWD: io_uring::types::Fd = io_uring::types::Fd(libc::AT_FDCWD); // represents the current working directory

/// A future that will complete once a file operation is complete.
/// Dropping a completion will not cancel the associated I/O operation which will still be
/// executed by the kernel.
/// # Example
/// ```ignore
/// let my_completion = ...;
/// let value = my_completion.await?;
/// ```
pub struct Completion<'a, T> {
    state: Arc<Mutex<CompletionKind>>,
    pub(crate) is_done: CompletionIsDone,
    func: fn(i32) -> T,
    marker: PhantomData<&'a ()>
}

impl<'a, T> future::Future for Completion<'a, T> {
    type Output = io::Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> task::Poll<Self::Output> {
        let mut guard = self.state.lock().expect("data lock poisoned");
        match &mut *guard {
            CompletionKind::Regular { waker, result } => {
                if let Some(code) = result {
                    if code.is_negative() {
                        let error = io::Error::from_raw_os_error(-*code);
                        drop(guard);
                        self.is_done = CompletionIsDone::Done;
                        task::Poll::Ready(Err(error))
                    } else {
                        let val = (self.func)(*code);
                        drop(guard);
                        self.is_done = CompletionIsDone::Done;
                        task::Poll::Ready(Ok(val))
                    }
                } else {
                    *waker = Some(cx.waker().clone());
                    task::Poll::Pending
                }
            },
            CompletionKind::WithError { error } => {
                let value = error.take().unwrap();
                drop(guard);
                self.is_done = CompletionIsDone::Done;
                task::Poll::Ready(Err(value))
            },
            CompletionKind::ExitNotification => {
                unreachable!();
            }
        }
    }
}

impl<'a, T> Drop for Completion<'a, T> {
    fn drop(&mut self) {
        if matches!(self.is_done, CompletionIsDone::NotDone) {
            // sadly we can't put a #[track_caller] on the drop function
            panic!("uring_fs: completion dropped without being awaited")
        }
    }
}

enum CompletionIsDone {
    NotDone,
    Done,
    ExitNotification
}

enum CompletionKind {
    Regular { waker: Option<task::Waker>, result: Option<i32> },
    WithError { error: Option<io::Error> },
    ExitNotification
}

impl CompletionKind {
    fn regular() -> Self {
        Self::Regular { waker: None, result: None }
    }
    fn with_error(error: io::Error) -> Self {
        Self::WithError { error: Some(error) }
    }
    fn exit_notification() -> Self {
        Self::ExitNotification
    }
}

/// In what mode to open a file.
///
/// It is not possible use [`std::fs::OpenOptions`] because it doesn't support retreiving the raw `flags`
/// that would be passed to a call to `openat`. (Please tell me if it does and I just didn't see it!)
/// # Example
/// ```no_run
/// # use uring_fs::OpenOptions;
/// let read_options = OpenOptions::READ;
/// let append_options = OpenOptions::APPEND;
/// let special_options = OpenOptions { flags: libc::O_RDWR | libc::O_TMPFILE };
/// ```
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

/// Represents an open file.
///
/// You can convert from/to a [`std::fs::File`] by converting to a raw fd and back.
pub struct File {
    fd: RawFd
}

impl FromRawFd for File {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self { fd }
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

/// Information about a file.
pub struct Stat {
    pub inner: libc::statx,
}

impl Stat {
    fn new(inner: libc::statx) -> Self {
        Self { inner }
    }
    /// The file size.
    pub fn size(&self) -> u64 {
        self.inner.stx_size
    }
}

/// The main io-uring context. Used to perform I/O operations and obtain their completions.
///
/// - [`new`](IoUring::new): default queue size of `8`
/// - [`new_with_size`](IoUring::new_with_size): custom queue size
///
/// Every request is immediatly submitted after it is created, which makes it really hard to
/// overflow the submission queue.
/// However it is possible to check using the [`is_queue_full`] function.
///
/// # Important caveats
/// - Trying to drop a [`Completion`] without `awaiting` it first, will result in a panic.
/// - Letting go of a [`Completion`] without it's destructor running (e.g. through `mem::forget`) may result in a data race.
///
/// For this reason some functions that create a completion are marked as unsafe.
///
/// # Example
/// ```no_run
/// # async fn foo() -> std::io::Result<()> {
/// use uring_fs::{IoUring, OpenOptions};
/// let io = IoUring::new()?;
/// let file = unsafe { io.open("foo.txt", OpenOptions::READ) }.await?;
/// let mut buf = [0; 1024];
/// let bytes_read = unsafe { io.read_at(&file, &mut buf, 0u64) }.await?;
/// # Ok(())
/// # };
/// ```
pub struct IoUring {
    shared: Arc<IoState>,
    reaper: Option<thread::JoinHandle<()>>,
}

impl IoUring {

    /// Starts a new `io_uring` system.
    ///
    /// Uses a default submission queue size of `8`.
    /// For changing this size see [`new_with_size`](IoUring::new_with_size).
    pub fn new() -> io::Result<Self> {
        Self::new_with_size(8)
    }

    /// Starts a new `io_uring` system with a specified submission queue size.
    pub fn new_with_size(size: u32) -> io::Result<Self> {

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

                        // it is safe to construct and later drop the Arc because for every
                        // submission request we will get exactely one event here
                        // this would break and produce undefined behaviour fore some opcodes!
                        let data = unsafe { Arc::from_raw(event.user_data() as *mut Mutex<CompletionKind>) };
                        let mut guard = data.lock().expect("data lock poisoned");
                        match &mut *guard {
                            CompletionKind::Regular { waker, result } => {
                                *result = Some(event.result());
                                if let Some(waker) = waker.take() { waker.wake() };
                            },
                            CompletionKind::WithError { .. } => (),
                            CompletionKind::ExitNotification => exit = true
                        }

                        drop(guard);
                        // the completion's data will also be dropped and cleaned up here
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

    fn submit<'b, T>(&self, entry: io_uring::squeue::Entry, kind: CompletionKind, func: fn(i32) -> T) -> Completion<'b, T> {

        let mut state = Arc::new(Mutex::new(kind));
        let prepared = entry.user_data(Arc::into_raw(Arc::clone(&state)) as u64);

        let result = self.shared.with_submission(|mut submission| {
            // SAFETY: `op` will be valid
            unsafe { submission.push(&prepared) }
        });
        if let Err(err) = result {
            // make sure to cleanup the completion's data
            unsafe { Arc::from_raw(prepared.get_user_data() as *mut Arc<Mutex<CompletionKind>>) };
            state = Arc::new(Mutex::new(CompletionKind::with_error(io::Error::new(io::ErrorKind::Other, err))));
        }

        self.shared.in_flight.fetch_add(1, atomic::Ordering::Relaxed);
        let result = self.shared.submitter().submit();
        if let Err(err) = result {
            // make sure to cleanup the completion's data
            unsafe { Arc::from_raw(prepared.get_user_data() as *mut Arc<Mutex<CompletionKind>>) };
            state = Arc::new(Mutex::new(CompletionKind::with_error(err)));
        }

        Completion {
            state,
            is_done: CompletionIsDone::NotDone,
            func,
            marker: PhantomData
        }

    }

    /// Opens a file. Returns a [`File`].
    ///
    /// Opening files can sometimes block if they need to be created, emptied or more. This
    /// function allows doing this in an asynchronous manner.
    /// For more notes see [`OpenOptions`].
    ///
    /// # Safety
    /// You must ensure that the returned `Completion` is never dropped without it's destructor
    /// running.
    pub unsafe fn open<'b, P: AsRef<path::Path> + 'b>(&self, path: P, options: OpenOptions) -> Completion<'b, File> {
        let op = io_uring::opcode::OpenAt::new(
            CWD,
            path.as_ref().as_os_str().as_bytes().as_ptr() as *const i8
        ).flags(options.flags).build();
        self.submit(op, CompletionKind::regular(), |fd| {
            assert!(fd > 0);
            unsafe { File::from_raw_fd(fd) }
        })
    }

    /// Reads data from a file at an offset. Returns how many bytes were read.
    ///
    /// If you wanna safely read a whole file you should use
    /// - [`read_all`](IoUring::read_all)
    /// - [`read_to_string`](IoUring::read_to_string)
    ///
    /// # Safety
    /// You must ensure that the returned `Completion` is never dropped without it's destructor
    /// running.
    pub unsafe fn read_at<'b>(&self, file: &File, buf: &'b mut [u8], offset: u64) -> Completion<'b, usize> {
        let op = io_uring::opcode::Read::new(
            io_uring::types::Fd(file.as_raw_fd()),
            buf.as_mut_ptr(),
            buf.len() as u32
        ).offset(offset).build();
        self.submit(op, CompletionKind::regular(), |val| val as usize)
    }

    /// Reads the whole file.
    pub async fn read_all<'b>(&self, file: &File) -> io::Result<Vec<u8>> {

        let mut buf = Vec::new();
        let mut to_read = 1024;
        let mut total_bytes_read = 0;
        loop {
            let len = buf.len();
            buf.resize(len + to_read, 0);
            let bytes_read = unsafe { self.read_at(&file, &mut buf[len..len + to_read], total_bytes_read as u64) }.await?;
            buf.truncate(len + bytes_read);
            to_read *= 2;
            total_bytes_read += bytes_read;
            if bytes_read == 0 { break }
        }

        Ok(buf)

    }

    /// Reads the whole file as a string.
    ///
    /// Returns error if the file was not valid utf8.
    /// You can check for it using the [`is_not_utf8`] function.
    pub async fn read_to_string<'b>(&self, file: &File) -> io::Result<String> {

        let buf = self.read_all(file).await?;
        String::from_utf8(buf).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))

    }

    /// Writes data to a file at an offset. Returns how many bytes were written.
    ///
    /// # Safety
    /// You must ensure that the returned `Completion` is never dropped without it's destructor
    /// running.
    pub unsafe fn write_at<'b>(&self, file: &File, buf: &'b [u8], offset: u64) -> Completion<'b, usize> {
        let op = io_uring::opcode::Write::new(
            io_uring::types::Fd(file.as_raw_fd()),
            buf.as_ptr(),
            buf.len() as u32
        ).offset(offset).build();
        self.submit(op, CompletionKind::regular(), |val| val as usize)
    }

    /// Writes all data to a file.
    pub async fn write_all<'b>(&self, file: &File, mut buf: &'b [u8]) -> io::Result<()> {
        let mut total_bytes_written = 0;
        loop {
            let bytes_written = unsafe { self.write_at(&file, buf, total_bytes_written as u64) }.await?;
            total_bytes_written += bytes_written;
            buf = &buf[bytes_written..];
            if buf.len() == 0 { break }
        }
        Ok(())
    }

    /// Stats a file.
    ///
    /// # Safety
    /// You must ensure that the returned future is never dropped without it's destructor
    /// running.
    pub async unsafe fn stat<'b, P: AsRef<path::Path> + 'b>(&self, path: P) -> io::Result<Stat> {
        let mut statx: mem::MaybeUninit<libc::statx> = mem::MaybeUninit::uninit();
        let op = io_uring::opcode::Statx::new(
            CWD,
            path.as_ref().as_os_str().as_bytes().as_ptr() as *const i8,
            statx.as_mut_ptr() as *mut io_uring::types::statx,
        ).build();
        self.submit(op, CompletionKind::regular(), |_| ()).await?;
        Ok(Stat::new(unsafe { statx.assume_init() }))
    }

    fn kill_reaper(&self) {

        let op = io_uring::opcode::Timeout::new(
            &io_uring::types::Timespec::from(time::Duration::ZERO)
        ).build();

        let mut completion = self.submit(op, CompletionKind::exit_notification(), |_| unreachable!());
        completion.is_done = CompletionIsDone::ExitNotification; // to make the check on drop happy

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
///
/// This is a possible error returned by any function that queues a new IO operation.
///
/// # Example
/// ```ignore
/// match unsafe { io.read_at(&file, &mut buf, 0) }.await {
///     Ok(..) => (),
///     Err(ref err) if is_queue_full(err) => ...,
///     Err(other) => ...,
/// };
/// ```
pub fn is_queue_full(error: &io::Error) -> bool {
    error.get_ref().map(|inner| inner.downcast_ref::<io_uring::squeue::PushError>().is_some()).unwrap_or(false)
}

/// Determines if this [`io::Error`] signals that the io-uring submission queue is full.
///
/// This is a possible error returned by [`IoUring::read_to_string`].
pub fn is_not_utf8(error: &io::Error) -> bool {
    error.get_ref().map(|inner| inner.downcast_ref::<std::string::FromUtf8Error>().is_some()).unwrap_or(false)
}

#[cfg(test)]
mod tests {

    #[test]
    fn read() {

        extreme::run(async {

            let io = crate::IoUring::new().unwrap();

            let file = unsafe { io.open("src/foo.txt", crate::OpenOptions::RDWR) }.await.unwrap(); // todo: absolute path doesnt work!!! also: copy the path string so this can be safe

            let info = unsafe { io.stat("src/foo.txt") }.await.unwrap();
            println!("File size: {}", info.size());

            let buf = io.read_all(&file).await.unwrap();
            let bytes_read = buf.len();
            println!("Bytes read: {}", bytes_read);

            let bytes_written = unsafe { io.write_at(&file, &buf, 0) }.await.unwrap();
            println!("Bytes written: {}", bytes_written);

            io.write_all(&file, b"Hello world, this is awesome!").await.unwrap();

            assert!(bytes_read > 0);
            assert!(bytes_read == info.size() as usize);
            assert!(bytes_read == bytes_written);

        });
        
    }

}

