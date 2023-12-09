
# uring-fs
Features:
- Truly asynchronous file operations using io-uring.
- Supports any async runtime.
- Linux only.
- Only depends on `io_uring` and `libc`.
# Links
[crates.io](https://crates.io/crates/uring-fs)
[docs.rs](https://docs.rs/uring-fs)
# Example
```rust
let io = uring_fs::IoUring::new().unwrap(); // implements Send + Sync
let file: std::fs::File = io.open("foo.txt", uring_fs::OpenOptions::READ).await.unwrap();
//        ^^^^^^^^^^^^^ you could also open the file using the standard library
let mut buffer = [0; 1024];
let bytes_read = io.read(file, &mut buffer).await.unwrap(); // awaiting returns io::Result
```
See `IoUring` documentation for more important infos and examples.
# Notes
This library will spawn a reaper thread that waits for io-uring completions and notifies the waker.
This is nesessary for compatibility across runtimes.

