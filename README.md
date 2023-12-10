
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
let io = uring_fs::IoUring::new()?; // IoUring implements Send + Sync
let file = unsafe { io.open("foo.txt", uring_fs::OpenOptions::READ) }.await?;
//         ^^^^^^ see IoUring docs for why this is unsafe
let mut buffer = [0; 1024];
let bytes_read = unsafe { io.read(&file, &mut buffer) }.await?; // awaiting returns io::Result
```
See `IoUring` documentation for more important infos and examples.

# Note
Please note that the code for this library is not tested very well and might
contain some subtle undefined behaviour. This library isn't very big and you can always
verify and fork it yourself.
