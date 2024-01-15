
# uring-fs
`uring-fs` is a library that allows you to safely read files using async-await in rust
without needing a threadpool. It uses `io_uring`.

# features
- Truly asynchronous and safe file operations using io-uring.
- Usable with any async runtime.
- Supports: open, stat, read, write.
- Depends on [io_uring](https://crates.io/crates/io_uring) and [libc](https://crates.io/crates/libc).
  So it doesn't run on any platforms that don't support these.
See [`IoUring`] documentation for more important infos and examples.

# example
```rust
let io = IoUring::new()?; // create a new io-uring context
let mut file: fs::File = io.open("src/foo.txt", Flags::RDONLY).await?;
//            ^^^^ you could also use File::open or OpenOptions
let info = io.stat("src/foo.txt").await?;
// there is also read_all, which doesn't require querying the file size
// using stat, however this is a bit more efficient since we only allocate the data buffer once
let content = io.read(&file, info.size()).await?;
println!("we read {} bytes", content.len());
// btw you can also seek the file using io::Seek
file.seek(SeekFrom::Current(-10));
```
# Notes
This library will spawn a reaper thread that waits for io-uring completions and notifies the apropriate future.
Still, this is light weight in comparison to using a thread pool.

This crate doesn't have the same soundness problems as `rio`, however it isn't as powerfull and doesn't provide a lot
of fine-grained controll.
If you want to use `rio`, remember to, at some point, enable the `no_metrics` feature!
Otherwise there will be a ~100ms initial allocation of space used to store the performance information.

Please also note that the code for this library is not tested as much as I'd like to and might
contain some subtle undefined behaviour. This library is kept small and hackable so
you can always verify and fork it yourself.

The whole code is contained inside `lib.rs`.
