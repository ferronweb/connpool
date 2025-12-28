# `connpool` - a concurrent, generic connection pool for Rust

`connpool` is a high-performance, generic connection pool crate designed for Rust applications.
It provides a thread-safe, asynchronous pool for managing and reusing connections (or any other resource)
with both global and local (per-key) limits.

## Features

- **Concurrent access** - uses `Arc` and `RwLock` for thread-safe operations.
- **Global and local limits** - enforce both global pool limits and per-key local limits using semaphores.
- **Asynchronous** - supports async Rust.
- **Automatic eviction** - automatically evicts items when the pool is full.
- **Flexible key/value types** - works with any key/value types that implement `Eq + Hash`.
- **Unbounded mode** - option to create a pool without a global limit.

## Usage

```rust
use connpool::Pool;

#[tokio::main]
async fn main() {
    // Create a new pool with a global limit of 10 connections
    let pool = Pool::new(10);

    // Pull a connection for a specific key
    let item = pool.pull("my_key").await;

    // Use the connection
    if let Some(value) = item.inner() {
        let value: usize = *value;
        println!("Got value: {:?}", value);
    }

    // The connection is automatically returned to the pool when `item` is dropped
    // Alternatively, drop the connection
    let _ = item.take();
}
```

## Advanced usage

- **Local limits** - set per-key limits to restrict the number of concurrent connections for specific keys.
- **Manual management** - use `pull_with_local_limit` for fine-grained control over local limits.

## License

This project is licensed under the MIT License.
