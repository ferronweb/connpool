//! # `connpool` - a concurrent, generic connection pool for Rust
//!
//! `connpool` is a high-performance, generic connection pool crate designed for Rust applications.
//! It provides a thread-safe, asynchronous pool for managing and reusing connections (or any other resource)
//! with both global and local (per-key) limits.
//!
//! ## Features
//!
//! - **Concurrent access** - uses `Arc` and `RwLock` for thread-safe operations.
//! - **Global and local limits** - enforce both global pool limits and per-key local limits using semaphores.
//! - **Asynchronous** - supports async Rust.
//! - **Automatic eviction** - automatically evicts items when the pool is full.
//! - **Flexible key/value types** - works with any key/value types that implement `Eq + Hash`.
//! - **Unbounded mode** - option to create a pool without a global limit.
//!
//! ## Usage
//!
//! ```rust
//! use connpool::Pool;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create a new pool with a global limit of 10 connections
//!     let pool = Pool::new(10);
//!
//!     // Pull a connection for a specific key
//!     let item = pool.pull("my_key").await;
//!
//!     // Use the connection
//!     if let Some(value) = item.inner() {
//!         let value: usize = *value;
//!         println!("Got value: {:?}", value);
//!     }
//!
//!     // The connection is automatically returned to the pool when `item` is dropped
//!     // Alternatively, drop the connection
//!     let _ = item.take();
//! }
//! ```
//!
//! ## Advanced usage
//!
//! - **Local limits** - set per-key limits to restrict the number of concurrent connections for specific keys.
//! - **Manual management** - use `pull_with_local_limit` for fine-grained control over local limits.
//!
//! ## License
//!
//! This project is licensed under the MIT License.

mod concurrent_limited_multimap;

use std::hash::Hash;
use std::sync::{Arc, RwLock};

use slab::Slab;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};

use self::concurrent_limited_multimap::ConcurrentLimitedMultimap;

/// A keep-alive connection pool.
pub struct Pool<K, I> {
  inner: Arc<ConcurrentLimitedMultimap<K, I, ahash::RandomState>>,
  local_limits: RwLock<Slab<Arc<Semaphore>>>,
  semaphore: Option<Semaphore>,
}

impl<K, I> Pool<K, I>
where
  K: Eq + std::hash::Hash,
{
  /// Creates a new connection pool with the given limit.
  pub fn new(capacity: usize) -> Self {
    Self {
      inner: Arc::new(ConcurrentLimitedMultimap::with_hasher(
        capacity,
        ahash::RandomState::new(),
      )),
      semaphore: Some(Semaphore::new(capacity)),
      local_limits: RwLock::new(Slab::new()),
    }
  }

  /// Creates a new connection pool with no limit.
  pub fn new_unbounded() -> Self {
    Self {
      inner: Arc::new(ConcurrentLimitedMultimap::with_hasher_unbounded(
        ahash::RandomState::new(),
      )),
      semaphore: None,
      local_limits: RwLock::new(Slab::new()),
    }
  }

  /// Sets a local limit for a key. Returns the index for the local limit.
  pub fn set_local_limit(&self, limit: usize) -> usize {
    let mut local_limits = self.local_limits.write().expect("local limits lock poisoned");
    local_limits.insert(Arc::new(Semaphore::new(limit)))
  }

  /// Pulls an item from the pool.
  /// This method waits, when the global limit is reached.
  pub async fn pull(&self, key: K) -> Item<'_, K, I> {
    self.pull_with_wait_local_limit(key, None).await
  }

  /// Attempts to pull an item from the pool (with local limit applied).
  /// This method waits, when the global limit is reached, and returns `None`, when a local limit is reached.
  pub async fn pull_with_local_limit(&self, key: K, local_limit_index: Option<usize>) -> Option<Item<'_, K, I>> {
    let local_guard = if let Some(index) = local_limit_index {
      let local_limits = self.local_limits.read().expect("local limits lock poisoned");
      if let Some(semaphore) = local_limits.get(index) {
        let semaphore = semaphore.clone();
        drop(local_limits);
        Some(semaphore.try_acquire_owned().ok()?)
      } else {
        None
      }
    } else {
      None
    };
    let guard = if let Some(semaphore) = &self.semaphore {
      Some(semaphore.acquire().await.expect("semaphore closed"))
    } else {
      None
    };

    let key = Arc::new(key);
    let inner_value = self.inner.remove(key.clone());
    Some(Item {
      pool_inner: self.inner.clone(),
      key: Some(key),
      inner: inner_value,
      _guard: guard,
      _local_guard: local_guard,
    })
  }

  /// Pulls an item from the pool (with local limit applied).
  /// This method waits, when either the global limit or a local limit is reached.
  #[allow(clippy::await_holding_lock)]
  pub async fn pull_with_wait_local_limit(&self, key: K, local_limit_index: Option<usize>) -> Item<'_, K, I> {
    let local_guard = if let Some(index) = local_limit_index {
      let local_limits = self.local_limits.read().expect("local limits lock poisoned");
      if let Some(semaphore) = local_limits.get(index) {
        let semaphore = semaphore.clone();
        drop(local_limits); // Ensure dropping the lock before awaiting
        Some(semaphore.acquire_owned().await.expect("semaphore closed"))
      } else {
        None
      }
    } else {
      None
    };
    let guard = if let Some(semaphore) = &self.semaphore {
      Some(semaphore.acquire().await.expect("semaphore closed"))
    } else {
      None
    };

    let key = Arc::new(key);
    let inner_value = self.inner.remove(key.clone());
    Item {
      pool_inner: self.inner.clone(),
      key: Some(key),
      inner: inner_value,
      _guard: guard,
      _local_guard: local_guard,
    }
  }

  /// Attempts to pull an item from the pool.
  /// This method returns `None`, when the global limit is reached.
  pub fn try_pull(&self, key: K) -> Option<Item<'_, K, I>> {
    self.try_pull_with_local_limit(key, None)
  }

  /// Attempts to pull an item from the pool (with local limit applied).
  /// This method returns `None`, when either the global limit or a local limit is reached.
  pub fn try_pull_with_local_limit(&self, key: K, local_limit_index: Option<usize>) -> Option<Item<'_, K, I>> {
    let local_guard = if let Some(index) = local_limit_index {
      let local_limits = self.local_limits.read().expect("local limits lock poisoned");
      if let Some(semaphore) = local_limits.get(index) {
        let semaphore = semaphore.clone();
        drop(local_limits);
        Some(semaphore.try_acquire_owned().ok()?)
      } else {
        None
      }
    } else {
      None
    };
    let guard = if let Some(semaphore) = &self.semaphore {
      Some(semaphore.try_acquire().ok()?)
    } else {
      None
    };

    let key = Arc::new(key);
    let inner_value = self.inner.remove(key.clone());
    Some(Item {
      pool_inner: self.inner.clone(),
      key: Some(key),
      inner: inner_value,
      _guard: guard,
      _local_guard: local_guard,
    })
  }
}

/// An item in the connection pool.
pub struct Item<'a, K: Eq + Hash, I> {
  pool_inner: Arc<ConcurrentLimitedMultimap<K, I, ahash::RandomState>>,
  key: Option<Arc<K>>,
  inner: Option<I>,
  _guard: Option<SemaphorePermit<'a>>,
  _local_guard: Option<OwnedSemaphorePermit>,
}

impl<'a, K: Eq + Hash, I> Item<'a, K, I> {
  /// Takes the inner value from the item. This will also ensure that the item won't be returned.
  pub fn take(mut self) -> Option<I> {
    self.inner.take()
  }

  /// Returns a reference to the inner value.
  pub fn inner(&self) -> &Option<I> {
    &self.inner
  }

  /// Returns a mutable reference to the inner value.
  pub fn inner_mut(&mut self) -> &mut Option<I> {
    &mut self.inner
  }
}

impl<'a, K: Eq + Hash, I> Drop for Item<'a, K, I> {
  fn drop(&mut self) {
    if let Some(inner) = self.inner.take() {
      self.pool_inner.insert(self.key.take().expect("key not set"), inner);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_pool_new() {
    let pool = Pool::<String, u32>::new(10);
    assert_eq!(pool.semaphore.as_ref().unwrap().available_permits(), 10);
  }

  #[tokio::test]
  async fn test_pool_pull_and_take() {
    let pool = Pool::<String, u32>::new(1);
    let item = pool.pull("key1".to_string()).await;
    assert!(item.take().is_none());
  }

  #[tokio::test]
  async fn test_pool_pull_and_replace() {
    let pool = Pool::<String, u32>::new(1);
    let mut item = pool.pull("key1".to_string()).await;
    *item.inner_mut() = Some(42);
    assert_eq!(item.inner(), &Some(42));
  }

  #[tokio::test]
  async fn test_pool_eviction_behavior() {
    let pool = Pool::<String, u32>::new(2);
    {
      let mut item1 = pool.pull("key1".to_string()).await;
      item1.inner_mut().replace(1);
    }
    {
      let mut item2 = pool.pull("key2".to_string()).await;
      item2.inner_mut().replace(2);
    }
    // Pull key1 again to make it recently used
    {
      let _item1 = pool.pull("key1".to_string()).await;
    }
    // Add a third item, which should evict key2 (least recently used)
    {
      let mut item3 = pool.pull("key3".to_string()).await;
      item3.inner_mut().replace(3);
    }
    // Check the number of entries
    let mut num_entries = 0;
    if pool.pull("key1".to_string()).await.inner().is_some() {
      num_entries += 1;
    }
    if pool.pull("key2".to_string()).await.inner().is_some() {
      num_entries += 1;
    }
    if pool.pull("key3".to_string()).await.inner().is_some() {
      num_entries += 1;
    }
    assert_eq!(num_entries, 2);
  }

  #[tokio::test]
  async fn test_pool_semaphore_limit() {
    let pool = Pool::<String, u32>::new(1);
    let item1 = pool.pull("key1".to_string()).await;
    let semaphore_permits = pool.semaphore.as_ref().unwrap().available_permits();
    assert_eq!(semaphore_permits, 0);
    drop(item1);
    assert_eq!(pool.semaphore.as_ref().unwrap().available_permits(), 1);
  }

  #[tokio::test]
  async fn test_set_and_get_local_limit() {
    let pool = Pool::<String, u32>::new(10);
    let index = pool.set_local_limit(2);
    let local_limits = pool.local_limits.read().expect("lock poisoned");
    assert!(local_limits.get(index).is_some());
    assert_eq!(local_limits[index].available_permits(), 2);
  }

  #[tokio::test]
  async fn test_pull_with_local_limit_success() {
    let pool = Pool::<String, u32>::new(10);
    let index = pool.set_local_limit(2);
    let item = pool.pull_with_local_limit("key1".to_string(), Some(index)).await;
    assert!(item.is_some());
  }

  #[tokio::test]
  async fn test_pull_with_local_limit_exhausted() {
    let pool = Pool::<String, u32>::new(10);
    let index = pool.set_local_limit(1);
    // Acquire the only permit
    let _item1 = pool.pull_with_local_limit("key1".to_string(), Some(index)).await;
    // Try to acquire again, should return None
    let item2 = pool.pull_with_local_limit("key2".to_string(), Some(index)).await;
    assert!(item2.is_none());
  }

  #[tokio::test]
  async fn test_pull_with_local_limit_after_release() {
    let pool = Pool::<String, u32>::new(10);
    let index = pool.set_local_limit(1);
    // Acquire the only permit
    let item1 = pool.pull_with_local_limit("key1".to_string(), Some(index)).await;
    assert!(item1.is_some());
    // Release the permit
    drop(item1);
    // Now we should be able to acquire again
    let item2 = pool.pull_with_local_limit("key2".to_string(), Some(index)).await;
    assert!(item2.is_some());
  }

  #[tokio::test]
  async fn test_pull_with_invalid_local_limit_index() {
    let pool = Pool::<String, u32>::new(10);
    // Try to pull with an invalid index
    let item = pool.pull_with_local_limit("key1".to_string(), Some(999)).await;
    assert!(item.is_some()); // Should succeed (no local limit applied)
  }
}
