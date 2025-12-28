#![allow(dead_code)]

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use quick_cache::sync::{Cache, DefaultLifecycle};
use quick_cache::UnitWeighter;

/// A concurrent multimap with limited capacity.
#[allow(clippy::type_complexity)]
pub struct ConcurrentLimitedMultimap<K, V, S = RandomState> {
  inner: Cache<(Arc<K>, usize), Arc<V>, UnitWeighter, S>,
  keys: RwLock<HashMap<Arc<K>, Arc<(AtomicUsize, AtomicUsize)>>>,
}

impl<K, V> ConcurrentLimitedMultimap<K, V>
where
  K: Eq + Hash,
{
  /// Creates a new ConcurrentLimitedMultimap with the given maximum capacity.
  pub fn new(max_capacity: usize) -> Self {
    Self {
      inner: Cache::with(
        max_capacity,
        max_capacity as u64,
        UnitWeighter,
        RandomState::new(),
        DefaultLifecycle::default(),
      ),
      keys: RwLock::new(HashMap::new()),
    }
  }

  /// Creates a new ConcurrentLimitedMultimap which doesn't evict any items.
  pub fn unbounded() -> Self {
    Self {
      inner: Cache::with(
        usize::MAX,
        u64::MAX,
        UnitWeighter,
        RandomState::new(),
        DefaultLifecycle::default(),
      ),
      keys: RwLock::new(HashMap::new()),
    }
  }
}

impl<K, V, S> ConcurrentLimitedMultimap<K, V, S>
where
  K: Eq + Hash,
  S: BuildHasher + Clone,
{
  /// Creates a new ConcurrentLimitedMultimap with the given maximum capacity and hasher.
  pub fn with_hasher(max_capacity: usize, hasher: S) -> Self {
    Self {
      inner: Cache::with(
        max_capacity,
        max_capacity as u64,
        UnitWeighter,
        hasher,
        DefaultLifecycle::default(),
      ),
      keys: RwLock::new(HashMap::new()),
    }
  }

  /// Creates a new ConcurrentLimitedMultimap with the given hasher which doesn't evict any items.
  pub fn with_hasher_unbounded(hasher: S) -> Self {
    Self {
      inner: Cache::with(usize::MAX, u64::MAX, UnitWeighter, hasher, DefaultLifecycle::default()),
      keys: RwLock::new(HashMap::new()),
    }
  }

  /// Inserts a key-value pair into the map.
  pub fn insert(&self, key: Arc<K>, value: V) {
    let keys_read = self.keys.read().expect("inner read-write lock poisoned");
    let (_, write_index) = if let Some(indexes) = keys_read.get(&key) {
      &*indexes.clone()
    } else {
      drop(keys_read);
      let indexes = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
      self
        .keys
        .write()
        .expect("inner read-write lock poisoned")
        .insert(key.clone(), indexes.clone());
      &*indexes.clone()
    };
    self
      .inner
      .insert((key, write_index.fetch_add(1, Ordering::Relaxed)), Arc::new(value));
  }

  /// Removes a key-value pair from the map.
  pub fn remove(&self, key: Arc<K>) -> Option<V> {
    let (read_index, write_index) = &*self
      .keys
      .read()
      .expect("inner read-write lock poisoned")
      .get(&key)?
      .clone();
    loop {
      let write_index = write_index.load(Ordering::Relaxed);
      let read_index = read_index
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
          // "==", not ">=", because what about integer overflow?
          if x == write_index {
            None
          } else {
            Some(x.checked_add(1).unwrap_or(0))
          }
        })
        .ok()?;
      if let Some((_, value)) = self.inner.remove(&(key.clone(), read_index)) {
        return Some(Arc::into_inner(value).expect("can't obtain value"));
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::collections::hash_map::RandomState;

  #[test]
  fn test_insert_and_get() {
    let map = ConcurrentLimitedMultimap::new(3);
    map.insert(Arc::new("a"), 1);
    map.insert(Arc::new("b"), 2);
    map.insert(Arc::new("c"), 3);

    assert_eq!(map.remove(Arc::new("a")), Some(1));
    assert_eq!(map.remove(Arc::new("b")), Some(2));
    assert_eq!(map.remove(Arc::new("c")), Some(3));
    assert_eq!(map.remove(Arc::new("d")), None);
  }

  #[test]
  fn test_eviction() {
    let map = ConcurrentLimitedMultimap::new(3);
    map.insert(Arc::new("a"), 1);
    map.insert(Arc::new("b"), 2);
    map.insert(Arc::new("c"), 3);
    map.insert(Arc::new("d"), 4); // Should evict one entry

    assert_eq!(map.inner.len(), 3);
  }

  #[test]
  fn test_remove() {
    let map = ConcurrentLimitedMultimap::new(3);
    map.insert(Arc::new("a"), 1);
    map.insert(Arc::new("b"), 2);
    map.insert(Arc::new("c"), 3);

    assert_eq!(map.remove(Arc::new("b")), Some(2));
    assert_eq!(map.remove(Arc::new("b")), None);
    assert_eq!(map.remove(Arc::new("a")), Some(1));
    assert_eq!(map.remove(Arc::new("c")), Some(3));
  }

  #[test]
  fn test_with_hasher() {
    let hasher = RandomState::new();
    let map = ConcurrentLimitedMultimap::with_hasher(3, hasher);
    map.insert(Arc::new("a"), 1);
    map.insert(Arc::new("b"), 2);
    map.insert(Arc::new("c"), 3);

    assert_eq!(map.remove(Arc::new("a")), Some(1));
    assert_eq!(map.remove(Arc::new("b")), Some(2));
    assert_eq!(map.remove(Arc::new("c")), Some(3));
  }

  #[test]
  fn test_insert_same_key_multiple_times() {
    let map = ConcurrentLimitedMultimap::new(3);
    map.insert(Arc::new("a"), 1);
    map.insert(Arc::new("a"), 2);

    assert_eq!(map.remove(Arc::new("a")), Some(1));
  }

  #[test]
  fn test_remove_then_insert() {
    let map = ConcurrentLimitedMultimap::new(3);
    map.remove(Arc::new("a"));
    map.insert(Arc::new("a"), 1);

    assert_eq!(map.remove(Arc::new("a")), Some(1));
  }
}
