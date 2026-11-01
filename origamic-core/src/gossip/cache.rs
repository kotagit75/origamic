//! Message-ID deduplication cache for the gossip engine.
//!
//! Each gossip message carries a unique string ID.  [`SeenCache`] records
//! which IDs have already been processed so that the node can drop duplicate
//! messages that arrive over multiple paths – a fundamental requirement of any
//! epidemic broadcast protocol.

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

const DEFAULT_SEEN_CACHE_TTL_SECS: u64 = 60;
const DEFAULT_SEEN_CACHE_CAPACITY: usize = 10_000;

/// A bounded, time-to-live (TTL) cache of previously seen gossip message IDs.
///
/// The cache evicts entries that are older than `ttl` **lazily** on every
/// [`check_and_insert`](SeenCache::check_and_insert) call.  When the cache
/// reaches its maximum capacity the oldest entry is evicted immediately to
/// make room for the new one.
pub struct SeenCache {
    /// The set of message IDs currently considered "seen".
    seen: HashSet<String>,
    /// Insertion-ordered queue used for TTL eviction and capacity enforcement.
    order: VecDeque<(String, Instant)>,
    /// How long a message ID is retained before it is eligible for eviction.
    ttl: Duration,
    /// Maximum number of entries held in memory at any given time.
    max_capacity: usize,
}

impl SeenCache {
    /// Create a new cache with an explicit `ttl` and `max_capacity`.
    pub fn new(ttl: Duration, max_capacity: usize) -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            ttl,
            max_capacity,
        }
    }

    /// Create a cache with sensible defaults (60-second TTL, 10 000 entries).
    pub fn with_default() -> Self {
        Self::new(
            Duration::from_secs(DEFAULT_SEEN_CACHE_TTL_SECS),
            DEFAULT_SEEN_CACHE_CAPACITY,
        )
    }

    /// Check whether `id` has been seen before, and record it if not.
    ///
    /// Returns `true` if `id` was **not** in the cache (i.e. this is the first
    /// time the message has been observed) and inserts it.
    /// Returns `false` if `id` was already present (duplicate message).
    ///
    /// Expired entries are evicted before the lookup is performed.
    pub fn check_and_insert(&mut self, id: &str) -> bool {
        self.cleanup();

        if self.seen.contains(id) {
            return false;
        }

        // Evict the oldest entry when the cache is full.
        if self.seen.len() >= self.max_capacity
            && let Some((old_id, _)) = self.order.pop_front()
        {
            self.seen.remove(&old_id);
        }

        let now = Instant::now();
        self.seen.insert(id.to_string());
        self.order.push_back((id.to_string(), now));
        true
    }

    /// Return `true` if `id` is currently in the cache (without inserting).
    pub fn contains(&self, id: &str) -> bool {
        self.seen.contains(id)
    }

    /// Remove all entries whose insertion time exceeds the configured TTL.
    fn cleanup(&mut self) {
        let now = Instant::now();
        while let Some((_, timestamp)) = self.order.front() {
            if now.duration_since(*timestamp) > self.ttl {
                if let Some((old_id, _)) = self.order.pop_front() {
                    self.seen.remove(&old_id);
                }
            } else {
                break;
            }
        }
    }
}
