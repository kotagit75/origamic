//! Peer address store for the gossip layer.
//!
//! [`PeerStore`] maintains the set of known peers that a node actively
//! communicates with. It is safe to clone and share across threads because all
//! interior state is protected by an `Arc<RwLock<_>>`.

use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

/// A thread-safe, bounded store of peer addresses.
///
/// `PeerStore<A>` keeps a deduplicated set of at most [`DEFAULT_MAX_PEERS`]
/// addresses. The node's own address (`self_addr`) is automatically excluded
/// from the set so it is never returned as a connectable peer.
///
/// [`DEFAULT_MAX_PEERS`]: PeerStore::DEFAULT_MAX_PEERS
#[derive(Clone, Debug)]
pub struct PeerStore<A: Clone + Eq + Hash + Send + Sync + 'static> {
    peers: Arc<RwLock<HashSet<A>>>,
    updates: broadcast::Sender<A>,
    self_addr: Option<A>,
    max_peers: usize,
}

impl<A: Clone + Eq + Hash + Send + Sync + 'static> PeerStore<A> {
    /// Maximum number of peers stored when no explicit capacity is given.
    pub const DEFAULT_MAX_PEERS: usize = 128;

    /// Creates a new `PeerStore` with the default capacity of [`DEFAULT_MAX_PEERS`].
    ///
    /// Any address in `initial_peers` that matches `self_addr` is silently
    /// ignored.
    ///
    /// [`DEFAULT_MAX_PEERS`]: PeerStore::DEFAULT_MAX_PEERS
    pub fn new(self_addr: Option<A>, initial_peers: impl IntoIterator<Item = A>) -> Self {
        Self::with_capacity(self_addr, initial_peers, Self::DEFAULT_MAX_PEERS)
    }

    /// Creates a new `PeerStore` with an explicit maximum peer count.
    ///
    /// Peers equal to `self_addr` are excluded from the initial set.
    /// `max_peers` acts as a soft cap: once the store reaches this size,
    /// [`add_peer`] rejects unknown addresses until existing entries are
    /// removed.
    ///
    /// [`add_peer`]: PeerStore::add_peer
    pub fn with_capacity(
        self_addr: Option<A>,
        initial_peers: impl IntoIterator<Item = A>,
        max_peers: usize,
    ) -> Self {
        let mut set = HashSet::new();
        for peer in initial_peers {
            if Some(&peer) != self_addr.as_ref() {
                set.insert(peer);
            }
        }
        let (updates, _) = broadcast::channel(256);

        Self {
            peers: Arc::new(RwLock::new(set)),
            updates,
            self_addr,
            max_peers,
        }
    }

    /// Returns the node's own address, if one was provided at construction.
    pub fn self_addr(&self) -> Option<&A> {
        self.self_addr.as_ref()
    }

    /// Inserts a peer address into the store.
    ///
    /// Returns `true` if the address was newly inserted, or `false` if:
    /// - `addr` equals the node's own address, or
    /// - the store is already at capacity and does not already contain `addr`.
    pub fn add_peer(&self, addr: A) -> bool {
        if Some(&addr) == self.self_addr.as_ref() {
            return false;
        }

        let mut lock = self.peers.write().unwrap_or_else(|e| e.into_inner());
        if lock.len() >= self.max_peers && !lock.contains(&addr) {
            return false;
        }
        let inserted = lock.insert(addr.clone());
        drop(lock);
        if inserted {
            let _ = self.updates.send(addr);
        }
        inserted
    }

    /// Subscribes to peer addresses newly inserted after this call.
    ///
    /// Subscribe before taking a snapshot with [`get_peers`](Self::get_peers)
    /// to avoid missing updates that happen during initialization.
    pub fn subscribe(&self) -> broadcast::Receiver<A> {
        self.updates.subscribe()
    }

    /// Inserts multiple peer addresses and returns the number newly added.
    ///
    /// Internally calls [`add_peer`] for each entry; addresses that are
    /// rejected (own address, duplicates, or capacity overflow) are silently
    /// skipped.
    ///
    /// [`add_peer`]: PeerStore::add_peer
    pub fn add_peers(&self, addrs: impl IntoIterator<Item = A>) -> usize {
        let mut added = 0;
        for addr in addrs {
            if self.add_peer(addr) {
                added += 1;
            }
        }
        added
    }

    /// Removes a peer address from the store.
    ///
    /// Returns `true` if the address was present and has been removed.
    pub fn remove_peer(&self, addr: &A) -> bool {
        let mut lock = self.peers.write().unwrap_or_else(|e| e.into_inner());
        lock.remove(addr)
    }

    /// Returns all currently stored peer addresses as a `Vec`.
    ///
    /// The order of addresses is unspecified.
    pub fn get_peers(&self) -> Vec<A> {
        let lock = self.peers.read().unwrap_or_else(|e| e.into_inner());
        lock.iter().cloned().collect()
    }

    /// Randomly samples up to `limit` peer addresses.
    ///
    /// If the store contains `limit` or fewer peers, all of them are returned.
    /// This is typically used when building a Peer Exchange (PEX) payload.
    pub fn sample_peers(&self, limit: usize) -> Vec<A> {
        let peers = self.get_peers();
        if peers.len() <= limit {
            return peers;
        }
        let mut rng = rand::thread_rng();
        peers.choose_multiple(&mut rng, limit).cloned().collect()
    }

    /// Returns the current number of stored peers.
    pub fn len(&self) -> usize {
        self.peers.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Returns `true` if no peers are currently stored.
    pub fn is_empty(&self) -> bool {
        self.peers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// Returns `true` if the given address is currently in the store.
    pub fn contains(&self, addr: &A) -> bool {
        self.peers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excludes_self_and_deduplicates_initial_and_new_peers() {
        let peers = PeerStore::new(Some(1), [1, 2, 2]);

        assert_eq!(peers.len(), 1);
        assert!(peers.contains(&2));
        assert!(!peers.contains(&1));
        assert!(!peers.add_peer(1));
        assert!(!peers.add_peer(2));
        assert!(peers.add_peer(3));
    }

    #[test]
    fn capacity_allows_existing_entries_but_rejects_new_ones() {
        let peers = PeerStore::with_capacity(None, [1, 2], 2);

        assert!(!peers.add_peer(3));
        assert!(!peers.add_peer(1));
        assert!(peers.remove_peer(&1));
        assert!(peers.add_peer(3));
        assert!(!peers.remove_peer(&1));
    }

    #[test]
    fn clones_share_peer_updates_and_sampling_is_bounded() {
        let peers = PeerStore::with_capacity(None, 0..10, 10);
        let clone = peers.clone();
        let mut updates = peers.subscribe();

        assert!(clone.remove_peer(&5));
        assert!(!peers.contains(&5));
        assert_eq!(peers.sample_peers(3).len(), 3);
        assert_eq!(peers.sample_peers(0).len(), 0);
        assert_eq!(peers.sample_peers(20).len(), peers.len());

        assert!(peers.add_peer(11));
        assert_eq!(updates.try_recv().unwrap(), 11);
    }
}
