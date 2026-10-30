use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub struct PeerStore<A: Clone + Eq + Hash + Send + Sync + 'static> {
    peers: Arc<RwLock<HashSet<A>>>,
    self_addr: Option<A>,
    max_peers: usize,
}

impl<A: Clone + Eq + Hash + Send + Sync + 'static> PeerStore<A> {
    pub const DEFAULT_MAX_PEERS: usize = 128;

    pub fn new(self_addr: Option<A>, initial_peers: impl IntoIterator<Item = A>) -> Self {
        Self::with_capacity(self_addr, initial_peers, Self::DEFAULT_MAX_PEERS)
    }

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
        Self {
            peers: Arc::new(RwLock::new(set)),
            self_addr,
            max_peers,
        }
    }

    pub fn self_addr(&self) -> Option<&A> {
        self.self_addr.as_ref()
    }

    /// ピアを追加。自身のアドレスや重複は弾く。新規追加された場合は true。
    pub fn add_peer(&self, addr: A) -> bool {
        if Some(&addr) == self.self_addr.as_ref() {
            return false;
        }

        let mut lock = self.peers.write().unwrap_or_else(|e| e.into_inner());
        if lock.len() >= self.max_peers && !lock.contains(&addr) {
            return false;
        }
        lock.insert(addr)
    }

    /// 複数ピアを一括追加し、新規追加された件数を返す。
    pub fn add_peers(&self, addrs: impl IntoIterator<Item = A>) -> usize {
        let mut added = 0;
        for addr in addrs {
            if self.add_peer(addr) {
                added += 1;
            }
        }
        added
    }

    /// ピアを削除。存在して削除された場合は true。
    pub fn remove_peer(&self, addr: &A) -> bool {
        let mut lock = self.peers.write().unwrap_or_else(|e| e.into_inner());
        lock.remove(addr)
    }

    /// 現在保持している全ピアのリストを取得。
    pub fn get_peers(&self) -> Vec<A> {
        let lock = self.peers.read().unwrap_or_else(|e| e.into_inner());
        lock.iter().cloned().collect()
    }

    /// PEXなどで共有するために最大 `limit` 件のピアをランダム抽出。
    pub fn sample_peers(&self, limit: usize) -> Vec<A> {
        let peers = self.get_peers();
        if peers.len() <= limit {
            return peers;
        }
        let mut rng = rand::thread_rng();
        peers.choose_multiple(&mut rng, limit).cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.peers.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    pub fn contains(&self, addr: &A) -> bool {
        self.peers.read().unwrap_or_else(|e| e.into_inner()).contains(addr)
    }
}
