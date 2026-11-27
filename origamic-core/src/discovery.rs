//! Peer discovery interface, separate from the transport [`Factory`](crate::factory::Factory).

use tokio::sync::broadcast;

/// Exposes known peer addresses and notifications when new peers are learned.
///
/// A transport factory determines how to connect to an address; this trait
/// independently provides addresses that may be used for those connections.
/// Subscribe before taking [`known_peers`](Self::known_peers)' snapshot so
/// additions racing with initialization are either in the snapshot or the
/// subscription.
pub trait PeerDiscovery: Send + Sync {
    /// Address type accepted by the corresponding transport factory.
    type Address: Clone + Send + Sync + 'static;

    /// Returns the currently known peers.
    fn known_peers(&self) -> Vec<Self::Address>;

    /// Subscribes to peers newly learned after subscription.
    fn subscribe_peers(&self) -> broadcast::Receiver<Self::Address>;
}

impl<T: ?Sized + PeerDiscovery> PeerDiscovery for std::sync::Arc<T> {
    type Address = T::Address;

    fn known_peers(&self) -> Vec<Self::Address> {
        (**self).known_peers()
    }

    fn subscribe_peers(&self) -> broadcast::Receiver<Self::Address> {
        (**self).subscribe_peers()
    }
}
