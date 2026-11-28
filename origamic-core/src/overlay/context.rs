//! Overlay network context – the primary user-facing API.
//!
//! [`OverlayContext`] combines a gossip node with a stream router. Incoming
//! gossip is forwarded by a dedicated I/O task to a router task, which owns
//! the virtual-stream state and creates streams for accepted connections.

use std::{future::Future, pin::Pin, sync::Arc};

use tokio::sync::{Mutex, mpsc, oneshot};
use uuid::Uuid;

use crate::{
    discovery::PeerDiscovery,
    error::SendError,
    factory::{Factory, Listener},
    gossip::node::{GossipNode, GossipNodeHandle, OutgoingLocalMessage, process_inbound_stream},
    overlay::{
        address::{Keypair, OverlayAddress},
        io::IoDriver,
        listener::OverlayListener,
        router::{RouterCommand, RouterTask},
        stream::OverlayStream,
    },
};

/// A type-erased interface for adding peers to the lower transport peer store.
pub trait AnyPeerAdder: Send + Sync {
    /// Attempts to add a peer address to the store.
    fn add_any(&self, peer: Box<dyn std::any::Any + Send>) -> bool;
}

struct TypedPeerAdder<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static> {
    store: crate::gossip::peers::PeerStore<A>,
}

impl<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static> AnyPeerAdder for TypedPeerAdder<A> {
    fn add_any(&self, peer: Box<dyn std::any::Any + Send>) -> bool {
        peer.downcast::<A>()
            .is_ok_and(|address| self.store.add_peer(*address))
    }
}

/// Main handle for an overlay network node.
///
/// The gossip engine forwards messages to a dedicated router task, which
/// demultiplexes stream frames and manages active virtual streams. The context
/// can itself be used as a [`Factory`] for a higher overlay.
#[derive(Clone)]
pub struct OverlayContext {
    network_id: Uuid,
    keypair: Arc<Keypair>,
    peer_adder: Arc<dyn AnyPeerAdder>,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
    router_tx: mpsc::UnboundedSender<RouterCommand>,
    inbound_stream_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>>,
    overlay_peers: crate::gossip::peers::PeerStore<OverlayAddress>,
}

impl std::fmt::Debug for OverlayContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverlayContext")
            .field("network_id", &self.network_id)
            .field("address", &self.address())
            .finish()
    }
}

const DEFAULT_GOSSIP_TTL: u8 = 7;
const DEFAULT_GOSSIP_BUFFER_SIZE: usize = 256;

impl OverlayContext {
    /// Creates a node on a fresh random network and starts its lower listener.
    pub async fn try_new<F: Factory>(
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
    ) -> std::io::Result<Self> {
        Self::try_with_network(Uuid::new_v4(), lower_factory, lower_peers).await
    }

    /// Creates a node that joins an existing network with a generated identity.
    pub async fn try_with_network<F: Factory>(
        network_id: Uuid,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
    ) -> std::io::Result<Self> {
        Self::try_with_network_and_keypair(
            network_id,
            Keypair::generate(),
            lower_factory,
            lower_peers,
        )
        .await
    }

    /// Creates a node with an explicit identity and no advertised lower address.
    pub async fn try_with_network_and_keypair<F: Factory>(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
    ) -> std::io::Result<Self> {
        Self::try_with_options(network_id, keypair, lower_factory, lower_peers, None).await
    }

    /// Creates a node with an advertised address and a generated identity.
    pub async fn try_with_advertise<F: Factory>(
        network_id: Uuid,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
        advertise_addr: F::Address,
    ) -> std::io::Result<Self> {
        Self::try_with_options(
            network_id,
            Keypair::generate(),
            lower_factory,
            lower_peers,
            Some(advertise_addr),
        )
        .await
    }

    /// Creates an overlay and starts its router, gossip I/O driver, and listener.
    pub async fn try_with_options<F: Factory>(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
        advertise_addr: Option<F::Address>,
    ) -> std::io::Result<Self> {
        Self::try_with_options_and_discovery(
            network_id,
            keypair,
            lower_factory,
            lower_peers,
            advertise_addr,
            None,
        )
        .await
    }

    /// Creates an overlay with peers supplied by an independent discovery provider.
    ///
    /// Subscribes before taking the provider's initial snapshot, then adds both
    /// initial and newly discovered addresses to the gossip peer table. The
    /// provider address type must match the lower factory address type.
    pub async fn try_with_options_and_discovery<F: Factory>(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
        advertise_addr: Option<F::Address>,
        peer_discovery: Option<Arc<dyn PeerDiscovery<Address = F::Address>>>,
    ) -> std::io::Result<Self> {
        let discovery_subscription = peer_discovery
            .as_ref()
            .map(|discovery| (discovery.clone(), discovery.subscribe_peers()));

        let GossipNodeHandle {
            address: _,
            network_id: _,
            local_tx,
            upper_rx,
            peer_tx,
            peer_store,
            overlay_peer_store,
        } = GossipNode::spawn(
            network_id,
            keypair.clone(),
            lower_factory.clone(),
            lower_peers,
            advertise_addr.clone(),
            DEFAULT_GOSSIP_TTL,
            DEFAULT_GOSSIP_BUFFER_SIZE,
        );

        if let Some((discovery, mut updates)) = discovery_subscription {
            peer_store.add_peers(discovery.known_peers());
            let peer_store = peer_store.clone();
            tokio::spawn(async move {
                loop {
                    match updates.recv().await {
                        Ok(peer) => {
                            peer_store.add_peer(peer);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            peer_store.add_peers(discovery.known_peers());
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        }

        let address = keypair.address;
        let (inbound_tx, inbound_rx) = mpsc::channel(128);
        let (router_tx, router_rx) = mpsc::unbounded_channel();
        RouterTask::new(
            address,
            router_rx,
            router_tx.clone(),
            local_tx.clone(),
            inbound_tx,
        )
        .spawn();
        IoDriver::new(upper_rx, router_tx.clone()).spawn();

        let mut listener = lower_factory.listen(advertise_addr).await?;
        let listener_peer_store = peer_store.clone();
        tokio::spawn(async move {
            while let Ok((stream, peer_addr)) = listener.accept().await {
                if let Some(peer_addr) = peer_addr {
                    listener_peer_store.add_peer(peer_addr);
                }
                let peer_tx = peer_tx.clone();
                tokio::spawn(async move {
                    let _ = process_inbound_stream(stream, peer_tx).await;
                });
            }
        });

        Ok(Self {
            network_id,
            keypair: Arc::new(keypair),
            peer_adder: Arc::new(TypedPeerAdder { store: peer_store }),
            local_tx,
            router_tx,
            inbound_stream_rx: Arc::new(Mutex::new(inbound_rx)),
            overlay_peers: overlay_peer_store,
        })
    }

    /// Adds a lower-transport peer to the gossip peer table.
    pub fn add_peer<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static>(
        &self,
        addr: A,
    ) -> bool {
        self.peer_adder.add_any(Box::new(addr))
    }

    /// Returns the UUID identifying this overlay network.
    pub fn network_id(&self) -> Uuid {
        self.network_id
    }

    /// Returns this node's public overlay address.
    pub fn address(&self) -> OverlayAddress {
        self.keypair.address
    }

    /// Creates a listener for incoming virtual streams.
    pub fn listen(&self) -> OverlayListener {
        OverlayListener::new(self.inbound_stream_rx.clone(), self.address())
    }

    /// Returns the shared receiver used by [`listen`](Self::listen).
    pub fn inbound_stream_rx(&self) -> Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>> {
        self.inbound_stream_rx.clone()
    }

    /// Broadcasts an application payload to the overlay network.
    pub async fn broadcast(&self, payload: Vec<u8>) -> Result<(), SendError> {
        self.local_tx
            .send(OutgoingLocalMessage::broadcast(payload))
            .await
            .map_err(|_| SendError::ChannelClosed)
    }

    /// Sends an application payload to a specific overlay peer.
    pub async fn send_to(
        &self,
        recipient: OverlayAddress,
        payload: Vec<u8>,
    ) -> Result<(), SendError> {
        self.local_tx
            .send(OutgoingLocalMessage::to(recipient, payload))
            .await
            .map_err(|_| SendError::ChannelClosed)
    }
}

impl PeerDiscovery for OverlayContext {
    type Address = OverlayAddress;

    fn known_peers(&self) -> Vec<Self::Address> {
        self.overlay_peers.get_peers()
    }

    fn subscribe_peers(&self) -> tokio::sync::broadcast::Receiver<Self::Address> {
        self.overlay_peers.subscribe()
    }
}

impl Factory for OverlayContext {
    type Stream = OverlayStream;
    type Listener = OverlayListener;
    type Address = OverlayAddress;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        let (respond_to, response) = oneshot::channel();
        let command = RouterCommand::Connect {
            target: addr,
            respond_to,
        };
        let result = self.router_tx.send(command);

        Box::pin(async move {
            result.map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "overlay router is closed")
            })?;
            response.await.map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "overlay router is closed")
            })
        })
    }

    fn listen(
        &self,
        _bind_addr: Option<Self::Address>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Listener>> + Send>> {
        let listener = self.listen();
        Box::pin(async move { Ok(listener) })
    }
}
