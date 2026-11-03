//! Overlay network context – the primary user-facing API.
//!
//! [`OverlayContext`] is the central handle for an overlay network node. It
//! owns the gossip layer, the virtual-stream multiplexer, and the node's
//! cryptographic identity. Because it is `Clone`, it can be freely shared
//! across tasks.
//!
//! # Virtual Streams
//!
//! `OverlayContext` implements [`Factory`] with `Stream = `[`OverlayStream`]
//! and `Address = `[`OverlayAddress`], enabling it to be stacked as the
//! lower-level transport for another `OverlayContext` (recursive overlay).
//!
//! Outbound streams are created via [`Factory::connect`]; inbound streams are
//! accepted via [`listen`].
//!
//! [`Factory`]: crate::factory::Factory
//! [`listen`]: OverlayContext::listen

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::{
    error::SendError,
    factory::Factory,
    gossip::{
        node::{GossipNode, GossipNodeHandle, OutgoingLocalMessage, process_inbound_stream},
        peers::PeerStore,
    },
    overlay::{
        address::{Keypair, OverlayAddress},
        message::{GossipMessage, StreamFrame},
        stream::{OverlayListener, OverlayStream},
    },
};

// Map of active virtual streams keyed by (remote address, stream id).
type ActiveStreamMap = HashMap<(OverlayAddress, u64), mpsc::Sender<Vec<u8>>>;

/// A type-erased interface for adding peers to the underlying [`PeerStore`].
///
/// `AnyPeerAdder` lets [`OverlayContext`] accept peers of the concrete lower
/// transport address type (`F::Address`) without exposing a generic type
/// parameter on `OverlayContext` itself. The concrete implementation
/// `TypedPeerAdder` performs a checked downcast at runtime.
///
/// [`PeerStore`]: crate::gossip::peers::PeerStore
pub trait AnyPeerAdder: Send + Sync {
    /// Attempts to add `peer` (a type-erased address) to the store.
    ///
    /// Returns `true` if the peer was newly inserted, or `false` if the
    /// downcast failed, the address matched the node's own address, or the
    /// store was at capacity.
    fn add_any(&self, peer: Box<dyn std::any::Any + Send>) -> bool;
}

// Private implementation of AnyPeerAdder that holds the concrete PeerStore.
struct TypedPeerAdder<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static> {
    store: PeerStore<A>,
}

impl<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static> AnyPeerAdder for TypedPeerAdder<A> {
    fn add_any(&self, peer: Box<dyn std::any::Any + Send>) -> bool {
        if let Ok(addr) = peer.downcast::<A>() {
            self.store.add_peer(*addr)
        } else {
            false
        }
    }
}

/// The main handle for an overlay network node.
///
/// `OverlayContext` combines:
/// - A gossip node (see [`GossipNode`]) running over a lower-level [`Factory`].
/// - A virtual-stream multiplexer that maps `(peer_addr, stream_id)` pairs to
///   async duplex streams.
/// - The node's [`Keypair`] for signing outgoing messages.
///
/// Because it is `Clone` and all internal state is reference-counted, multiple
/// handles to the same node can be held across tasks.
///
/// [`GossipNode`]: crate::gossip::node::GossipNode
/// [`Factory`]: crate::factory::Factory
#[derive(Clone)]
pub struct OverlayContext {
    network_id: Uuid,
    keypair: Arc<Keypair>,
    peer_adder: Arc<dyn AnyPeerAdder>,
    pub(crate) local_tx: mpsc::Sender<OutgoingLocalMessage>,
    pub(crate) active_streams: Arc<Mutex<ActiveStreamMap>>,
    pub(crate) inbound_stream_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>>,
}

impl std::fmt::Debug for OverlayContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverlayContext")
            .field("network_id", &self.network_id)
            .field("address", &self.address())
            .finish()
    }
}

// Default gossip TTL (maximum hop count for forwarded messages).
const DEFAULT_GOSSIP_TTL: u8 = 7;
// Internal channel buffer size for gossip messages.
const DEFAULT_GOSSIP_BUFFER_SIZE: usize = 256;

impl OverlayContext {
    /// Creates a node on a fresh random network and awaits listener startup.
    ///
    /// A new random [`Uuid`] is used as the network identifier. The lower
    /// transport listener is started in a background task.
    ///
    /// Returns an error if the lower transport listener fails to bind.
    pub async fn try_new<F: Factory>(
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
    ) -> std::io::Result<Self> {
        Self::try_with_network(Uuid::new_v4(), lower_factory, lower_peers).await
    }

    /// Creates a node that joins an existing network and awaits listener startup.
    ///
    /// A random [`Keypair`] is generated for this node. The lower transport
    /// listener is started in a background task.
    ///
    /// Returns an error if the lower transport listener fails to bind.
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

    /// Creates a node with an explicit keypair and awaits listener startup.
    ///
    /// The lower transport listener is started in a background task.
    ///
    /// Returns an error if the lower transport listener fails to bind.
    pub async fn try_with_network_and_keypair<F: Factory>(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
    ) -> std::io::Result<Self> {
        Self::try_with_options(network_id, keypair, lower_factory, lower_peers, None).await
    }

    /// Creates a node with an advertised address and awaits listener startup.
    ///
    /// `advertise_addr` is broadcast to peers via Peer Exchange so that they
    /// can connect to this node. A random keypair is generated. The lower
    /// transport listener is started in a background task.
    ///
    /// Returns an error if the lower transport listener fails to bind.
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

    /// Creates a node with all options specified and awaits listener startup.
    ///
    /// Spawns the gossip node and the virtual-stream router as background
    /// tasks, and starts the lower transport listener.
    pub async fn try_with_options<F: Factory>(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
        advertise_addr: Option<F::Address>,
    ) -> std::io::Result<Self> {
        let address = keypair.address;
        let GossipNodeHandle {
            address: _,
            network_id: _,
            local_tx,
            upper_rx,
            peer_tx,
            peer_store,
        } = GossipNode::spawn(
            network_id,
            keypair.clone(),
            lower_factory.clone(),
            lower_peers,
            advertise_addr.clone(),
            DEFAULT_GOSSIP_TTL,
            DEFAULT_GOSSIP_BUFFER_SIZE,
        );

        let active_streams: Arc<Mutex<ActiveStreamMap>> = Arc::new(Mutex::new(HashMap::new()));
        let (inbound_tx, inbound_rx) = mpsc::channel(128);

        start_stream_router_task(
            address,
            upper_rx,
            active_streams.clone(),
            local_tx.clone(),
            inbound_tx,
        );

        // Await listener startup and propagate errors to the caller.
        lower_factory
            .start_listener(advertise_addr, peer_tx)
            .await?;

        Ok(Self {
            network_id,
            keypair: Arc::new(keypair),
            peer_adder: Arc::new(TypedPeerAdder { store: peer_store }),
            local_tx,
            active_streams,
            inbound_stream_rx: Arc::new(Mutex::new(inbound_rx)),
        })
    }

    // -----------------------------------------------------------------------
    // Node API
    // -----------------------------------------------------------------------

    /// Adds a peer address of the lower transport type to the gossip node's
    /// peer store.
    ///
    /// The address is type-erased via [`AnyPeerAdder`] so that `OverlayContext`
    /// itself does not need to be generic over the transport address type.
    ///
    /// Returns `true` if the peer was newly inserted.
    pub fn add_peer<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static>(
        &self,
        addr: A,
    ) -> bool {
        self.peer_adder.add_any(Box::new(addr))
    }

    /// Returns the UUID that identifies this node's overlay network.
    pub fn network_id(&self) -> Uuid {
        self.network_id
    }

    /// Returns the node's [`OverlayAddress`] (derived from its Ed25519 public key).
    pub fn address(&self) -> OverlayAddress {
        self.keypair.address
    }

    /// Creates an [`OverlayListener`] that yields incoming virtual streams.
    ///
    /// Multiple listeners may be created; they share the same underlying
    /// channel so each incoming stream is delivered to exactly one listener.
    pub fn listen(&self) -> OverlayListener {
        OverlayListener::new(self.inbound_stream_rx.clone())
    }

    /// Returns the shared receiver for incoming virtual streams.
    ///
    /// This is the lower-level counterpart to [`listen`]. Prefer [`listen`]
    /// unless you need direct access to the channel for advanced use cases.
    ///
    /// [`listen`]: OverlayContext::listen
    pub fn inbound_stream_rx(&self) -> Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>> {
        self.inbound_stream_rx.clone()
    }

    /// Broadcasts a raw payload to all nodes on this overlay network.
    ///
    /// The gossip node wraps `payload` in a signed [`GossipMessage`] and
    /// propagates it to all connected peers.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::ChannelClosed`] if the internal gossip channel has
    /// been closed (i.e. the gossip node task has terminated).
    ///
    /// [`GossipMessage`]: crate::overlay::message::GossipMessage
    pub async fn broadcast(&self, payload: Vec<u8>) -> Result<(), SendError> {
        self.local_tx
            .send(OutgoingLocalMessage::broadcast(payload))
            .await
            .map_err(|_| SendError::ChannelClosed)
    }

    /// Sends a raw payload to a specific node identified by its [`OverlayAddress`].
    ///
    /// The gossip node signs and forwards the message through the network.
    /// Delivery is best-effort; no acknowledgement is returned.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::ChannelClosed`] if the internal gossip channel has
    /// been closed.
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

impl Factory for OverlayContext {
    type Stream = OverlayStream;
    type Address = OverlayAddress;

    /// Opens a virtual stream to the node at `addr`.
    ///
    /// Internally spawns an outbound bridge task that:
    /// 1. Registers a `(addr, stream_id)` entry in the active stream map.
    /// 2. Reads data written by the caller and wraps it in [`StreamFrame`]
    ///    messages sent via the gossip layer.
    /// 3. Forwards incoming [`StreamFrame`]s from the router to the read side
    ///    of the returned [`OverlayStream`].
    ///
    /// The returned future resolves synchronously (the stream object is
    /// created before any network I/O occurs).
    ///
    /// [`StreamFrame`]: crate::overlay::message::StreamFrame
    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        let (user_stream, internal_stream) = tokio::io::duplex(65536);
        let stream_id: u64 = rand::random();
        let (to_stream_tx, from_router_rx) = mpsc::channel(256);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        spawn_outbound_stream_bridge(
            addr,
            stream_id,
            internal_stream,
            to_stream_tx,
            from_router_rx,
            shutdown_tx,
            self.active_streams.clone(),
            self.local_tx.clone(),
        );

        let overlay_stream = OverlayStream::with_shutdown_rx(user_stream, addr, shutdown_rx);
        Box::pin(async move { Ok(overlay_stream) })
    }

    /// Starts forwarding inbound overlay streams to `peer_tx` as gossip messages.
    ///
    /// When `OverlayContext` is used as the lower-layer transport for another
    /// `OverlayContext` (recursive overlay), this method bridges the inner
    /// overlay's inbound stream channel to the outer gossip node's peer
    /// message channel. The `_bind_addr` parameter is ignored because
    /// addressing is handled at the overlay level.
    fn start_listener(
        &self,
        _bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> {
        let mut listener = self.listen();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tx = peer_tx.clone();
                tokio::spawn(async move {
                    let _ = process_inbound_stream(stream, tx).await;
                });
            }
        });
        Box::pin(async move { Ok(()) })
    }
}

/// Spawns a background task that bridges an outbound virtual stream to the
/// gossip network.
///
/// The task:
/// - Registers `(addr, stream_id)` in `active_streams`.
/// - Reads bytes written to `internal_stream` and sends them as [`StreamFrame`]
///   messages via `local_tx`. The very first chunk is sent as an *init* frame
///   so the receiver can identify new streams.
/// - Writes bytes received from `from_router_rx` to the read side of
///   `internal_stream`.
/// - On EOF or error, sends a close frame, fires `shutdown_tx`, and removes
///   itself from `active_streams`.
///
/// [`StreamFrame`]: crate::overlay::message::StreamFrame
pub(crate) fn spawn_outbound_stream_bridge(
    addr: OverlayAddress,
    stream_id: u64,
    internal_stream: tokio::io::DuplexStream,
    to_stream_tx: mpsc::Sender<Vec<u8>>,
    mut from_router_rx: mpsc::Receiver<Vec<u8>>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    active_streams: std::sync::Arc<
        tokio::sync::Mutex<HashMap<(OverlayAddress, u64), mpsc::Sender<Vec<u8>>>>,
    >,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
) {
    tokio::spawn(async move {
        {
            let mut map = active_streams.lock().await;
            map.insert((addr, stream_id), to_stream_tx);
        }

        let (mut read_half, mut write_half) = tokio::io::split(internal_stream);

        // Send bridge: reads from internal_stream and sends StreamFrames to the gossip network.
        let mut shutdown_tx = Some(shutdown_tx);
        let send_task = async {
            let mut buf = vec![0u8; 4096];
            let mut is_first = true;
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out = OutgoingLocalMessage::to_with_done(addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                    Ok(n) => {
                        let frame = if is_first {
                            is_first = false;
                            StreamFrame::new_init(stream_id, buf[..n].to_vec())
                        } else {
                            StreamFrame::new_data(stream_id, buf[..n].to_vec())
                        };

                        if let Ok(encoded) = frame.encode() {
                            let out = OutgoingLocalMessage::to(addr, encoded);
                            if local_tx.send(out).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out = OutgoingLocalMessage::to_with_done(addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                }
            }
        };

        // Receive bridge: forwards data from the router to the read side of internal_stream.
        let recv_task = async {
            while let Some(data) = from_router_rx.recv().await {
                if write_half.write_all(&data).await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        };

        tokio::select! {
            _ = send_task => {},
            _ = recv_task => {},
        }

        // Clean up: remove this stream from the active map.
        let mut map = active_streams.lock().await;
        map.remove(&(addr, stream_id));
    });
}

/// Spawns the stream router task that demultiplexes incoming gossip messages
/// into individual virtual streams.
///
/// The router receives [`GossipMessage`]s from the gossip node via `upper_rx`,
/// decodes the embedded [`StreamFrame`], and dispatches data to the
/// appropriate sender in `active_streams`. If no matching stream exists and the
/// frame is an init or non-empty data frame, a new inbound stream bridge is
/// spawned and the resulting [`OverlayStream`] is sent to `inbound_tx`.
///
/// Messages addressed to a different node are silently dropped.
///
/// [`GossipMessage`]: crate::overlay::message::GossipMessage
/// [`StreamFrame`]: crate::overlay::message::StreamFrame
fn start_stream_router_task(
    self_address: OverlayAddress,
    mut upper_rx: mpsc::Receiver<GossipMessage>,
    active_streams: Arc<Mutex<ActiveStreamMap>>,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
    inbound_tx: mpsc::Sender<(OverlayStream, OverlayAddress)>,
) {
    tokio::spawn(async move {
        while let Some(msg) = upper_rx.recv().await {
            // Drop messages that are not addressed to this node.
            if let Some(target) = msg.recipient
                && target != self_address
            {
                continue;
            }

            // Decode the StreamFrame from the message payload.
            let Ok(frame) = StreamFrame::decode(&msg.payload) else {
                continue;
            };

            let key = (msg.sender, frame.stream_id);
            let mut streams = active_streams.lock().await;

            if frame.is_close {
                streams.remove(&key);
                continue;
            }

            if let Some(stream_tx) = streams.get(&key) {
                let _ = stream_tx.send(frame.data).await;
            } else if frame.is_init || !frame.data.is_empty() {
                // New inbound stream: spawn a bridge and deliver it to the listener.
                spawn_inbound_stream_bridge(
                    key,
                    frame.data,
                    &mut streams,
                    active_streams.clone(),
                    local_tx.clone(),
                    &inbound_tx,
                )
                .await;
            }
        }
    });
}

/// Spawns the bridge task for a newly accepted inbound virtual stream.
///
/// Creates a `tokio::io::duplex` pair: one end (`user_stream`) is wrapped in
/// an [`OverlayStream`] and sent to the application via `inbound_tx`; the
/// other end (`internal_stream`) is managed by the bridge task, which:
/// - Reads data written by the application and forwards it as [`StreamFrame`]
///   messages to the sender.
/// - Writes data received from the router (`from_router_rx`) to the read side
///   of `user_stream`.
///
/// On close or error, a close frame is sent, `shutdown_tx` is fired, and the
/// entry is removed from `active_streams`.
///
/// [`StreamFrame`]: crate::overlay::message::StreamFrame
async fn spawn_inbound_stream_bridge(
    (sender_addr, stream_id): (OverlayAddress, u64),
    initial_data: Vec<u8>,
    streams: &mut ActiveStreamMap,
    active_streams: Arc<Mutex<ActiveStreamMap>>,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
    inbound_tx: &mpsc::Sender<(OverlayStream, OverlayAddress)>,
) {
    let (user_stream, internal_stream) = tokio::io::duplex(65536);
    let (to_stream_tx, mut from_router_rx) = mpsc::channel(256);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let _ = to_stream_tx.send(initial_data).await;
    streams.insert((sender_addr, stream_id), to_stream_tx);

    let (mut read_half, mut write_half) = tokio::io::split(internal_stream);

    // Bridge task for the inbound stream.
    let active_streams_clone = active_streams.clone();
    tokio::spawn(async move {
        let mut shutdown_tx = Some(shutdown_tx);
        let send_task = async {
            let mut buf = vec![0u8; 4096];
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out =
                                OutgoingLocalMessage::to_with_done(sender_addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                    Ok(n) => {
                        let frame = StreamFrame::new_data(stream_id, buf[..n].to_vec());
                        if let Ok(encoded) = frame.encode() {
                            let out = OutgoingLocalMessage::to(sender_addr, encoded);
                            if local_tx.send(out).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out =
                                OutgoingLocalMessage::to_with_done(sender_addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                }
            }
        };

        let recv_task = async {
            while let Some(data) = from_router_rx.recv().await {
                if write_half.write_all(&data).await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        };

        tokio::select! {
            _ = send_task => {},
            _ = recv_task => {},
        }

        // Clean up: remove this stream from the active map.
        let mut map = active_streams_clone.lock().await;
        map.remove(&(sender_addr, stream_id));
    });

    let overlay_stream = OverlayStream::with_shutdown_rx(user_stream, sender_addr, shutdown_rx);
    let _ = inbound_tx.send((overlay_stream, sender_addr)).await;
}
