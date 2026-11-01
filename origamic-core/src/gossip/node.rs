//! Gossip node – the epidemic broadcast engine.
//!
//! [`GossipNode`] runs an async event loop that implements a push-based
//! epidemic broadcast protocol:
//!
//! 1. **Outgoing messages** from the local application arrive via the
//!    `local_rx` channel, are signed, deduplicated and fanned out to every
//!    currently known peer.
//! 2. **Incoming messages** from peers arrive via the `peer_rx` channel,
//!    pass signature verification and the seen-cache check, then are either
//!    relayed to other peers (TTL decrement) or delivered to the upper layer.
//! 3. **Peer Exchange (PEX)** control messages are broadcast periodically so
//!    the node's view of the network converges automatically.
//!
//! The node is spawned onto the Tokio runtime via [`GossipNode::spawn`], which
//! returns a [`GossipNodeHandle`] that the caller uses to communicate with the
//! event loop.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

use crate::{
    factory::Factory,
    gossip::{cache::SeenCache, peers::PeerStore},
    overlay::{
        address::{Keypair, OverlayAddress},
        message::{GossipMessage, PeerExchangePayload},
    },
};

/// A message originating from the local application that should be broadcast
/// (or unicast) into the gossip network.
///
/// This is an internal type used to communicate between [`OverlayContext`](crate::overlay::context::OverlayContext)
/// and the [`GossipNode`] event loop.
#[derive(Debug)]
pub struct OutgoingLocalMessage {
    /// `None` means broadcast; `Some(addr)` means unicast to a specific peer.
    pub recipient: Option<OverlayAddress>,
    /// Raw application payload bytes.
    pub payload: Vec<u8>,
    /// Optional oneshot sender that is fired once the message has been
    /// dispatched to all known peers.
    pub done_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl OutgoingLocalMessage {
    /// Create a broadcast message with no specific recipient.
    pub fn broadcast(payload: Vec<u8>) -> Self {
        Self {
            recipient: None,
            payload,
            done_tx: None,
        }
    }

    /// Create a unicast message addressed to `recipient`.
    pub fn to(recipient: OverlayAddress, payload: Vec<u8>) -> Self {
        Self {
            recipient: Some(recipient),
            payload,
            done_tx: None,
        }
    }

    /// Create a unicast message with a completion notification channel.
    ///
    /// `done_tx` will be signalled once the message has been sent to all
    /// currently reachable peers.
    pub fn to_with_done(
        recipient: OverlayAddress,
        payload: Vec<u8>,
        done_tx: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            recipient: Some(recipient),
            payload,
            done_tx: Some(done_tx),
        }
    }
}

/// A handle returned by [`GossipNode::spawn`] that gives the caller access to
/// the channels and shared state of the running gossip event loop.
pub struct GossipNodeHandle<F: ?Sized + Factory> {
    /// The Ed25519 public key address of this node.
    pub address: OverlayAddress,
    /// The UUID that identifies the overlay network this node belongs to.
    pub network_id: Uuid,
    /// Channel for sending outgoing messages from the local application into
    /// the gossip event loop.
    pub local_tx: mpsc::Sender<OutgoingLocalMessage>,
    /// Channel from which the upper layer (e.g. `OverlayContext`) receives
    /// gossip messages addressed to this network.
    pub upper_rx: mpsc::Receiver<GossipMessage>,
    /// Channel for injecting raw [`GossipMessage`] frames from inbound streams
    /// into the gossip event loop.
    pub peer_tx: mpsc::Sender<GossipMessage>,
    /// Shared peer address store.  The upper layer can call
    /// [`PeerStore::add_peer`] to seed the initial peer set.
    pub peer_store: PeerStore<F::Address>,
}

/// The gossip node event loop.
///
/// Callers should **not** interact with `GossipNode` directly; use
/// [`GossipNode::spawn`] and operate through the returned [`GossipNodeHandle`].
pub struct GossipNode<F: ?Sized + Factory> {
    /// UUID of the overlay network.
    pub network_id: Uuid,
    /// Ed25519 key pair used to sign outgoing messages.
    pub keypair: Arc<Keypair>,
    /// Lower-layer transport used to open outgoing connections to peers.
    pub lower_factory: Arc<F>,
    /// Set of known lower-layer peer addresses.
    pub peer_store: PeerStore<F::Address>,
    /// Deduplication cache that prevents re-processing the same message.
    pub cache: SeenCache,
    /// Receives messages from the local application via [`OutgoingLocalMessage`].
    pub local_rx: mpsc::Receiver<OutgoingLocalMessage>,
    /// Receives raw [`GossipMessage`] frames forwarded from inbound peer streams.
    pub peer_rx: mpsc::Receiver<GossipMessage>,
    /// Sends messages destined for the upper overlay layer.
    pub upper_tx: mpsc::Sender<GossipMessage>,
    /// Default TTL assigned to freshly created messages.
    pub default_ttl: u8,
}

impl<F: ?Sized + Factory> GossipNode<F> {
    /// Spawn a new gossip node onto the Tokio runtime.
    ///
    /// The node runs as an independent task and communicates with its owner
    /// exclusively through the channels contained in the returned
    /// [`GossipNodeHandle`].
    ///
    /// # Parameters
    ///
    /// - `network_id` – UUID identifying the overlay network.
    /// - `keypair` – Ed25519 key pair for signing outgoing messages.
    /// - `lower_factory` – transport used to connect to known peers.
    /// - `lower_peers` – seed list of peer addresses (typically the bootstrap nodes).
    /// - `advertise_addr` – the local address advertised to peers via PEX;
    ///   `None` means this node does not accept inbound connections.
    /// - `default_ttl` – number of hops a freshly created message travels.
    /// - `buffer_size` – capacity of the internal mpsc channels.
    pub fn spawn(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
        advertise_addr: Option<F::Address>,
        default_ttl: u8,
        buffer_size: usize,
    ) -> GossipNodeHandle<F> {
        let address = keypair.address;
        let keypair = Arc::new(keypair);

        let (local_tx, local_rx) = mpsc::channel(buffer_size);
        let (peer_tx, peer_rx) = mpsc::channel(buffer_size);
        let (upper_tx, upper_rx) = mpsc::channel(buffer_size);

        let peer_store = PeerStore::new(advertise_addr, lower_peers);

        let node = Self {
            network_id,
            keypair,
            lower_factory: Arc::clone(&lower_factory),
            peer_store: peer_store.clone(),
            cache: SeenCache::with_default(),
            local_rx,
            peer_rx,
            upper_tx,
            default_ttl,
        };

        tokio::spawn(node.run());

        GossipNodeHandle {
            address,
            network_id,
            local_tx,
            upper_rx,
            peer_tx,
            peer_store,
        }
    }

    /// Run the gossip event loop until all senders are dropped.
    pub async fn run(mut self) {
        // Announce ourselves immediately so peers learn our address.
        self.send_peer_exchange();

        let mut discovery_interval = tokio::time::interval(tokio::time::Duration::from_secs(3));
        discovery_interval.tick().await;

        loop {
            tokio::select! {
                _ = discovery_interval.tick() => {
                    self.send_peer_exchange();
                }
                Some(local_msg) = self.local_rx.recv() => {
                    self.handle_local(local_msg).await;
                }
                Some(msg) = self.peer_rx.recv() => {
                    self.handle_peer(msg).await;
                }
                else => break,
            }
        }
    }

    /// Build and broadcast a PEX control message containing our own listen
    /// address and a random sample of known peers.
    fn send_peer_exchange(&mut self) {
        let pex = PeerExchangePayload::new(
            self.peer_store.self_addr().cloned(),
            self.peer_store.sample_peers(10),
        );
        if let Ok(payload) = pex.encode() {
            let id = Uuid::new_v4().to_string();
            let msg = GossipMessage::new_control(
                &self.keypair.signing_key,
                self.network_id,
                self.keypair.address,
                id,
                payload,
                self.default_ttl,
            );
            self.cache.check_and_insert(&msg.id);
            self.broadcast(&msg);
        }
    }

    /// Handle a message originating from the local application.
    async fn handle_local(&mut self, local_msg: OutgoingLocalMessage) {
        let done_tx = local_msg.done_tx;
        let msg = self.create_signed_message(local_msg.recipient, local_msg.payload);
        self.cache.check_and_insert(&msg.id);
        self.broadcast_with_done(&msg, done_tx);
    }

    /// Handle a message received from a peer.
    ///
    /// Processing pipeline:
    /// 1. Verify the Ed25519 signature.
    /// 2. Deduplicate via the seen-cache.
    /// 3. If it is a control (PEX) message, update the peer store and relay.
    /// 4. Otherwise relay the message (TTL decrement) and deliver to the upper
    ///    layer if the `network_id` matches.
    async fn handle_peer(&mut self, msg: GossipMessage) {
        // 1. Signature check
        if !msg.verify() {
            return;
        }

        // 2. Deduplication
        if !self.cache.check_and_insert(&msg.id) {
            return;
        }

        // 3. Control messages (PEX)
        if msg.is_control {
            if let Ok(pex) = PeerExchangePayload::<F::Address>::decode(&msg.payload) {
                if let Some(listen_addr) = pex.listen_addr {
                    self.peer_store.add_peer(listen_addr);
                }
                self.peer_store.add_peers(pex.known_peers);
            }
            // Control messages are still relayed to maintain network connectivity.
            self.maybe_relay(&msg);
            return;
        }

        // 4. Relay then deliver
        self.maybe_relay(&msg);
        self.maybe_deliver_upper(msg).await;
    }

    /// Sign and wrap `payload` into a [`GossipMessage`].
    fn create_signed_message(
        &self,
        recipient: Option<OverlayAddress>,
        payload: Vec<u8>,
    ) -> GossipMessage {
        let id = Uuid::new_v4().to_string();
        GossipMessage::new_signed(
            &self.keypair.signing_key,
            self.network_id,
            self.keypair.address,
            recipient,
            id,
            payload,
            self.default_ttl,
        )
    }

    /// Forward `msg` to all peers after decrementing its TTL, if TTL > 1.
    fn maybe_relay(&self, msg: &GossipMessage) {
        if msg.ttl > 1 {
            let mut relay_msg = msg.clone();
            relay_msg.ttl -= 1;
            self.broadcast(&relay_msg);
        }
    }

    /// Deliver `msg` to the upper overlay layer, but only if its `network_id`
    /// matches the network this node is participating in.
    async fn maybe_deliver_upper(&self, msg: GossipMessage) {
        if msg.network_id == self.network_id {
            let _ = self.upper_tx.send(msg).await;
        }
    }

    /// Fan out `msg` to all currently known peers (fire-and-forget).
    fn broadcast(&self, msg: &GossipMessage) {
        self.broadcast_with_done(msg, None);
    }

    /// Fan out `msg` to all currently known peers and optionally signal
    /// `done_tx` when the last peer send completes.
    fn broadcast_with_done(
        &self,
        msg: &GossipMessage,
        done_tx: Option<tokio::sync::oneshot::Sender<()>>,
    ) {
        let Ok(encoded) = msg.encode() else {
            if let Some(tx) = done_tx {
                let _ = tx.send(());
            }
            return;
        };
        let bytes = Bytes::from(encoded);
        let peers = self.peer_store.get_peers();

        if peers.is_empty() {
            if let Some(tx) = done_tx {
                let _ = tx.send(());
            }
            return;
        }

        let total = peers.len();
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let done_tx_shared = Arc::new(tokio::sync::Mutex::new(done_tx));

        for peer_addr in peers {
            let factory = self.lower_factory.clone();
            let data = bytes.clone();
            let counter = counter.clone();
            let done_tx_shared = done_tx_shared.clone();

            tokio::spawn(async move {
                if let Ok(stream) = factory.connect(peer_addr).await {
                    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
                    let _ = framed.send(data).await;
                }
                let prev = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if prev + 1 == total {
                    if let Some(tx) = done_tx_shared.lock().await.take() {
                        let _ = tx.send(());
                    }
                }
            });
        }
    }
}

/// Read length-delimited gossip frames from `stream` and forward each decoded
/// [`GossipMessage`] to the gossip node via `peer_tx`.
///
/// This function is called once per accepted inbound connection and runs until
/// the stream is closed or `peer_tx` is dropped.
///
/// # Errors
///
/// Returns an `std::io::Error` if the underlying stream produces a framing error.
pub async fn process_inbound_stream<S>(
    stream: S,
    peer_tx: mpsc::Sender<GossipMessage>,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    while let Some(result) = framed.next().await {
        match result {
            Ok(bytes) => {
                if let Ok(msg) = GossipMessage::decode(&bytes)
                    && peer_tx.send(msg).await.is_err()
                {
                    break;
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
