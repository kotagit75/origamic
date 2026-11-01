//! Wire message types for the overlay gossip protocol.
//!
//! This module defines three primary message structures:
//!
//! - [`GossipMessage`] – the signed, TTL-bearing envelope carried by the
//!   gossip layer.
//! - [`PeerExchangePayload`] – the body of a Peer Exchange (PEX) control
//!   message.
//! - [`StreamFrame`] – a multiplexed virtual-stream chunk embedded in a
//!   gossip message payload.
//!
//! All types support bincode serialisation via [`encode`] / [`decode`] helpers
//! that return [`ProtocolError`] on failure.
//!
//! [`encode`]: GossipMessage::encode
//! [`decode`]: GossipMessage::decode
//! [`ProtocolError`]: crate::error::ProtocolError

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ProtocolError;
use crate::overlay::address::OverlayAddress;

/// A signed gossip message propagated through the overlay network.
///
/// Each message carries a UUID network identifier, a unique message `id`, an
/// Ed25519 `signature` over the canonical digest (see [`digest_bytes`]), and a
/// `ttl` that is decremented at each hop to bound propagation depth.
///
/// Setting `recipient` to `None` marks the message as a broadcast; `Some(addr)`
/// targets a specific node. The `is_control` flag distinguishes Peer Exchange
/// messages from data payloads so routers can handle them specially.
///
/// [`digest_bytes`]: GossipMessage::digest_bytes
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipMessage {
    /// UUID of the overlay network this message belongs to.
    pub network_id: Uuid,
    /// Unique message identifier (typically a UUID string) used for
    /// deduplication in the [`SeenCache`].
    ///
    /// [`SeenCache`]: crate::gossip::cache::SeenCache
    pub id: String,
    /// [`OverlayAddress`] of the node that originally created and signed this
    /// message.
    pub sender: OverlayAddress,
    /// Intended recipient. `None` means broadcast to all nodes; `Some(addr)`
    /// means the message is addressed to a specific node.
    pub recipient: Option<OverlayAddress>,
    /// Opaque application payload (e.g. a serialised [`StreamFrame`] or
    /// [`PeerExchangePayload`]).
    pub payload: Vec<u8>,
    /// Time-to-live counter. Decremented at each hop; messages with `ttl == 0`
    /// are dropped rather than forwarded.
    pub ttl: u8,
    /// Ed25519 signature over [`digest_bytes`] produced by the sender.
    ///
    /// [`digest_bytes`]: GossipMessage::digest_bytes
    pub signature: Vec<u8>,
    /// When `true`, this is a control message (e.g. Peer Exchange) and should
    /// be processed by the gossip layer rather than delivered to the application.
    #[serde(default)]
    pub is_control: bool,
}

impl GossipMessage {
    /// Creates a signed data or unicast message.
    ///
    /// This is a convenience wrapper around [`new_signed_with_control`] with
    /// `is_control = false`.
    ///
    /// [`new_signed_with_control`]: GossipMessage::new_signed_with_control
    pub fn new_signed(
        signing_key: &SigningKey,
        network_id: Uuid,
        sender: OverlayAddress,
        recipient: Option<OverlayAddress>,
        id: String,
        payload: Vec<u8>,
        ttl: u8,
    ) -> Self {
        Self::new_signed_with_control(
            signing_key,
            network_id,
            sender,
            recipient,
            id,
            payload,
            ttl,
            false,
        )
    }

    /// Creates a signed message with explicit control-flag configuration.
    ///
    /// The signature is computed over [`digest_bytes`], which intentionally
    /// excludes mutable hop fields like `ttl`.
    ///
    /// [`digest_bytes`]: GossipMessage::digest_bytes
    pub fn new_signed_with_control(
        signing_key: &SigningKey,
        network_id: Uuid,
        sender: OverlayAddress,
        recipient: Option<OverlayAddress>,
        id: String,
        payload: Vec<u8>,
        ttl: u8,
        is_control: bool,
    ) -> Self {
        let sign_bytes =
            Self::digest_bytes(&network_id, &id, &sender, &recipient, &payload, is_control);
        let signature = signing_key.sign(&sign_bytes);
        Self {
            network_id,
            id,
            sender,
            recipient,
            payload,
            ttl,
            signature: signature.to_vec(),
            is_control,
        }
    }

    /// Creates a broadcast data message (`recipient = None`, `is_control = false`).
    pub fn new_broadcast(
        signing_key: &SigningKey,
        network_id: Uuid,
        sender: OverlayAddress,
        id: String,
        payload: Vec<u8>,
        ttl: u8,
    ) -> Self {
        Self::new_signed(signing_key, network_id, sender, None, id, payload, ttl)
    }

    /// Creates a broadcast control message (`recipient = None`, `is_control = true`).
    ///
    /// Used internally to send Peer Exchange payloads.
    pub fn new_control(
        signing_key: &SigningKey,
        network_id: Uuid,
        sender: OverlayAddress,
        id: String,
        payload: Vec<u8>,
        ttl: u8,
    ) -> Self {
        Self::new_signed_with_control(
            signing_key,
            network_id,
            sender,
            None,
            id,
            payload,
            ttl,
            true,
        )
    }

    /// Computes the canonical byte sequence that is signed and verified.
    ///
    /// The digest covers all immutable fields: `network_id`, `id`, `sender`,
    /// `recipient`, `is_control`, and `payload`. Mutable hop fields such as
    /// `ttl` are deliberately excluded so that relaying nodes can decrement
    /// the TTL without invalidating the signature.
    pub fn digest_bytes(
        network_id: &Uuid,
        id: &str,
        sender: &OverlayAddress,
        recipient: &Option<OverlayAddress>,
        payload: &[u8],
        is_control: bool,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(network_id.as_bytes());
        buf.extend_from_slice(id.as_bytes());
        buf.extend_from_slice(sender.as_bytes());
        if let Some(r) = recipient {
            buf.push(1);
            buf.extend_from_slice(r.as_bytes());
        } else {
            buf.push(0);
        }
        buf.push(if is_control { 1 } else { 0 });
        buf.extend_from_slice(payload);
        buf
    }

    /// Verifies the Ed25519 signature using the sender's embedded public key.
    ///
    /// Returns `false` if the sender address cannot be converted to a valid
    /// verifying key, if the signature bytes are malformed, or if the signature
    /// does not match the message digest.
    pub fn verify(&self) -> bool {
        let Ok(verifying_key) = self.sender.to_verifying_key() else {
            return false;
        };
        let Ok(signature) = Signature::from_slice(&self.signature) else {
            return false;
        };
        let digest = Self::digest_bytes(
            &self.network_id,
            &self.id,
            &self.sender,
            &self.recipient,
            &self.payload,
            self.is_control,
        );
        verifying_key.verify(&digest, &signature).is_ok()
    }

    /// Serialises the message to bytes using bincode.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Serialization`] if serialisation fails.
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        bincode::serialize(self).map_err(|e| ProtocolError::Serialization(e.to_string()))
    }

    /// Deserialises a `GossipMessage` from a byte slice.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Deserialization`] if deserialisation fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        bincode::deserialize(bytes).map_err(|e| ProtocolError::Deserialization(e.to_string()))
    }
}

/// Payload carried inside a Peer Exchange (PEX) control message.
///
/// When a node receives a PEX message it learns both the sender's public
/// listen address (`listen_addr`) and a set of additional known peers
/// (`known_peers`) that can be used to grow the local [`PeerStore`].
///
/// [`PeerStore`]: crate::gossip::peers::PeerStore
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerExchangePayload<A> {
    /// The sender's advertised listen address, if any.
    pub listen_addr: Option<A>,
    /// A sample of peers known to the sender.
    pub known_peers: Vec<A>,
}

impl<A: Serialize + serde::de::DeserializeOwned> PeerExchangePayload<A> {
    /// Creates a new `PeerExchangePayload`.
    pub fn new(listen_addr: Option<A>, known_peers: Vec<A>) -> Self {
        Self {
            listen_addr,
            known_peers,
        }
    }

    /// Serialises the payload to bytes using bincode.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Serialization`] if serialisation fails.
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        bincode::serialize(self).map_err(|e| ProtocolError::Serialization(e.to_string()))
    }

    /// Deserialises a `PeerExchangePayload` from a byte slice.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Deserialization`] if deserialisation fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        bincode::deserialize(bytes).map_err(|e| ProtocolError::Deserialization(e.to_string()))
    }
}

/// A multiplexed virtual-stream frame embedded in a [`GossipMessage`] payload.
///
/// The overlay layer uses `StreamFrame` to multiplex multiple logical duplex
/// streams over a single gossip channel. Each stream is uniquely identified by
/// a `(sender_address, stream_id)` pair, where `stream_id` is chosen randomly
/// by the initiating side.
///
/// Three kinds of frames exist:
/// - **init** (`is_init = true`) – opens a new stream and carries the first
///   chunk of data.
/// - **data** (`is_init = false`, `is_close = false`) – carries subsequent
///   data chunks.
/// - **close** (`is_close = true`, empty `data`) – signals that the sender has
///   finished writing and the stream can be cleaned up.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamFrame {
    /// Identifies the virtual stream within the `(sender, stream_id)` pair.
    pub stream_id: u64,
    /// Payload bytes for this frame. Empty for close frames.
    pub data: Vec<u8>,
    /// `true` for the first frame of a new stream.
    pub is_init: bool,
    /// `true` when the sender signals end-of-stream.
    pub is_close: bool,
}

impl StreamFrame {
    /// Creates a regular data frame for an already-open stream.
    pub fn new_data(stream_id: u64, data: Vec<u8>) -> Self {
        Self {
            stream_id,
            data,
            is_init: false,
            is_close: false,
        }
    }

    /// Creates an init frame that opens a new stream and carries initial data.
    pub fn new_init(stream_id: u64, data: Vec<u8>) -> Self {
        Self {
            stream_id,
            data,
            is_init: true,
            is_close: false,
        }
    }

    /// Creates a close frame signalling end-of-stream.
    ///
    /// The `data` field is empty for close frames.
    pub fn new_close(stream_id: u64) -> Self {
        Self {
            stream_id,
            data: Vec::new(),
            is_init: false,
            is_close: true,
        }
    }

    /// Serialises the frame to bytes using bincode.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Serialization`] if serialisation fails.
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        bincode::serialize(self).map_err(|e| ProtocolError::Serialization(e.to_string()))
    }

    /// Deserialises a `StreamFrame` from a byte slice.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Deserialization`] if deserialisation fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        bincode::deserialize(bytes).map_err(|e| ProtocolError::Deserialization(e.to_string()))
    }
}

/// A message that arrives at the gossip node's internal routing loop.
///
/// The router distinguishes between messages that originated from a remote
/// peer (`FromPeer`) and locally injected messages that have not yet been
/// signed or wrapped (`FromLocal`).
#[derive(Debug)]
pub enum IncomingMessage {
    /// A [`GossipMessage`] that was received from a remote peer.
    FromPeer {
        /// The overlay address of the peer that sent the message.
        sender: OverlayAddress,
        /// The full gossip message.
        msg: GossipMessage,
    },
    /// A message injected by the local application via
    /// [`OverlayContext::broadcast`] or [`OverlayContext::send_to`].
    ///
    /// [`OverlayContext::broadcast`]: crate::overlay::context::OverlayContext::broadcast
    /// [`OverlayContext::send_to`]: crate::overlay::context::OverlayContext::send_to
    FromLocal {
        /// Target address. `None` = broadcast; `Some(addr)` = unicast.
        recipient: Option<OverlayAddress>,
        /// Raw payload bytes to be wrapped in a [`GossipMessage`].
        payload: Vec<u8>,
    },
}
