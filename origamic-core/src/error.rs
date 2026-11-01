//! Error types for the origamic-core library.
//!
//! All public APIs that can fail use one of the structured error types defined here,
//! giving callers fine-grained information without exposing internal implementation
//! details such as channel message types.

use thiserror::Error;

/// Top-level error type that encompasses all failures in origamic-core.
///
/// Individual variants wrap the more specific error types so that callers can
/// use the `?` operator to propagate any origamic error through a single
/// `Result<_, origamic_core::Error>` return type.
#[derive(Error, Debug)]
pub enum Error {
    /// An [`OverlayAddress`](crate::overlay::address::OverlayAddress) could not be parsed.
    #[error("Address error: {0}")]
    Address(#[from] AddressParseError),

    /// A gossip message or stream frame could not be serialized or deserialized.
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// A message could not be delivered to the overlay node's internal channel.
    #[error("Send error: {0}")]
    Send(#[from] SendError),

    /// An I/O error from the underlying transport or async runtime.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors that occur when parsing an [`OverlayAddress`](crate::overlay::address::OverlayAddress)
/// from a hex string or via [`std::str::FromStr`].
///
/// # Examples
///
/// ```
/// use origamic_core::overlay::address::OverlayAddress;
/// use origamic_core::error::AddressParseError;
/// use std::str::FromStr;
///
/// let err = OverlayAddress::from_str("short").unwrap_err();
/// assert!(matches!(err, AddressParseError::InvalidLength { expected: 64, actual: 5 }));
/// ```
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AddressParseError {
    /// The input string had the wrong number of hex characters.
    ///
    /// A valid `OverlayAddress` is exactly 32 bytes, represented as 64 lowercase
    /// hexadecimal characters.
    #[error("Invalid address length: expected {expected} hex characters, got {actual}")]
    InvalidLength { expected: usize, actual: usize },

    /// A character that is not a valid hexadecimal digit was found.
    ///
    /// `index` is the zero-based byte offset within the input string and
    /// `character` is the offending character.
    #[error("Invalid hex character at index {index}: '{character}'")]
    InvalidHex { index: usize, character: char },

    /// The 32 raw bytes form a valid hex string but do not represent a valid
    /// Ed25519 compressed public key point.
    #[error("Invalid Ed25519 public key")]
    InvalidPublicKey,
}

/// Errors that arise from encoding, decoding, or verifying overlay protocol messages.
///
/// These errors are returned by [`GossipMessage`](crate::overlay::message::GossipMessage),
/// [`StreamFrame`](crate::overlay::message::StreamFrame), and
/// [`PeerExchangePayload`](crate::overlay::message::PeerExchangePayload).
#[derive(Error, Debug)]
pub enum ProtocolError {
    /// `bincode` serialization of an outgoing message failed.
    #[error("Serialization failed: {0}")]
    Serialization(String),

    /// `bincode` deserialization of received bytes failed – the data was malformed
    /// or the buffer was truncated.
    #[error("Deserialization failed: {0}")]
    Deserialization(String),

    /// The Ed25519 signature embedded in a [`GossipMessage`](crate::overlay::message::GossipMessage)
    /// did not verify against the claimed sender public key.
    #[error("Signature verification failed")]
    InvalidSignature,

    /// A [`StreamFrame`](crate::overlay::message::StreamFrame) contained inconsistent
    /// or otherwise invalid field values.
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
}

impl From<bincode::Error> for ProtocolError {
    fn from(err: bincode::Error) -> Self {
        ProtocolError::Deserialization(err.to_string())
    }
}

/// Errors that occur when sending a message to an overlay node's internal channel.
///
/// This type intentionally hides the concrete channel message type
/// (`OutgoingLocalMessage`) so it does not appear in public API signatures.
///
/// # Example
///
/// ```
/// use origamic_core::error::SendError;
///
/// async fn example() {
///     let (tx, rx) = tokio::sync::mpsc::channel::<u32>(1);
///     drop(rx);
///     let err: SendError = tx.send(42).await.map_err(Into::into).unwrap_err();
///     assert_eq!(err, SendError::ChannelClosed);
/// }
/// ```
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// The receiving end of the internal channel has been dropped, which typically
    /// means the overlay node has shut down.
    #[error("Internal channel closed; the overlay node may have shut down")]
    ChannelClosed,
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for SendError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        SendError::ChannelClosed
    }
}
