//! The [`Factory`] trait – the transport abstraction layer for origamic.
//!
//! Any type that knows how to **open outgoing connections** and **accept
//! incoming connections** can implement `Factory` and be used as a lower-layer
//! transport for [`OverlayContext`](crate::overlay::context::OverlayContext).
//!
//! The built-in TCP implementation lives in the `origamic-tcp` crate.

use crate::{overlay::message::GossipMessage, stream::LayerStream};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Transport abstraction that drives the overlay network.
///
/// Implement this trait to plug in a custom transport (e.g. QUIC, WebSocket,
/// in-memory `DuplexStream` for testing).  A blanket implementation for
/// `Arc<T: Factory>` is provided so that `Arc`-wrapped factories can be
/// composed and stacked without extra boilerplate.
///
/// # Associated Types
///
/// - `Stream` – the bidirectional byte-stream type produced by [`connect`](Factory::connect)
///   and consumed by the gossip engine.
/// - `Address` – the addressing scheme (e.g. `SocketAddr` for TCP,
///   [`OverlayAddress`](crate::overlay::address::OverlayAddress) for a higher
///   overlay layer).
pub trait Factory: Send + Sync + Unpin + 'static {
    /// The bidirectional stream type returned by [`connect`](Factory::connect).
    type Stream: LayerStream + Send + Unpin + 'static;

    /// The network address type used to identify peers.
    ///
    /// Must be serializable so it can be exchanged via peer-exchange (PEX)
    /// gossip messages.
    type Address: Clone
        + Send
        + Sync
        + Unpin
        + Serialize
        + DeserializeOwned
        + Eq
        + Hash
        + Debug
        + 'static;

    /// Open an outgoing connection to `addr` and return the resulting stream.
    ///
    /// # Errors
    ///
    /// Returns an `std::io::Error` if the connection attempt fails (e.g. the
    /// remote host is unreachable or the address is invalid).
    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>>;

    /// Start accepting inbound connections and forward each new stream to the
    /// gossip engine via `peer_tx`.
    ///
    /// The default implementation is a no-op, which is appropriate for
    /// client-only or outbound-only transports.
    ///
    /// # Parameters
    ///
    /// - `bind_addr` – the local address to listen on.  When `None` the
    ///   implementation should choose a suitable default (or do nothing).
    /// - `peer_tx` – the channel into which raw [`GossipMessage`] frames
    ///   decoded from each accepted stream should be sent.
    ///
    /// # Errors
    ///
    /// Returns an `std::io::Error` if binding or starting the listener fails
    /// (e.g. the port is already in use).
    fn start_listener(
        &self,
        bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> {
        let _ = (bind_addr, peer_tx);
        Box::pin(async move { Ok(()) })
    }
}

/// Blanket implementation so that `Arc<F: Factory>` is itself a `Factory`.
///
/// This allows stacking overlay layers without unwrapping the `Arc`.
impl<T: ?Sized + Factory> Factory for Arc<T> {
    type Stream = T::Stream;
    type Address = T::Address;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        (**self).connect(addr)
    }

    fn start_listener(
        &self,
        bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> {
        (**self).start_listener(bind_addr, peer_tx)
    }
}
