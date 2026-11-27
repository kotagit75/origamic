//! The [`Factory`] trait – the transport abstraction layer for origamic.
//!
//! Any type that knows how to **open outgoing connections** and **accept
//! incoming connections** can implement `Factory` and be used as a lower-layer
//! transport for [`OverlayContext`](crate::overlay::context::OverlayContext).
//!
//! The built-in TCP implementation lives in the `origamic-tcp` crate.

use crate::stream::IoStream;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

/// Transport-specific listener for accepted inbound streams.
///
/// This abstraction deliberately contains no gossip or overlay protocol types.
pub trait Listener: Send + Unpin + 'static {
    type Stream: IoStream + Send + Unpin + 'static;
    /// Address type accepted by the corresponding factory's `connect` method.
    type Address;

    /// Accepts an inbound stream and optionally returns a dial-back address.
    ///
    /// `Some(address)` must be an address that can be passed to the matching
    /// factory's `connect` method to reach the remote peer. Return `None` when
    /// the observed transport endpoint is not a reusable peer address.
    fn accept(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Option<Self::Address>)>> + Send + '_>>;
}

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
    type Stream: IoStream + Send + Unpin + 'static;

    /// The listener type used to accept inbound streams.
    type Listener: Listener<Stream = Self::Stream, Address = Self::Address>;

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

    /// Create a listener for inbound connections.
    ///
    /// # Errors
    ///
    /// Returns an `std::io::Error` if binding or starting the listener fails
    /// (e.g. the port is already in use).
    fn listen(
        &self,
        bind_addr: Option<Self::Address>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::Listener>> + Send>>;
}

/// Blanket implementation so that `Arc<F: Factory>` is itself a `Factory`.
///
/// This allows stacking overlay layers without unwrapping the `Arc`.
impl<T: ?Sized + Factory> Factory for Arc<T> {
    type Stream = T::Stream;
    type Listener = T::Listener;
    type Address = T::Address;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        (**self).connect(addr)
    }

    fn listen(
        &self,
        bind_addr: Option<Self::Address>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::Listener>> + Send>> {
        (**self).listen(bind_addr)
    }
}
