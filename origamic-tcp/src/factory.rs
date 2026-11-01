//! TCP-backed implementation of the [`Factory`] trait.
//!
//! [`TcpFactory`] connects to peers via plain TCP and optionally binds a
//! [`TcpListener`] to accept inbound gossip connections.
//!
//! [`Factory`]: origamic_core::factory::Factory

use origamic_core::{
    factory::Factory, gossip::node::process_inbound_stream, overlay::message::GossipMessage,
};
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// A [`Factory`] implementation that uses TCP as the transport layer.
///
/// `TcpFactory` can operate in two modes:
///
/// - **Outbound-only** (created with [`new`]): calls to
///   [`start_listener`] are a no-op unless the caller passes an explicit bind
///   address.
/// - **Listening** (created with [`listening`]): binds a TCP listener on the
///   given address when [`start_listener`] is called.
///
/// The factory is `Copy` and cheap to clone, so it can be freely shared.
///
/// [`Factory`]: origamic_core::factory::Factory
/// [`new`]: TcpFactory::new
/// [`listening`]: TcpFactory::listening
/// [`start_listener`]: TcpFactory::start_listener
#[derive(Clone, Copy, Debug, Default)]
pub struct TcpFactory {
    bind_addr: Option<SocketAddr>,
}

impl TcpFactory {
    /// Creates an outbound-only `TcpFactory` with no pre-configured bind
    /// address.
    ///
    /// [`start_listener`] will be a no-op unless the caller passes an explicit
    /// `bind_addr`.
    ///
    /// [`start_listener`]: TcpFactory::start_listener
    pub fn new() -> Self {
        Self { bind_addr: None }
    }

    /// Creates a `TcpFactory` that will bind a listener on `bind_addr` when
    /// [`start_listener`] is called.
    ///
    /// [`start_listener`]: TcpFactory::start_listener
    pub fn listening(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr: Some(bind_addr),
        }
    }

    /// Returns the pre-configured bind address, if any.
    pub fn bind_addr(&self) -> Option<SocketAddr> {
        self.bind_addr
    }
}

impl Factory for TcpFactory {
    type Stream = TcpStream;
    type Address = SocketAddr;

    /// Opens a TCP connection to `addr`.
    ///
    /// The returned future resolves to a [`TcpStream`] on success, or an
    /// [`std::io::Error`] if the connection cannot be established.
    fn connect(
        &self,
        addr: Self::Address,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Self::Stream>> + Send>>
    {
        Box::pin(async move { Self::Stream::connect(addr).await })
    }

    /// Binds a TCP listener and begins accepting inbound gossip connections.
    ///
    /// The effective bind address is resolved as follows (first wins):
    /// 1. The address stored in `self` (set by [`listening`]).
    /// 2. The `bind_addr` argument passed by the caller.
    ///
    /// If neither is present the function is a no-op and returns `Ok(())`.
    ///
    /// On success, a background task is spawned that accepts TCP connections
    /// and hands each new stream to [`process_inbound_stream`], which drives
    /// the gossip handshake and forwards decoded messages to `peer_tx`.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] if [`TcpListener::bind`] fails (e.g. the
    /// port is already in use or the address is invalid).
    ///
    /// [`listening`]: TcpFactory::listening
    /// [`process_inbound_stream`]: origamic_core::gossip::node::process_inbound_stream
    fn start_listener(
        &self,
        bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>> {
        let target_addr = self.bind_addr.or(bind_addr);
        if let Some(addr) = target_addr {
            Box::pin(async move {
                let listener = TcpListener::bind(addr).await?;
                tokio::spawn(async move {
                    while let Ok((stream, _)) = listener.accept().await {
                        let tx = peer_tx.clone();
                        tokio::spawn(async move {
                            let _ = process_inbound_stream(stream, tx).await;
                        });
                    }
                });
                Ok(())
            })
        } else {
            Box::pin(async move { Ok(()) })
        }
    }
}
