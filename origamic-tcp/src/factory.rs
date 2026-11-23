//! TCP-backed implementation of the [`Factory`] trait.
//!
//! [`TcpFactory`] connects to peers via plain TCP and optionally binds a
//! [`TcpListener`] to accept inbound transport streams.
//!
//! [`Factory`]: origamic_core::factory::Factory

use origamic_core::factory::Factory;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::{TcpListener, TcpStream};

use crate::listener::TcpListenerAdapter;

/// A [`Factory`] implementation that uses TCP as the transport layer.
///
/// `TcpFactory` can operate in two modes:
///
/// - **Outbound-only** (created with [`new`]): [`listen`] creates no listener
///   unless the caller passes an explicit bind address.
/// - **Listening** (created with [`listening`]): binds a TCP listener on the
///   given address when [`listen`] is called.
///
/// The factory is `Copy` and cheap to clone, so it can be freely shared.
///
/// [`Factory`]: origamic_core::factory::Factory
/// [`new`]: TcpFactory::new
/// [`listening`]: TcpFactory::listening
/// [`listen`]: Factory::listen
#[derive(Clone, Copy, Debug, Default)]
pub struct TcpFactory {
    bind_addr: Option<SocketAddr>,
}

impl TcpFactory {
    /// Creates an outbound-only `TcpFactory` with no pre-configured bind
    /// address.
    ///
    /// [`listen`] will create no listener unless the caller passes an explicit
    /// `bind_addr`.
    ///
    /// [`listen`]: Factory::listen
    pub fn new() -> Self {
        Self { bind_addr: None }
    }

    /// Creates a `TcpFactory` that will bind a listener on `bind_addr` when
    /// [`listen`] is called.
    ///
    /// [`listen`]: Factory::listen
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
    type Listener = TcpListenerAdapter;
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

    /// Binds a TCP listener for accepting inbound transport streams.
    ///
    /// The effective bind address is resolved as follows (first wins):
    /// 1. The address stored in `self` (set by [`listening`]).
    /// 2. The `bind_addr` argument passed by the caller.
    ///
    /// If neither is present, the returned listener waits forever without
    /// accepting any streams.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] if [`TcpListener::bind`] fails (e.g. the
    /// port is already in use or the address is invalid).
    ///
    /// [`listening`]: TcpFactory::listening
    fn listen(
        &self,
        bind_addr: Option<Self::Address>,
    ) -> Pin<Box<dyn std::future::Future<Output = std::io::Result<Self::Listener>> + Send>> {
        let target_addr = self.bind_addr.or(bind_addr);
        Box::pin(async move {
            let listener = match target_addr {
                Some(addr) => Some(TcpListener::bind(addr).await?),
                None => None,
            };
            Ok(TcpListenerAdapter { listener })
        })
    }
}
