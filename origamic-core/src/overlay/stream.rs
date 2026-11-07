//! Virtual stream types for the overlay network.
//!
//! This module provides:
//!
//! - [`OverlayStream`] – a duplex async stream that represents a single
//!   logical connection between two overlay nodes. It implements both
//!   [`AsyncRead`] and [`AsyncWrite`] and supports graceful shutdown via an
//!   optional [`oneshot`] signal.
//! - [`OverlayListener`] – accepts incoming [`OverlayStream`]s from remote
//!   peers.
//!
//! [`AsyncRead`]: tokio::io::AsyncRead
//! [`AsyncWrite`]: tokio::io::AsyncWrite
//! [`oneshot`]: tokio::sync::oneshot

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::overlay::address::OverlayAddress;

/// A logical duplex stream between two overlay network nodes.
///
/// `OverlayStream` wraps a [`DuplexStream`] created by
/// [`tokio::io::duplex`] and adds:
///
/// - A `peer_addr` field that identifies the remote node.
/// - An optional `shutdown_rx` oneshot receiver that, when present, keeps
///   [`poll_shutdown`] pending until the corresponding sender fires. This lets
///   the outbound bridge task signal to the caller that all buffered data has
///   been flushed to the gossip network before the stream is considered closed.
///
/// [`poll_shutdown`]: OverlayStream::poll_shutdown
pub struct OverlayStream {
    pub(crate) inner: DuplexStream,
    pub(crate) peer_addr: OverlayAddress,
    pub(crate) shutdown_rx: Option<oneshot::Receiver<()>>,
}

impl Unpin for OverlayStream {}

impl OverlayStream {
    /// Creates an `OverlayStream` without a shutdown receiver.
    ///
    /// `poll_shutdown` completes as soon as the inner [`DuplexStream`] shuts
    /// down.
    pub fn new(inner: DuplexStream, peer_addr: OverlayAddress) -> Self {
        Self {
            inner,
            peer_addr,
            shutdown_rx: None,
        }
    }

    /// Creates an `OverlayStream` with a shutdown receiver for graceful close
    /// coordination.
    ///
    /// `poll_shutdown` will not return `Ready` until the inner stream shuts
    /// down **and** `shutdown_rx` fires, ensuring the bridge task has
    /// acknowledged the end-of-stream condition.
    pub fn with_shutdown_rx(
        inner: DuplexStream,
        peer_addr: OverlayAddress,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            inner,
            peer_addr,
            shutdown_rx: Some(shutdown_rx),
        }
    }

    /// Returns the [`OverlayAddress`] of the remote peer this stream is
    /// connected to.
    pub fn peer_address(&self) -> OverlayAddress {
        self.peer_addr
    }
}

/// Delegates reads to the inner [`DuplexStream`].
impl AsyncRead for OverlayStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

/// Delegates writes and flush to the inner [`DuplexStream`].
///
/// `poll_shutdown` additionally waits for the optional shutdown signal so that
/// the bridge task can complete any in-flight close-frame delivery before the
/// stream is considered fully shut down.
impl AsyncWrite for OverlayStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = Pin::new(&mut self.inner).poll_shutdown(cx)?;
        if let Some(rx) = self.shutdown_rx.as_mut() {
            match Pin::new(rx).poll(cx) {
                Poll::Ready(_) => {
                    self.shutdown_rx = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

/// Accepts incoming virtual streams from remote overlay peers.
///
/// `OverlayListener` wraps the receiving end of the inbound stream channel
/// that is populated by [`OverlayContext`]'s internal routing task. Multiple
/// handles can share the same underlying receiver via `Arc<Mutex<_>>`.
///
/// [`OverlayContext`]: crate::overlay::context::OverlayContext
pub struct OverlayListener {
    pub(crate) inbound_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>>,
    local_addr: OverlayAddress,
}

impl OverlayListener {
    /// Creates an `OverlayListener` from a shared inbound stream receiver.
    pub fn new(
        inbound_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>>,
        local_addr: OverlayAddress,
    ) -> Self {
        Self {
            inbound_rx,
            local_addr,
        }
    }

    /// Waits for and returns the next incoming stream along with the remote
    /// peer's [`OverlayAddress`].
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] with kind [`BrokenPipe`] when the internal
    /// channel has been closed (i.e. the owning [`OverlayContext`] has been
    /// dropped).
    ///
    /// [`BrokenPipe`]: std::io::ErrorKind::BrokenPipe
    /// [`OverlayContext`]: crate::overlay::context::OverlayContext
    pub async fn accept(&mut self) -> io::Result<(OverlayStream, OverlayAddress)> {
        let mut rx = self.inbound_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "OverlayListener closed"))
    }

    pub fn local_addr(&self) -> io::Result<OverlayAddress> {
        Ok(self.local_addr)
    }
}
