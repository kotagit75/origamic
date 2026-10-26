use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::overlay::address::OverlayAddress;

pub struct OverlayStream {
    pub(crate) inner: DuplexStream,
    pub(crate) peer_addr: OverlayAddress,
    pub(crate) shutdown_rx: Option<oneshot::Receiver<()>>,
}

impl Unpin for OverlayStream {}

impl OverlayStream {
    pub fn new(inner: DuplexStream, peer_addr: OverlayAddress) -> Self {
        Self {
            inner,
            peer_addr,
            shutdown_rx: None,
        }
    }

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

    pub fn peer_address(&self) -> OverlayAddress {
        self.peer_addr
    }
}

impl AsyncRead for OverlayStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for OverlayStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
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

pub struct OverlayListener {
    pub(crate) inbound_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>>,
}

impl OverlayListener {
    pub fn new(inbound_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>>) -> Self {
        Self { inbound_rx }
    }

    pub async fn accept(&mut self) -> std::io::Result<(OverlayStream, OverlayAddress)> {
        let mut rx = self.inbound_rx.lock().await;
        rx.recv().await.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "OverlayListener closed")
        })
    }
}
