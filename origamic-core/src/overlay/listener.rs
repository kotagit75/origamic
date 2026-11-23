use std::{pin::Pin, sync::Arc};

use tokio::{
    io,
    sync::{Mutex, mpsc},
};

use crate::{
    factory::Listener,
    overlay::{address::OverlayAddress, stream::OverlayStream},
};

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

impl Listener for OverlayListener {
    type Stream = OverlayStream;

    fn accept(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send + '_>> {
        Box::pin(async move {
            let (stream, _) = self.accept().await?;
            Ok(stream)
        })
    }
}
