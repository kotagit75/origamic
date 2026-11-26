//! Lower-layer listener adapter that performs Noise responder handshakes.

use std::{future::Future, pin::Pin};

use origamic_core::factory::{Factory, Listener};

use crate::{
    factory::{NoiseConfig, perform_responder_handshake},
    stream::NoiseStream,
};

/// Listener that performs the responder side of a Noise handshake for every
/// accepted lower-layer connection.
///
/// The application receives a [`NoiseStream`] only after the handshake
/// completes and the configured remote-key verifier accepts the peer.
pub struct NoiseListener<F: Factory> {
    inner: F::Listener,
    config: NoiseConfig,
}

impl<F: Factory> NoiseListener<F> {
    pub(crate) fn new(inner: F::Listener, config: NoiseConfig) -> Self {
        Self { inner, config }
    }
}

impl<F: Factory> Listener for NoiseListener<F> {
    type Stream = NoiseStream<F::Stream>;

    fn accept(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send + '_>> {
        let accept = self.inner.accept();
        let config = self.config.clone();
        Box::pin(async move {
            let lower = accept.await?;
            perform_responder_handshake(lower, &config).await
        })
    }
}
