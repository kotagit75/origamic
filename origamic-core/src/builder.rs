use crate::{
    factory::Factory,
    overlay::{
        builder::{Empty, OverlayContextBuilder},
        context::OverlayContext,
    },
};
use std::{
    future::{Future, Ready, ready},
    io::Result,
    sync::Arc,
};

/// Composes transport factories and builds an overlay network.
///
/// A builder starts with a base [`Factory`], can add asynchronous factory
/// layers with [`layer`](Self::layer), and can finish by adding an
/// [`OverlayContext`]. Errors from
/// any stage are propagated through the builder and returned by [`build`](Self::build).
pub struct NetworkBuilder<F> {
    future: F,
}

impl<B: Factory> NetworkBuilder<Ready<Result<B>>> {
    /// Starts a factory stack with an already-created base transport.
    pub fn new(base: B) -> Self {
        Self {
            future: ready(Ok(base)),
        }
    }
}

impl<A: Factory + Send, F: Future<Output = Result<A>> + Send> NetworkBuilder<F> {
    /// Adds an asynchronous factory layer above the current transport.
    ///
    /// The closure receives the factory produced by the preceding stage. Its
    /// result becomes the transport for subsequent layers.
    pub fn layer<C: Factory, Fut, Fun>(
        self,
        layer: Fun,
    ) -> NetworkBuilder<impl Future<Output = Result<C>> + Send>
    where
        Fut: Future<Output = Result<C>> + Send,
        Fun: FnOnce(A) -> Fut + Send,
    {
        NetworkBuilder {
            future: async move { layer(self.future.await?).await },
        }
    }

    /// Adds an overlay context above the current factory.
    ///
    /// The overlay context uses the configured lower factory from `builder`.
    pub fn overlay(
        self,
        builder: OverlayContextBuilder<A, Empty>,
    ) -> NetworkBuilder<impl Future<Output = Result<OverlayContext>> + Send> {
        self.layer(async |lower| builder.lower_factory(Arc::new(lower)).build().await)
    }

    /// Runs all configured stages and returns the final factory or an error.
    pub async fn build(self) -> F::Output {
        self.future.await
    }
}
