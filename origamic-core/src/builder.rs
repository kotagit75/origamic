use crate::{
    discovery::PeerDiscovery,
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

impl<
    Addr: Send + Sync,
    A: Factory<Address = Addr> + PeerDiscovery<Address = Addr> + Send + Clone,
    F: Future<Output = Result<A>> + Send,
> NetworkBuilder<F>
{
    /// Adds an overlay that discovers peers from the preceding overlay layer.
    ///
    /// The factory produced by the previous stage is used both as the new
    /// overlay's lower transport and as its [`PeerDiscovery`] provider. Known
    /// peers are added during initialization, and peers discovered later are
    /// added as updates arrive. The transport and discovery address types must
    /// match.
    ///
    /// This is useful when stacking overlays: the new layer can use peers
    /// already known to the lower [`OverlayContext`] without configuring them
    /// again in `builder`.
    ///
    /// # Errors
    ///
    /// The error from a previous builder stage or from creating the overlay
    /// context is propagated by [`build`](Self::build).
    pub fn overlay_with_peer_discovery(
        self,
        builder: OverlayContextBuilder<A, Empty>,
    ) -> NetworkBuilder<impl Future<Output = Result<OverlayContext>> + Send> {
        self.layer(async |lower| {
            builder
                .peer_discovery(Arc::new(lower.clone()))
                .lower_factory(Arc::new(lower))
                .build()
                .await
        })
    }
}
