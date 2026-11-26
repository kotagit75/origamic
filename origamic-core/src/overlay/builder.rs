//! Builder for configuring and creating an [`OverlayContext`].
//!
//! [`OverlayContext`] is created after configuring a lower-layer [`Factory`],
//! network identifier, peer addresses, and other optional settings.
//!
//! [`Factory`]: crate::factory::Factory
//! [`OverlayContext`]: crate::overlay::context::OverlayContext

use std::{marker::PhantomData, sync::Arc};

use uuid::Uuid;

use crate::{
    factory::Factory,
    overlay::{address::Keypair, context::OverlayContext},
};

/// Marker type indicating that the lower-layer factory has not been configured.
pub struct Filled;

/// Marker type indicating that the lower-layer factory has been configured.
pub struct Empty;

/// Builder for creating an [`OverlayContext`].
///
/// The builder uses the typestate pattern to ensure that a lower-layer factory
/// is configured before [`build`](Self::build) can be called.
///
/// A random network identifier and keypair are generated when they are not
/// explicitly provided. No lower-layer peers are configured by default, and
/// inbound connections are disabled unless an advertised address is provided.
pub struct OverlayContextBuilder<F: Factory, LowerFactory> {
    network_id: Option<Uuid>,
    lower_factory: Option<Arc<F>>,
    lower_factory_state: PhantomData<LowerFactory>,
    keypair: Option<Keypair>,
    lower_peers: Vec<F::Address>,
    advertise_addr: Option<F::Address>,
}

impl<F: Factory> Default for OverlayContextBuilder<F, Empty> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: Factory> OverlayContextBuilder<F, Empty> {
    /// Creates an empty builder.
    ///
    /// The lower-layer factory must be configured with
    /// [`lower_factory`](Self::lower_factory) before the overlay context can
    /// be built.
    pub fn new() -> Self {
        Self {
            network_id: None,
            lower_factory: None,
            lower_factory_state: PhantomData,
            keypair: None,
            lower_peers: Vec::new(),
            advertise_addr: None,
        }
    }
}

impl<F: Factory, LowerFactory> OverlayContextBuilder<F, LowerFactory> {
    /// Sets the UUID identifying the overlay network.
    ///
    /// Nodes must use the same network identifier to exchange messages within
    /// the same overlay network. When this method is not called, a random UUID
    /// is generated during [`build`](Self::build).
    pub fn network_id(mut self, network_id: Uuid) -> Self {
        self.network_id = Some(network_id);
        self
    }

    /// Sets the keypair used to identify and sign messages from this node.
    ///
    /// When this method is not called, a new keypair is generated during
    /// [`build`](Self::build).
    pub fn keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = Some(keypair);
        self
    }

    /// Sets the initial peers used to join the overlay network.
    ///
    /// The addresses are passed to the lower-layer gossip transport when the
    /// overlay context is built.
    pub fn lower_peers(mut self, lower_peers: Vec<F::Address>) -> Self {
        self.lower_peers = lower_peers;
        self
    }

    /// Sets the address advertised to other peers.
    ///
    /// The address is used by the lower-layer transport to accept inbound
    /// connections. When this method is not called, the node does not advertise
    /// an address for inbound connections.
    pub fn advertise_addr(mut self, advertise_addr: F::Address) -> Self {
        self.advertise_addr = Some(advertise_addr);
        self
    }
}

impl<F: Factory> OverlayContextBuilder<F, Empty> {
    /// Sets the lower-layer transport and enables building the overlay context.
    ///
    /// This method changes the builder state from [`Empty`] to [`Filled`].
    pub fn lower_factory(self, lower_factory: Arc<F>) -> OverlayContextBuilder<F, Filled> {
        OverlayContextBuilder {
            network_id: self.network_id,
            lower_factory: Some(lower_factory),
            lower_factory_state: PhantomData,
            keypair: self.keypair,
            lower_peers: self.lower_peers,
            advertise_addr: self.advertise_addr,
        }
    }
}

impl<F: Factory> OverlayContextBuilder<F, Filled> {
    /// Builds the configured [`OverlayContext`].
    ///
    /// A random network identifier and keypair are generated for values that
    /// were not explicitly configured. The lower-layer listener is started
    /// before this method returns.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] if the lower-layer listener fails to
    /// start.
    pub async fn build(self) -> std::io::Result<OverlayContext> {
        OverlayContext::try_with_options(
            self.network_id.unwrap_or_else(Uuid::new_v4),
            self.keypair.unwrap_or_else(Keypair::generate),
            self.lower_factory.unwrap(),
            self.lower_peers,
            self.advertise_addr,
        )
        .await
    }
}
