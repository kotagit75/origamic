use std::{marker::PhantomData, sync::Arc};

use uuid::Uuid;

use crate::{
    factory::Factory,
    overlay::{address::Keypair, context::OverlayContext},
};

pub struct Filled;
pub struct Empty;

pub struct OverlayContextBuilder<F: Factory, LowerFactory> {
    network_id: Option<Uuid>,
    lower_factory: Option<Arc<F>>,
    lower_factory_state: PhantomData<LowerFactory>,
    keypair: Option<Keypair>,
    lower_peers: Vec<F::Address>,
    advertise_addr: Option<F::Address>,
}

impl<F: Factory> OverlayContextBuilder<F, Empty> {
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
    pub fn network_id(mut self, network_id: Uuid) -> Self {
        self.network_id = Some(network_id);
        self
    }
    pub fn keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = Some(keypair);
        self
    }
    pub fn lower_peers(mut self, lower_peers: Vec<F::Address>) -> Self {
        self.lower_peers = lower_peers;
        self
    }
    pub fn advertise_addr(mut self, advertise_addr: F::Address) -> Self {
        self.advertise_addr = Some(advertise_addr);
        self
    }
}

impl<F: Factory> OverlayContextBuilder<F, Empty> {
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
    pub async fn build(self) -> std::io::Result<OverlayContext> {
        OverlayContext::try_with_options(
            self.network_id.unwrap_or_else(|| Uuid::new_v4()),
            self.keypair.unwrap_or_else(Keypair::generate),
            self.lower_factory.unwrap(),
            self.lower_peers,
            self.advertise_addr,
        )
        .await
    }
}
