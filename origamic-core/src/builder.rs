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

pub struct NetworkBuilder<F> {
    future: F,
}

impl<B: Factory> NetworkBuilder<Ready<Result<B>>> {
    pub fn new(base: B) -> Self {
        Self {
            future: ready(Ok(base)),
        }
    }
}

impl<A: Factory + Send, F: Future<Output = Result<A>> + Send> NetworkBuilder<F> {
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

    pub fn overlay(
        self,
        builder: OverlayContextBuilder<A, Empty>,
    ) -> NetworkBuilder<impl Future<Output = Result<OverlayContext>> + Send> {
        self.layer(async |lower| builder.lower_factory(Arc::new(lower)).build().await)
    }

    pub async fn build(self) -> F::Output {
        self.future.await
    }
}
