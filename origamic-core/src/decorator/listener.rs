use std::marker::PhantomData;

use crate::{
    decorator::{decorate::Decorate, stream::DecoratedStream},
    factory::{Factory, Listener},
};

pub struct DecoratedListener<F: Factory, D: Decorate> {
    inner: F::Listener,
    phantom_data: PhantomData<D>,
}

impl<F: Factory, D: Decorate> DecoratedListener<F, D> {
    pub fn new(inner: F::Listener) -> Self {
        Self {
            inner,
            phantom_data: PhantomData,
        }
    }
}

impl<F: Factory, D: Decorate> Listener for DecoratedListener<F, D> {
    type Stream = DecoratedStream<F::Stream, D>;

    fn accept(
        &mut self,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send + '_>> {
        Box::pin(async move { Ok(DecoratedStream::new(self.inner.accept().await?)) })
    }
}
