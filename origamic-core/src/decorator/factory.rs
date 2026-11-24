use std::marker::PhantomData;

use crate::{
    decorator::{decorate::Decorate, listener::DecoratedListener, stream::DecoratedStream},
    factory::Factory,
};

pub struct Decorator<F: Factory, D: Decorate> {
    inner_factory: F,
    phantom_data: PhantomData<D>,
}

impl<F: Factory, D: Decorate> Decorator<F, D> {
    pub fn new(inner_factory: F) -> Self {
        Self {
            inner_factory,
            phantom_data: PhantomData,
        }
    }
}

impl<F: Factory, D: Decorate> Factory for Decorator<F, D> {
    type Address = F::Address;
    type Stream = DecoratedStream<F::Stream, D>;
    type Listener = DecoratedListener<F, D>;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        let future = self.inner_factory.connect(addr);
        Box::pin(async move {
            let inner_stream = future.await?;
            Ok(DecoratedStream::new(inner_stream))
        })
    }

    fn listen(
        &self,
        bind_addr: Option<Self::Address>,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<Self::Listener>> + Send>> {
        let future = self.inner_factory.listen(bind_addr);
        Box::pin(async move { Ok(DecoratedListener::new(future.await?)) })
    }
}
