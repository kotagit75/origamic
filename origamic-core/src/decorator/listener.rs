//! Listener wrapper for decorated transport streams.

use std::marker::PhantomData;

use crate::{
    decorator::{decorate::Decorate, stream::DecoratedStream},
    factory::{Factory, Listener},
};

/// Listener that wraps streams accepted by an inner factory listener.
///
/// Accepted streams retain the lower listener's lifecycle and address
/// semantics while each stream is exposed as a [`DecoratedStream`].
pub struct DecoratedListener<F: Factory, D: Decorate> {
    inner: F::Listener,
    phantom_data: PhantomData<D>,
}

impl<F: Factory, D: Decorate> DecoratedListener<F, D> {
    /// Creates a decorated listener around `inner`.
    pub fn new(inner: F::Listener) -> Self {
        Self {
            inner,
            phantom_data: PhantomData,
        }
    }
}

impl<F: Factory, D: Decorate> Listener for DecoratedListener<F, D> {
    type Stream = DecoratedStream<F::Stream, D>;
    type Address = F::Address;

    fn accept(
        &mut self,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = std::io::Result<(Self::Stream, Option<Self::Address>)>> + Send + '_,
        >,
    > {
        Box::pin(async move {
            let (stream, address) = self.inner.accept().await?;
            Ok((DecoratedStream::new(stream), address))
        })
    }
}
