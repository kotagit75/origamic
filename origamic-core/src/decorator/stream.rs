use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::decorator::decorate::Decorate;

pub struct DecoratedStream<S, D: Decorate> {
    inner: S,
    phantom_data: PhantomData<D>,
}

impl<S, D: Decorate> DecoratedStream<S, D> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            phantom_data: PhantomData,
        }
    }
}

impl<S: AsyncRead + Unpin, D: Decorate> AsyncRead for DecoratedStream<S, D> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = poll {
            D::from_lower(buf);
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin, D: Decorate> AsyncWrite for DecoratedStream<S, D> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let decorated = D::from_upper(buf);
        Pin::new(&mut self.inner).poll_write(cx, &decorated)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
