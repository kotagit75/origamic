use bytes::{Bytes, BytesMut};
use futures::{Sink, Stream};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{Decoder, Encoder, Framed};

/// Adapts a framed byte codec to Tokio's [`AsyncRead`] and [`AsyncWrite`] traits.
///
/// Each decoded frame is exposed as a byte stream. If a frame is larger than
/// the caller's read buffer, the unconsumed bytes are retained for subsequent
/// reads. Writes are encoded as individual frames; callers should flush when
/// buffered frames need to be sent to the underlying stream.
pub struct CodecStream<S, C> {
    /// The underlying framed transport and its codec.
    pub framed: Framed<S, C>,
    leftover: BytesMut,
}

impl<S, C> CodecStream<S, C> {
    /// Wraps `inner` with `codec` and initializes an empty read buffer.
    pub fn new_codec(inner: S, codec: C) -> Self {
        Self {
            framed: Framed::new(inner, codec),
            leftover: BytesMut::new(),
        }
    }
}

impl<S, C> AsyncRead for CodecStream<S, C>
where
    S: AsyncRead + AsyncWrite + Unpin,
    C: Decoder<Error = io::Error> + Unpin,
    C::Item: AsRef<[u8]>,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.leftover.is_empty() {
            let take_len = std::cmp::min(buf.remaining(), self.leftover.len());
            let data = self.leftover.split_to(take_len);
            buf.put_slice(&data);
            return Poll::Ready(Ok(()));
        }

        let mut framed = Pin::new(&mut self.framed);
        match futures::ready!(framed.as_mut().poll_next(cx)) {
            Some(Ok(frame)) => {
                let frame_bytes = frame.as_ref();
                let take_len = std::cmp::min(buf.remaining(), frame_bytes.len());

                buf.put_slice(&frame_bytes[..take_len]);

                if take_len < frame_bytes.len() {
                    self.leftover.extend_from_slice(&frame_bytes[take_len..]);
                }

                Poll::Ready(Ok(()))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl<S, C> AsyncWrite for CodecStream<S, C>
where
    S: AsyncRead + AsyncWrite + Unpin,
    C: Encoder<Bytes, Error = io::Error> + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut framed = Pin::new(&mut self.framed);
        futures::ready!(framed.as_mut().poll_ready(cx))?;

        let item = Bytes::copy_from_slice(buf);
        let len = buf.len();

        framed.as_mut().start_send(item)?;

        Poll::Ready(Ok(len))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.framed).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.framed).poll_close(cx)
    }
}
