//! Encrypted bidirectional stream implementation.

use std::{
    cmp,
    pin::Pin,
    task::{Context, Poll},
};

use snow::TransportState;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::factory::{MAX_NOISE_FRAME, invalid_data};

const TRANSPORT_TAG_LEN: usize = 16;
const MAX_PLAINTEXT_FRAME: usize = MAX_NOISE_FRAME - TRANSPORT_TAG_LEN;

/// Bidirectional stream that encrypts writes and decrypts reads with Noise.
///
/// Each write is encoded as one length-prefixed Noise transport record on the
/// lower stream. Reads restore the plaintext byte stream, independent of
/// lower-layer read boundaries. The record length prefix is visible to the
/// lower transport; the record contents are encrypted.
pub struct NoiseStream<S> {
    inner: S,
    transport: TransportState,
    read_prefix: [u8; 4],
    read_prefix_filled: usize,
    read_frame_len: Option<usize>,
    read_frame: Vec<u8>,
    read_frame_filled: usize,
    plaintext: Vec<u8>,
    plaintext_pos: usize,
    write_pending: Option<Vec<u8>>,
    write_pending_filled: usize,
    write_pending_plaintext_len: usize,
}

impl<S> NoiseStream<S> {
    pub(crate) fn new(inner: S, transport: TransportState) -> Self {
        Self {
            inner,
            transport,
            read_prefix: [0u8; 4],
            read_prefix_filled: 0,
            read_frame_len: None,
            read_frame: Vec::new(),
            read_frame_filled: 0,
            plaintext: Vec::new(),
            plaintext_pos: 0,
            write_pending: None,
            write_pending_filled: 0,
            write_pending_plaintext_len: 0,
        }
    }

    /// Returns the remote Noise static public key learned during the
    /// handshake, if available.
    ///
    /// This key is not automatically bound to an Origamic
    /// [`OverlayAddress`](origamic_core::overlay::address::OverlayAddress).
    pub fn remote_static_key(&self) -> Option<&[u8]> {
        self.transport.get_remote_static()
    }

    fn reset_read_prefix(&mut self) {
        self.read_prefix = [0u8; 4];
        self.read_prefix_filled = 0;
        self.read_frame_len = None;
        self.read_frame.clear();
        self.read_frame_filled = 0;
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for NoiseStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.as_mut().get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.plaintext_pos < this.plaintext.len() {
            let take = cmp::min(this.plaintext.len() - this.plaintext_pos, buf.remaining());
            buf.put_slice(&this.plaintext[this.plaintext_pos..this.plaintext_pos + take]);
            this.plaintext_pos += take;
            return Poll::Ready(Ok(()));
        }

        loop {
            if this.read_prefix_filled < 4 {
                let remaining = 4 - this.read_prefix_filled;
                let start = this.read_prefix_filled;
                let mut read_buf = ReadBuf::new(&mut this.read_prefix[start..start + remaining]);
                match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let filled = read_buf.filled().len();
                        if filled == 0 {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Noise stream ended inside a frame length prefix",
                            )));
                        }
                        this.read_prefix_filled += filled;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
                if this.read_prefix_filled < 4 {
                    continue;
                }

                let len = u32::from_be_bytes(this.read_prefix) as usize;
                if len == 0 || len > MAX_NOISE_FRAME {
                    return Poll::Ready(Err(invalid_data(
                        "Noise frame length is zero or exceeds the maximum size",
                    )));
                }
                this.read_frame_len = Some(len);
                this.read_frame = vec![0u8; len];
                this.read_frame_filled = 0;
            }

            let frame_len = this.read_frame_len.unwrap_or(0);
            if this.read_frame_filled < frame_len {
                let remaining = frame_len - this.read_frame_filled;
                let filled = this.read_frame_filled;
                let mut read_buf = ReadBuf::new(&mut this.read_frame[filled..filled + remaining]);
                match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let filled = read_buf.filled().len();
                        if filled == 0 {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Noise stream ended inside a ciphertext frame",
                            )));
                        }
                        this.read_frame_filled += filled;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
                continue;
            }

            let mut decrypted = vec![0u8; frame_len];
            match this
                .transport
                .read_message(&this.read_frame, &mut decrypted)
            {
                Ok(len) => {
                    decrypted.truncate(len);
                    this.plaintext = decrypted;
                    this.plaintext_pos = 0;
                    this.reset_read_prefix();
                }
                Err(error) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error,
                    )));
                }
            }

            if this.plaintext.is_empty() {
                continue;
            }
            let take = cmp::min(this.plaintext.len(), buf.remaining());
            buf.put_slice(&this.plaintext[..take]);
            this.plaintext_pos = take;
            return Poll::Ready(Ok(()));
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for NoiseStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.as_mut().get_mut();
        loop {
            if let Some(pending) = this.write_pending.clone() {
                let start = this.write_pending_filled;
                match Pin::new(&mut this.inner).poll_write(cx, &pending[start..]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "lower Noise transport accepted zero bytes",
                        )));
                    }
                    Poll::Ready(Ok(written)) => {
                        this.write_pending_filled += written;
                        if this.write_pending_filled == pending.len() {
                            let plaintext_len = this.write_pending_plaintext_len;
                            this.write_pending = None;
                            this.write_pending_filled = 0;
                            this.write_pending_plaintext_len = 0;
                            return Poll::Ready(Ok(plaintext_len));
                        }
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
                continue;
            }

            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }

            let plaintext_len = cmp::min(buf.len(), MAX_PLAINTEXT_FRAME);
            let mut ciphertext = vec![0u8; plaintext_len + TRANSPORT_TAG_LEN];
            let ciphertext_len = match this
                .transport
                .write_message(&buf[..plaintext_len], &mut ciphertext)
            {
                Ok(len) => len,
                Err(error) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error,
                    )));
                }
            };
            ciphertext.truncate(ciphertext_len);

            let mut frame = Vec::with_capacity(4 + ciphertext_len);
            frame.extend_from_slice(&(ciphertext_len as u32).to_be_bytes());
            frame.extend_from_slice(&ciphertext);
            this.write_pending = Some(frame);
            this.write_pending_filled = 0;
            this.write_pending_plaintext_len = plaintext_len;
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
