use origamic_core::codec::CodecStream;
use snow::TransportState;

use crate::codec::NoiseCodec;

/// Constructs Noise-encrypted streams and exposes the remote Noise identity.
///
/// Implemented for [`NoiseStream`], this extension trait provides a
/// constructor that binds a Snow transport state to a lower-level I/O stream.
pub trait NoiseStreamExt<S> {
    /// Creates a stream that encrypts writes and decrypts reads with `transport`.
    fn new(inner: S, transport: TransportState) -> Self;

    /// Returns the remote static public key learned during the handshake, if available.
    fn remote_static_key(&self) -> Option<&[u8]>;
}

/// A framed I/O stream that encrypts and decrypts data with the Noise protocol.
///
/// Data is encoded as length-prefixed Noise transport records. As with other
/// [`AsyncWrite`](tokio::io::AsyncWrite) streams, flushing is required when
/// buffered records should be pushed to the underlying transport.
pub type NoiseStream<S> = CodecStream<S, NoiseCodec>;

impl<S> NoiseStreamExt<S> for NoiseStream<S> {
    fn new(inner: S, transport: TransportState) -> Self {
        Self::new_codec(inner, NoiseCodec::new(transport))
    }
    fn remote_static_key(&self) -> Option<&[u8]> {
        self.framed.codec().transport.get_remote_static()
    }
}
