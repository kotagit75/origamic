use origamic_core::codec::CodecStream;
use snow::TransportState;

use crate::codec::NoiseCodec;

pub trait NoiseStreamExt<S> {
    fn new(inner: S, transport: TransportState) -> Self;
    fn remote_static_key(&self) -> Option<&[u8]>;
}

pub type NoiseStream<S> = CodecStream<S, NoiseCodec>;

impl<S> NoiseStreamExt<S> for NoiseStream<S> {
    fn new(inner: S, transport: TransportState) -> Self {
        Self::new_codec(inner, NoiseCodec::new(transport))
    }
    fn remote_static_key(&self) -> Option<&[u8]> {
        self.framed.codec().transport.get_remote_static()
    }
}
