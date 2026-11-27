use tokio::io;
use tokio_util::{
    bytes::{Buf, BufMut, Bytes, BytesMut},
    codec::{Decoder, Encoder},
};

pub struct NoiseCodec {
    pub(crate) transport: snow::TransportState,
}

impl NoiseCodec {
    pub fn new(transport: snow::TransportState) -> Self {
        Self { transport }
    }
}

impl Decoder for NoiseCodec {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(
        &mut self,
        src: &mut tokio_util::bytes::BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 2 {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 2];
        length_bytes.copy_from_slice(&src[..2]);
        let frame_len = u16::from_be_bytes(length_bytes) as usize;

        if src.len() < 2 + frame_len {
            return Ok(None);
        }

        src.advance(2);
        let payload = src.split_to(frame_len);

        let mut decrypted = vec![0u8; 65535];
        match self.transport.read_message(&payload, &mut decrypted) {
            Ok(actual_len) => {
                decrypted.truncate(actual_len);
                Ok(Some(Bytes::from(decrypted)))
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        }
    }
}

impl Encoder<Bytes> for NoiseCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let estimated_len = item.len() + 16;

        if estimated_len > 65535 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Payload too large for Noise protocol",
            ));
        }

        let mut encrypted = vec![0u8; estimated_len];
        let actual_len = match self.transport.write_message(&item, &mut encrypted) {
            Ok(len) => len,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        };

        dst.reserve(2 + actual_len);
        dst.put_u16(actual_len as u16);
        dst.put_slice(&encrypted[..actual_len]);

        Ok(())
    }
}
