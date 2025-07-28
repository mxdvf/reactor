use std::marker::PhantomData;
use tokio_util::bytes::{Bytes, BytesMut};

#[derive(Clone)]
pub struct BincodeCodec<E, D> {
    config: bincode::config::Configuration,
    length_codec: tokio_util::codec::LengthDelimitedCodec,
    e: PhantomData<E>,
    d: PhantomData<D>,
}
impl<E, D> Default for BincodeCodec<E, D> {
    fn default() -> Self {
        BincodeCodec {
            config: bincode::config::standard(),
            length_codec: tokio_util::codec::LengthDelimitedCodec::builder()
                .length_field_length(4)
                .max_frame_length(u32::MAX as usize)
                .new_codec(),
            e: PhantomData,
            d: PhantomData,
        }
    }
}

impl<E, D: bincode::Decode<()>> tokio_util::codec::Decoder for BincodeCodec<E, D> {
    type Item = D;
    type Error = std::io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let frame = match self.length_codec.decode(src).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to decode length-delimited data",
            )
        })? {
            Some(frame) => frame,
            None => return Ok(None),
        };
        let (message, _) = bincode::decode_from_slice(&frame, self.config).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to decode length-delimited data",
            )
        })?;
        Ok(Some(message))
    }
}

impl<E: bincode::Encode, D> tokio_util::codec::Encoder<E> for BincodeCodec<E, D> {
    type Error = std::io::Error;
    fn encode(&mut self, item: E, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_data = bincode::encode_to_vec(&item, self.config).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to encode data")
        })?;
        self.length_codec
            .encode(Bytes::from(encoded_data), dst)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Couldn't encode length-delimited data",
                )
            })?;
        Ok(())
    }
}
