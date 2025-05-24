use tensile_core::actor::DecodeErr;
use tokio_util::bytes::{Bytes, BytesMut};

use crate::QPMsg;

#[derive(Clone)]
pub struct QpMsgCodec<I, O> {
    config: bincode::config::Configuration,
    length_codec: tokio_util::codec::LengthDelimitedCodec,
    _marker: std::marker::PhantomData<I>,
    _marker2: std::marker::PhantomData<O>,
}

impl<I, O> QpMsgCodec<I, O> {
    pub fn new() -> Self {
        QpMsgCodec {
            config: bincode::config::standard(),
            length_codec: tokio_util::codec::LengthDelimitedCodec::builder()
                .length_field_length(4)
                .max_frame_length(u32::MAX as usize)
                .new_codec(),
            _marker: std::marker::PhantomData,
            _marker2: std::marker::PhantomData,
        }
    }
}

impl<I: bincode::Decode<()>, O> tokio_util::codec::Decoder for QpMsgCodec<I, O> {
    type Item = QPMsg<I>;
    type Error = DecodeErr;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<QPMsg<I>>, Self::Error> {
        let frame = match self.length_codec.decode(src).map_err(|_| DecodeErr)? {
            Some(frame) => frame,
            None => return Ok(None), // Not enough data yet
        };

        let (message, _) =
            bincode::decode_from_slice(&frame, self.config).map_err(|_| DecodeErr)?;

        Ok(Some(message))
    }
}

impl<I, O: bincode::Encode> tokio_util::codec::Encoder<QPMsg<O>> for QpMsgCodec<I, O> {
    type Error = std::io::Error;

    fn encode(&mut self, item: QPMsg<O>, dst: &mut BytesMut) -> Result<(), Self::Error> {
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
