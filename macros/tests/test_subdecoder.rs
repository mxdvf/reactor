#[cfg(test)]
mod tests {
    use reactor_macros::msg_converter;
    use tokio_util::{
        bytes::{Bytes, BytesMut},
        codec::Encoder as _,
    };

    #[derive(
        Default,
        Debug,
        PartialEq,
        bincode::Encode,
        bincode::Decode,
        Clone,
        ::reactor_macros::DefaultPrio,
        ::reactor_macros::Msg,
    )]
    pub struct GeneratorOut;

    #[derive(
        Default,
        Debug,
        PartialEq,
        bincode::Encode,
        bincode::Decode,
        Clone,
        ::reactor_macros::DefaultPrio,
        ::reactor_macros::Msg,
    )]
    pub struct WriteOut;
    #[derive(
        Default,
        Debug,
        PartialEq,
        bincode::Encode,
        bincode::Decode,
        Clone,
        ::reactor_macros::DefaultPrio,
        ::reactor_macros::Msg,
    )]
    pub struct ReadOut;

    #[derive(
        Default,
        Debug,
        PartialEq,
        bincode::Encode,
        bincode::Decode,
        Clone,
        ::reactor_macros::DefaultPrio,
        ::reactor_macros::Msg,
    )]
    pub struct ReadAck;
    #[derive(
        Default,
        Debug,
        PartialEq,
        bincode::Encode,
        bincode::Decode,
        Clone,
        ::reactor_macros::DefaultPrio,
        ::reactor_macros::Msg,
    )]
    pub struct WriteAck;

    msg_converter! {
       Unions: [
           WriterIn = WriteAck, GeneratorOut;
           ServerOut = ReadAck, WriteAck;
           ServerIn = ReadOut, WriteOut;
           ReaderIn = ReadAck, GeneratorOut;
       ];

       Adapters: [
           ReaderIn from ServerOut via ReadAck;
           WriterIn from ServerOut via WriteAck;
       ];

       Decoders: [
           server_decoder can decode ReadOut, WriteOut to ServerIn;
           reader_decoder can decode ReadAck, ServerOut to ReaderIn;
           writer_decoder can decode WriteAck, ServerOut to WriterIn;
       ];
    }
    impl PartialEq for ReaderIn {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (ReaderIn::ReadAck(read_ack), ReaderIn::ReadAck(read_ack2)) => {
                    read_ack == read_ack2
                }
                (ReaderIn::GeneratorOut(generator_out), ReaderIn::GeneratorOut(generator_out2)) => {
                    generator_out == generator_out2
                }
                _ => false,
            }
        }
    }
    impl PartialEq for WriterIn {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (WriterIn::WriteAck(write_ack), WriterIn::WriteAck(write_ack2)) => {
                    write_ack == write_ack2
                }
                (WriterIn::GeneratorOut(generator_out), WriterIn::GeneratorOut(generator_out2)) => {
                    generator_out == generator_out2
                }
                _ => false,
            }
        }
    }
    impl PartialEq for ServerIn {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (ServerIn::ReadOut(read_out), ServerIn::ReadOut(read_out2)) => {
                    read_out == read_out2
                }
                (ServerIn::WriteOut(write_out), ServerIn::WriteOut(write_out2)) => {
                    write_out == write_out2
                }
                _ => false,
            }
        }
    }
    impl PartialEq for ServerOut {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (ServerOut::ReadAck(read_ack), ServerOut::ReadAck(read_ack2)) => {
                    read_ack == read_ack2
                }
                (ServerOut::WriteAck(write_ack), ServerOut::WriteAck(write_ack2)) => {
                    write_ack == write_ack2
                }
                _ => false,
            }
        }
    }

    #[test]
    fn test_coversion() {
        let _box: ::std::boxed::Box<_> = Box::new(1);
        // Direct enum conversions
        let w: WriterIn = WriteAck::default().into();
        assert_eq!(WriterIn::from(WriteAck::default()), w);
        assert_eq!(WriteAck::from(w), WriteAck);

        let w2: WriterIn = GeneratorOut::default().into();
        assert_eq!(WriterIn::from(GeneratorOut::default()), w2);
        assert_eq!(GeneratorOut::from(w2), GeneratorOut);

        let s: ServerOut = ReadAck::default().into();
        assert_eq!(ServerOut::from(ReadAck::default()), s);

        let s2: ServerOut = WriteAck::default().into();
        assert_eq!(ServerOut::from(WriteAck::default()), s2);

        let s_in: ServerIn = ReadOut::default().into();
        assert_eq!(ServerIn::from(ReadOut::default()), s_in);

        let s_in2: ServerIn = WriteOut::default().into();
        assert_eq!(ServerIn::from(WriteOut::default()), s_in2);

        let r: ReaderIn = ReadAck::default().into();
        assert_eq!(ReaderIn::from(ReadAck::default()), r);

        let r2: ReaderIn = GeneratorOut::default().into();
        assert_eq!(ReaderIn::from(GeneratorOut::default()), r2);

        // Adapter conversions: ServerOut -> ReadAck -> ReaderIn
        let so: ServerOut = ReadAck::default().into();
        let ri: ReaderIn = so.into(); // via ReadAck
        assert_eq!(ReaderIn::from(ServerOut::from(ReadAck::default())), ri);

        // Adapter conversions: ReaderIn -> ReadAck -> ServerOut
        let so: ServerOut = ReadAck::default().into();
        let ri: ReaderIn = so.into();
        assert_eq!(
            ServerOut::from(ri.clone()),
            ServerOut::ReadAck(ReadAck::default())
        );

        let so2: ServerOut = WriteAck::default().into();
        let wi: WriterIn = so2.into(); // via WriteAck
        assert_eq!(WriterIn::from(ServerOut::from(WriteAck::default())), wi);
    }

    fn encode<T: bincode::Encode>(t: T, dst: &mut BytesMut) {
        let config = bincode::config::standard();
        let encoded_data = bincode::encode_to_vec(&t, config)
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to encode data")
            })
            .unwrap();
        let mut codec = tokio_util::codec::LengthDelimitedCodec::builder()
            .length_field_length(4)
            .max_frame_length(u32::MAX as usize)
            .new_codec();
        codec
            .encode(Bytes::from(encoded_data), dst)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Couldn't encode length-delimited data",
                )
            })
            .unwrap();
    }

    #[test]
    fn test_reader_decoder_from_readack() {
        let mut encoded = BytesMut::new();
        encode(ReadAck::default(), &mut encoded);

        let Some(provider) = reader_decoder("test_subdecoder::tests::ReadAck") else {
            panic!("Decoder not found");
        };

        let mut decoder = (provider.decoder_cons)();

        let result = decoder.decode(&mut encoded).unwrap().unwrap();
        assert_eq!(result, ReaderIn::from(ReadAck::default()));
    }

    #[test]
    fn test_reader_decoder_from_serverout() {
        let mut encoded = BytesMut::new();
        encode(ServerOut::ReadAck(ReadAck::default()), &mut encoded);

        let Some(provider) = reader_decoder("test_subdecoder::tests::ReadAck") else {
            panic!("Decoder not found");
        };

        let mut decoder = (provider.decoder_cons)();

        let result = decoder.decode(&mut encoded).unwrap().unwrap();
        assert_eq!(result, ReaderIn::from(ReadAck::default()));
    }
}
