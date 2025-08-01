#[cfg(test)]

macro_rules! union {
    ($enum_name:ident, $($variant:ident),+) => {
        #[derive(Debug, PartialEq, bincode::Decode)]
        pub enum $enum_name {
            $(
                $variant($variant),
            )+
        }

        $(
            impl From<$variant> for $enum_name {
                fn from(value: $variant) -> Self {
                    $enum_name::$variant(value)
                }
            }

            impl From<$enum_name> for $variant {
                fn from(value: $enum_name) -> Self {
                    match value {
                        $enum_name::$variant(inner) => inner,
                        _ => panic!(concat!("Not a ", stringify!($variant))),
                    }
                }
            }
        )+
    };
}

macro_rules! impl_convert_via {
    ($from:ty, $via:ty, $to:ty) => {
        impl From<$from> for $to {
            fn from(value: $from) -> Self {
                let intermediate: $via = value.into();
                intermediate.into()
            }
        }
    };
}

macro_rules! gen_decoders {
    ($func_name:ident, $input_ty:ty, $( $variant:ident ),+ $(,)?) => {
        fn $func_name(name: &str) -> Option<reactor_actor::DecoderProvider<$input_ty>> {
            $(
                if name == stringify!($variant) {
                    fn decoder_cons(
                    ) -> Box<dyn tokio_util::codec::Decoder<Item = $input_ty, Error = std::io::Error> + Sync + Send> {
                        Box::new(BincodeSubdecoder::<$variant, $input_ty>::default())
                    }
                    fn any_to_m(msg: Box<dyn std::any::Any>) -> $input_ty {
                        let msg = msg.downcast::<$variant>().unwrap();
                        (*msg).into()
                    }
                    return Some(reactor_actor::DecoderProvider {
                        decoder_cons,
                        any_to_m,
                    });
                }
            )+
            None
        }
    };
}

mod tests {
    use reactor_actor::codec::BincodeSubdecoder;

    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct WriteOut;

    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct ReadOut;

    union!(ServerIn, ReadOut, WriteOut);

    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct ReadAck;
    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct WriteAck;

    // From Server to Clients
    union!(ServerOut, ReadAck, WriteAck);

    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct GeneratorOut;
    // Clients can receive message from server as well as generator
    union!(ReadIn, ReadAck, GeneratorOut);
    union!(WriteIn, WriteAck, GeneratorOut);

    impl_convert_via!(ServerOut, ReadAck, ReadIn);
    impl_convert_via!(ServerOut, WriteAck, WriteIn);

    gen_decoders!(server_in_decoders, ServerIn, ReadOut, WriteOut);
    gen_decoders!(reader_in_decoders, ReadIn, ReadAck, ServerOut);
    gen_decoders!(writer_in_decoders, WriteIn, WriteAck, ServerOut);

    #[test]
    fn test_blah() {
        let _my_enum: ServerIn = WriteOut.into();
        let server_out: ServerOut = ServerOut::ReadAck(ReadAck);
        let read_ack: ReadAck = server_out.into();
        let _read_in: ReadIn = read_ack.into();

        let server_out: ServerOut = ServerOut::ReadAck(ReadAck);
        let _read_in: ReadIn = server_out.into();
        let decoder = server_in_decoders("ReadOut").unwrap();
    }
}
