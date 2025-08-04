// #![feature(trace_macros)]
// trace_macros!(true);

// #[macro_export]
// macro_rules! union {
//     ($enum_name:ident, $($variant:ident),+) => {
//         // #[derive(Debug, PartialEq, DefaultPrio, DeriveMsg, bincode::Encode, bincode::Decode, Clone )]
//         pub enum $enum_name {
//             $(
//                 $variant($variant),
//             )+
//         }

//         $(
//             impl From<$variant> for $enum_name {
//                 fn from(value: $variant) -> Self {
//                     $enum_name::$variant(value)
//                 }
//             }

//             impl From<$enum_name> for $variant {
//                 fn from(value: $enum_name) -> Self {
//                     match value {
//                         $enum_name::$variant(inner) => inner,
//                         _ => panic!(concat!("Not a ", stringify!($variant))),
//                     }
//                 }
//             }
//         )+
//     };
// }

// #[macro_export]
// macro_rules! impl_convert_via {
//     ($from:ty, $via:ty, $to:ty) => {
//         impl From<$from> for $to {
//             fn from(value: $from) -> Self {
//                 let intermediate: $via = value.into();
//                 intermediate.into()
//             }
//         }
//     };
// }

// #[macro_export]
// macro_rules! gen_decoders {
//     ($func_name:ident, $input_ty:ty, $( $variant:ident ),+ $(,)?) => {
//         fn $func_name(name: &str) -> Option<reactor_actor::DecoderProvider<$input_ty>> {
//             $(
//                 if name == stringify!($variant) {
//                     fn decoder_cons(
//                     ) -> Box<dyn tokio_util::codec::Decoder<Item = $input_ty, Error = std::io::Error> + Sync + Send> {
//                         Box::new(BincodeSubdecoder::<$variant, $input_ty>::default())
//                     }
//                     fn any_to_m(msg: Box<dyn std::any::Any>) -> $input_ty {
//                         let msg = msg.downcast::<$variant>().unwrap();
//                         (*msg).into()
//                     }
//                     return Some(reactor_actor::DecoderProvider {
//                         decoder_cons,
//                         any_to_m,
//                     });
//                 }
//             )+
//             None
//         }
//     };
// }

// mod tests {
//     use crate as reactor_actor;
//     use crate::codec::BincodeSubdecoder;
//     #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
//     pub struct GeneratorOut;

//     #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
//     pub struct ReadAck;
//     #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
//     pub struct ReadOut;
//     log_syntax!(union!(ReadIn, ReadAck, GeneratorOut));

//     #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
//     pub struct WriteOut;
//     #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
//     pub struct WriteAck;
//     union!(WriteIn, WriteAck, GeneratorOut);

//     // Server Out
//     union!(ServerIn, ReadOut, WriteOut);
//     union!(ServerOut, ReadAck, WriteAck);

//     impl_convert_via!(ServerOut, ReadAck, ReadIn);
//     impl_convert_via!(ServerOut, WriteAck, WriteIn);

//     gen_decoders!(server_in_decoders, ServerIn, ReadOut, WriteOut);
//     gen_decoders!(reader_in_decoders, ReadIn, ReadAck, ServerOut);
//     gen_decoders!(writer_in_decoders, WriteIn, WriteAck, ServerOut);

//     #[test]
//     fn test_blah() {
//         let _my_enum: ServerIn = WriteOut.into();
//         let server_out: ServerOut = ServerOut::ReadAck(ReadAck);
//         let read_ack: ReadAck = server_out.into();
//         let _read_in: ReadIn = read_ack.into();

//         let server_out: ServerOut = ServerOut::ReadAck(ReadAck);
//         let _read_in: ReadIn = server_out.into();
//         let decoder = server_in_decoders("ReadOut").unwrap();
//     }
// }
mod tests {
    use crate as reactor_actor;
    use crate::codec::BincodeSubdecoder;
    use bincode::Encode;

    #[derive(bincode::Encode)]
    pub struct ReadAck;
    #[derive(bincode::Encode)]
    pub struct ReadOut;
    // union!(ReadIn, ReadAck, ReadOut);

    // impl ::bincode::Encode for ReadIn {
    //     fn encode<__E: ::bincode::enc::Encoder>(
    //         &self,
    //         encoder: &mut __E,
    //     ) -> core::result::Result<(), ::bincode::error::EncodeError> {
    //         match self {
    //             Self::ReadAck(field_0) => {
    //                 <u32 as ::bincode::Encode>::encode(&(0u32), encoder)?;
    //                 ::bincode::Encode::encode(field_0, encoder)?;
    //                 core::result::Result::Ok(())
    //             }
    //             Self::ReadOut(field_0) => {
    //                 <u32 as ::bincode::Encode>::encode(&(1u32), encoder)?;
    //                 ::bincode::Encode::encode(field_0, encoder)?;
    //                 core::result::Result::Ok(())
    //             }
    //         }
    //     }
    // }

    #[test]
    fn test_blah() {
        // let x = ReadIn::ReadOut(ReadAck);
    }
}
