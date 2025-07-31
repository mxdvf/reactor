#[cfg(test)]
mod tests {

    use std::any::Any;

    use reactor_macros::SubDecoder;
    use tokio_util::codec::Decoder;
    use tokio_util::{
        bytes::{Bytes, BytesMut},
        codec::Encoder as _,
    };

    use reactor_actor::codec::BincodeSubdecoder;

    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct Foo;

    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct Bar;

    #[derive(Debug, PartialEq, bincode::Decode, SubDecoder)]
    pub enum MyEnum {
        Foo(Foo),
        Bar(Bar),
        Twoint((usize, usize)),
    }

    #[derive(Debug, PartialEq, bincode::Decode, SubDecoder)]
    pub enum MyEnum2 {
        Foo(Foo),
        Bar(Bar),
        Twoint((usize, usize)),
    }
    #[test]
    fn test_from_trait() {
        let foo: MyEnum = Foo.into();
        let bar: MyEnum = Bar.into();
        let c: MyEnum = (1, 2).into();

        assert_eq!(foo, MyEnum::Foo(Foo));
        assert_eq!(bar, MyEnum::Bar(Bar));
        assert_eq!(c, MyEnum::Twoint((1, 2)));
    }

    #[test]
    fn test_sub_decoder() {
        let foo = Foo;
        let config = bincode::config::standard();
        let mut length_codec = tokio_util::codec::LengthDelimitedCodec::builder()
            .length_field_length(4)
            .max_frame_length(u32::MAX as usize)
            .new_codec();
        let encoded_foo: Vec<u8> = bincode::encode_to_vec(&foo, config)
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to encode data")
            })
            .unwrap();
        let mut length_encoded_foo = BytesMut::new();
        length_codec
            .encode(Bytes::from(encoded_foo), &mut length_encoded_foo)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Couldn't encode length-delimited data",
                )
            })
            .unwrap();

        let mut decoder = BincodeSubdecoder::<Foo, MyEnum>::default();
        let decoded = decoder.decode(&mut length_encoded_foo).unwrap().unwrap();

        assert_eq!(decoded, MyEnum::Foo(Foo));
    }
    #[test]
    fn test_decoder_map() {
        let foo = Foo;
        let config = bincode::config::standard();
        let mut length_codec = tokio_util::codec::LengthDelimitedCodec::builder()
            .length_field_length(4)
            .max_frame_length(u32::MAX as usize)
            .new_codec();
        let encoded_foo: Vec<u8> = bincode::encode_to_vec(&foo, config)
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to encode data")
            })
            .unwrap();
        let mut length_encoded_foo = BytesMut::new();
        length_codec
            .encode(Bytes::from(encoded_foo), &mut length_encoded_foo)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Couldn't encode length-delimited data",
                )
            })
            .unwrap();

        let decoder = MyEnum_DECODER_MAP("Foo").unwrap();
        let mut decoder = (decoder.decoder_cons)();
        let decoded = decoder.decode(&mut length_encoded_foo).unwrap().unwrap();

        assert_eq!(decoded, MyEnum::Foo(Foo));
    }

    #[test]
    fn test_downcast() {
        let foo = Foo;
        let any_foo: Box<dyn Any + Send> = Box::new(foo);

        let decoder = MyEnum2_DECODER_MAP("Foo").unwrap();
        let my_enum = (decoder.any_to_m)(any_foo);

        assert_eq!(my_enum, MyEnum2::Foo(Foo));
    }
}
