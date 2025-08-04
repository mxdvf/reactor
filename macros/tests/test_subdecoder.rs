#[cfg(test)]
mod tests {
    use reactor_actor::codec::BincodeSubdecoder;
    use reactor_macros::union;

    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct WriteOut;
    #[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct ReadOut;
    union!(ServerIn, ReadOut, WriteOut);

    #[test]
    fn test_blah() {
        let server_in: ServerIn = WriteOut.into();
        let write_out: WriteOut = server_in.into();
    }
}
