mod reader;
mod writer;
mod client_utils;
mod server;

pub use reactor_actor::setup_shared_logger_ref;

use reactor_actor::Msg;
use reactor_actor::RuntimeCtx;
use reactor_macros::{msg_converter, Msg as DeriveMsg};
use std::collections::HashMap;
use crate::reader::_reader;
use crate::server::_server;
use crate::writer::_writer;
// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////

msg_converter! {
   Unions: [
       WriterIn = WriteAck, GeneratorOut;
       ReaderIn = ReadAck, GeneratorOut;

       ServerIn = ReadOut, WriteOut;
       ServerOut = ReadAck, WriteAck;
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


// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[unsafe(no_mangle)]
pub fn server(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let reader_addr = payload.remove("reader_addr").unwrap();
    let writer_addr = payload.remove("writer_addr").unwrap();
    RUNTIME.spawn(_server(
        ctx,
        reader_addr.as_str().unwrap().to_string().leak(),
        writer_addr.as_str().unwrap().to_string().leak(),
    ));
}

#[unsafe(no_mangle)]
pub fn writer(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server_addr = payload.remove("server_addr").unwrap();
    RUNTIME.spawn(_writer(
        ctx,
        server_addr.as_str().unwrap().to_string().leak(),
    ));
}

#[unsafe(no_mangle)]
pub fn reader(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server_addr = payload.remove("server_addr").unwrap();
    RUNTIME.spawn(_reader(
        ctx,
        server_addr.as_str().unwrap().to_string().leak(),
    ));
}
