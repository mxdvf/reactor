mod client_utils;
mod reader;
mod server;
mod writer;

pub use reactor_actor::setup_shared_logger_ref;

use crate::client_utils::GeneratorOut;
use crate::reader::reader as reader_behaviour;
use crate::reader::ReadAck;
use crate::reader::ReadOut;
use crate::server::server as server_behaviour;
use crate::writer::writer as writer_behaviour;
use crate::writer::WriteAck;
use crate::writer::WriteOut;
use reactor_actor::RuntimeCtx;
use reactor_macros::msg_converter;
use std::collections::HashMap;
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
fn server(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let reader_addr = payload.remove("reader_addr").unwrap();
    let writer_addr = payload.remove("writer_addr").unwrap();
    RUNTIME.spawn(server_behaviour(
        ctx,
        reader_addr.as_str().unwrap().to_string().leak(),
        writer_addr.as_str().unwrap().to_string().leak(),
        server_decoder,
    ));
}

#[unsafe(no_mangle)]
fn writer(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server_addr = payload.remove("server_addr").unwrap();
    RUNTIME.spawn(writer_behaviour(
        ctx,
        server_addr.as_str().unwrap().to_string().leak(),
        writer_decoder,
    ));
}

#[unsafe(no_mangle)]
fn reader(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server_addr = payload.remove("server_addr").unwrap();
    RUNTIME.spawn(reader_behaviour(
        ctx,
        server_addr.as_str().unwrap().to_string().leak(),
        reader_decoder,
    ));
}
