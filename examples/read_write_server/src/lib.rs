pub use reactor_actor::setup_shared_logger_ref;

use reactor_actor::codec::BincodeCodec;
use reactor_actor::Msg;
use reactor_actor::RuntimeCtx;
use reactor_actor::{ActorAddrRef, BehaviourBuilder};
use reactor_macros::{msg_converter, DefaultPrio, Msg as DeriveMsg};
use std::collections::HashMap;
use std::marker::PhantomData;

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct GeneratorOut;

#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct WriteOut;
#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct ReadOut;

#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct ReadAck;
#[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct WriteAck;

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
//                                  Processor
// //////////////////////////////////////////////////////////////////////////////
struct Server;
impl reactor_actor::ActorProcess for Server {
    type IMsg = ServerIn;
    type OMsg = ServerOut;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            ServerIn::ReadOut(_) => vec![ServerOut::ReadAck(ReadAck)],
            ServerIn::WriteOut(_) => vec![ServerOut::WriteAck(WriteAck)],
        }
    }
}

struct ReadClient;
impl reactor_actor::ActorProcess for ReadClient {
    type IMsg = ReaderIn;
    type OMsg = ReadOut;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            ReaderIn::ReadAck(_) => {
                log::info!("Read Ack recvd");
                vec![]
            }
            ReaderIn::GeneratorOut(_) => {
                vec![ReadOut]
            }
        }
    }
}

struct WriteClient;
impl reactor_actor::ActorProcess for WriteClient {
    type IMsg = WriterIn;
    type OMsg = WriteOut;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            WriterIn::WriteAck(_) => {
                log::info!("Write Ack recvd");
                vec![]
            }
            WriterIn::GeneratorOut(_) => {
                vec![WriteOut]
            }
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
struct ClientSender<R> {
    server_addr: Vec<ActorAddrRef>,
    response: PhantomData<R>,
}
impl<R: Msg> reactor_actor::ActorSend for ClientSender<R> {
    type OMsg = R;

    async fn before_send(&mut self, _output: &Self::OMsg) -> &Vec<ActorAddrRef> {
        &self.server_addr
    }
}
impl<R> ClientSender<R> {
    fn new(server_addr: ActorAddrRef) -> Self {
        ClientSender {
            server_addr: vec![server_addr],
            response: PhantomData,
        }
    }
}

struct ServerSender {
    read_client_addr: Vec<ActorAddrRef>,
    write_client_addr: Vec<ActorAddrRef>,
}
impl reactor_actor::ActorSend for ServerSender {
    type OMsg = ServerOut;

    async fn before_send(&mut self, output: &Self::OMsg) -> &Vec<ActorAddrRef> {
        match output {
            ServerOut::WriteAck(_) => &self.write_client_addr,
            ServerOut::ReadAck(_) => &self.read_client_addr,
        }
    }
}
impl ServerSender {
    fn new(r_client_addrs: ActorAddrRef, w_client_addrs: ActorAddrRef) -> Self {
        ServerSender {
            read_client_addr: vec![r_client_addrs],
            write_client_addr: vec![w_client_addrs],
        }
    }
}
// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

pub async fn _server(ctx: RuntimeCtx, reader_addr: ActorAddrRef, writer_addr: ActorAddrRef) {
    BehaviourBuilder::new(Server {}, BincodeCodec::default())
        .send(ServerSender::new(reader_addr, writer_addr))
        .sub_decoders(server_decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
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

pub async fn _writer(ctx: RuntimeCtx, server_addr: ActorAddrRef) {
    BehaviourBuilder::new(WriteClient {}, BincodeCodec::default())
        .send(ClientSender::new(server_addr))
        .generator_if(true, || vec![WriterIn::GeneratorOut(GeneratorOut); 10].into_iter())
        .sub_decoders(writer_decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
}
#[unsafe(no_mangle)]
pub fn writer(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server_addr = payload.remove("server_addr").unwrap();
    RUNTIME.spawn(_writer(
        ctx,
        server_addr.as_str().unwrap().to_string().leak(),
    ));
}

pub async fn _reader(ctx: RuntimeCtx, server_addr: ActorAddrRef) {
    BehaviourBuilder::new(ReadClient {}, BincodeCodec::default())
        .send(ClientSender::new(server_addr))
        .generator_if(true, || vec![ReaderIn::GeneratorOut(GeneratorOut); 10].into_iter())
        .sub_decoders(reader_decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
}
#[unsafe(no_mangle)]
pub fn reader(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server_addr = payload.remove("server_addr").unwrap();
    RUNTIME.spawn(_reader(
        ctx,
        server_addr.as_str().unwrap().to_string().leak(),
    ));
}
