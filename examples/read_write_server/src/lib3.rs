pub use reactor_actor::setup_shared_logger_ref;

use bincode::{Decode, Encode};
use reactor_actor::codec::BincodeCodec;
use reactor_actor::codec::BincodeSubdecoder;
use reactor_actor::union;
use reactor_actor::HasPriority;
use reactor_actor::Msg;
use reactor_actor::RuntimeCtx;
use reactor_actor::{ActorAddrRef, BehaviourBuilder};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};
use std::collections::HashMap;
use std::marker::PhantomData;

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////

// From WriterClient to Server
#[derive(Encode, Decode)]
pub struct WriteOut;

// From ReaderClient to Server
#[derive(Encode, Decode)]
pub struct ReadOut;

// #[derive(Encode, Decode)]
// pub struct ReadAck;
// #[derive(Encode, Decode)]
// pub struct WriteAck;

union!(ServerIn, ReadOut, WriteOut);
// impl ::bincode::Encode for ServerIn {
//     fn encode<__E: ::bincode::enc::Encoder>(
//         &self,
//         encoder: &mut __E,
//     ) -> core::result::Result<(), ::bincode::error::EncodeError> {
//         match self {
//             Self::ReadOut(field_0) => {
//                 <u32 as ::bincode::Encode>::encode(&(0u32), encoder)?;
//                 ::bincode::Encode::encode(field_0, encoder)?;
//                 core::result::Result::Ok(())
//             }
//             Self::WriteOut(field_0) => {
//                 <u32 as ::bincode::Encode>::encode(&(1u32), encoder)?;
//                 ::bincode::Encode::encode(field_0, encoder)?;
//                 core::result::Result::Ok(())
//             }
//         }
//     }
// }
// union!(ServerOut, ReadAck, WriteAck);

// #[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
// struct GeneratorOut;

// union!(ReadIn, ReadAck, GeneratorOut);
// union!(WriteIn, WriteAck, GeneratorOut);

// Clients can receive message from server as well as generator
// #[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg, SubDecoder)]
// enum ClientIn {
//     FromServer(ServerOut),
//     FromGenerator(GeneratorOut),
// }

// //////////////////////////////////////////////////////////////////////////////
//                                  Processor
// //////////////////////////////////////////////////////////////////////////////
// struct Server;
// impl reactor_actor::ActorProcess for Server {
//     type IMsg = ServerIn;
//     type OMsg = ServerOut;

//     fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
//         match input {
//             ServerIn::Read(_) => vec![ServerOut::ReadAck],
//             ServerIn::Write(_) => vec![ServerOut::WriteAck],
//         }
//     }
// }

// struct ReadClient;
// impl reactor_actor::ActorProcess for ReadClient {
//     type IMsg = ReadIn;
//     type OMsg = ReadAck;

//     fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
//         match input {
//             ReadIn::ReadAck(_) => {
//                 log::info!("Read Ack recvd");
//                 vec![]
//             }
//             ReadIn::GeneratorOut(_) => {
//                 vec![ReadOut]
//             }

//         }
//         vec![]
//     }
// }

// struct WriteClient;
// impl reactor_actor::ActorProcess for WriteClient {
//     type IMsg = WriteIn;
//     type OMsg = WriteAck;

//     fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
//         match input {
//             WriteIn::WriteAck(_) => {
//                 log::info!("Write Ack recvd");
//                 vec![]
//             }
//             WriteIn::FromServer(_) => {
//                 vec![WriteOut]
//             }
//         }
//     }
// }
// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
// struct ClientSender<R> {
//     server_addr: Vec<ActorAddrRef>,
//     decoder_name: String,
//     response: PhantomData<R>,
// }
// impl<R: Msg> reactor_actor::ActorSend for ClientSender<R> {
//     type OMsg = R;

//     async fn before_send(&mut self, _output: &Self::OMsg) -> &Vec<ActorAddrRef> {
//         &self.server_addr
//     }

//     fn sub_decoder_name(&self) -> Option<String> {
//         Some(self.decoder_name.clone())
//     }
// }
// impl<R> ClientSender<R> {
//     fn new(server_addr: ActorAddrRef, decoder_name: String) -> Self {
//         ClientSender {
//             server_addr: vec![server_addr],
//             decoder_name,
//             response: PhantomData,
//         }
//     }
// }

// struct ServerSender {
//     read_client_addr: Vec<ActorAddrRef>,
//     write_client_addr: Vec<ActorAddrRef>,
// }
// impl reactor_actor::ActorSend for ServerSender {
//     type OMsg = ServerOut;

//     async fn before_send(&mut self, output: &Self::OMsg) -> &Vec<ActorAddrRef> {
//         match output {
//             ServerOut::WriteAck(_) => &self.write_client_addr,
//             ServerOut::ReadAck(_) => &self.read_client_addr,
//         }
//     }
//     fn sub_decoder_name(&self) -> Option<String> {
//         Some("FromServer".to_string())
//     }
// }
// impl ServerSender {
//     fn new(r_client_addrs: ActorAddrRef, w_client_addrs: ActorAddrRef) -> Self {
//         ServerSender {
//             read_client_addr: vec![r_client_addrs],
//             write_client_addr: vec![w_client_addrs],
//         }
//     }
// }
// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

// lazy_static::lazy_static! {
//     static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
// }

// pub async fn _server(ctx: RuntimeCtx, reader_addr: ActorAddrRef, writer_addr: ActorAddrRef) {
//     BehaviourBuilder::new(Server {}, BincodeCodec::default())
//         .send(ServerSender::new(reader_addr, writer_addr))
//         // .sub_decoders(ServerIn_DECODER_MAP)
//         .build()
//         .run(ctx)
//         .await
//         .unwrap();
// }

// #[unsafe(no_mangle)]
// pub fn server(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
//     let reader_addr = payload.remove("reader_addr").unwrap();
//     let writer_addr = payload.remove("writer_addr").unwrap();
//     RUNTIME.spawn(_server(
//         ctx,
//         reader_addr.as_str().unwrap().to_string().leak(),
//         writer_addr.as_str().unwrap().to_string().leak(),
//     ));
// }

// pub async fn _writer(ctx: RuntimeCtx, server_addr: ActorAddrRef) {
//     BehaviourBuilder::new(WriteClient {}, BincodeCodec::default())
//         .send(ClientSender::new(server_addr, "Write".to_string()))
//         .generator_if(true, || vec![ClientIn::FromGenerator(GeneratorOut); 10].into_iter())
//         .sub_decoders(ClientIn_DECODER_MAP)
//         .build()
//         .run(ctx)
//         .await
//         .unwrap();
// }
// #[unsafe(no_mangle)]
// pub fn writer(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
//     let server_addr = payload.remove("server_addr").unwrap();
//     RUNTIME.spawn(_writer(
//         ctx,
//         server_addr.as_str().unwrap().to_string().leak(),
//     ));
// }

// pub async fn _reader(ctx: RuntimeCtx, server_addr: ActorAddrRef) {
//     BehaviourBuilder::new(ReadClient {}, BincodeCodec::default())
//         .send(ClientSender::new(server_addr, "Read".to_string()))
//         .generator_if(true, || vec![ClientIn::FromGenerator(GeneratorOut); 10].into_iter())
//         .sub_decoders(ClientIn_DECODER_MAP)
//         .build()
//         .run(ctx)
//         .await
//         .unwrap();
// }
// #[unsafe(no_mangle)]
// pub fn reader(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
//     let server_addr = payload.remove("server_addr").unwrap();
//     RUNTIME.spawn(_reader(
//         ctx,
//         server_addr.as_str().unwrap().to_string().leak(),
//     ));
// }
