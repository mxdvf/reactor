use crate::reader::{ReadAck, ReadOut};
use crate::writer::{WriteAck, WriteOut};
use reactor_actor::SubDecoderStore;
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{ActorAddrRef, BehaviourBuilder, RuntimeCtx};
use reactor_macros::msg_converter;

msg_converter! {
   Unions: [
       ServerIn = ReadOut, WriteOut;
       ServerOut = ReadAck, WriteAck;
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

impl ServerSender {
    fn new(r_client_addrs: ActorAddrRef, w_client_addrs: ActorAddrRef) -> Self {
        ServerSender {
            read_client_addr: vec![r_client_addrs],
            write_client_addr: vec![w_client_addrs],
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

pub(crate) async fn server(
    ctx: RuntimeCtx,
    reader_addr: ActorAddrRef,
    writer_addr: ActorAddrRef,
    decoder: SubDecoderStore<ServerIn>,
) {
    BehaviourBuilder::new(Server {}, BincodeCodec::default())
        .send(ServerSender::new(reader_addr, writer_addr))
        .sub_decoders(decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
}
