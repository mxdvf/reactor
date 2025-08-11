use crate::client_utils::ClientSender;
use crate::client_utils::GeneratorOut;
use crate::server::ServerOut;
use reactor_actor::SubDecoderStore;
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RuntimeCtx};
use reactor_macros::DefaultPrio;
use reactor_macros::Msg as DeriveMsg;
use reactor_macros::msg_converter;

#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct WriteOut;

#[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct WriteAck;

msg_converter! {
   Unions: [
       WriterIn = WriteAck, GeneratorOut;
   ];

   Adapters: [
       WriterIn from ServerOut via WriteAck;
   ];
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
                log::info!("Write sent");
                vec![WriteOut]
            }
        }
    }
}

pub(crate) async fn writer(
    ctx: RuntimeCtx,
    server_addr: String,
    decoder: SubDecoderStore<WriterIn>,
) {
    BehaviourBuilder::new(WriteClient {}, BincodeCodec::default())
        .send(ClientSender::new(server_addr))
        .generator_if(true, || {
            vec![WriterIn::GeneratorOut(GeneratorOut); 10].into_iter()
        })
        .sub_decoders(decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
}
