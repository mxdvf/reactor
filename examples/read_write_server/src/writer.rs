use crate::DeriveMsg;
use reactor_actor::{ActorAddrRef, BehaviourBuilder, RuntimeCtx};
use reactor_macros::{msg_converter, DefaultPrio};
use crate::client_utils::GeneratorOut;

msg_converter! {
   Unions: [
       WriterIn = WriteAck, GeneratorOut;
   ];
}

#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct WriteOut;

#[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct WriteAck;

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

pub async fn _writer(ctx: RuntimeCtx, server_addr: ActorAddrRef, decoder: SubDecoderStore<WriterIn>) {
    BehaviourBuilder::new(WriteClient {}, BincodeCodec::default())
        .send(ClientSender::new(server_addr))
        .generator_if(true, || vec![WriterIn::GeneratorOut(GeneratorOut); 10].into_iter())
        .sub_decoders(decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
}
