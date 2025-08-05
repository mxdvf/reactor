use reactor_actor::{ActorAddrRef, BehaviourBuilder, RuntimeCtx};
use reactor_actor::codec::BincodeCodec;
use reactor_macros::DefaultPrio;
use crate::client_utils::{ClientSender, GeneratorOut};
use crate::DeriveMsg;

#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct ReadOut;

#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct ReadAck;

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

pub async fn _reader(ctx: RuntimeCtx, server_addr: ActorAddrRef, decoder: SubDecoderStore<ReaderIn>) {
    BehaviourBuilder::new(ReadClient {}, BincodeCodec::default())
        .send(ClientSender::new(server_addr))
        .generator_if(true, || vec![ReaderIn::GeneratorOut(GeneratorOut); 10].into_iter())
        .sub_decoders(decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
}
