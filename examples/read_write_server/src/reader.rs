use crate::ServerOut;
use crate::client_utils::{ClientSender, GeneratorOut};
use reactor_actor::SubDecoderStore;
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RuntimeCtx};
use reactor_macros::Msg as DeriveMsg;
use reactor_macros::{DefaultPrio, msg_converter};

#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct ReadOut;

#[derive(
    Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone, DeriveMsg, DefaultPrio,
)]
pub struct ReadAck;

msg_converter! {
   Unions: [
       ReaderIn = ReadAck, GeneratorOut;
   ];

   Adapters: [
       ReaderIn from ServerOut via ReadAck;
   ];
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
                log::info!("Read sent");
                vec![ReadOut]
            }
        }
    }
}

pub(crate) async fn reader(
    ctx: RuntimeCtx,
    server_addr: String,
    decoder: SubDecoderStore<ReaderIn>,
) {
    BehaviourBuilder::new(ReadClient {}, BincodeCodec::default())
        .send(ClientSender::new(server_addr))
        .generator_if(true, || {
            vec![ReaderIn::GeneratorOut(GeneratorOut); 10].into_iter()
        })
        .sub_decoders(decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
}
