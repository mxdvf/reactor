pub use reactor_actor::setup_shared_logger_ref;

use bincode::{Decode, Encode};
use reactor_actor::{
    ActorAddrRef, DecodeErr, Msg,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::bytes::{Bytes, BytesMut};

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode, Debug, Clone)]
pub enum PingPongMsg {
    Ping,
    Pong,
}

impl Msg for PingPongMsg {}

// //////////////////////////////////////////////////////////////////////////////
//                                  Processor
// //////////////////////////////////////////////////////////////////////////////
struct Processor;
impl reactor_actor::ActorProcess for Processor {
    type IMsg = PingPongMsg;
    type OMsg = PingPongMsg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        std::thread::sleep(Duration::from_secs(1));
        println!("{:?}", input);
        match input {
            PingPongMsg::Ping => vec![PingPongMsg::Pong],
            PingPongMsg::Pong => vec![PingPongMsg::Ping],
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
struct Sender {
    other_addr: Vec<ActorAddrRef>,
}
impl reactor_actor::ActorSend for Sender {
    type OMsg = PingPongMsg;

    async fn before_send(&mut self, _output: &Self::OMsg) -> &Vec<ActorAddrRef> {
        &self.other_addr
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                  CODEC
// //////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct PingPongCodec {
    config: bincode::config::Configuration,
    length_codec: tokio_util::codec::LengthDelimitedCodec,
}
impl PingPongCodec {
    pub fn new() -> Self {
        PingPongCodec {
            config: bincode::config::standard(),
            length_codec: tokio_util::codec::LengthDelimitedCodec::builder()
                .length_field_length(4)
                .max_frame_length(u32::MAX as usize)
                .new_codec(),
        }
    }
}
impl Default for PingPongCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl tokio_util::codec::Decoder for PingPongCodec {
    type Item = PingPongMsg;
    type Error = DecodeErr;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let frame = match self.length_codec.decode(src).map_err(|_| DecodeErr)? {
            Some(frame) => frame,
            None => return Ok(None),
        };
        let (message, _) =
            bincode::decode_from_slice(&frame, self.config).map_err(|_| DecodeErr)?;

        Ok(Some(message))
    }
}
impl tokio_util::codec::Encoder<PingPongMsg> for PingPongCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: PingPongMsg, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_data = bincode::encode_to_vec(&item, self.config).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to encode data")
        })?;
        self.length_codec
            .encode(Bytes::from(encoded_data), dst)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Couldn't encode length-delimited data",
                )
            })?;

        Ok(())
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

pub async fn actor(node_comm: reactor_actor::NodeComm, my_addr: ActorAddrRef, other_addr: ActorAddrRef) {
    let mut behaviour = reactor_actor::Behaviour::with_send(
        Processor {},
        Sender {
            other_addr: vec![other_addr],
        },
    );
    if my_addr == "pinger" {
        behaviour.add_generator(Box::new(vec![PingPongMsg::Ping].into_iter()));
    }

    reactor_actor::actor(&my_addr, behaviour, PingPongCodec::new(), node_comm)
        .await
        .unwrap();
}

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[unsafe(no_mangle)]
pub fn pingpong(
    actor_name: &'static str,
    node_comm: reactor_actor::NodeComm,
    mut payload: HashMap<String, String>,
) {
    let other = payload.remove("other").unwrap();
    RUNTIME.spawn(actor(node_comm, actor_name, other.leak()));
}
