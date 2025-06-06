use std::{sync::Arc, time::Duration, sync::Mutex};

use bincode::{Decode, Encode};
use reactor_actor::{
    ActorAddr, ChannelAction, ControlInst, ControlReq, DecodeErr, Generator, Msg, RState, SState,
    State, common::sender_task,
};
use tokio::sync::mpsc;
use tokio_util::bytes::{Bytes, BytesMut};

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode, Debug)]
pub enum PingPongMsg {
    Ping,
    Pong,
}

impl Msg for PingPongMsg {}

// //////////////////////////////////////////////////////////////////////////////
//                                RECEIVER STATE
// //////////////////////////////////////////////////////////////////////////////
#[derive(Default)]
struct ChannelState {}
impl RState for ChannelState {}

// //////////////////////////////////////////////////////////////////////////////
//                                PROCESSOR STATE
// //////////////////////////////////////////////////////////////////////////////
#[derive(Default)]
pub struct PingPongState {}
impl State for PingPongState {}

// //////////////////////////////////////////////////////////////////////////////
//                                  SENDER STATE
// //////////////////////////////////////////////////////////////////////////////
#[derive(Default)]
struct RouterState {
    _my_addr: ActorAddr,
    other_addr: ActorAddr,
}
impl SState for RouterState {}

// //////////////////////////////////////////////////////////////////////////////
//                                  CALLBACKS
// //////////////////////////////////////////////////////////////////////////////
fn after_recv(_msg: &PingPongMsg, _channel_state: &Arc<Mutex<ChannelState>>) -> ChannelAction {
    println!("Rcvd {_msg:?}");
    ChannelAction::PASS
}

fn processor(msg: PingPongMsg, _state: &mut PingPongState) -> PingPongMsg {
    std::thread::sleep(Duration::from_secs(1));
    match msg {
        PingPongMsg::Ping => PingPongMsg::Pong,
        PingPongMsg::Pong => PingPongMsg::Ping,
    }
}

fn before_send(_msg: &PingPongMsg, state: &mut RouterState) -> ActorAddr {
    println!("Sent {_msg:?} to {}", state.other_addr);
    state.other_addr
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

fn gen_ping(_i: u64, _s: &mut ()) -> PingPongMsg {
    println!("Generating ping");
    PingPongMsg::Ping
}

pub async fn actor(
    controller_rx: mpsc::UnboundedReceiver<ControlInst>,
    controller_tx: mpsc::Sender<ControlReq>,
    my_addr: ActorAddr,
    other_addr: ActorAddr,
) {
    println!("Myaddr {my_addr}, OtherAddr {other_addr}");
    let mut generators = Vec::new();
    if my_addr == "pinger" {
        generators.push(Generator {
            s: (),
            callback: gen_ping,
            start: Duration::from_millis(1),
            interval: Duration::from_secs(1),
            max: Some(1),
        });
    }
    reactor_actor::actor(
        my_addr,
        generators,
        ChannelState::default(),
        after_recv,
        PingPongState{},
        processor,
        RouterState {
            _my_addr: my_addr,
            other_addr,
        },
        before_send,
        PingPongCodec::new(),
        controller_rx,
        controller_tx,
        sender_task,
    )
    .await
    .unwrap();
}

