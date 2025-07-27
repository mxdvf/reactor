pub use reactor_actor::setup_shared_logger_ref;

use bincode::{Decode, Encode};

use reactor_actor::RuntimeCtx;
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{ActorAddrRef, BehaviourBuilder};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};
use std::collections::HashMap;
use std::time::Duration;

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
pub enum PingPongMsg {
    Ping,
    Pong,
}

// //////////////////////////////////////////////////////////////////////////////
//                                  Processor
// //////////////////////////////////////////////////////////////////////////////
struct Processor;
impl reactor_actor::ActorProcess for Processor {
    type IMsg = PingPongMsg;
    type OMsg = PingPongMsg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        std::thread::sleep(Duration::from_secs(1));
        println!("{input:?}");
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
impl Sender {
    fn new(other_actor: ActorAddrRef) -> Self {
        Sender {
            other_addr: vec![other_actor],
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

pub async fn actor(ctx: RuntimeCtx, other_addr: ActorAddrRef) {
    BehaviourBuilder::new(Processor {})
        .send(Sender::new(other_addr))
        .generator_if(ctx.addr == "pinger", || {
            vec![PingPongMsg::Ping].into_iter()
        })
        .build()
        .run(ctx, BincodeCodec::default())
        .await
        .unwrap();
}

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[unsafe(no_mangle)]
pub fn pingpong(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let other = payload.remove("other").unwrap();
    RUNTIME.spawn(actor(ctx, other.as_str().unwrap().to_string().leak()));
}
