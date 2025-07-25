pub use reactor_actor::setup_shared_logger_ref;

use bincode::{Decode, Encode};
use reactor_actor::{ActorAddrRef, Msg, BehaviourBuilder};
use std::collections::HashMap;
use std::time::Duration;
use reactor_actor::HasPriority;
use reactor_actor::codec::LengthDelimitedCodec;
use reactor_macros::MsgWithDefaultPriority;

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode, Debug, Clone, MsgWithDefaultPriority)]
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

// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

pub async fn actor(node_comm: reactor_actor::NodeComm, my_addr: ActorAddrRef, other_addr: ActorAddrRef) {
    let mut bb = BehaviourBuilder::new(Processor{}).send(
            Sender {
                other_addr: vec![other_addr],
            }
        );
    if my_addr == "pinger" {
        bb = bb.generator(Box::new(vec![PingPongMsg::Ping].into_iter()));
    }
    let behaviour = bb.build();

    reactor_actor::actor(my_addr, behaviour, LengthDelimitedCodec::default(), node_comm)
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
    mut payload: HashMap<String, serde_json::Value>,
) {
    let other = payload.remove("other").unwrap();
    RUNTIME.spawn(actor(node_comm, actor_name, other.as_str().unwrap().to_string().leak()));
}
