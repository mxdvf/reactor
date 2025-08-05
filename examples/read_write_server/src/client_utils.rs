use std::marker::PhantomData;
use reactor_actor::{ActorAddrRef, Msg};

#[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct GeneratorOut;

// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
pub struct ClientSender<R> {
    server_addr: Vec<ActorAddrRef>,
    response: PhantomData<R>,
}

impl<R: Msg> reactor_actor::ActorSend for ClientSender<R> {
    type OMsg = R;

    async fn before_send(&mut self, _output: &Self::OMsg) -> &Vec<ActorAddrRef> {
        &self.server_addr
    }
}

impl<R> ClientSender<R> {
    pub fn new(server_addr: ActorAddrRef) -> Self {
        ClientSender {
            server_addr: vec![server_addr],
            response: PhantomData,
        }
    }
}
