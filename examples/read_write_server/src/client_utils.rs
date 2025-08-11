use reactor_actor::{ActorAddrs, Msg};
use std::marker::PhantomData;

#[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct GeneratorOut;

// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
pub struct ClientSender<R> {
    server_addr: Vec<String>,
    response: PhantomData<R>,
}

impl<R: Msg> reactor_actor::ActorSend for ClientSender<R> {
    type OMsg = R;

    async fn before_send<'a>(&'a mut self, _output: &Self::OMsg) -> ActorAddrs<'a> {
        ActorAddrs::borrowed(&self.server_addr)
    }
}

impl<R> ClientSender<R> {
    pub fn new(server_addr: String) -> Self {
        ClientSender {
            server_addr: vec![server_addr],
            response: PhantomData,
        }
    }
}
