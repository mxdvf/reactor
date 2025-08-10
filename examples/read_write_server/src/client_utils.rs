use reactor_actor::{ActorAddrRef, Msg};
use std::{borrow::Cow, marker::PhantomData};

#[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct GeneratorOut;

// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
pub struct ClientSender<R> {
    server_addr: Vec<ActorAddrRef<'static>>,
    response: PhantomData<R>,
}

impl<R: Msg> reactor_actor::ActorSend for ClientSender<R> {
    type OMsg = R;

    async fn before_send<'a>(&'a mut self, _output: &Self::OMsg) -> Cow<'a, [ActorAddrRef<'a>]> {
        (&self.server_addr).into()
    }
}

impl<R> ClientSender<R> {
    pub fn new(server_addr: ActorAddrRef<'static>) -> Self {
        ClientSender {
            server_addr: vec![server_addr],
            response: PhantomData,
        }
    }
}
