//! Query Processing Engine (L2) built on top of tensile core (L1)
use std::sync::Arc;

use bincode::{Decode, Encode};
use codec::QpMsgCodec;
use tensile_core::{
    actor::{
        ActorAddr, ChannelAction, GState, Generator, Msg, RState, SState, State,
        common::sender_task,
    },
    node::{ControlInst, ControlReq},
};
use tokio::sync::{Mutex, mpsc};

mod codec;

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode)]
pub enum QPMsg<R> {
    Ckpt,
    Row(R),
}

impl<O, S, I> Msg for QPMsg<I> where I: Row<Output = O, State = S> {}

pub trait Row: Send + Sized + Clone + bincode::Decode<()> + bincode::Encode {
    type State: QPSubState;
    type Output: Row;
    fn process(&self, state: &mut Self::State) -> Self::Output;
}

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
pub struct QPState<S> {
    processor_state: S,
}
impl<S: QPSubState> State for QPState<S> {}
impl<S> QPState<S> {
    fn get_mut_proc_state(&mut self) -> &mut S {
        &mut self.processor_state
    }
}

pub trait QPSubState: Default + Send {}

// //////////////////////////////////////////////////////////////////////////////
//                                  SENDER STATE
// //////////////////////////////////////////////////////////////////////////////
#[derive(Default)]
struct RouterState {}
impl SState for RouterState {}

// //////////////////////////////////////////////////////////////////////////////
//                                  CALLBACKS
// //////////////////////////////////////////////////////////////////////////////
fn after_recv<R: Row>(msg: &QPMsg<R>, channel_state: &Arc<Mutex<ChannelState>>) -> ChannelAction {
    match msg {
        QPMsg::Ckpt => ChannelAction::SYNC(1),
        QPMsg::Row(_) => ChannelAction::PASS,
    }
}

fn processor<I: Row<Output = O, State = S>, S: QPSubState, O: Row>(
    msg: QPMsg<I>,
    state: &mut QPState<S>,
) -> QPMsg<O> {
    match msg {
        QPMsg::Ckpt => todo!(),
        QPMsg::Row(r) => QPMsg::Row(r.process(state.get_mut_proc_state())),
    }
}

fn before_send<R: Row>(msg: &QPMsg<R>, state: &mut RouterState) -> ActorAddr {
    match msg {
        QPMsg::Ckpt => todo!(),
        QPMsg::Row(_) => todo!(),
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                ACTOR
// //////////////////////////////////////////////////////////////////////////////
pub async fn actor<O, I, GS>(
    my_addr: ActorAddr,
    generators: Vec<Generator<GS, QPMsg<I>>>,
    controller_rx: mpsc::UnboundedReceiver<ControlInst>,
    controller_tx: mpsc::Sender<ControlReq>,
) where
    O: Row + 'static,
    I: Row<Output = O> + 'static,
    GS: GState + 'static,
{
    tensile_core::actor::actor(
        my_addr,
        generators,
        ChannelState::default(),
        after_recv,
        processor,
        RouterState::default(),
        before_send,
        QpMsgCodec::<I, O>::new(),
        controller_rx,
        controller_tx,
        sender_task,
    )
    .await;
}
