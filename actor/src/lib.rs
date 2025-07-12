use std::marker::PhantomData;

use bincode::{Decode, Encode};
use err::ActorError;
use futures::future::join_all;
use recv::rx2;
use send::tx2;
use tokio::{
    sync::mpsc::{self},
    task::JoinHandle,
};
use tokio_util::codec::{Decoder, Encoder};
pub use tracing_shared::setup_shared_logger_ref;
// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub mod common;
mod err;
mod node_comm;
mod recv;
mod send;
pub use node_comm::{Connection, ControlInst, ControlReq, NodeComm};

/// State of the Processor
pub trait State: Default + Send {}
/// State of the Receiver
pub trait RState: Default + Send {}
/// State of the Sender
pub trait SState: Default + Send {}

/// Messages that can flow between the actors.
pub trait Msg: Send + Sync + std::fmt::Debug + 'static + Clone {}

/// Addr of the actors
pub type ActorAddr = &'static str;

#[derive(Encode, Decode, Debug, Clone)]
pub struct EmptyMsg;
impl Msg for EmptyMsg {}

/// Represents the action to take after receiving and decoding a message from a channel.
///
/// This enum is used by the message receiver to determine
/// how to handle an incoming message based on its content and the current channel state.
///
/// # Variants
///
/// - `PASS`:
///   Forward the message to the next stage in the pipeline (e.g., processor).
///
/// - `PANIC`:
///   Indicates a critical error. Triggers a panic, typically used to fail fast on invalid or unexpected input.
//
/// - `DROP`:
///   Silently discard the message without processing or forwarding.
///
/// - `SYNC(u16)`:
///   Perform a synchronization operation with the provided sync identifier (e.g., a sync round or epoch number).
///
/// - `CLOSE`:
///   Signals that the channel should be closed and no more messages will be received from it.
#[derive(Debug)]
pub enum ChannelAction {
    PASS,
    PANIC,
    DROP,
    SYNC(u16),
    CLOSE,
}

pub struct DecodeErr;
impl From<std::io::Error> for DecodeErr {
    fn from(_: std::io::Error) -> Self {
        DecodeErr
    }
}

enum R2PMsg<T> {
    Msg(T),
    Exit,
}

pub trait ActorRecv: Send + 'static {
    type IMsg: Msg;
    fn after_recv(
        &mut self,
        input: &Self::IMsg,
    ) -> impl std::future::Future<Output = ChannelAction> + Send;
}

pub struct NoOpActorRecv<M> {
    m: PhantomData<M>,
}
impl<M: Msg> ActorRecv for NoOpActorRecv<M> {
    type IMsg = M;
    async fn after_recv(&mut self, _input: &Self::IMsg) -> ChannelAction {
        panic!("This Shouldn't be used")
    }
}

pub trait ActorProcess: Send + 'static {
    type IMsg: Msg;
    type OMsg: Msg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg>;
}

pub trait ActorSend: Send + 'static {
    type OMsg: Msg;
    fn before_send(
        &mut self,
        output: &Self::OMsg,
    ) -> impl std::future::Future<Output = &Vec<ActorAddr>> + Send;
}
pub struct NoOpActorSend<M> {
    m: PhantomData<M>,
}
impl<M: Msg> ActorSend for NoOpActorSend<M> {
    type OMsg = M;

    async fn before_send(&mut self, _output: &Self::OMsg) -> &Vec<ActorAddr> {
        panic!("This Shouldn't be used")
    }
}

pub struct Behaviour<R, P, S, M> {
    recv: Option<R>,
    proc: P,
    send: Option<S>,
    generators: Vec<Box<dyn Iterator<Item = M> + Send>>,
}
impl<P, IM, OM> Behaviour<NoOpActorRecv<IM>, P, NoOpActorSend<OM>, EmptyMsg>
where
    P: ActorProcess<IMsg = IM, OMsg = OM>,
{
    pub fn with_proc_only(proc: P) -> Behaviour<NoOpActorRecv<IM>, P, NoOpActorSend<OM>, IM> {
        Behaviour {
            recv: None,
            proc,
            send: None,
            generators: Vec::new(),
        }
    }
}

impl<O, P, S, M> Behaviour<NoOpActorRecv<M>, P, S, EmptyMsg>
where
    P: ActorProcess<OMsg = O, IMsg = M>,
    S: ActorSend<OMsg = O>,
{
    pub fn with_send(proc: P, send: S) -> Behaviour<NoOpActorRecv<M>, P, S, M> {
        Behaviour {
            recv: None,
            proc,
            send: Some(send),
            generators: Vec::new(),
        }
    }
}

impl<I, O, R, P> Behaviour<R, P, NoOpActorSend<O>, I>
where
    R: ActorRecv<IMsg = I>,
    P: ActorProcess<IMsg = I, OMsg = O>,
{
    pub fn with_recv(proc: P, recv: R) -> Behaviour<R, P, NoOpActorSend<O>, I> {
        Behaviour {
            recv: Some(recv),
            proc,
            send: None,
            generators: Vec::new(),
        }
    }
}

impl<I, O, R, P, S> Behaviour<R, P, S, I>
where
    R: ActorRecv<IMsg = I>,
    P: ActorProcess<IMsg = I, OMsg = O>,
    S: ActorSend<OMsg = O>,
{
    pub fn with_recv_send(proc: P, recv: R, send: S) -> Self {
        Behaviour {
            recv: Some(recv),
            proc,
            send: Some(send),
            generators: Vec::new(),
        }
    }

    pub fn add_generator(&mut self, generator: Box<dyn Iterator<Item = I> + Send>) {
        self.generators.push(generator);
    }
}

impl<R, P, S, M> Behaviour<R, P, S, M> {
    fn take_recv(&mut self) -> Option<R> {
        self.recv.take()
    }
    fn take_send(&mut self) -> Option<S> {
        self.send.take()
    }
    fn take_generators(&mut self) -> Vec<Box<dyn Iterator<Item = M> + Send>> {
        std::mem::take(&mut self.generators)
    }
}

pub async fn actor<I, O, R, P, S, CD>(
    addr: ActorAddr,
    mut behaviour: Behaviour<R, P, S, I>,
    codec: CD,
    node_comm: NodeComm,
) -> Result<(), ActorError>
where
    I: Msg,
    O: Msg,
    R: ActorRecv<IMsg = I>,
    P: ActorProcess<IMsg = I, OMsg = O>,
    S: ActorSend<OMsg = O>,
    CD: Encoder<O> + Decoder<Item = I, Error = DecodeErr> + Send + Sync + Clone + 'static,
{
    let my_addr = addr.to_string();
    let (r2p_tx, mut r2p_rx) = mpsc::unbounded_channel::<R2PMsg<I>>();
    let (p2s_tx, p2s_rx) = mpsc::unbounded_channel::<O>();

    let (controller_rx, controller_tx) = node_comm.split();

    let reciever = behaviour.take_recv();
    let sender = behaviour.take_send();
    let mut generators = behaviour.take_generators();
    let mut processor = behaviour.proc;

    let gen_handles: Vec<tokio::task::JoinHandle<_>> = generators
        .drain(..)
        .map(|gene| tokio::spawn(generator(gene, r2p_tx.clone())))
        .collect();

    let rx_handle = tokio::spawn(rx2(
        my_addr.clone().leak(),
        reciever,
        r2p_tx,
        codec.clone(),
        controller_rx,
    ));

    let addr = my_addr.clone();
    let proc_handle: JoinHandle<Result<(), ActorError>> =
        tokio::task::spawn_blocking(move || -> Result<(), ActorError> {
            tracing::info!("[ACTOR][{}] Processor Started", addr);
            while let Some(i) = r2p_rx.blocking_recv() {
                if let R2PMsg::Msg(msg) = i {
                    let processed_messages = processor.process(msg);

                    for message in processed_messages {
                        p2s_tx.send(message).map_err(|_| ActorError::P2SErr)?;
                    }
                } else {
                    break;
                }
            }
            tracing::info!("[ACTOR][{}] Processor Ended", addr);
            Ok(())
        });
    let tx_handle = tokio::spawn(tx2(my_addr.leak(), sender, p2s_rx, controller_tx, codec));
    rx_handle.await??;
    proc_handle.await??;
    tx_handle.await?;
    join_all(gen_handles).await;
    Ok(())
}

type SendBuffer<M> = mpsc::UnboundedSender<M>;
async fn generator<G, M>(
    generator: G,
    p_tx: mpsc::UnboundedSender<R2PMsg<M>>,
) -> Result<(), ActorError>
where
    G: Iterator<Item = M>,
    M: Msg + 'static,
{
    for m in generator {
        p_tx.send(R2PMsg::Msg(m)).map_err(|_| ActorError::R2PErr)?;
    }
    Ok(())
}
