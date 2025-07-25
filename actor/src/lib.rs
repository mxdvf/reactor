use std::marker::PhantomData;

use bincode::{Decode, Encode};
use err::{ActorError, DecodeErr};
use futures::future::join_all;
use prio_channel::{PriorityChannelTx, priority_channel};
use recv::rx;
use send::tx;
use tokio::{
    sync::mpsc::{self, error::TryRecvError},
    task::JoinHandle,
};
use tokio_util::codec::{Decoder, Encoder};
pub use tracing_shared::setup_shared_logger_ref;

pub mod common;
pub mod err;
mod node_comm;
mod prio_channel;
mod recv;
mod send;
pub use node_comm::{Connection, ControlInst, ControlReq, NodeComm};
pub use prio_channel::{HasPriority, MAX_PRIO};

/// Messages that can flow between the actors.
pub trait Msg: Send + Sync + std::fmt::Debug + HasPriority + 'static + Clone {}

/// Addr of the actors
pub type ActorAddrRef = &'static str;
pub type ActorAddr = String;

#[derive(Encode, Decode, Debug, Clone)]
pub struct EmptyMsg;
impl HasPriority for EmptyMsg {}
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

#[derive(Debug, PartialEq, Clone)]
enum R2PMsg<T> {
    Msg(T),
    Exit,
}

impl<T: HasPriority> HasPriority for R2PMsg<T> {
    fn priority(&self) -> usize {
        match self {
            R2PMsg::Msg(t) => t.priority(),
            R2PMsg::Exit => MAX_PRIO, // How to make it highest priority?!
        }
    }
}

/// The `ActorRecv` trait defines what action to take based on incoming message.
///
/// It defines a single asynchronous method, [`ActorRecv::after_recv`], that is called after a message is received.
///
/// # Type Parameters
/// - `IMsg`: The type of the message this actor receives. It must implement the [`Msg`] trait.
///
pub trait ActorRecv: Send + 'static {
    /// The input message type that this actor receives.
    type IMsg: Msg;
    /// Called after the actor receives a message.
    ///
    /// This asynchronous method is invoked with:
    /// - `worker_id`: A reference to the address of the sending actor.
    /// - `input`: A reference to the message that was received.
    ///
    /// It returns [`ChannelAction`] that determines how the actor should proceed.
    fn after_recv(
        &mut self,
        worker_id: ActorAddrRef,
        input: &Self::IMsg,
    ) -> impl std::future::Future<Output = ChannelAction> + Send;
}

pub struct NoOpActorRecv<M> {
    m: PhantomData<M>,
}
impl<M: Msg> ActorRecv for NoOpActorRecv<M> {
    type IMsg = M;
    async fn after_recv(&mut self, _addr: ActorAddrRef, _input: &Self::IMsg) -> ChannelAction {
        panic!("This Shouldn't be used")
    }
}

/// The `ActorProcess` trait defines the processing logic for an actor that transforms
/// input messages into one or more output messages.
///
/// # Example
/// ```ignore
/// struct Incrementer;
///
/// impl ActorProcess for Incrementer {
///     type IMsg = i32;
///     type OMsg = i32;
///
///     fn process(&mut self, input: i32) -> Vec<i32> {
///         vec![input + 1]
///     }
/// }
///
pub trait ActorProcess: Send + 'static {
    /// The type of messages this actor accepts as input.
    type IMsg: Msg;

    /// The type of messages this actor produces as output.
    type OMsg: Msg;

    /// Processes an input message and returns a list of output messages.
    ///
    /// # Arguments
    ///
    /// * `input` - The input message to be processed.
    ///
    /// # Returns
    ///
    /// A vector of output messages of type [`Self::OMsg`].
    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg>;
}

/// The `ActorSend` trait defines how an actor determines the recipients of a message
/// before it is sent.
///
/// # Example
/// ```ignore
/// struct Broadcaster {
///     peers: Vec<ActorAddrRef>,
/// }
///
/// impl ActorSend for Broadcaster {
///     type OMsg = MyMessage;
///
///     async fn before_send<'a>(
///         &'a mut self,
///         _output: &Self::OMsg,
///     ) -> &'a Vec<ActorAddrRef> {
///         &self.peers
///     }
/// }
///
pub trait ActorSend: Send + 'static {
    /// The type of output messages that this actor sends.
    type OMsg: Msg;

    /// Called before an output message is sent.
    ///
    /// This asynchronous method returns the list of recipients that the message should be sent to.
    ///
    /// # Arguments
    ///
    /// * `output` - A reference to the message that is about to be sent.
    ///
    /// # Returns
    ///
    /// a list of [`ActorAddrRef`] indicating the recipient actors.
    fn before_send<'a>(
        &'a mut self,
        output: &Self::OMsg,
    ) -> impl std::future::Future<Output = &'a Vec<ActorAddrRef>> + Send;
}
pub struct NoOpActorSend<M> {
    m: PhantomData<M>,
}
impl<M: Msg> ActorSend for NoOpActorSend<M> {
    type OMsg = M;

    async fn before_send(&mut self, _output: &Self::OMsg) -> &Vec<ActorAddrRef> {
        panic!("This Shouldn't be used")
    }
}

/// The `Behaviour` struct encapsulates the complete behavior of an actor,
/// including how it receives messages, processes them, sends output,
/// and optionally generates new messages.
///
/// # Type Parameters
///
/// - `R`: The type implementing the receiving behavior (must implement [`ActorRecv`]).
/// - `P`: The type implementing the processing behavior (must implement [`ActorProcess`]).
/// - `S`: The type implementing the sending behavior (must implement [`ActorSend`]).
/// - `M`: The type of message generated internally (e.g., from generators).
///
/// # Fields
///
/// - `recv`: Optional receiver logic implementing `ActorRecv`.
/// - `proc`: The core processing logic implementing `ActorProcess`.
/// - `send`: Optional sender logic implementing `ActorSend`.
/// - `generators`: A list of internal message generators, producing messages of type `M`.
///
pub struct Behaviour<R, P, S, M> {
    recv: Option<R>,
    proc: P,
    send: Option<S>,
    generators: Vec<Box<dyn Iterator<Item = M> + Send>>,
    num_prios: Option<usize>,
}

impl<P, IM, OM> Behaviour<NoOpActorRecv<IM>, P, NoOpActorSend<OM>, EmptyMsg>
where
    P: ActorProcess<IMsg = IM, OMsg = OM>,
{
    /// Constructs a [`Behaviour`] that only has processing logic, with no explicit
    /// receive or send behavior.
    ///
    /// This is useful for actors that dont have a logic to handle received
    /// messages and send message to a blackhole.
    ///
    /// # Arguments
    ///
    /// * `proc` - The processing logic implementing [`ActorProcess`].
    ///
    /// # Returns
    ///
    /// A `Behaviour` with `NoOpActorRecv` and `NoOpActorSend` as defaults for `recv` and `send`.
    ///
    /// # Example
    /// ```ignore
    /// let behaviour = Behaviour::with_proc_only(MyProcessor {});
    /// ```
    pub fn with_proc_only(proc: P) -> Behaviour<NoOpActorRecv<IM>, P, NoOpActorSend<OM>, IM> {
        Behaviour {
            recv: None,
            proc,
            send: None,
            generators: Vec::new(),
            num_prios: None,
        }
    }
}

impl<O, P, S, M> Behaviour<NoOpActorRecv<M>, P, S, EmptyMsg>
where
    P: ActorProcess<OMsg = O, IMsg = M>,
    S: ActorSend<OMsg = O>,
{
    /// Constructs a [`Behaviour`] that only has processing logic and a routing logic with no explicit
    /// receive behavior.
    ///
    /// This is useful for actors that dont have a logic to handle received
    /// messages.
    ///
    /// # Arguments
    ///
    /// * `proc` - The processing logic implementing [`ActorProcess`].
    /// * `send` - The routing logic implementing [`ActorSend`].
    ///
    /// # Returns
    ///
    /// A `Behaviour` with `NoOpActorRecv` for `recv`.
    ///
    /// # Example
    /// ```ignore
    /// let behaviour = Behaviour::with_send(MyProcessor {});
    /// ```
    pub fn with_send(proc: P, send: S) -> Behaviour<NoOpActorRecv<M>, P, S, M> {
        Behaviour {
            recv: None,
            proc,
            send: Some(send),
            generators: Vec::new(),
            num_prios: None,
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
            num_prios: None,
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
            num_prios: None,
        }
    }

    pub fn add_generator(&mut self, generator: Box<dyn Iterator<Item = I> + Send>) {
        self.generators.push(generator);
    }
    pub fn num_prios(&mut self, prios: usize) {
        if prios == 0 {
            self.num_prios = None;
        } else {
            self.num_prios = Some(prios)
        }
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
    addr: ActorAddrRef,
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
    // let (r2p_tx, mut r2p_rx) = mpsc::unbounded_channel::<R2PMsg<I>>();
    let (p2s_tx, p2s_rx) = mpsc::unbounded_channel::<O>();
    let (r2p_tx, mut r2p_rx) = priority_channel::<R2PMsg<I>>(behaviour.num_prios.unwrap_or(1));

    let (controller_rx, controller_tx) = node_comm.split();

    let reciever = behaviour.take_recv();
    let sender = behaviour.take_send();
    let mut generators = behaviour.take_generators();
    let mut processor = behaviour.proc;

    let gen_handles: Vec<tokio::task::JoinHandle<_>> = generators
        .drain(..)
        .map(|gene| {
            let tx = r2p_tx.clone();
            tokio::spawn(generator(gene, tx))
        })
        .collect();

    let rx_handle = tokio::spawn(rx(
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
            loop {
                match r2p_rx.try_recv() {
                    Ok(R2PMsg::Msg(m)) => {
                        let processed = processor.process(m);
                        for o in processed {
                            p2s_tx.send(o).map_err(|_| ActorError::P2SErr)?;
                        }
                    }
                    Ok(R2PMsg::Exit) => {
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
            tracing::info!("[ACTOR][{}] Processor Ended", addr);
            Ok(())
        });
    let tx_handle = tokio::spawn(tx(my_addr.leak(), sender, p2s_rx, controller_tx, codec));
    rx_handle.await??;
    proc_handle.await??;
    tx_handle.await?;
    join_all(gen_handles).await;
    Ok(())
}

async fn generator<G, M>(generator: G, p_tx: PriorityChannelTx<R2PMsg<M>>) -> Result<(), ActorError>
where
    G: Iterator<Item = M>,
    M: Msg + 'static,
{
    for m in generator {
        p_tx.send(R2PMsg::Msg(m)).unwrap();
    }
    Ok(())
}
