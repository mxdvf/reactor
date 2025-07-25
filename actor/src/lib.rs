use err::{ActorError, RecieverErr};
use futures::{StreamExt, future::join_all};
use prio_channel::{HasPriority, MAX_PRIO, PriorityChannelTx, priority_channel};
use socket2::{Domain, Socket, Type};
use std::{
    any::Any,
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    io::AsyncRead,
    net::TcpListener,
    sync::{
        mpsc::{self, error::TryRecvError},
        oneshot,
    },
    task::{JoinHandle, JoinSet},
};
use tokio_util::{
    codec::{Decoder, Encoder, FramedRead},
    sync::CancellationToken,
};
pub use tracing_shared::setup_shared_logger_ref;

pub mod common;
mod err;
mod prio_channel;

/// State of the Processor
pub trait State: Default + Send {}
/// State of the Receiver
pub trait RState: Default + Send {}
/// State of the Sender
pub trait SState: Default + Send {}

pub trait Msg: Send + std::fmt::Debug + HasPriority + Clone {}

/// Addr of the actors
pub type ActorAddr = &'static str;

/// Type of channel that is used to send message from one local actor to the other.
pub type LocalChannelTx = mpsc::Sender<Box<dyn Any + Send>>;
pub type LocalChannelRx = mpsc::Receiver<Box<dyn Any + Send>>;

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

/// Instructions that are sent by the local controller to the actor
pub enum ControlInst {
    StartLocalRecv(LocalChannelRx),
    StartTcpRecv(u16),
    Stop,
}

/// Type of connection
#[derive(Debug)]
pub enum Connection {
    Remote(SocketAddr),
    Local(LocalChannelTx),
}

/// Requests that can be sent to the local controller by the actor or some external
/// API
pub enum ControlReq {
    Resolve {
        addr: ActorAddr,
        resp_tx: oneshot::Sender<Connection>,
    },
}

/// Asynchronously drives the message processing pipeline from input to output distribution.
///
/// This function wires together the three stages of a message processing system:
///
/// 1. **Receiver** (`rx`) — receives input messages and routes them using `ar` into a processing queue.
/// 2. **Processor** — runs on a blocking task, taking messages from the input queue, applying the `processor` logic,
///    and sending results to the output queue.
/// 3. **Transmitter** (`tx`) — consumes processed messages, determines their target `Addr` using `bs`,
///    and spawns dedicated sender tasks to forward messages per address.
///
/// The `sender_task` is a user-provided function that returns a `Future` which handles all outgoing messages
/// for a given address.
///
/// # Type Parameters
/// - `I`: Input message type (must implement `Msg`).
/// - `CS`:
/// - `RS`:
/// - `S`: State used during processing (must implement `State` and `Default`).
/// - `O`: Output message type (must implement `Msg`).
/// - `AR`: Function used to determine how incoming messages affect global channel (`&I -> ChannelAction`).
/// - `P`: Processor function that transforms an input message and mutable state into an output message.
/// - `BS`: Function mapping an output message to an `Addr`.
///
/// # Arguments
/// - `ar`: Function to classify input messages for routing.
/// - `processor`: The core processing function transforming `I` to `O` using state `S`.
/// - `bs`: Address resolution function to route `O` to an `Addr`.
/// - `sender_task`: Task factory that handles messages per address, returning a pinned future.
///
/// # Spawns
/// - One task for the receiver (`rx`).
/// - One blocking task for processing messages using `processor`.
/// - One task for transmitting results to their respective destinations (`tx`).
///
/// # Notes
/// - Uses unbounded channels internally for communication between stages.
/// - Automatically creates per-address sender tasks as messages are emitted.
///
/// # Panics
/// - Will panic if sending to the processing or sending channel fails (should not happen unless channels are closed).
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub async fn actor<I, S, RS, CD, SS, O, AR, P, BS>(
    my_addr: ActorAddr,
    mut generators: Vec<Box<dyn Iterator<Item = I> + Send>>,
    rs: RS,
    after_recieve: AR,
    mut processor_state: S,
    processor: P,
    bs: SS,
    before_send: BS,
    codec: CD,
    controller_rx: mpsc::UnboundedReceiver<ControlInst>,
    controller_tx: mpsc::Sender<ControlReq>,
    sender_task: fn(
        ActorAddr,
        mpsc::UnboundedReceiver<O>,
        CD,
        mpsc::Sender<ControlReq>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    num_prio: Option<usize>,
) -> Result<(), ActorError>
where
    I: Msg + 'static,
    S: State + 'static,
    RS: RState + 'static,
    SS: SState + 'static,
    O: Msg + 'static,
    AR: Fn(&I, &Arc<std::sync::Mutex<RS>>) -> ChannelAction + Send + Sync + 'static + Clone,
    P: Fn(I, &mut S) -> Vec<O> + Send + 'static,
    BS: Fn(&O, &mut SS) -> ActorAddr + Send + Sync + 'static,
    CD: Encoder<O> + Decoder<Item = I, Error = DecodeErr> + Send + Sync + Clone + 'static,
{
    let (p2s_tx, p2s_rx) = mpsc::unbounded_channel::<O>();
    let (p_tx, mut p_rx) = priority_channel::<R2PMsg<I>>(num_prio.unwrap_or(1));

    let gen_handles: Vec<tokio::task::JoinHandle<_>> = generators
        .drain(..)
        .map(|gene| {
            let tx = p_tx.clone(); // Clone the full PriorityChannelTx
            tokio::spawn(generator(gene, tx))
        })
        .collect();

    let rx_handle = tokio::spawn(rx(
        my_addr,
        after_recieve,
        rs,
        p_tx.clone(),
        codec.clone(),
        controller_rx,
    ));

    let proc_handle: JoinHandle<Result<(), ActorError>> =
        tokio::task::spawn_blocking(move || -> Result<(), ActorError> {
            tracing::info!("[ACTOR][{}] Processor Started", my_addr);

            loop {
                match p_rx.try_recv() {
                    Ok(R2PMsg::Msg(m)) => {
                        let processed = processor(m, &mut processor_state);
                        for o in processed {
                            p2s_tx.send(o).map_err(|_| ActorError::P2SErr)?;
                        }
                    }
                    Ok(R2PMsg::Exit) => {
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
            Ok(())
        });

    let tx_handle = tokio::spawn(tx(
        my_addr,
        before_send,
        bs,
        p2s_rx,
        controller_tx,
        codec,
        sender_task,
    ));
    rx_handle.await??;
    proc_handle.await??;
    tx_handle.await?;
    join_all(gen_handles).await;
    Ok(())
}

async fn remote_parent_recv_subtask<M, C, D, AR, RX>(
    after_recv: AR,
    pc_tx: PriorityChannelTx<R2PMsg<M>>,
    cstate: Arc<Mutex<C>>,
    mut framed_reader: FramedRead<RX, D>,
) where
    M: Msg,
    C: RState,
    D: Decoder<Item = M>,
    AR: Fn(&M, &Arc<Mutex<C>>) -> ChannelAction + 'static + Clone,
    RX: AsyncRead + Unpin,
{
    tracing::info!("[ACTOR] SubRx Started");
    loop {
        if let Some(Ok(msg)) = framed_reader.next().await {
            match after_recv(&msg, &cstate) {
                ChannelAction::PASS => {}
                ChannelAction::PANIC => {
                    panic!()
                }
                ChannelAction::DROP => {
                    continue;
                }
                ChannelAction::SYNC(_) => todo!(),
                ChannelAction::CLOSE => {
                    break;
                }
            }

            pc_tx.send(R2PMsg::Msg(msg)).unwrap();
        }
    }
    tracing::info!("[ACTOR] SubRx Ended");
}

async fn local_parent_recv_subtask<M, C, AR>(
    after_recv: AR,
    pc_tx: PriorityChannelTx<R2PMsg<M>>,
    cstate: Arc<Mutex<C>>,
    mut local_rx: LocalChannelRx,
) where
    M: Msg + 'static,
    C: RState,
    AR: Fn(&M, &Arc<Mutex<C>>) -> ChannelAction + 'static + Clone,
{
    tracing::info!("[ACTOR] SubRx Started");
    loop {
        if let Some(msg) = local_rx.recv().await {
            let msg = msg.downcast::<M>().unwrap();
            match after_recv(&msg, &cstate) {
                ChannelAction::PASS => {}
                ChannelAction::PANIC => {
                    panic!()
                }
                ChannelAction::DROP => {
                    continue;
                }
                ChannelAction::SYNC(_) => todo!(),
                ChannelAction::CLOSE => {
                    break;
                }
            }

            pc_tx.send(R2PMsg::Msg(*msg)).unwrap();
        }
    }
    tracing::info!("[ACTOR] SubRx Ended");
}
/// Spawns tasks to receive messages from incoming network or local control channels,
/// decode them, and forward them for processing based on channel state.
///
/// This function listens for `ControlMsg` commands on a controller channel and handles two cases:
///
/// 1. **`StartTcpRecv(port)`**:
///    Binds a non-blocking TCP socket on the given port. For every incoming connection, it:
///     - Spawns a task (`parent_recv_subtask`) to receive bytes from the socket,
///     - Decodes messages using `decoder`,
///     - Passes messages to the processor via `p_tx`,
///     - Determines their routing via `after_recv` and shared state `CS`.
///
/// 2. **`StartLocalRecv(simplex_stream)`**:
///    Similar to the TCP case but uses a provided `simplex_stream` (e.g., for local testing or IPC),
///    spawning a new receive subtask accordingly.
///
/// # Type Parameters
/// - `M`: The type of decoded message. Must implement `Msg`.
/// - `CS`: The shared channel state type. Must implement `CState`.
/// - `AR`: A closure used to inspect each decoded message and state, returning a `ChannelAction`.
/// - `D`: A decoder capable of converting raw bytes into messages of type `M`.
///
/// # Arguments
/// - `after_recv`: A closure invoked on each message after decoding, allowing for routing decisions.
/// - `p_tx`: Unbounded channel sender to forward messages for further processing.
/// - `decoder`: The decoder used by all subtasks to interpret incoming byte streams.
/// - `controller_rx`: Control channel used to dynamically start new receiver tasks.
///
/// # Notes
/// - All state is shared via `Arc<Mutex<CS>>`, ensuring thread-safe mutation.
/// - `after_recv` and `decoder` must be `Clone` as they are moved into spawned tasks.
///
/// # Panics
/// - Will panic if socket binding, listening, or accepting fails (errors are unwrapped).
///   Spawns tasks to receive messages from incoming network or local control channels,
///   decode them, and forward them for processing based on channel state.
///
async fn rx<M, CS, AR, D>(
    my_addr: ActorAddr,
    after_recv: AR,
    channel_state: CS,
    pc_tx: PriorityChannelTx<R2PMsg<M>>,
    decoder: D,
    mut controller_rx: mpsc::UnboundedReceiver<ControlInst>,
) -> Result<(), ActorError>
where
    M: Msg + 'static,
    CS: RState + 'static,
    AR: Fn(&M, &Arc<Mutex<CS>>) -> ChannelAction + Send + Sync + 'static + Clone,
    D: Decoder<Item = M, Error = DecodeErr> + Clone + Send + Sync + 'static,
{
    tracing::info!("[ACTOR][{}] Rx Started", my_addr);
    let cancel_token = CancellationToken::new();
    let mut tcp_server_set: JoinSet<Result<(), ActorError>> = JoinSet::new();
    let mut local_recv_set = JoinSet::new();
    let channel_state = Arc::new(Mutex::new(channel_state));

    while let Some(msg) = controller_rx.recv().await {
        match msg {
            ControlInst::StartTcpRecv(port) => {
                let decoder_clone = decoder.clone();
                let cstate_clone = channel_state.clone();
                let after_recv_clone = after_recv.clone();
                let pc_tx_clone = pc_tx.clone();
                let cancel_token = cancel_token.clone();
                tcp_server_set.spawn(async move {
                    let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;
                    socket
                        .set_reuse_port(true)
                        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;
                    socket
                        .bind(&SocketAddr::from((Ipv4Addr::new(0, 0, 0, 0), port)).into())
                        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;
                    socket
                        .listen(128)
                        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;
                    socket
                        .set_nonblocking(true)
                        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;

                    let parent_listener = TcpListener::from_std(socket.into())
                        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;

                    let mut remote_recv_set = JoinSet::new();
                    loop {
                        tokio::select! {
                            _ = cancel_token.cancelled() => {
                                break;
                            }
                            accept_result = parent_listener.accept() => {
                                let (socket, _) = accept_result.map_err(|e| {
                                    ActorError::RecieverErr(RecieverErr::TcpStartErr(e))
                                })?;
                                let (rx, _) = socket.into_split();
                                let framed_reader = FramedRead::new(rx, decoder_clone.clone());
                                remote_recv_set.spawn(remote_parent_recv_subtask(
                                    after_recv_clone.clone(),
                                    pc_tx_clone.clone(),
                                    cstate_clone.clone(),
                                    framed_reader,
                                ));
                            }
                        }
                    }
                    remote_recv_set.abort_all();
                    Ok(())
                });
            }
            ControlInst::StartLocalRecv(local_rx) => {
                local_recv_set.spawn(local_parent_recv_subtask(
                    after_recv.clone(),
                    pc_tx.clone(),
                    //p_txs.clone(),
                    channel_state.clone(),
                    local_rx,
                ));
            }
            ControlInst::Stop => {
                cancel_token.cancel();
                pc_tx.send(R2PMsg::Exit).unwrap();
                break;
            }
        }
    }
    tcp_server_set.abort_all();
    local_recv_set.abort_all();
    tracing::info!("[ACTOR][{}] Rx Ended", my_addr);
    Ok(())
}

type SendBuffer<M> = mpsc::UnboundedSender<M>;

/// Launches an async dispatcher that routes messages to per-address sending tasks.
///
/// This function listens for incoming messages on the provided `p_rx` channel. For each
/// message, it uses the `before_send` function to determine the destination `Addr`.
///
/// If no sender task exists for that address, it:
/// - Creates a new unbounded channel for that address,
/// - Spawns the `sender_task` future (which consumes the address and receiver),
/// - Stores the sending half in an internal map.
///
/// Each message is then forwarded to the corresponding per-address `SendBuffer`.
///
/// # Type Parameters
/// - `M`: The message type. Must be `Send + 'static`.
/// - `BS`: A function that maps a message reference to its destination address.
///
/// # Arguments
/// - `before_send`: A function that returns the target `Addr` for a given message.
/// - `p_rx`: An `UnboundedReceiver<M>` providing messages to dispatch.
/// - `sender_task`: A function that returns a pinned future handling messages for a given `Addr`.
///
#[allow(clippy::type_complexity)]
async fn tx<M, C, BS, RS>(
    my_addr: ActorAddr,
    before_send: BS,
    mut state: RS,
    mut p_rx: mpsc::UnboundedReceiver<M>,
    controller_tx: mpsc::Sender<ControlReq>,
    codec: C,
    sender_task: fn(
        ActorAddr,
        mpsc::UnboundedReceiver<M>,
        C,
        mpsc::Sender<ControlReq>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
) where
    M: Msg + 'static + Send,
    RS: SState,
    BS: Fn(&M, &mut RS) -> ActorAddr,
    C: Encoder<M> + 'static + Send + Clone,
{
    let mut addr_to_buff: HashMap<ActorAddr, SendBuffer<M>> = HashMap::new();

    let mut sub_senders = JoinSet::new();
    tracing::info!("[ACTOR][{}] Tx Started", my_addr);
    while let Some(m) = p_rx.recv().await {
        let addr = before_send(&m, &mut state);
        let sender = addr_to_buff.entry(addr).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel::<M>();
            let task = sender_task(addr, rx, codec.clone(), controller_tx.clone());
            sub_senders.spawn(task);
            tx
        });
        let _ = sender.send(m);
    }
    sub_senders.abort_all();
    tracing::info!("[ACTOR][{}] Tx Ended", my_addr);
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
