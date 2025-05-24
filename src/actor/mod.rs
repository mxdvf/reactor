use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use socket2::{Domain, Socket, Type};
use tokio::{
    io::AsyncRead,
    net::TcpListener,
    sync::{
        Mutex,
        mpsc::{self},
    },
    time::Instant,
};
use tokio_util::codec::{Decoder, Encoder, FramedRead};

use crate::node::{ControlInst, ControlReq};

pub mod common;

/// State of the Processor
pub trait State: Default + Send {}
/// State of the Generator
pub trait GState: Default + Send {}
/// State of the Receiver
pub trait RState: Default + Send {}
/// State of the Sender
pub trait SState: Default + Send {}

/// Messages that can flow between the actors.
pub trait Msg: Send {}

/// Addr of the actors
pub type ActorAddr = &'static str;

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
///
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
pub async fn actor<I, S, GS, RS, CD, SS, O, AR, P, BS>(
    my_addr: ActorAddr,
    mut generators: Vec<Generator<GS, I>>,
    cs: RS,
    after_recieve: AR,
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
) where
    I: Msg + 'static,
    S: State + 'static,
    GS: GState + 'static,
    RS: RState + 'static,
    SS: SState + 'static,
    O: Msg + 'static,
    // AR: Fn(&I, &Arc<Mutex<CS>>) -> Fut + Clone + Send + 'static,
    // Fut: Future<Output = ChannelAction> + Send + 'static,
    AR: Fn(&I, &Arc<Mutex<RS>>) -> ChannelAction + Send + 'static + Clone,
    P: Fn(I, &mut S) -> O + Send + 'static,
    BS: Fn(&O, &mut SS) -> ActorAddr + Send + 'static,
    CD: Encoder<O> + Decoder<Item = I, Error = DecodeErr> + Send + Clone + 'static,
{
    let (r2p_tx, mut r2p_rx) = mpsc::unbounded_channel::<I>();
    let (p2s_tx, p2s_rx) = mpsc::unbounded_channel::<O>();

    let gen_handles: Vec<tokio::task::JoinHandle<_>> = generators
        .drain(..)
        .map(|gene: Generator<GS, I>| tokio::spawn(generator(gene, r2p_tx.clone())))
        .collect();

    let rx_handle = tokio::spawn(rx(
        my_addr,
        after_recieve,
        cs,
        r2p_tx,
        codec.clone(),
        controller_rx,
    ));

    let proc_handle = tokio::task::spawn_blocking(move || {
        log::info!("{} Processor Started", my_addr);
        let mut s = S::default();
        while let Some(i) = r2p_rx.blocking_recv() {
            let o = processor(i, &mut s);
            p2s_tx.send(o).unwrap();
        }
        log::info!("{} Processor Ended", my_addr);
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
    rx_handle.await.unwrap();
    proc_handle.await.unwrap();
    tx_handle.await.unwrap();
}

async fn parent_recv_subtask<M, C, D, AR, RX>(
    after_recv: AR,
    row_q: mpsc::UnboundedSender<M>,
    cstate: Arc<Mutex<C>>,
    mut framed_reader: FramedRead<RX, D>,
) where
    M: Msg,
    C: RState,
    D: Decoder<Item = M>,
    // AR: Fn(&M, &Arc<Mutex<C>>) -> Fut,
    AR: Fn(&M, &Arc<Mutex<C>>) -> ChannelAction + 'static + Clone,
    RX: AsyncRead + Unpin,
    // Fut: Future<Output = ChannelAction> + Send,
{
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
            row_q.send(msg).unwrap();
        }
    }
}

/// Spawns tasks to receive messages from incoming network or local control channels,
/// decode them, and forward them for processing based on channel state.
///
/// This function listens for `ControlMsg` commands on a controller channel and handles two cases:
///
/// 1. **`StartTcpRecv(port)`**:  
///     Binds a non-blocking TCP socket on the given port. For every incoming connection, it:
///     - Spawns a task (`parent_recv_subtask`) to receive bytes from the socket,
///     - Decodes messages using `decoder`,
///     - Passes messages to the processor via `p_tx`,
///     - Determines their routing via `after_recv` and shared state `CS`.
///
/// 2. **`StartLocalRecv(simplex_stream)`**:  
///     Similar to the TCP case but uses a provided `simplex_stream` (e.g., for local testing or IPC),
///     spawning a new receive subtask accordingly.
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
/// Spawns tasks to receive messages from incoming network or local control channels,
/// decode them, and forward them for processing based on channel state.
///
async fn rx<M, CS, AR, D>(
    my_addr: ActorAddr,
    after_recv: AR,
    channel_state: CS,
    p_tx: mpsc::UnboundedSender<M>,
    decoder: D,
    mut controller_rx: mpsc::UnboundedReceiver<ControlInst>,
) where
    M: Msg + 'static,
    CS: RState + 'static,
    // AR: Fn(&M, &Arc<Mutex<CS>>) -> Fut + Clone + Send + 'static,
    // Fut: Future<Output = ChannelAction> + Send + 'static,
    AR: Fn(&M, &Arc<Mutex<CS>>) -> ChannelAction + Send + 'static + Clone,
    D: Decoder<Item = M, Error = DecodeErr> + Clone + Send + 'static,
{
    log::info!("{} Rx Started", my_addr);
    let channel_state = Arc::new(Mutex::new(channel_state));
    while let Some(msg) = controller_rx.recv().await {
        match msg {
            ControlInst::StartTcpRecv(port) => {
                let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
                socket.set_reuse_port(true).unwrap();
                socket
                    .bind(&SocketAddr::from((Ipv4Addr::new(0, 0, 0, 0), port)).into())
                    .unwrap();
                socket.listen(128).unwrap();
                socket.set_nonblocking(true).unwrap();

                let parent_listener = TcpListener::from_std(socket.into()).unwrap();
                loop {
                    let r = parent_listener.accept().await;
                    let (socket, _) = r.unwrap();
                    let (rx, _) = socket.into_split();
                    let framed_reader = FramedRead::new(rx, decoder.clone());
                    tokio::spawn(parent_recv_subtask(
                        after_recv.clone(),
                        p_tx.clone(),
                        channel_state.clone(),
                        framed_reader,
                    ));
                }
            }
            ControlInst::StartLocalRecv(simplex_stream) => {
                let framed_reader = FramedRead::new(simplex_stream, decoder.clone());
                tokio::spawn(parent_recv_subtask(
                    after_recv.clone(),
                    p_tx.clone(),
                    channel_state.clone(),
                    framed_reader,
                ));
            }
        }
    }
    println!("{}", controller_rx.is_closed());
    log::info!("{} Rx Ended", my_addr);
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

    log::info!("{} Tx Started", my_addr);
    while let Some(m) = p_rx.recv().await {
        let addr = before_send(&m, &mut state);
        let sender = addr_to_buff.entry(addr).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel::<M>();
            let task = sender_task(addr, rx, codec.clone(), controller_tx.clone());
            tokio::spawn(task);
            tx
        });
        let _ = sender.send(m);
    }
    log::info!("{} Tx Ended", my_addr);
}

pub struct Generator<S, M> {
    pub s: S,
    pub callback: fn(u64, &mut S) -> M,
    pub start: Duration,
    pub interval: Duration,
    pub max: Option<u64>,
}

async fn generator<GS, M>(generator: Generator<GS, M>, p_tx: mpsc::UnboundedSender<M>)
where
    GS: GState + 'static,
    M: Msg + 'static,
{
    let Generator {
        mut s,
        callback,
        interval,
        start,
        max,
    } = generator;
    // TODO:- What to do after u64::MAX?
    let max_events = max.unwrap_or(u64::MAX);
    tokio::time::sleep(start).await;

    let mut last_emit = Instant::now();
    for i in 0..max_events {
        let time_since_last_emit = Instant::now() - last_emit;
        if time_since_last_emit < interval {
            tokio::time::sleep(interval - time_since_last_emit).await;
        }
        let output = callback(i, &mut s);
        p_tx.send(output).unwrap();
        last_emit = Instant::now();
    }
}
