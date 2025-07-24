use std::{
    any::Any,
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
};
use std::sync::mpmc::SendError;
use err::{ActorError, RecieverErr};
use futures::{StreamExt, future::join_all};
// use reactor_node::{ControlInst, ControlReq};
use socket2::{Domain, Socket, Type};
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
// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub mod common;
mod err;

/// State of the Processor
pub trait State: Default + Send {}
/// State of the Receiver
pub trait RState: Default + Send {}
/// State of the Sender
pub trait SState: Default + Send {}

// #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
// pub enum Priority {
//     High,
//     Medium,
//     Low,
// }
//
// impl Priority {
//     pub fn to_index(self) -> usize {
//         match self {
//             Priority::High => 0,
//             Priority::Medium => 1,
//             Priority::Low => 2,
//         }
//     }
// }
//
// impl Default for Priority {
//     fn default() -> Self {
//         Priority::Low
//     }
// }

/*#[derive(Debug)]
pub struct MyMessage {
    pub priority: Option<Priority>,
}*/

/// Messages that can flow between the actors.
//pub trait Msg: Send + std::fmt::Debug {}
pub trait HasPriority {
    //fn priority(&self) -> Priority;
    fn priority(&self) -> u8 {
        0   // 0 is highest priority
    }
}

pub trait Msg: Send + std::fmt::Debug + HasPriority {}

/*enum R2PMsg<T> {
    Msg { value: T, priority: Priority },
    Exit,
}*/

/*impl Msg for MyMessage {
    fn priority(&self) -> Priority {
        self.priority.unwrap_or(Priority::Low)
    }
}*/

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

#[derive(Debug, PartialEq)]
enum R2PMsg<T> {
    Msg(T),
    Exit,
}

impl<T> HasPriority for R2PMsg<T> {
    fn priority(&self) -> u8 {
        match self {
            R2PMsg::Msg(t) => t.priority(),
            R2PMsg::Exit => 0,  // How to make it highest priority?!
        }
    }
}

impl<T: Clone> Clone for R2PMsg<T> {
    fn clone(&self) -> Self {
        match self {
            R2PMsg::Msg(t) => R2PMsg::Msg(t.clone()),
            R2PMsg::Exit => R2PMsg::Exit,
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

#[derive(Clone)]
pub struct PriorityChannelTx<T> {
    //senders: Vec<mpsc::UnboundedSender<R2PMsg<T>>>,
    senders: Vec<mpsc::UnboundedSender<T>>,
}

pub struct PriorityChannelRx<T> {
    //receivers: Vec<mpsc::UnboundedReceiver<R2PMsg<T>>>,
    receivers: Vec<mpsc::UnboundedReceiver<T>>,
}

// #[derive(Debug, PartialEq)]
// pub enum PriorityChannelError {
//     Disconnected, // All channels closed
//     Empty,        // No message yet, some still open
// }

// impl<T> Clone for PriorityChannelTx<T> {
//     fn clone(&self) -> Self {
//         Self {
//             senders: self.senders.clone(),
//         }
//     }
// }

impl<T: HasPriority> PriorityChannelTx<T> {
    //pub fn put_row(&self, prio: Priority, msg: R2PMsg<T>) {
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let idx = msg.priority();
        if let Some(tx) = self.senders.get(idx) {
            (*tx).send(msg)
        } else {
            SendError(msg)
        }
    }
}

/*impl<T: Clone> PriorityChannelTx<T> {
    pub fn broadcast_row(&self, msg: T) {
        for tx in &self.senders {
            // Since `UnboundedSender::send` returns Result<(), SendError<T>>, you can handle errors here
            if let Err(e) = tx.send(msg.clone()) {
                eprintln!("Broadcast failed: {:?}", e);
            }
        }
    }
}*/

// impl<T> PriorityChannelTx<R2PMsg<T>> {
//     pub fn broadcast_exit(&self) {
//         for tx in &self.senders {
//             let _ = tx.send(R2PMsg::Exit);
//         }
//     }
// }

impl<T> PriorityChannelRx<T> {
    //pub fn get_row(&mut self) -> Option<T> {
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut disconnected_count = 0;

        for rx in &mut self.receivers {
            match rx.try_recv() {
                Ok(msg) => return Ok(msg),
                Err(TryRecvError::Empty) => continue, // Keep checking other priorities
                Err(TryRecvError::Disconnected) => disconnected_count += 1,
            }
        }

        // If all channels are disconnected, return None
        if disconnected_count == self.receivers.len() {
            Err(TryRecvError::Disconnected)
        } else {
            // No message found, but at least one channel still open → wait and try again later
            // Leave it to caller to sleep or retry
            Err(TryRecvError::Empty)
        }
    }
}

/*impl<T> PriorityChannel<T> {
    pub fn new() -> Self {
        let mut receivers = Vec::new();
        let mut senders = Vec::new();

        for _ in &[Priority::High, Priority::Medium, Priority::Low] {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.push(tx);
            receivers.push(rx);
        }

        PriorityChannel { receivers, senders }
    }

    pub fn put_row(&self, prio: Priority, msg: T) -> Result<(), T> {
        let idx = prio.to_index();
        if let Some(tx) = self.senders.get(idx) {
            let _ = tx.send(msg);
        }
        else {
            Err(msg)
        }
    }

    pub fn get_row(&mut self) -> Option<T> {
        for &prio in &[Priority::High, Priority::Medium, Priority::Low] {
            let idx = prio.to_index();
            if let Some(rx) = self.receivers.get_mut(idx) {
                if let Ok(msg) = rx.try_recv() {
                    return Some(msg);
                }
            }
        }
        None
    }

    pub fn get_senders(&self) -> Vec<mpsc::UnboundedSender<T>> {
        self.senders.clone()
    }
}*/

pub fn priority_channel<T>(num_prios: u8) -> (PriorityChannelTx<T>, PriorityChannelRx<T>) {
    let mut senders = Vec::new();
    let mut receivers = Vec::new();

    for _ in 0..num_prios {
        let (tx, rx) = mpsc::unbounded_channel::<T>();
        senders.push(tx);
        receivers.push(rx);
    }

    (PriorityChannelTx { senders }, PriorityChannelRx { receivers })
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
) -> Result<(), ActorError>
where
    I: Msg + 'static,
    S: State + 'static,
    RS: RState + 'static,
    SS: SState + 'static,
    O: Msg + 'static,
    // AR: Fn(&I, &Arc<Mutex<CS>>) -> Fut + Clone + Send + 'static,
    // Fut: Future<Output = ChannelAction> + Send + 'static,
    AR: Fn(&I, &Arc<std::sync::Mutex<RS>>) -> ChannelAction + Send + Sync + 'static + Clone,
    P: Fn(I, &mut S) -> Vec<O> + Send + 'static,
    BS: Fn(&O, &mut SS) -> ActorAddr + Send + Sync + 'static,
    CD: Encoder<O> + Decoder<Item = I, Error = DecodeErr> + Send + Sync + Clone + 'static,
{
    //let (r2p_tx, mut r2p_rx) = mpsc::unbounded_channel::<R2PMsg<I>>();
    let (p2s_tx, p2s_rx) = mpsc::unbounded_channel::<O>();
    //let mut prio_channel: PriorityChannel<R2PMsg<I>> = PriorityChannel::new();
    //let mut priority_receivers: Vec<mpsc::UnboundedReceiver<R2PMsg<I>>> = vec![];
    //let mut priority_senders: Vec<mpsc::UnboundedSender<R2PMsg<I>>> = vec![];

    /*for _ in &[Priority::High, Priority::Medium, Priority::Low] {
        let (tx, rx) = mpsc::unbounded_channel();
        priority_senders.push(tx);
        priority_receivers.push(rx);
    }*/

    let (p_tx, mut p_rx) = priority_channel::<R2PMsg<I>>();

    //TODO Don't assume to send the generator to the lowest priority channel.
    /*let gen_handles: Vec<tokio::task::JoinHandle<_>> = generators
    .drain(..)
    .map(|gene| {
        tokio::spawn(generator(
            gene,
            priority_senders[Priority::default().to_index()].clone(),
        ))
    })
    .collect();*/

    /*let gen_handles: Vec<tokio::task::JoinHandle<_>> = generators
    .drain(..)
    .map(|gene| tokio::spawn(generator(gene, default_tx.clone())))
    .collect();*/

    /*let rx_handle = tokio::spawn(rx(
        my_addr,
        after_recieve,
        rs,
        r2p_tx,
        codec.clone(),
        controller_rx,
    ));*/

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
        //r2p_tx,
        //priority_senders,
        p_tx.clone(),
        codec.clone(),
        controller_rx,
    ));

    /*let proc_handle: JoinHandle<Result<(), ActorError>> =
    tokio::task::spawn_blocking(move || -> Result<(), ActorError> {
        tracing::info!("[ACTOR][{}] Processor Started", my_addr);
        while let Some(i) = r2p_rx.blocking_recv() {
            if let R2PMsg::Msg(msg) = i {
                let processed_messages = processor(msg, &mut processor_state);

                for message in processed_messages {
                    p2s_tx.send(message).map_err(|_| ActorError::P2SErr)?;
                }
            } else {
                break;
            }
        }
        tracing::info!("[ACTOR][{}] Processor Ended", my_addr);
        Ok(())
    });*/
    /*let proc_handle: JoinHandle<Result<(), ActorError>> =
    tokio::task::spawn_blocking(move || -> Result<(), ActorError> {
        tracing::info!("[ACTOR][{}] Processor Started", my_addr);
        loop {
            let mut found = false;
            for rx in &mut priority_receivers {
                match rx.try_recv() {
                    Ok(R2PMsg::Msg(msg)) => {
                        let processed = processor(msg, &mut processor_state);
                        for o in processed {
                            p2s_tx.send(o).map_err(|_| ActorError::P2SErr)?;
                        }
                        found = true;
                        break;
                    }
                    Ok(_) | Err(_) => continue,
                }
            }
            if !found {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    });*/

    let proc_handle: JoinHandle<Result<(), ActorError>> =
        tokio::task::spawn_blocking(move || -> Result<(), ActorError> {
            tracing::info!("[ACTOR][{}] Processor Started", my_addr);

            /*loop {
                if let Some(R2PMsg::Msg(msg)) = p_rx.get_row() {
                    let processed = processor(msg, &mut processor_state);
                    for o in processed {
                        p2s_tx.send(o).map_err(|_| ActorError::P2SErr)?;
                    }
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }*/

            let mut exit_count = 0;
            loop {
                match p_rx.get_row() {
                    /*Some(R2PMsg::Msg(m)) => {
                        let processed = processor(m, &mut processor_state);
                        for o in processed {
                            p2s_tx.send(o).map_err(|_| ActorError::P2SErr)?;
                        }
                    }
                    Some(R2PMsg::Exit) => {
                        tracing::info!("Received Exit. Terminating processor.");
                        break;
                    }
                    None => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }*/
                    Ok(R2PMsg::Msg(m)) => {
                        let processed = processor(m, &mut processor_state);
                        for o in processed {
                            p2s_tx.send(o).map_err(|_| ActorError::P2SErr)?;
                        }
                    }
                    Ok(R2PMsg::Exit) => {
                        println!("Received Exit message: {exit_count}/{expected_exit_count}");
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(TryRecvError::Disconnected) => {
                        // Optional: may break early or continue waiting for Exit
                        println!("All channels disconnected, but exit count = {exit_count}");
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
    //row_q: mpsc::UnboundedSender<R2PMsg<M>>,
    //row_q: Vec<mpsc::UnboundedSender<R2PMsg<M>>>,
    pc_tx: PriorityChannelTx<R2PMsg<M>>,
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

            /*let idx = msg.priority().to_index();
            if row_q[idx].send(R2PMsg::Msg(msg)).is_err() {
                break;
            }*/
            pc_tx.send(R2PMsg::Msg(msg));
        }
    }
    tracing::info!("[ACTOR] SubRx Ended");
}

async fn local_parent_recv_subtask<M, C, AR>(
    after_recv: AR,
    //row_q: mpsc::UnboundedSender<R2PMsg<M>>,
    //row_q: Vec<mpsc::UnboundedSender<R2PMsg<M>>>,
    pc_tx: PriorityChannelTx<R2PMsg<M>>,
    cstate: Arc<Mutex<C>>,
    mut local_rx: LocalChannelRx,
) where
    M: Msg + 'static,
    C: RState,
    // AR: Fn(&M, &Arc<Mutex<C>>) -> Fut,
    AR: Fn(&M, &Arc<Mutex<C>>) -> ChannelAction + 'static + Clone,
    // Fut: Future<Output = ChannelAction> + Send,
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

            //let idx = msg.priority();

            //pc_tx.put_row(prio, R2PMsg::Msg(m))
            /*if row_q[idx].send(R2PMsg::Msg(*msg)).is_err() {
                break;
            }*/
            pc_tx.send(R2PMsg::Msg(*msg));
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
    //p_tx: mpsc::UnboundedSender<R2PMsg<M>>,
    //p_txs: Vec<mpsc::UnboundedSender<R2PMsg<M>>>,
    pc_tx: PriorityChannelTx<R2PMsg<M>>,
    decoder: D,
    mut controller_rx: mpsc::UnboundedReceiver<ControlInst>,
) -> Result<(), ActorError>
where
    M: Msg + 'static,
    CS: RState + 'static,
    // AR: Fn(&M, &Arc<Mutex<CS>>) -> Fut + Clone + Send + 'static,
    // Fut: Future<Output = ChannelAction> + Send + 'static,
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
                //let p_txs_clone = p_txs.clone();
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
                                    //p_txs_clone.clone(),
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
                //p_tx.send(R2PMsg::Exit).map_err(|_| ActorError::R2PErr)?;
                pc_tx.send(R2PMsg::Exit);
                /*for p_tx in &p_txs {
                    p_tx.send(R2PMsg::Exit).map_err(|_| ActorError::R2PErr)?;
                }*/
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
        //p_tx.send(R2PMsg::Msg(m)).map_err(|_| ActorError::R2PErr)?;
        p_tx.send(R2PMsg::Msg(m)).unwrap();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::hash::Hash;
    use tokio::sync::mpsc;

    // Dummy message type
    #[derive(Debug, Clone, PartialEq)]
    enum TestMsg {
        Low,
        Medium,
        High
    }

    impl HasPriority for TestMsg {
        fn priority(&self) -> u8 {
            match self {
                TestMsg::Low => 2,
                TestMsg::Medium => 1,
                TestMsg::High => 0,
            }
        }
    }

    #[test]
    fn test_priority_order() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3);

        tx.send(R2PMsg::Msg(TestMsg::Low))
            .unwrap();

        tx.send(R2PMsg::Msg(TestMsg::Medium))
            .unwrap();

        tx.send(R2PMsg::Msg(TestMsg::High))
            .unwrap();

        assert_eq!(
            rx.try_recv(),
            Ok(R2PMsg::Msg(TestMsg::High))
        );
        assert_eq!(
            rx.try_recv(),
            Ok(R2PMsg::Msg(TestMsg::Medium))
        );
        assert_eq!(
            rx.try_recv(),
            Ok(R2PMsg::Msg(TestMsg::Low))
        );
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn test_disconnected_behavior() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>();
        drop(tx.senders); // Drop all senders to simulate disconnect

        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn test_exit_message() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>();
        tx.send(R2PMsg::Exit).unwrap(); // Send Exit on High priority

        assert_eq!(rx.try_recv(), Ok(R2PMsg::Exit));
    }

    #[test]
    fn test_empty_channel() {
        let (_tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>();
        assert_eq!(rx.try_recv(), PriorityChannelStatus::Empty);
    }
    #[test]
    /*fn test_partial_disconnect_behavior() {
        let (mut tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>();

        // Drop only the High priority sender
        tx.senders.remove(Priority::High.to_index());
        println!("Getting the channel in high index");
        println!("{:?}", tx.senders.get(Priority::High.to_index()));
        // If you try to send to High now, it should fail (simulate disconnection)
        assert!(tx.senders.get(Priority::High.to_index()).is_none());

        // The channel should still be "connected" from the perspective of Medium/Low
        assert_ne!(rx.get_row(), PriorityChannelStatus::Disconnected);
    }*/
    #[test]
    fn test_partial_disconnect_behavior() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>();

        // Drop only High priority sender.
        drop(tx.senders[Priority::High.to_index()].clone());

        // Should NOT be Disconnected because Medium and Low still exist.
        assert!(matches!(rx.get_row(), PriorityChannelStatus::Empty));

        drop(tx); // drop remaining senders
    }

    #[test]
    fn test_empty_all_channels() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>();

        // No messages queued, no disconnections.
        assert_eq!(rx.get_row(), PriorityChannelStatus::Empty);

        drop(tx); // cleanup
    }

    #[test]
    fn test_medium_then_empty_behavior() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>();

        // Send only to Medium, leaving High and Low empty.
        tx.senders[Priority::Medium.to_index()]
            .send(R2PMsg::Msg(TestMsg("medium1")))
            .unwrap();

        // First call → should return the Medium message.
        assert_eq!(
            rx.get_row(),
            PriorityChannelStatus::Message(R2PMsg::Msg(TestMsg("medium1")))
        );

        // Second call → no messages left, should return Empty.
        assert_eq!(rx.get_row(), PriorityChannelStatus::Empty);

        drop(tx); // cleanup
    }
}
