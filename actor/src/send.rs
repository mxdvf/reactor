use std::{any::Any, collections::HashMap, time::Duration};

use futures::SinkExt as _;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinSet,
};
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    ActorAddrRef, ActorSend, Msg, SendBuffer,
    node_comm::{Connection, ControlReq},
};

#[allow(clippy::type_complexity)]
pub(crate) async fn tx2<M, E, BS>(
    my_addr: ActorAddrRef,
    mut before_send: Option<BS>,
    mut p_rx: mpsc::UnboundedReceiver<M>,
    controller_tx: mpsc::Sender<ControlReq>,
    codec: E,
) where
    M: Msg,
    BS: ActorSend<OMsg = M>,
    E: Encoder<M> + 'static + Send + Clone,
{
    let mut addr_to_buff: HashMap<ActorAddrRef, SendBuffer<M>> = HashMap::new();

    let mut sub_senders = JoinSet::new();
    tracing::info!("[ACTOR][{}] Tx Started", my_addr);
    while let Some(m) = p_rx.recv().await {
        let addrs = match before_send.as_mut() {
            Some(before_send) => before_send.before_send(&m).await,
            None => &vec![],
        };
        for addr in addrs {
            let sender = addr_to_buff.entry(addr).or_insert_with(|| {
                let (tx, rx) = mpsc::unbounded_channel::<M>();
                sub_senders.spawn(sender_task(
                    my_addr,
                    addr,
                    rx,
                    codec.clone(),
                    controller_tx.clone(),
                ));
                tx
            });
            let _ = sender.send(m.clone());
        }
    }
    sub_senders.abort_all();
    tracing::info!("[ACTOR][{}] Tx Ended", my_addr);
}

async fn sender_task<M, E>(
    my_addr: ActorAddrRef,
    send_addr: ActorAddrRef,
    rx: mpsc::UnboundedReceiver<M>,
    encoder: E,
    controller_tx: mpsc::Sender<ControlReq>,
) where
    M: Msg,
    E: Encoder<M> + 'static + Send,
{
    async fn remote_sender<C: Encoder<M> + 'static + Send, M>(
        my_addr: ActorAddrRef,
        mut tx: impl AsyncWrite + Unpin,
        mut rx: mpsc::UnboundedReceiver<M>,
        encoder: C,
    ) {
        log::info!("[ACTOR] SubTx Started");
        send_handshake(&mut tx, my_addr).await;
        let mut framed_writer = FramedWrite::new(tx, encoder);
        loop {
            if let Some(msg) = rx.recv().await {
                if framed_writer.send(msg).await.is_err() {
                    break;
                }
            }
        }
        log::info!("[ACTOR] SubTx Ended");
    }

    async fn local_sender<M: std::fmt::Debug + Send + 'static + Clone>(
        my_addr: ActorAddrRef,
        tx: mpsc::Sender<Box<dyn Any + Send>>,
        mut rx: mpsc::UnboundedReceiver<M>,
    ) {
        log::info!("[ACTOR] SubTx Started (Local)");
        tx.send(Box::new(my_addr.to_string())).await.unwrap();
        loop {
            if let Some(msg) = rx.recv().await {
                if tx.send(Box::new(msg)).await.is_err() {
                    break;
                }
            }
        }
        log::info!("[ACTOR] SubTx Ended");
    }

    let (c_tx, c_rx) = tokio::sync::oneshot::channel();
    controller_tx
        .send(ControlReq::Resolve {
            resp_tx: c_tx,
            addr: send_addr,
        })
        .await
        .unwrap();

    match c_rx.await.unwrap() {
        Connection::Remote(socket_addr) => loop {
            match TcpStream::connect(socket_addr).await {
                Ok(s) => {
                    let (_, tx) = s.into_split();
                    break remote_sender(my_addr, tx, rx, encoder).await;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    todo!();
                }
            }
        },
        Connection::Local(write_half) => {
            local_sender(my_addr, write_half, rx).await;
        }
    };
}

pub async fn send_handshake(tx: &mut (impl AsyncWrite + Unpin), handshake: &str) {
    let bytes = handshake.as_bytes();
    let len = bytes.len();

    // Step 1: Write the length as a 4-byte big-endian integer
    tx.write_u32(len as u32).await.unwrap();

    // Step 2: Write the UTF-8 bytes
    tx.write_all(bytes).await.unwrap();
}
