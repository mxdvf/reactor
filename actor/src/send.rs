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
    ActorAddrRef, ActorSend, Msg,
    node_comm::{Connection, ControlReq},
};

#[allow(clippy::type_complexity)]
pub(crate) async fn tx<M, E, BS>(
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
    let mut addr_to_buff: HashMap<ActorAddrRef, mpsc::UnboundedSender<M>> = HashMap::new();

    let mut sub_senders = JoinSet::new();
    tracing::info!("[ACTOR][{}] Tx Started", my_addr);
    let decoder_name = if let Some(before_send) = before_send.as_ref() {
        before_send.sub_decoder_name()
    } else {
        None
    };
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
                    decoder_name.clone(),
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
    decoder_name: Option<String>,
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
        decoder_name: Option<String>,
        mut tx: impl AsyncWrite + Unpin,
        mut rx: mpsc::UnboundedReceiver<M>,
        encoder: C,
    ) {
        log::info!("[ACTOR] SubTx Started");
        send_handshake(&mut tx, my_addr, decoder_name).await;
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
        decoder_name: Option<String>,
        tx: mpsc::Sender<Box<dyn Any + Send>>,
        mut rx: mpsc::UnboundedReceiver<M>,
    ) {
        log::info!("[ACTOR] SubTx Started (Local)");
        send_local_handshake(&tx, my_addr, decoder_name).await;
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
                    break remote_sender(my_addr, decoder_name, tx, rx, encoder).await;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    todo!();
                }
            }
        },
        Connection::Local(write_half) => {
            local_sender(my_addr, decoder_name, write_half, rx).await;
        }
    };
}

async fn send_handshake(
    tx: &mut (impl AsyncWrite + Unpin),
    my_name: &str,
    type_name: Option<String>,
) {
    let bytes = my_name.as_bytes();
    let len = bytes.len();
    tx.write_u32(len as u32).await.unwrap();
    tx.write_all(bytes).await.unwrap();

    if let Some(type_name) = type_name {
        let bytes = type_name.as_bytes();
        let len = bytes.len();
        tx.write_u32(len as u32).await.unwrap();
        tx.write_all(bytes).await.unwrap();
    } else {
        tx.write_u32(0).await.unwrap();
    }
}
async fn send_local_handshake(
    tx: &mpsc::Sender<Box<dyn Any + Send>>,
    my_name: &str,
    type_name: Option<String>,
) {
    let to_send = if let Some(type_name) = type_name {
        (my_name.to_string(), Some(type_name.to_string()))
    } else {
        (my_name.to_string(), None)
    };
    tx.send(Box::new(to_send)).await.unwrap();
}
