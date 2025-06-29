use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::mpsc,
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::{
    ActorAddr, Msg,
    node_comm::{Connection, ControlReq},
};

use super::{ChannelAction, RState, SState, State};

pub fn receiver_task<M, C, D, AR, RX>(
    rx: RX,
    state: Arc<Mutex<C>>,
    after_recv: AR,
    row_q: mpsc::UnboundedSender<M>,
    decoder: D,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
where
    RX: AsyncRead + Unpin + 'static + Send,
    M: Send + 'static,
    C: Send + 'static,
    D: Decoder<Item = M> + Send + 'static,
    AR: Fn(&M, &Arc<Mutex<C>>) -> ChannelAction + Send + 'static + Clone,
{
    let mut framed_reader = FramedRead::new(rx, decoder);
    Box::pin(async move {
        while let Some(Ok(msg)) = framed_reader.next().await {
            match after_recv(&msg, &state) {
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
    })
}

pub fn sender_task<M, C>(
    addr: ActorAddr,
    rx: mpsc::UnboundedReceiver<M>,
    encoder: C,
    controller_tx: mpsc::Sender<ControlReq>,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
where
    M: Msg + 'static,
    C: Encoder<M> + 'static + Send,
{
    // Common sender for both local and remote actor.
    async fn remote_sender<C: Encoder<M> + 'static + Send, M>(
        tx: impl AsyncWrite + Unpin,
        mut rx: mpsc::UnboundedReceiver<M>,
        encoder: C,
    ) {
        log::info!("[ACTOR] SubTx Started");
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
    async fn local_sender<M: Send + 'static>(
        tx: mpsc::Sender<Box<dyn Any + Send>>,
        mut rx: mpsc::UnboundedReceiver<M>,
    ) {
        log::info!("[ACTOR] SubTx Started (Local)");
        loop {
            if let Some(msg) = rx.recv().await {
                if tx.send(Box::new(msg)).await.is_err() {
                    break;
                }
            }
        }
        log::info!("[ACTOR] SubTx Ended");
    }
    Box::pin(async move {
        let (c_tx, c_rx) = tokio::sync::oneshot::channel();
        controller_tx
            .send(ControlReq::Resolve {
                addr,
                resp_tx: c_tx,
            })
            .await
            .unwrap();
        match c_rx.await.unwrap() {
            Connection::Remote(socket_addr) => loop {
                match TcpStream::connect(socket_addr).await {
                    Ok(s) => {
                        let (_, tx) = s.into_split();
                        break remote_sender(tx, rx, encoder).await;
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        todo!();
                    }
                }
            },
            Connection::Local(write_half) => {
                local_sender(write_half, rx).await;
            }
        };
    })
}

fn _blackhole_sender<M, C>(
    _addr: ActorAddr,
    mut rx: mpsc::UnboundedReceiver<M>,
    _encoder: C,
    _controller_tx: mpsc::Sender<ControlReq>,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
where
    M: Send + 'static,
    C: Encoder<M> + 'static + Send,
{
    Box::pin(async move { while rx.recv().await.is_some() {} })
}

impl RState for () {}
impl SState for () {}
impl State for () {}
