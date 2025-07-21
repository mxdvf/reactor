use std::{any::Any, net::SocketAddr};

use tokio::sync::{mpsc, oneshot};

use crate::ActorAddrRef;

/// Type of channel that is used to send message from one local actor to the other.
pub type LocalChannelTx = mpsc::Sender<Box<dyn Any + Send>>;
pub type LocalChannelRx = mpsc::Receiver<Box<dyn Any + Send>>;

#[derive(Debug)]
pub enum Connection {
    Remote(SocketAddr),
    Local(LocalChannelTx),
}

pub enum ControlReq {
    Resolve {
        addr: ActorAddrRef,
        resp_tx: oneshot::Sender<Connection>,
    },
}

/// Instructions that are sent by the local controller to the actor
pub enum ControlInst {
    StartLocalRecv(LocalChannelRx),
    StartTcpRecv(u16),
    Stop,
}

pub struct NodeComm {
    pub controller_rx: mpsc::Receiver<ControlInst>,
    pub controller_tx: mpsc::Sender<ControlReq>,
}

impl NodeComm {
    pub fn split(self) -> (mpsc::Receiver<ControlInst>, mpsc::Sender<ControlReq>) {
        (self.controller_rx, self.controller_tx)
    }
}
