//! One Node Controller task will be spawned on each physical nodes.
use std::net::SocketAddr;

use tokio::{
    io::{ReadHalf, SimplexStream, WriteHalf, simplex},
    net::{ToSocketAddrs, lookup_host},
    sync::{
        mpsc::{Receiver, UnboundedSender},
        oneshot,
    },
};

use std::collections::HashMap;

use crate::actor::ActorAddr;

mod rpc;
use rpc::{self as node_rpc, webserver};

pub type NodeAddr = &'static str;

/// Instructions that are sent by the controller to the actor
pub enum ControlInst {
    StartLocalRecv(ReadHalf<SimplexStream>),
    StartTcpRecv(u16),
}

/// Requests that can be sent to the controller by the actor or some external
/// API
pub enum ControlReq {
    Resolve {
        addr: ActorAddr,
        resp_tx: oneshot::Sender<Connection>,
    },
    ActorAdded {
        node_addr: NodeAddr,
        addr: ActorAddr,
        recv_port: u16,
        actor_control_tx: UnboundedSender<ControlInst>,
    },
}

/// Type of connection
#[derive(Debug)]
pub enum Connection {
    Remote(SocketAddr),
    Local(WriteHalf<SimplexStream>),
}
struct LocalActor {
    handle: UnboundedSender<ControlInst>,
}
struct RemoteActor {
    remote_actor_addr: SocketAddr,
}

impl RemoteActor {
    async fn new<T: ToSocketAddrs>(remote_addr: T) -> RemoteActor {
        let remote_addr = tokio::net::lookup_host(remote_addr)
            .await
            .unwrap()
            .last()
            .unwrap();
        RemoteActor {
            remote_actor_addr: remote_addr,
        }
    }
}

pub async fn node_controller(mut rx: Receiver<ControlReq>, myaddr: NodeAddr) {
    log::info!("Controller Started at {myaddr}");

    let mut local_actors: HashMap<ActorAddr, LocalActor> = HashMap::new();
    let mut remote_actors: HashMap<ActorAddr, RemoteActor> = HashMap::new();
    tokio::spawn(webserver());
    while let Some(req) = rx.recv().await {
        match req {
            ControlReq::Resolve { addr, resp_tx } => {
                if let Some(local) = local_actors.get(addr) {
                    let (read_half, write_half) = simplex(1 << 20);
                    local
                        .handle
                        .send(ControlInst::StartLocalRecv(read_half))
                        .unwrap();
                    resp_tx.send(Connection::Local(write_half)).unwrap();
                } else if let Some(local) = remote_actors.get(addr) {
                    resp_tx
                        .send(Connection::Remote(local.remote_actor_addr))
                        .unwrap();
                }
            }
            ControlReq::ActorAdded {
                node_addr,
                addr,
                actor_control_tx,
                recv_port,
            } => {
                if node_addr == myaddr {
                    local_actors.insert(
                        addr,
                        LocalActor {
                            handle: actor_control_tx,
                        },
                    );
                } else {
                    let node_addr = lookup_host(node_addr).await.unwrap().last().unwrap();
                    remote_actors.insert(
                        addr,
                        RemoteActor {
                            remote_actor_addr: (node_addr.ip(), recv_port).into(),
                        },
                    );
                }
            }
        }
    }
    log::info!("Controller Ended");
}
