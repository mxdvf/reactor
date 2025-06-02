//! One Node Controller task will be spawned on each physical nodes.
use reactor_actor::{Connection, ControlInst, ControlReq};
use std::net::SocketAddr;
use tracing_shared::SharedLogger;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use code_gen::CodeGenerator;
use lib_builder::LibBuilder;
use serde_json::Value;
use tokio::{
    io::simplex,
    sync::{
        mpsc::{self, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
        oneshot,
    },
};

use libloading::Library;
use std::collections::HashMap;

pub mod code_gen;
mod lib_builder;
mod rpc;
use rpc::webserver;

pub type NodeAddr = &'static str;
pub type ActorSpawnCB =
    fn(mpsc::UnboundedReceiver<ControlInst>, mpsc::Sender<ControlReq>, ActorAddr);

pub type SetupSharedLogger = fn(&SharedLogger);

type ActorAddr = &'static str;

#[derive(Debug)]
pub(crate) struct SpawnResult {
    port: u16,
}

#[derive(Debug)]
pub(crate) struct RegisterResult {}

/// Global Controller
pub(crate) enum JobControllerReq {
    RegisterOp {
        name: String,
        args: HashMap<String, Value>,
        resp_tx: oneshot::Sender<Option<RegisterResult>>,
    },
    SpawnActor {
        addr: ActorAddr,
        op_name: String,
        resp_tx: oneshot::Sender<Option<SpawnResult>>,
    },
    RemoteActorAdded {
        addr: ActorAddr,
        sock_addr: SocketAddr,
    },
    StopAllActors,
}

struct LocalActor {
    handle: UnboundedSender<ControlInst>,
}
struct RemoteActor {
    remote_actor_addr: SocketAddr,
}

pub async fn node_controller<CG: CodeGenerator + Send + Sync + 'static>(code_gen: CG, port: u16) {
    // tracing_log::LogTracer::init().expect("Failed to set logger");

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!(
                    "info,{}=info,tower_http=debug,axum::rejection=trace",
                    env!("CARGO_CRATE_NAME")
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    // tracing_subscriber::FmtSubscriber::builder()
    //     .without_time()
    //     .init();
    // tracing_subscriber::fmt()
    //     .with_env_filter("info") // optional: use env filter like RUST_LOG
    //     .init();
    // env_logger::init();
    log::info!("[Node] Controller Start");

    let (job_control_tx, job_control_rx) = unbounded_channel();
    let server_handle = tokio::spawn(webserver(job_control_tx, port));
    let control_loop = tokio::spawn(actor_control_loop(job_control_rx, code_gen));

    server_handle.await.unwrap();
    control_loop.await.unwrap();

    log::info!("[Node] Controller Ended");
}

async fn actor_control_loop<CG: CodeGenerator + Send>(
    mut job_control_rx: UnboundedReceiver<JobControllerReq>,
    code_gen: CG,
) {
    let mut local_actors: HashMap<ActorAddr, LocalActor> = HashMap::new();
    let mut remote_actors: HashMap<ActorAddr, RemoteActor> = HashMap::new();
    let mut libs = HashMap::new();
    let (actor_control_tx, mut actor_control_rx) = channel(20);

    loop {
        tokio::select! {
            req = actor_control_rx.recv() => {
                match req {
                    Some(req) => {
                        handle_actor_req(req, &local_actors, &remote_actors).await;
                    },
                    None => break,
                }
            }
            req = job_control_rx.recv() => {
                match req {
                    Some(req) => {
                        handle_job_req(req, &code_gen, &mut libs, &mut local_actors, &mut remote_actors, &actor_control_tx).await;
                    },
                    None => break,
                }
            }
        }
    }
}

async fn handle_actor_req(
    req: ControlReq,
    local_actors: &HashMap<ActorAddr, LocalActor>,
    remote_actors: &HashMap<ActorAddr, RemoteActor>,
) {
    match req {
        ControlReq::Resolve { addr, resp_tx } => {
            log::debug!("[Node] Resolving {addr}");
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
            } else {
                panic!("Couldn't Resolve {}", addr);
            }
        }
    }
}

async fn handle_job_req<CG: CodeGenerator + Send>(
    req: JobControllerReq,
    code_gen: &CG,
    op_lib: &mut HashMap<String, Library>,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    remote_actors: &mut HashMap<ActorAddr, RemoteActor>,
    actor_contrl_tx: &Sender<ControlReq>,
) {
    match req {
        JobControllerReq::RegisterOp {
            args,
            resp_tx,
            name,
        } => {
            log::info!("[Node] Registering Op: {name}");
            let (code, deps) = code_gen.generate(&name, args);
            let lib = LibBuilder::build(code, deps).unwrap();
            op_lib.insert(name.clone(), lib);
            resp_tx.send(Some(RegisterResult {})).unwrap();
        }
        JobControllerReq::SpawnActor {
            addr,
            op_name,
            resp_tx,
        } => {
            log::info!("[Node] Spawing Actor {addr} with op: {op_name}");
            let (control_tx, control_rx) = unbounded_channel();
            unsafe {
                let lib = op_lib.get(&op_name).unwrap();
                let actor_callback: libloading::Symbol<ActorSpawnCB> =
                    lib.get(b"actor_callback").unwrap();
                let setup_shared_logger_ref: libloading::Symbol<SetupSharedLogger> =
                    lib.get(b"setup_shared_logger_ref").unwrap();
                let logger = SharedLogger::new();
                setup_shared_logger_ref(&logger);
                actor_callback(control_rx, actor_contrl_tx.clone(), addr);
            }
            let port: u16 = 6000;
            resp_tx.send(Some(SpawnResult { port })).unwrap();
            control_tx.send(ControlInst::StartTcpRecv(port)).unwrap();
            local_actors.insert(addr, LocalActor { handle: control_tx });
        }
        JobControllerReq::RemoteActorAdded { addr, sock_addr } => {
            log::info!("[Node] Remote Actor {addr} Added");
            remote_actors.insert(
                addr,
                RemoteActor {
                    remote_actor_addr: sock_addr,
                },
            );
        }
        JobControllerReq::StopAllActors => {
            local_actors.drain().for_each(|(name, actor)| {
                log::info!("[Node] Stopping Actor {name}");
                actor.handle.send(ControlInst::Stop).unwrap();
            });
        }
    }
}
