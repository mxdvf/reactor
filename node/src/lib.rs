//! One Node Controller task will be spawned on each physical nodes.
use core::panic;
use op_lib_manager::OpLibrary;
use reactor_actor::{Connection, ControlInst, ControlReq};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::{
    io::simplex,
    sync::{
        mpsc::{self, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
        oneshot,
    },
};
use tracing_shared::SharedLogger;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(feature = "dynop")]
use code_gen::CodeGenerator;
#[cfg(feature = "dynop")]
use lib_builder::LibBuilder;
#[cfg(not(feature = "dynop"))]
use std::path::PathBuf;

#[cfg(feature = "dynop")]
pub mod code_gen;
mod rpc;
use rpc::webserver;
mod op_lib_manager;

#[cfg(feature = "dynop")]
mod lib_builder;

pub type NodeAddr = &'static str;
pub type ActorSpawnCB =
    fn(mpsc::UnboundedReceiver<ControlInst>, mpsc::Sender<ControlReq>, ActorAddr);

pub type SetupSharedLogger = fn(SharedLogger);

type ActorAddr = &'static str;
type LibName = String;

#[derive(Debug)]
pub(crate) struct SpawnResult {
    port: u16,
}

#[cfg(feature = "dynop")]
#[derive(Debug)]
pub(crate) struct RegisterResult {}

/// Global Controller
pub(crate) enum JobControllerReq {
    #[cfg(feature = "dynop")]
    RegisterOps {
        lib_name: String,
        args: HashMap<String, Value>,
        resp_tx: oneshot::Sender<Option<RegisterResult>>,
    },
    SpawnActor {
        addr: ActorAddr,
        lib_name: String,
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

#[cfg(not(feature = "dynop"))]
pub async fn node_controller(port: u16, operator_dir: PathBuf) {
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
    log::info!("[Node] Controller Start");

    let ops = load_ops(operator_dir);

    let (job_control_tx, job_control_rx) = unbounded_channel();
    let server_handle = tokio::spawn(webserver(job_control_tx, port));
    let control_loop = tokio::spawn(actor_control_loop(job_control_rx, ops));

    server_handle.await.unwrap();
    control_loop.await.unwrap();

    log::info!("[Node] Controller Ended");
}

#[cfg(not(feature = "dynop"))]
fn load_ops(operator_dir: PathBuf) -> OpLibrary {
    use std::ffi::OsStr;
    use std::fs;

    use libloading::Library;

    let mut op_libs = OpLibrary::default();

    if operator_dir.is_dir() {
        for entry in fs::read_dir(operator_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension() == Some(OsStr::new("so")) {
                let file_stem = path.file_stem().unwrap().to_string_lossy().to_string();
                let lib_name = file_stem
                    .strip_prefix("lib")
                    .unwrap_or(&file_stem)
                    .to_string();
                unsafe {
                    let lib = Library::new(&path).unwrap();
                    log::info!("[Node] Loading Library named {lib_name}");
                    op_libs.add_lib(lib_name, lib);
                }
            }
        }
    }
    op_libs
}

#[cfg(not(feature = "dynop"))]
async fn actor_control_loop(
    mut job_control_rx: UnboundedReceiver<JobControllerReq>,
    op_lib: OpLibrary,
) {
    let mut local_actors: HashMap<ActorAddr, LocalActor> = HashMap::new();
    let mut remote_actors: HashMap<ActorAddr, RemoteActor> = HashMap::new();
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
                        handle_job_req(req, &op_lib, &mut local_actors, &mut remote_actors, &actor_control_tx).await;
                    },
                    None => break,
                }
            }
        }
    }
}

#[cfg(not(feature = "dynop"))]
async fn handle_job_req(
    req: JobControllerReq,
    op_lib: &OpLibrary,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    remote_actors: &mut HashMap<ActorAddr, RemoteActor>,
    actor_contrl_tx: &Sender<ControlReq>,
) {
    match req {
        JobControllerReq::SpawnActor {
            addr,
            op_name,
            resp_tx,
            lib_name,
        } => {
            log::info!("[Node] Spawing Actor {addr} with op: {op_name}");
            let (control_tx, control_rx) = unbounded_channel();

            let lib = op_lib.get_lib(&lib_name);
            unsafe {
                let shared_logger: libloading::Symbol<SetupSharedLogger> =
                    lib.get(b"setup_shared_logger_ref").unwrap();
                let logger = SharedLogger::new();
                shared_logger(logger);
                let op: libloading::Symbol<ActorSpawnCB> = lib.get(op_name.as_bytes()).unwrap();
                op(control_rx, actor_contrl_tx.clone(), addr);
                let port: u16 = 6000;
                resp_tx.send(Some(SpawnResult { port })).unwrap();
                control_tx.send(ControlInst::StartTcpRecv(port)).unwrap();
                local_actors.insert(addr, LocalActor { handle: control_tx });
            }
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

// //////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////// Dynamic OPerators ////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "dynop")]
pub async fn node_controller<CG: CodeGenerator + Send + Sync + 'static>(code_gen: CG, port: u16) {
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
    log::info!("[Node] Controller Start");

    let (job_control_tx, job_control_rx) = unbounded_channel();
    let server_handle = tokio::spawn(webserver(job_control_tx, port));
    let control_loop = tokio::spawn(actor_control_loop(job_control_rx, code_gen));

    server_handle.await.unwrap();
    control_loop.await.unwrap();

    log::info!("[Node] Controller Ended");
}

#[cfg(feature = "dynop")]
async fn actor_control_loop<CG: CodeGenerator + Send>(
    mut job_control_rx: UnboundedReceiver<JobControllerReq>,
    code_gen: CG,
) {
    let mut local_actors: HashMap<ActorAddr, LocalActor> = HashMap::new();
    let mut remote_actors: HashMap<ActorAddr, RemoteActor> = HashMap::new();
    let mut libs = OpLibrary::default();
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

#[cfg(feature = "dynop")]
async fn handle_job_req<CG: CodeGenerator + Send>(
    req: JobControllerReq,
    code_gen: &CG,
    op_lib: &mut OpLibrary,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    remote_actors: &mut HashMap<ActorAddr, RemoteActor>,
    actor_contrl_tx: &Sender<ControlReq>,
) {
    match req {
        JobControllerReq::RegisterOps {
            args,
            resp_tx,
            lib_name,
        } => {
            log::info!("[Node] Registering Op from lib: {lib_name}");
            let (code, deps) = code_gen.generate(args);
            let lib = LibBuilder::build(code, deps).unwrap();
            op_lib.add_lib(lib_name.to_string(), lib);
            resp_tx.send(Some(RegisterResult {})).unwrap();
        }
        JobControllerReq::SpawnActor {
            addr,
            op_name,
            resp_tx,
            lib_name,
        } => {
            log::info!("[Node] Spawing Actor {addr} with op: {op_name}");
            let (control_tx, control_rx) = unbounded_channel();

            let lib = op_lib.get_lib(&lib_name);
            unsafe {
                let shared_logger: libloading::Symbol<SetupSharedLogger> =
                    lib.get(b"setup_shared_logger_ref").unwrap();
                let logger = SharedLogger::new();
                shared_logger(logger);
                let op: libloading::Symbol<ActorSpawnCB> = lib.get(op_name.as_bytes()).unwrap();
                op(control_rx, actor_contrl_tx.clone(), addr);
                let port: u16 = 6000;
                resp_tx.send(Some(SpawnResult { port })).unwrap();
                control_tx.send(ControlInst::StartTcpRecv(port)).unwrap();
                local_actors.insert(addr, LocalActor { handle: control_tx });
            }
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
