//! One Node Controller task will be spawned on each physical nodes.
use core::panic;
use op_lib_manager::OpLibrary;
use reactor_actor::{Connection, ControlInst, ControlReq, NodeComm, RuntimeCtx};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::{
    mpsc::{self, Sender, UnboundedReceiver, channel, unbounded_channel},
    oneshot,
};
use tracing::{error, info};
use tracing_shared::SharedLogger;
// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(feature = "instrument")]
pub mod instrument;

#[cfg(feature = "dynop")]
use code_gen::CodeGenerator;
#[cfg(feature = "dynop")]
use lib_builder::LibBuilder;
use serde_json::Value;
#[cfg(not(feature = "dynop"))]
use std::path::PathBuf;

#[cfg(feature = "dynop")]
pub mod code_gen;
#[cfg(feature = "dynop")]
pub mod lib_builder;
mod rpc;
use rpc::webserver;
mod op_lib_manager;

pub type NodeAddr = &'static str;
pub type ActorSpawnCB = fn(RuntimeCtx, HashMap<String, serde_json::Value>);

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
        payload: HashMap<String, Value>,
    },
    RemoteActorAdded {
        addr: ActorAddr,
        sock_addr: SocketAddr,
    },
    StopAllActors,
}

struct LocalActor {
    handle: Sender<ControlInst>,
}
struct RemoteActor {
    remote_actor_addr: SocketAddr,
}

#[cfg(not(feature = "dynop"))]
pub async fn node_controller(port: u16, operator_dir: PathBuf) {
    use opentelemetry::KeyValue;
    use tracing::info_span;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = info_span!("init_node_controller");
    let ops = span.in_scope(|| load_ops(operator_dir));

    let (job_control_tx, job_control_rx) = unbounded_channel();

    let server_handle = tokio::spawn(webserver(job_control_tx, port));
    span.add_event(
        "spawned_http_server",
        vec![KeyValue::new("port", port as i64)],
    );
    let control_loop = tokio::spawn(actor_control_loop(job_control_rx, ops));
    span.add_event("spawned_control_loop", vec![]);

    drop(span);

    server_handle.await.unwrap();
    control_loop.await.unwrap();

    info!("[Node] Controller Ended");
}

#[cfg(not(feature = "dynop"))]
#[tracing::instrument(fields(operator_dir = ?operator_dir, loaded_lib = tracing::field::Empty))]
fn load_ops(operator_dir: PathBuf) -> OpLibrary {
    use std::ffi::OsStr;
    use std::fs;

    use libloading::Library;
    use opentelemetry::KeyValue;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let mut op_libs = OpLibrary::default();

    if operator_dir.is_dir() {
        for entry in fs::read_dir(operator_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension() == Some(OsStr::new("so"))
                || path.extension() == Some(OsStr::new("dylib"))
            {
                let file_stem = path.file_stem().unwrap().to_string_lossy().to_string();
                let lib_name = file_stem
                    .strip_prefix("lib")
                    .unwrap_or(&file_stem)
                    .to_string();
                tracing::Span::current().add_event(
                    "loaded_lib",
                    vec![KeyValue::new("lib", lib_name.to_string())],
                );
                unsafe {
                    let lib = Library::new(&path).unwrap();
                    op_libs.add_lib(lib_name, lib);
                }
            }
        }
    } else {
        error!("Path is not a directory");
    }
    op_libs
}

#[cfg(not(feature = "dynop"))]
// #[tracing::instrument(skip(job_control_rx, op_lib))]
async fn actor_control_loop(
    mut job_control_rx: UnboundedReceiver<JobControllerReq>,
    op_lib: OpLibrary,
) {
    let mut local_actors: HashMap<ActorAddr, LocalActor> = HashMap::new();
    let mut remote_actors: HashMap<ActorAddr, RemoteActor> = HashMap::new();
    let (actor_control_tx, mut actor_control_rx) = channel(20);
    let mut port: u16 = 6000;

    loop {
        tokio::select! {
            req = actor_control_rx.recv() => {
                match req {
                    Some(req) => {
                        info!("[Node] Handling Actor Start");
                        handle_actor_req(req, &local_actors, &remote_actors).await;
                    },
                    None => break,
                }
            }
            req = job_control_rx.recv() => {
                match req {
                    Some(req) => {
                        handle_job_req(req, &op_lib, &mut local_actors, &mut remote_actors, &actor_control_tx, port).await;
                        port += 1;
                    },
                    None => break,
                }
            }
        }
    }
}

#[cfg(not(feature = "dynop"))]
#[tracing::instrument(skip(req, op_lib, local_actors, remote_actors, actor_contrl_tx))]
async fn handle_job_req(
    req: JobControllerReq,
    op_lib: &OpLibrary,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    remote_actors: &mut HashMap<ActorAddr, RemoteActor>,
    actor_contrl_tx: &Sender<ControlReq>,
    port: u16,
) {
    match req {
        JobControllerReq::SpawnActor {
            addr,
            op_name,
            resp_tx,
            lib_name,
            payload,
        } => {
            info!("[Node] Spawing Actor {addr} with op: {op_name}");
            let (control_tx, control_rx) = channel(20);

            let lib = op_lib.get_lib(&lib_name);
            unsafe {
                let shared_logger: libloading::Symbol<SetupSharedLogger> =
                    lib.get(b"setup_shared_logger_ref").unwrap();
                let logger = SharedLogger::new();
                shared_logger(logger);
                let op: libloading::Symbol<ActorSpawnCB> = lib.get(op_name.as_bytes()).unwrap();
                op(
                    RuntimeCtx::new(addr, NodeComm::new(control_rx, actor_contrl_tx.clone())),
                    payload,
                );
                resp_tx.send(Some(SpawnResult { port })).unwrap();
                control_tx
                    .send(ControlInst::StartTcpRecv(port))
                    .await
                    .unwrap();
                local_actors.insert(addr, LocalActor { handle: control_tx });
            }
        }
        JobControllerReq::RemoteActorAdded { addr, sock_addr } => {
            info!("[Node] Remote Actor {addr} Added");
            remote_actors.insert(
                addr,
                RemoteActor {
                    remote_actor_addr: sock_addr,
                },
            );
        }
        JobControllerReq::StopAllActors => {
            for (name, actor) in local_actors.drain() {
                info!("[Node] Stopping Actor {name}");
                actor.handle.send(ControlInst::Stop).await.unwrap();
            }
        }
    }
}

#[tracing::instrument(skip(local_actors, remote_actors, req))]
async fn handle_actor_req(
    req: ControlReq,
    local_actors: &HashMap<ActorAddr, LocalActor>,
    remote_actors: &HashMap<ActorAddr, RemoteActor>,
) {
    match req {
        ControlReq::Resolve { addr, resp_tx } => {
            info!("[Node] Resolving {addr}");
            if let Some(local) = local_actors.get(addr) {
                let (write_half, read_half) = mpsc::channel(1 << 10);
                local
                    .handle
                    .send(ControlInst::StartLocalRecv(read_half))
                    .await
                    .unwrap();
                resp_tx.send(Connection::Local(write_half)).unwrap();
            } else if let Some(local) = remote_actors.get(addr) {
                resp_tx
                    .send(Connection::Remote(local.remote_actor_addr))
                    .unwrap();
            } else {
                error!("Couldn't Resolve {}", addr);
                panic!("Couldn't Resolve {}", addr);
            }
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////// Dynamic Operators ////////////////////////////////////////
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
            payload,
        } => {
            log::info!("[Node] Spawing Actor {addr} with op: {op_name}");
            let (control_tx, control_rx) = channel(20);

            let lib = op_lib.get_lib(&lib_name);
            unsafe {
                let shared_logger: libloading::Symbol<SetupSharedLogger> =
                    lib.get(b"setup_shared_logger_ref").unwrap();
                let logger = SharedLogger::new();
                shared_logger(logger);
                let op: libloading::Symbol<ActorSpawnCB> = lib.get(op_name.as_bytes()).unwrap();
                op(
                    addr,
                    NodeComm {
                        controller_rx: control_rx,
                        controller_tx: actor_contrl_tx.clone(),
                    },
                    payload,
                );
                let port: u16 = 6000;
                resp_tx.send(Some(SpawnResult { port })).unwrap();
                control_tx
                    .send(ControlInst::StartTcpRecv(port))
                    .await
                    .unwrap();
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
            for (name, actor) in local_actors.drain() {
                log::info!("[Node] Stopping Actor {name}");
                actor.handle.send(ControlInst::Stop).await.unwrap();
            }
        }
    }
}
