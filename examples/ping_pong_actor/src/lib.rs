use lazy_static;
use ping_pong::actor;
pub use reactor_actor::setup_shared_logger_ref;
use reactor_actor::ControlInst;
use reactor_actor::ControlReq;
use tokio::sync::{mpsc, Mutex};

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[unsafe(no_mangle)]
pub extern "C" fn pinger(
    inst_recv: mpsc::UnboundedReceiver<ControlInst>,
    req_send: mpsc::Sender<ControlReq>,
    actor_name: &'static str,
) {
    RUNTIME.spawn(actor(inst_recv, req_send, actor_name, "ponger"));
}

#[unsafe(no_mangle)]
pub extern "C" fn ponger(
    inst_recv: mpsc::UnboundedReceiver<ControlInst>,
    req_send: mpsc::Sender<ControlReq>,
    actor_name: &'static str,
) {
    RUNTIME.spawn(actor(inst_recv, req_send, actor_name, "pinger"));
}
