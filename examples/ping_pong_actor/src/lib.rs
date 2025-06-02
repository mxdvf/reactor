use ping_pong::actor;
use tokio::sync::{Mutex, mpsc};
use reactor_node::ControlReq;
use reactor_node::ControlInst;

async fn actor_callback(
    inst_recv: mpsc::UnboundedReceiver<ControlInst>,
    req_send: mpsc::Sender<ControlReq>,
    actor_name: &'static str,
) {
    tokio::spawn(actor(inst_recv, req_send, actor_name, "ping"));
}
