use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum RecieverErr {
    #[error("TCP Server start error: {0}")]
    TcpStartErr(#[from] std::io::Error),
}

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum ActorError {
    #[error("Generator couldn't send message to Processor")]
    G2PErr,
    #[error("Receiver couldn't send message to Processor")]
    R2PErr,
    #[error("Processor couldn't send message to Sender")]
    P2SErr,
    #[error("{0}")]
    RecieverErr(#[from] RecieverErr),
    #[error("Task Join Error: {0}")]
    JoinErr(#[from] JoinError),
}

pub struct DecodeErr;
impl From<std::io::Error> for DecodeErr {
    fn from(_: std::io::Error) -> Self {
        DecodeErr
    }
}
