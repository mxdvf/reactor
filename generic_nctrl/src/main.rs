//! a generic node controller. use this if you dont intend to modify any
//! node_controller logic

use clap::{Parser, arg};
use reactor_node::node_controller;
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "Node Controller", about = "Run reactor Node controller")]
pub struct Cli {
    /// Port to run the reactor node on
    #[arg(short, long)]
    pub port: u16,

    /// Directory path
    pub dir: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
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
    node_controller(cli.port, cli.dir).await;
}
