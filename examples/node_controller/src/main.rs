//! Node Controller

use clap::{Parser, Subcommand, arg};
//#[cfg(feature="dynop")]
//use reactor_node::code_gen::CodeGenerator;
use reactor_node::node_controller;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "Reactor", about = "Run a reactor node")]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run a reactor node
    Node {
        #[arg(short, long)]
        port: u16,
        dir: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Node { port, dir } => node_controller(port, dir).await,
    }
}
