use std::process::exit;

use clap::Parser;
use raft::consensus::NodeClient;
use raft::log_stuffs;
use tracing::trace;

#[derive(Parser)]
struct Cli {
    node_id: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    log_stuffs();

    let config_path = "config.ron";
    let mut raft_node = match NodeClient::new(config_path, cli.node_id).await {
        Err(e) => {
            eprintln!(
                "Error in creating node using config path '{config_path}': {:#?}",
                e
            );
            exit(1);
        }
        Ok(n) => n,
    };
    loop {
        trace!("Ticking RaftNode");
        raft_node.tick().await?;
    }

    // Ok(())
}
