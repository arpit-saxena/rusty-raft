use std::process::exit;

use raft::consensus::Node;
use raft::log_stuffs;
use tracing::trace;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    log_stuffs();

    let config_path = "config.ron";
    let mut raft_node = match Node::new(config_path) {
        Err(e) => {
            eprintln!("Error in creating node using config path '{config_path}': {e}");
            exit(1);
        }
        Ok(n) => n
    };
    loop {
        trace!("Ticking RaftNode");
        raft_node.tick().await;
    }

    // Ok(())
}
