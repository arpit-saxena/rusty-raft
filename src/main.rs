use std::{fs::File, path::Path, process::exit};

use clap::{Parser, Subcommand};
use raft::{cluster::Cluster, consensus::NodeClient};

#[derive(Debug, Subcommand)]
enum CliMode {
    Node {
        config_path: Box<Path>,
        node_id: u32,
    },
    Cluster {
        config_path: Box<Path>,
    },
}

#[derive(Parser, Debug)]
struct Cli {
    #[clap(subcommand)]
    mode: CliMode,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    match cli.mode {
        CliMode::Node {
            config_path,
            node_id,
        } => {
            run_node(node_id, config_path).await?;
        }
        CliMode::Cluster { config_path } => run_cluster(config_path).await?,
    }

    Ok(())
}

async fn run_node(node_id: u32, config_path: Box<Path>) -> Result<(), Box<dyn std::error::Error>> {
    let config_file = File::open(config_path.clone())?;
    let mut raft_node = match NodeClient::from_config_reader(config_file, node_id).await {
        Err(e) => {
            eprintln!(
                "Error in creating node using config path '{:?}': {:#?}",
                config_path.to_str(),
                e
            );
            exit(1);
        }
        Ok(n) => n,
    };
    loop {
        // trace!("Ticking RaftNode");
        raft_node.tick().await?;
    }
}

async fn run_cluster(config_path: Box<Path>) -> Result<(), Box<dyn std::error::Error>> {
    let config_file = File::open(config_path.clone())?;
    let _ = Cluster::from_reader(config_file)?;
    Ok(())
}
