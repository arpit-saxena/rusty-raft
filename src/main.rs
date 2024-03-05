use std::{fs::File, path::Path, process::exit};

use anyhow::Result;
use clap::{Parser, Subcommand};
use raft::{cluster::Cluster, consensus::Node};

#[derive(Debug, Subcommand)]
enum CliMode {
    Node {
        config_path: Box<Path>,
        node_id: u32,
    },
    Cluster {
        config_path: Box<Path>,
        num_nodes: u16,
    },
}

#[derive(Parser, Debug)]
struct Cli {
    #[clap(subcommand)]
    mode: CliMode,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    match cli.mode {
        CliMode::Node {
            config_path,
            node_id,
        } => {
            run_node(node_id, config_path).await?;
        }
        CliMode::Cluster {
            config_path,
            num_nodes,
        } => run_cluster(config_path, num_nodes).await?,
    }

    Ok(())
}

async fn run_node(node_id: u32, config_path: Box<Path>) -> Result<()> {
    let config_file = File::open(config_path.clone())?;
    let raft_node = match Node::from_config_reader(config_file, node_id).await {
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

    raft_node.tick_forever().await?;
    Ok(())
}

async fn run_cluster(config_path: Box<Path>, num_nodes: u16) -> Result<()> {
    let config_file = File::open(config_path.clone())?;
    let mut cluster = Cluster::from_reader(config_file, num_nodes).await?;
    cluster.join_all().await?;
    Ok(())
}
