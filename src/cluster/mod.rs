use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::{
    ops::RangeInclusive,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::task::{JoinError, JoinSet};
use tracing::trace;

use crate::consensus::Config as NodeConfig;
use crate::consensus::NodeClient;

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct ClusterConfig {
    data_path: Box<Path>,
    election_timeout_interval: RangeInclusive<f32>,
    #[serde_as(as = "serde_with::DurationMilliSeconds")]
    heartbeat_interval: Duration,
    #[serde(default = "default_port_range_start")]
    port_range_start: u16,
}

const fn default_port_range_start() -> u16 {
    54000
}

pub struct Cluster {
    _node_config: NodeConfig,
    jobs: JoinSet<()>,
}

impl Cluster {
    #[tracing::instrument(skip(reader))]
    pub async fn from_reader<Reader: std::io::Read>(
        reader: Reader,
        num_nodes: u16,
    ) -> Result<Cluster, Box<dyn std::error::Error>> {
        let config: ClusterConfig = ron::de::from_reader(reader)?;
        trace!("Parsed config {:?}", config);

        let cluster_members: Vec<String> = (0..num_nodes)
            .map(|id| id + config.port_range_start)
            .map(|port| format!("127.0.0.1:{}", port))
            .collect();

        let mut state_file_path = PathBuf::from(config.data_path);
        state_file_path.push("data.raft");
        let node_config = NodeConfig {
            persistent_state_file: state_file_path.into_boxed_path(),
            cluster_members,
            election_timeout_interval: config.election_timeout_interval,
            heartbeat_interval: config.heartbeat_interval,
        };

        let mut nodes = Vec::new();
        for node_id in 0..num_nodes {
            nodes.push(NodeClient::from_config(node_config.clone(), node_id as u32).await?);
        }

        let mut jobs = JoinSet::new();
        for mut node in nodes.into_iter() {
            jobs.spawn(async move {
                loop {
                    node.tick().await.unwrap()
                }
            });
        }

        let cluster = Cluster {
            _node_config: node_config,
            jobs,
        };
        Ok(cluster)
    }

    pub async fn join_all(&mut self) -> Result<(), JoinError> {
        while let Some(res) = self.jobs.join_next().await {
            res?
        }
        Ok(())
    }
}
