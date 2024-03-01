use serde::{Deserialize, Serialize};
use std::{ops::RangeInclusive, path::Path, time::Duration};
use tracing::trace;

// use crate::consensus::Config as NodeConfig;

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct ClusterConfig {
    data_path: Box<Path>,
    election_timeout_interval: RangeInclusive<f32>,
    #[serde_as(as = "serde_with::DurationMilliSeconds")]
    heartbeat_interval: Duration,
}

pub struct Cluster {
    config: ClusterConfig,
}

impl Cluster {
    #[tracing::instrument(skip(reader))]
    pub fn from_reader<Reader: std::io::Read>(
        reader: Reader,
    ) -> Result<Cluster, Box<dyn std::error::Error>> {
        let config: ClusterConfig = ron::de::from_reader(reader)?;
        let cluster = Cluster { config };
        trace!("Parsed config {:?}", cluster.config);
        Ok(cluster)
    }
}
