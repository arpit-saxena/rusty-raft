use std::fs::File;
use std::ops::RangeInclusive;

use std::str::FromStr;

use futures::future::join_all;
use rand::distributions::{Distribution, Uniform};

use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::fs::File as TokioFile;
use tokio::time::{Duration, Instant};
use tonic::transport::{Endpoint, Uri};
use tracing::trace;

use super::pb::raft_client::RaftClient;
use super::state;
use super::{Node, PeerNode};

type Message = Vec<u8>;

pub trait StateMachine {
    fn transition(msg: Message);
}

/// Configuration for Raft Consensus, can be read from any file, currently only RON is supported
#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct Config {
    persistent_state_file: String,
    cluster_members: Vec<String>, // vector of Uri's
    election_timeout_interval: RangeInclusive<f32>,
    #[serde_as(as = "serde_with::DurationMilliSeconds")]
    heartbeat_interval: Duration,
}

impl Config {
    fn from_file(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
        let config_file = File::open(path)?;
        let config: Config = ron::de::from_reader(config_file)?;
        trace!("Parsed config from file {path}: \n{:#?}", config);
        Ok(config)
    }
}

impl Node<TokioFile> {
    pub async fn new(
        config_path: &str,
        node_index: usize,
    ) -> Result<Node<TokioFile>, Box<dyn std::error::Error>> {
        let mut config = Config::from_file(config_path)?;
        let mut heartbeat_interval = Box::pin(tokio::time::interval(config.heartbeat_interval));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let persistent_state_path = std::mem::take(&mut config.persistent_state_file);
        let persistent_state_file = tokio::fs::OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(persistent_state_path)
            .await?;

        let distribution = Uniform::from(config.election_timeout_interval.clone());
        let rng = SmallRng::from_entropy();
        let (peers, listen_addr) = Self::peers_from_config(config, node_index).await?;
        let mut node = Node {
            persistent_state: state::Persistent::new(persistent_state_file).await?,
            election_timer: Box::pin(tokio::time::sleep(Duration::from_secs(0))),
            heartbeat_interval,
            timer_distribution: distribution,
            rng,
            peers,
            listen_addr,
        };
        node.reset_election_timer();
        Ok(node)
    }

    async fn peers_from_config(
        config: Config,
        node_index: usize,
    ) -> Result<(Vec<PeerNode>, Uri), Box<dyn std::error::Error>> {
        let cluster_members = config.cluster_members;
        if node_index >= cluster_members.len() {
            return Err(format!(
                "node_index {node_index} not valid for {} cluster members",
                cluster_members.len()
            )
            .into());
        }

        let listen_addr = Uri::from_str(&cluster_members[node_index])?;
        let peer_node_futures = cluster_members
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| *idx != node_index)
            .map(|(idx, uri_str)| -> Result<_, Box<dyn std::error::Error>> {
                let uri = Uri::from_str(&uri_str)?;
                trace!("Calling PeerNode constructor for index {idx}");
                Ok(PeerNode::from_address(uri, idx))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let peers: Vec<PeerNode> = join_all(peer_node_futures)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        Ok((peers, listen_addr))
    }

    fn reset_election_timer(&mut self) {
        let timeout = Duration::from_micros(
            (self.timer_distribution.sample(&mut self.rng) * 1000_f32).floor() as u64,
        );
        trace!("New election timeout is {} millis", timeout.as_millis());
        self.election_timer.as_mut().reset(Instant::now() + timeout);
    }

    /// Process the next event
    // TODO: Should this be async?
    pub async fn tick(&mut self) {
        // Tick the election timer
        let election_timer = self.election_timer.as_mut();
        tokio::select! {
            () = election_timer => {
                trace!("Hey the election timer just expired do something about it");
                // call function which will increment current term and call RequestVote RPC to all peers
                // On receiving response update some info about number of votes received and when majority
                // votes are received, election is done, send heartbeats.
            }
            _ = self.heartbeat_interval.tick() => {
                trace!("Heartbeat interval expired send heartbeat");
                // TODO: Send heartbeat
            }
        };
        self.reset_election_timer();
    }
}

impl PeerNode {
    async fn from_address(
        address: Uri,
        node_index: usize,
    ) -> Result<PeerNode, Box<dyn std::error::Error>> {
        trace!("Creating peer node with index {node_index} to address {address}");

        let endpoint = Endpoint::new(address.clone())?;
        let channel = endpoint.connect_lazy();
        let rpc_client = RaftClient::new(channel);
        let peer_node: PeerNode = PeerNode {
            address,
            rpc_client,
            node_index,
        };
        Ok(peer_node)
    }
}

mod test {
    /*
     * TODO: tests
     * 1. Node::new
     * 2. election timer reset
     * 3. Config::from_file
     */
}
