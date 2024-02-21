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
use tracing::{debug, trace};

use crate::consensus::pb;

use super::pb::raft_client::RaftClient;
use super::state;
use super::{Node, PeerNode};

use super::service;

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
        node_index: u32,
    ) -> Result<Node<TokioFile>, Box<dyn std::error::Error>> {
        let mut config = Config::from_file(config_path)?;
        let mut heartbeat_interval = Box::pin(tokio::time::interval(config.heartbeat_interval));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let persistent_state_path = std::mem::take(&mut config.persistent_state_file);
        let persistent_state_file = tokio::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(persistent_state_path)
            .await?;

        let distribution = Uniform::from(config.election_timeout_interval.clone());
        let rng = SmallRng::from_entropy();
        let (peers, listen_addr) = Self::peers_from_config(config, node_index as usize).await?;
        let election_timeout = Duration::from_secs(0);
        let mut node = Node {
            persistent_state: state::Persistent::new(persistent_state_file).await?,
            common_volatile_state: state::VolatileCommon::new(),
            leader_volatile_state: None,
            node_index,
            election_timeout,
            election_timer: Box::pin(tokio::time::sleep(election_timeout)),
            heartbeat_interval,
            timer_distribution: distribution,
            rng,
            peers,
            listen_addr,
        };
        node.set_new_election_timeout();
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

    fn set_new_election_timeout(&mut self) {
        let timeout = Duration::from_micros(
            (self.timer_distribution.sample(&mut self.rng) * 1000_f32).floor() as u64,
        );
        trace!("New election timeout is {} millis", timeout.as_millis());
        self.election_timeout = timeout;
    }

    fn restart_election_timer(&mut self) {
        self.election_timer.as_mut().reset(Instant::now() + self.election_timeout);
    }

    /// Process the next event
    // TODO: Should this be async?
    pub async fn tick(&mut self) {
        // Tick the election timer
        let election_timer = self.election_timer.as_mut();
        tokio::select! {
            () = election_timer => {
                trace!("Hey the election timer just expired do something about it");
                // TODO: call function which will increment current term and call RequestVote RPC to all peers
                // On receiving response update some info about number of votes received and when majority
                // votes are received, election is done, send heartbeats.
                self.set_new_election_timeout();
            }
            _ = self.heartbeat_interval.tick() => {
                // TODO: Don't tick heartbeat if not leader
                trace!("Heartbeat interval expired send heartbeat");
                if self.leader_volatile_state.is_some() {
                    self.send_heartbeat().await;
                }
            }
        };
        self.restart_election_timer();
    }

    async fn send_heartbeat(&mut self) {
        trace!("Sending heartbeat to all peers");

        let append_entries_request = pb::AppendEntriesRequest{
            term: self.persistent_state.current_term(),
            leader_id: self.node_index,
            prev_log_index: 0, // FIXME
            prev_log_term: 0, // FIXME
            leader_commit: self.common_volatile_state.commit_index,
        };

        let mut heartbeat_futures = Vec::new();
        for peer in &mut self.peers {
            heartbeat_futures.push(peer.rpc_client.append_entries(tonic::Request::new(append_entries_request.clone())));
        }

        for (idx, result) in join_all(heartbeat_futures).await.into_iter().enumerate() {
            // TODO: Convert to match and check response. If response's term is greater than our term, revert to follower
            if let Err(e) = result {
                // TODO: Have to retry here. All failed RPC's have to retried indefinitely (Paper section 5.5)
                let peer = &self.peers[idx];
                debug!("Unable to send heartbeat to peer {}, address {}: {}", peer.node_index, peer.address, e);
            }
        }
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
