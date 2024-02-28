use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::File;
use std::net::SocketAddr;
use std::ops::{DerefMut, RangeInclusive};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use super::pb::raft_client::RaftClient;
use super::pb::raft_server::RaftServer;
use super::{pb, PeerNode, TaskResult};
use super::{state, NodeClient, NodeCommon, NodeRole, NodeServer};
use futures::future::join_all;
use rand::distributions::{Distribution, Uniform};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::fs::File as TokioFile;
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Duration, Instant, Sleep};
use tonic::transport::{Endpoint, Server, Uri};
use tracing::{debug, error, info, trace, warn};

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

impl NodeClient<TokioFile> {
    pub async fn new(
        config_path: &str,
        node_index: u32,
    ) -> Result<NodeClient<TokioFile>, Box<dyn std::error::Error>> {
        let mut config = Config::from_file(config_path)?;

        let mut persistent_state_path =
            PathBuf::from(std::mem::take(&mut config.persistent_state_file));
        let file_name_suffix = persistent_state_path
            .file_name()
            .ok_or("File name should be present in persistent_state_file")?
            .to_str()
            .ok_or("persistent_state_file should have valid unicode characters")?;
        persistent_state_path.set_file_name(format!("node_{}_{}", node_index, file_name_suffix));

        let persistent_state_file = tokio::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(persistent_state_path)
            .await?;

        let distribution = Uniform::from(config.election_timeout_interval.clone());
        let rng = SmallRng::from_entropy();
        let heartbeat_interval = config.heartbeat_interval;
        let (peers, listen_addr) = Self::peers_from_config(config, node_index as usize).await?;
        let election_timeout = Duration::from_secs(0);
        let timer = Box::pin(tokio::time::sleep(election_timeout));

        let node_common = Arc::new(NodeCommon {
            persistent_state: tokio::sync::Mutex::new(
                state::Persistent::new(persistent_state_file).await?,
            ),
            node_index,
        });

        let last_leader_message_time = Arc::new(std::sync::Mutex::new(Instant::now()));
        let mut node = NodeClient {
            node_common: Arc::clone(&node_common),
            common_volatile_state: state::VolatileCommon::new(),
            role: NodeRole::Follower,
            election_timeout,
            election_timer_distribution: distribution,
            heartbeat_interval,
            timer,
            last_leader_message_time: Arc::clone(&last_leader_message_time),
            rng,
            peers,
            jobs: JoinSet::new(),
        };
        node.set_new_election_timeout();
        node.update_election_timer();

        trace!("Spawning server to listen on {}", listen_addr);
        let node_server = NodeServer { node_common, last_leader_message_time };
        tokio::spawn(
            Server::builder()
                .add_service(RaftServer::new(node_server))
                .serve(listen_addr),
        );

        Ok(node)
    }

    async fn peers_from_config(
        config: Config,
        node_index: usize,
    ) -> Result<(HashMap<usize, PeerNode>, SocketAddr), Box<dyn std::error::Error>> {
        let cluster_members = config.cluster_members;
        if node_index >= cluster_members.len() {
            return Err(format!(
                "node_index {node_index} not valid for {} cluster members",
                cluster_members.len()
            )
            .into());
        }

        let listen_addr: SocketAddr = cluster_members[node_index].parse()?;
        let peer_node_futures = cluster_members
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| *idx != node_index)
            .map(|(idx, uri_str)| -> Result<_, Box<dyn std::error::Error>> {
                let uri = Uri::builder()
                    .scheme("http")
                    .authority(uri_str)
                    .path_and_query("/")
                    .build()?;
                trace!("Calling PeerNode constructor for index {idx}");
                Ok(PeerNode::from_address(uri, idx))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let peers: Vec<PeerNode> = join_all(peer_node_futures)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let peers = peers.into_iter().map(|p| (p.node_index, p)).collect();

        Ok((peers, listen_addr))
    }

    fn set_new_election_timeout(&mut self) {
        let timeout = Duration::from_micros(
            (self.election_timer_distribution.sample(&mut self.rng) * 1000_f32).floor() as u64,
        );
        trace!("New election timeout is {} millis", timeout.as_millis());
        self.election_timeout = timeout;
    }

    fn update_election_timer(&mut self) {
        match &self.role {
            NodeRole::Leader(_) => {}
            _ => {
                self.timer
                .as_mut()
                .reset(*self.last_leader_message_time.lock().unwrap() + self.election_timeout);
            }
        }
    }

    fn reset_and_update_election_timer(&mut self) {
        {
            let mut last_leader_message_time = self.last_leader_message_time.lock().unwrap();
            *last_leader_message_time = Instant::now();
        }
        self.update_election_timer();
    }

    fn restart_heartbeat_timer(&mut self) {
        self.timer.as_mut().reset(Instant::now() + self.heartbeat_interval);
    }

    /// Process the next event
    // TODO: Should this be async?
    pub async fn tick(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.update_election_timer();
        let timer = self.timer.as_mut();
        tokio::select! {
            () = timer => {
                // Update election timer again. If it has elapsed then do election stuff
                self.update_election_timer();
                if self.timer.is_elapsed() {
                    match &self.role {
                        NodeRole::Follower => {
                            self.reset_and_update_election_timer();
                            self.become_candidate().await?;
                        }
                        NodeRole::Candidate(_) => {
                            // in case of a split vote, the election timer may expire when we're still a candidate
                            self.reset_and_update_election_timer();
                            self.become_candidate().await?;
                        }
                        NodeRole::Leader(_) => {
                            self.restart_heartbeat_timer();
                            self.send_heartbeats().await;
                        }
                    }
                } else {
                    debug!("This tick won't do anything since timer got updated after select");
                }
            }
            Some(res) = self.jobs.join_next(), if self.jobs.len() > 0 => {
                match res {
                    Err(join_error) => {
                        if join_error.is_panic() {
                            error!("Some task panicked..");
                        }
                    }
                    Ok(TaskResult::VoteResponse(vote_response)) => {
                        if vote_response.vote_granted {
                            trace!("Received vote");
                            if let NodeRole::Candidate(candidate_state) = &mut self.role {
                                candidate_state.votes_received += 1;
                                if candidate_state.votes_received > (1 + self.peers.len()) / 2 {
                                    info!("Elected as leader woohoo");
                                    self.role = NodeRole::Leader(state::VolatileLeader::new(&self.peers));
                                    self.send_heartbeats().await;
                                    self.restart_heartbeat_timer();
                                }
                            } else {
                                // This can happen when one response was delayed and we got majority votes before
                                debug!("Received vote response from peer, however current role is not candidate");
                            }
                        } else {
                            let mut persistent_state = self.node_common.persistent_state.lock().await;
                            if vote_response.term > persistent_state.current_term() {
                                // Our term was stale, update it and abort all
                                trace!(current_term = persistent_state.current_term(), vote_response.term);
                                persistent_state.update_current_term(vote_response.term).await?;
                                self.jobs.shutdown().await;
                            }
                        }
                    }
                    Ok(TaskResult::VoteFail(peer_id)) => {
                        if let NodeRole::Candidate(_) = &self.role {
                            self.request_vote(peer_id).await;
                        }
                    }
                    Ok(TaskResult::HeartbeatSuccess(peer_id, append_response)) => {
                        trace!(task_result = "Heartbeat success", peer_id, append_response.term, append_response.success);
                        self.peers.get_mut(&peer_id).unwrap().pending_heartbeat = false;
                    }
                    Ok(TaskResult::HeartbeatFail(peer_id)) => {
                        if let NodeRole::Leader(_) = &self.role {
                            self.retry_heartbeat(peer_id).await;
                        } else {
                            debug!("Had to retry heartbeat to {}, but not doing so since we're not leader anymore", peer_id);
                        }
                    }
                }
            }
        };

        Ok(())
    }

    async fn request_vote(&mut self, peer_id: usize) {
        let peer = self
            .peers
            .get_mut(&peer_id)
            .expect("Expected peer_id to be valid");
        let mut rpc_client = peer.rpc_client.clone();

        let request_votes_request = pb::VoteRequest {
            term: self
                .node_common
                .persistent_state
                .lock()
                .await
                .current_term(),
            candidate_id: self.node_common.node_index,
            last_log_index: 0, // TODO
            last_log_term: 0,
        };
        let request = tonic::Request::new(request_votes_request.clone());

        self.jobs.spawn(async move {
            trace!("Running job to request vote for peer id {}", peer_id);
            match rpc_client.request_vote(request).await {
                Err(e) => {
                    trace!("Request to vote to peer {} failed: {}", peer_id, e);
                    TaskResult::VoteFail(peer_id)
                }
                Ok(response) => {
                    let response = response.into_inner();
                    TaskResult::VoteResponse(response)
                }
            }
        });
    }

    async fn become_candidate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // convert to candidate and request votes.
        // Drop previous RPCs, might be due to a split vote
        trace!("Becoming candidate");
        self.jobs.shutdown().await;
        let candidate_state = state::VolatileCandidate { votes_received: 0 };
        self.role = NodeRole::Candidate(candidate_state);

        self.node_common
            .persistent_state
            .lock()
            .await
            .increment_current_term_and_vote(self.node_common.node_index as i32)
            .await?;

        // FIXME: Don't make this vec. Need to fix borrow checker issues
        let peer_ids: Vec<usize> = self.peers.iter().map(|(id, _)| *id).collect();
        for peer_id in peer_ids.into_iter() {
            self.request_vote(peer_id).await;
        }
        Ok(())
    }

    async fn send_heartbeat(&mut self, peer_id: usize) {
        let peer = self
            .peers
            .get_mut(&peer_id)
            .expect("Expected peer_id to be valid");

        if peer.pending_heartbeat {
            return;
        }

        let append_entries_request = pb::AppendEntriesRequest {
            term: self
                .node_common
                .persistent_state
                .lock()
                .await
                .current_term(),
            leader_id: self.node_common.node_index,
            prev_log_index: 0, // FIXME
            prev_log_term: 0,  // FIXME
            leader_commit: self.common_volatile_state.commit_index,
        };

        peer.pending_heartbeat = true;
        let mut rpc_client = peer.rpc_client.clone();
        let append_entries_request = append_entries_request.clone();
        self.jobs.spawn(async move {
            let request = tonic::Request::new(append_entries_request);
            match rpc_client.append_entries(request).await {
                Err(e) => {
                    trace!("Append entries request to peer {} failed: {}", peer_id, e);
                    TaskResult::HeartbeatFail(peer_id)
                }
                Ok(response) => {
                    let response = response.into_inner();
                    TaskResult::HeartbeatSuccess(peer_id, response)
                }
            }
        });
    }

    async fn retry_heartbeat(&mut self, peer_id: usize) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.pending_heartbeat = false;
            self.send_heartbeat(peer_id).await;
        } else {
            warn!("retry_heartbeat: Got wrong peer_id {}", peer_id);
        }
    }

    async fn send_heartbeats(&mut self) {
        trace!("Sending heartbeat to all peers");

        // FIXME: Don't make this vec. Need to fix borrow checker issues
        let peer_ids: Vec<usize> = self.peers.iter().map(|(id, _)| *id).collect();
        for peer_id in peer_ids.into_iter() {
            self.send_heartbeat(peer_id).await;
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
            pending_heartbeat: false,
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
