use std::collections::HashMap;
use std::fs::File;
use std::net::SocketAddr;
use std::ops::{DerefMut, RangeInclusive};
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

        let persistent_state_path = std::mem::take(&mut config.persistent_state_file);
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

        let mut node = NodeClient {
            node_common: Arc::clone(&node_common),
            common_volatile_state: state::VolatileCommon::new(),
            leader_volatile_state: None,
            role: NodeRole::Follower,
            election_timeout,
            election_timer_distribution: distribution,
            heartbeat_interval,
            timer,
            rng,
            peers,
            jobs: JoinSet::new(),
        };
        node.set_new_election_timeout();
        node.restart_election_timer();

        let node_server = NodeServer { node_common };
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

    fn restart_election_timer(&mut self) {
        trace!("Restarting election timer");
        self.timer
            .as_mut()
            .reset(Instant::now() + self.election_timeout);
    }

    /// Process the next event
    // TODO: Should this be async?
    pub async fn tick(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let timer = self.timer.as_mut();
        tokio::select! {
            () = timer => {
                // trace!("Timer expired, current role is {:?}", self.role);
                match &self.role {
                    NodeRole::Follower => {
                        // Election timer expired, convert to candidate and request votes
                        // Drop previous RPCs, might be due to a split vote
                        self.jobs = JoinSet::new();
                        let candidate_state = state::VolatileCandidate{votes_received: 0};
                        self.role = NodeRole::Candidate(candidate_state);
                        self.request_votes().await;
                    }
                    NodeRole::Candidate(_) => {}
                    NodeRole::Leader(_) => {}
                }
                self.restart_election_timer();
                // do something about the timer
            }
            Some(res) = self.jobs.join_next(), if self.jobs.len() > 0 => {
                match res {
                    Err(join_error) => {
                        if join_error.is_panic() {
                            info!("Some task panicked..");
                        }
                    }
                    Ok(TaskResult::VoteResponse(vote_response)) => {
                        if vote_response.vote_granted {
                            trace!("Received vote");
                            if let NodeRole::Candidate(candidate_state) = &mut self.role {
                                candidate_state.votes_received += 1;
                                if candidate_state.votes_received > (1 + self.peers.len()) / 2 {
                                    info!("Elected as leader woohoo");
                                }
                            } else {
                                warn!("Received vote response from peer, however current role is not candidate");
                            }
                        } else {
                            let mut persistent_state = self.node_common.persistent_state.lock().await;
                            if vote_response.term > persistent_state.current_term() {
                                // Our term was stale, update it and abort all RPCs
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
                }
            }
        };

        // let election_timer = self.election_timer.as_mut();
        // tokio::select! {
        //     () = election_timer => {
        //         trace!("Hey the election timer just expired do something about it");
        //         self.set_new_election_timeout();
        //         self.restart_election_timer();

        //         // Need to initiate sending votes but also let election timer tick...
        //         // Observation: Election timer and heartbeat timer will never tick simultaneously
        //         // heartbeat interval ticks when leader, election timer ticks when a follower or candidate

        //         /* Keep all spawned stuffs inside a JoinSet. Note they need to have the same return type, can use an enum for that
        //          * JoinSet will await on
        //          * 1. election timer and
        //          *      a. request vote RPCs (when candidate). If election timer expires before election converges, abort all the RPCs. Otherwise fine.
        //          *      b. Nothing (when follower)
        //          * 2. heartbeat timer (when leader) and append request RPCs.
        //          *      - When heartbeat timer ends, schedule more heartbeat append request RPCs aborting previous heartbeat RPCs if any
        //          *      - When append entries RPC ends, all cool
        //          */
        //         // TODO: call function which will increment current term and call RequestVote RPC to all peers
        //         // On receiving response update some info about number of votes received and when majority
        //         // votes are received, election is done, send heartbeats.
        //     }
        //     _ = self.heartbeat_interval.tick() => {
        //         // TODO: Don't tick heartbeat if not leader
        //         trace!("Heartbeat interval expired send heartbeat");
        //         if self.leader_volatile_state.is_some() {
        //             self.send_heartbeat().await;
        //         }
        //         self.restart_election_timer();
        //     }
        // };

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

    async fn request_votes(&mut self) {
        trace!("Requesting votes from all peers");

        // FIXME: Don't make this vec. Need to fix borrow checker issues
        let peer_ids: Vec<usize> = self.peers.iter().map(|(id, _)| *id).collect();
        for peer_id in peer_ids.into_iter() {
            self.request_vote(peer_id).await;
        }
    }

    async fn send_heartbeat(&mut self) {
        trace!("Sending heartbeat to all peers");

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

        // let mut heartbeat_futures = Vec::new();
        // for (_, peer) in &mut self.peers {
        //     heartbeat_futures.push(peer.rpc_client.append_entries(tonic::Request::new(append_entries_request.clone())));
        // }

        // for (idx, result) in join_all(heartbeat_futures).await.into_iter().enumerate() {
        //     // TODO: Convert to match and check response. If response's term is greater than our term, revert to follower
        //     if let Err(e) = result {
        //         // TODO: Have to retry here. All failed RPC's have to retried indefinitely (Paper section 5.5)
        //         let peer = &self.peers[idx];
        //         debug!("Unable to send heartbeat to peer {}, address {}: {}", peer.node_index, peer.address, e);
        //     }
        // }
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
