use std::cell::RefCell;
use std::collections::HashMap;

use std::net::SocketAddr;
use std::ops::{Deref, RangeInclusive};
use std::path::{Path, PathBuf};

use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::consensus::state::AppendLogEntry;

use super::atomic_util::{AtomicDuration, AtomicInstant};
use super::pb::raft_client::RaftClient;
use super::pb::raft_server::RaftServer;
use super::state::VolatileCandidate;
use super::{pb, PeerNode, TaskResult};
use super::{state, AtomicNodeRole, Node, NodeRole, NodeServer};
use anyhow::{anyhow, bail, Context, Result};
use futures::future::join_all;
use rand::distributions::{Distribution, Uniform};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::fs::File as TokioFile;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, Instant, Sleep};
use tonic::transport::{Endpoint, Server, Uri};
use tracing::{debug, error, info, trace, warn};
use void::Void as Never;

// type Message = Vec<u8>;

// pub trait StateMachine {
//     fn transition(msg: Message);
// }

thread_local! {
    static RNG: RefCell<SmallRng> = RefCell::new(SmallRng::from_entropy());
}

/// Configuration for Raft Consensus, can be read from any file, currently only RON is supported
#[serde_with::serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub persistent_state_file: Box<Path>,
    pub cluster_members: Vec<String>, // vector of Uri's
    pub election_timeout_interval: RangeInclusive<f32>,
    #[serde_as(as = "serde_with::DurationMilliSeconds")]
    pub heartbeat_interval: Duration,
}

impl Config {
    pub fn from_reader<Reader: std::io::Read>(config_file: Reader) -> Result<Config> {
        let config: Config = ron::de::from_reader(config_file)?;
        trace!("Parsed config from reader: \n{:#?}", config);
        Ok(config)
    }
}

#[derive(Debug)]
struct NodeClientState {
    candidate_state: state::VolatileCandidate,
    jobs: JoinSet<TaskResult>,
}

struct AbortTaskOnDrop<T> {
    handle: JoinHandle<T>,
}

impl<T> Drop for AbortTaskOnDrop<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Node<TokioFile> {
    #[tracing::instrument(skip(config_file))]
    pub async fn from_config_reader<Reader: std::io::Read>(
        config_file: Reader,
        node_index: u32,
    ) -> Result<Node<TokioFile>> {
        let config = Config::from_reader(config_file)?;
        Node::from_config(config, node_index).await
    }

    #[tracing::instrument]
    pub async fn from_config(config: Config, node_index: u32) -> Result<Node<TokioFile>> {
        let mut persistent_state_path = PathBuf::from(config.persistent_state_file.clone());
        let file_name_suffix = persistent_state_path
            .file_name()
            .ok_or(anyhow!(
                "File name should be present in persistent_state_file"
            ))?
            .to_str()
            .ok_or(anyhow!(
                "persistent_state_file should have valid unicode characters"
            ))?;
        persistent_state_path.set_file_name(format!("node_{}_{}", node_index, file_name_suffix));

        let persistent_state_file_path = persistent_state_path.clone().into_os_string();
        let persistent_state_file = tokio::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(persistent_state_path)
            .await
            .with_context(|| {
                format!(
                    "Error in opening persistent state file at path {:?}",
                    persistent_state_file_path
                )
            })?;

        let distribution = Uniform::from(config.election_timeout_interval.clone());
        let heartbeat_interval = config.heartbeat_interval;
        let (peers, listen_addr) = Self::peers_from_config(config, node_index as usize).await?;
        let election_timeout = Duration::from_secs(0);
        let election_timeout = AtomicDuration::new(election_timeout).with_context(|| {
            format!(
                "Node id {}: Error in convert election_timeout to AtomicDuration",
                node_index
            )
        })?;

        let node = Node {
            persistent_state: tokio::sync::Mutex::new(
                state::Persistent::new(persistent_state_file)
                    .await
                    .with_context(|| {
                        format!(
                            "Unable to init persistent state from file path {:?}",
                            persistent_state_file_path
                        )
                    })?,
            ),
            node_index,
            listen_addr,
            role: AtomicNodeRole::new(NodeRole::Follower),
            common_volatile_state: state::VolatileCommon::new(),
            election_timeout,
            election_timer_distribution: distribution,
            heartbeat_interval,
            last_leader_message_time: AtomicInstant::new(Instant::now())?,
            peers,
        };
        node.set_new_election_timeout()?;

        Ok(node)
    }

    pub fn id(&self) -> u32 {
        self.node_index
    }

    #[tracing::instrument]
    async fn peers_from_config(
        config: Config,
        node_index: usize,
    ) -> Result<(HashMap<usize, PeerNode>, SocketAddr)> {
        let cluster_members = config.cluster_members;
        if node_index >= cluster_members.len() {
            bail!(
                "node_index {node_index} not valid for {} cluster members",
                cluster_members.len()
            );
        }

        let listen_addr: SocketAddr = cluster_members[node_index].parse()?;
        let peer_node_futures = cluster_members
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| *idx != node_index)
            .map(|(idx, uri_str)| -> Result<_> {
                let uri = Uri::builder()
                    .scheme("http")
                    .authority(uri_str.as_str())
                    .path_and_query("/")
                    .build()
                    .with_context(|| format!("Node id {node_index}: Error in building URI for peer {idx}, authority {uri_str}"))?;
                Ok(PeerNode::from_address(uri, idx, node_index))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let peers: Vec<PeerNode> = join_all(peer_node_futures)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let peers = peers.into_iter().map(|p| (p.node_index, p)).collect();

        Ok((peers, listen_addr))
    }

    #[tracing::instrument(skip_all, fields(id = self.node_index))]
    fn set_new_election_timeout(&self) -> Result<()> {
        let timeout = RNG.with_borrow_mut(|rng| {
            Duration::from_micros(
                (self.election_timer_distribution.sample(rng) * 1000_f32).floor() as u64,
            )
        });
        trace!("New election timeout is {} millis", timeout.as_millis());
        self.election_timeout
            .store(timeout, Ordering::SeqCst)
            .with_context(|| {
                format!(
                    "Node id {}: Error in storing new election timeout",
                    self.node_index
                )
            })?;
        Ok(())
    }

    async fn update_election_timer(&self, timer: &mut Pin<Box<Sleep>>) {
        match self.role.load(Ordering::SeqCst) {
            NodeRole::Leader => {}
            _ => {
                let election_timeout = self.election_timeout.load(Ordering::SeqCst);
                timer
                    .as_mut()
                    .reset(self.last_leader_message_time.load(Ordering::SeqCst) + election_timeout);
            }
        }
    }

    async fn reset_and_update_election_timer(&self, timer: &mut Pin<Box<Sleep>>) {
        self.last_leader_message_time
            .store(Instant::now(), Ordering::SeqCst)
            .context(
                "Error in updating last leader message time to Instant::now, some bug in Instant!",
            )
            .unwrap();
        self.update_election_timer(timer).await;
    }

    /// Start ticking and keep doing that forever.
    /// Note that this function only returns in case of an error in which case Node is
    /// dropped. So we always start out this function as a fresh follower node
    #[tracing::instrument(skip(self), fields(id = self.node_index))]
    pub async fn tick_forever(self) -> Result<Never> {
        let node = Arc::new(self);
        let (sender, receiver) = watch::channel(0_u64);
        let node_server = NodeServer {
            node: Arc::clone(&node),
            _entries_informer: sender,
        };
        let join_handle = tokio::spawn(
            Server::builder()
                .add_service(RaftServer::new(node_server))
                .serve(node.listen_addr),
        );
        // Ensure that the server is also stopped once this function exits.
        let _server_task_handle = AbortTaskOnDrop {
            handle: join_handle,
        };

        node.tick_client_forever(receiver).await
    }

    /// Starting syncing entries to given follower. New entries are indicated by receiving on entries_receiver. Task exists when entries_receiver is closed
    /// and all other log entries have been synced
    async fn sync_log_to_follower(
        &self,
        peer_id: usize,
        mut follower_state: state::VolatileFollowerState,
        mut last_log_index_watch: tokio::sync::watch::Receiver<u64>,
    ) -> state::VolatileFollowerState {
        // Setting zero duration initially since we want to send heartbeats to assert leadership initially
        let mut heartbeat_timer = Box::pin(tokio::time::sleep(Duration::ZERO));
        let _ = last_log_index_watch.borrow_and_update(); // Current value doesn't matter since we'll use that when doing heartbeats
                                                          // TODO: Update function to mark_unchanged after updating tokio version

        // Will access this and store for use in the future since it won't change in duration of this function
        // Is there a way to ensure this by types??
        let term = self.persistent_state.lock().await.current_term();

        loop {
            let unboxed_heartbeat_timer = heartbeat_timer.as_mut();
            tokio::select! {
                () = unboxed_heartbeat_timer => {}
                res = last_log_index_watch.changed() => {
                    if matches!(res, Err(_)) {
                        // sender is dropped, exit the loop
                        break;
                    }
                }
            }

            // FIXME: Locking persistent_state effectively serializes all the tasks doing. Can we do better since we're only reading the log here?
            loop {
                let AppendLogEntry {
                    entries,
                    last_log_idx,
                    last_log_term,
                } = self
                    .persistent_state
                    .lock()
                    .await
                    .get_entries_from(follower_state.next_index)
                    .await
                    .unwrap(); // FIXME: Error handling
                let append_entries_request = pb::AppendEntriesRequest {
                    term,
                    leader_id: self.node_index,
                    prev_log_index: last_log_idx,
                    prev_log_term: last_log_term,
                    entries,
                    leader_commit: self.common_volatile_state.commit_index,
                };

                match follower_state
                    .rpc_client
                    .append_entries(tonic::Request::new(append_entries_request))
                    .await
                {
                    Err(e) => {
                        trace!(
                            "Some error in sending to peer_id {}, will retry: {}",
                            peer_id,
                            e
                        );
                    }
                    Ok(response) => {
                        let response = response.into_inner();
                        if response.success {
                            follower_state.next_index = last_log_idx + 1;
                            follower_state.match_index = last_log_idx;
                            break;
                        }

                        // Not successful, can be either due to log matching or stale term
                        let persistent_state = self.persistent_state.lock().await;
                        if response.term > persistent_state.current_term() {
                            warn!(
                                "Current term {} is stale, we should use term {}",
                                response.term,
                                persistent_state.current_term()
                            );
                            // TODO: Update term. Somehow need to inform the client process. Use a mpsc channel.
                        } else {
                            // Issue in log matching, decrement next index and try again
                            follower_state.next_index -= 1;
                        }
                    }
                }
            }

            heartbeat_timer
                .as_mut()
                .reset(Instant::now() + self.heartbeat_interval);
        }

        follower_state
    }

    /// This function is meant to be spawned in a task, and its only function is to send entries to followers
    /// This function will synchronize follower's logs to our log. Any new entry should be added to our log by
    /// the caller and then indicated by updating the last_log_index_watch
    #[tracing::instrument(skip_all, fields(id = self.node_index))]
    async fn send_entries_to_followers(
        self: Arc<Self>,
        last_log_index_watch: tokio::sync::watch::Receiver<u64>,
    ) {
        let mut heartbeat_timer = Box::pin(tokio::time::sleep(self.heartbeat_interval));
        let mut jobs = JoinSet::new(); // All jobs will be aborted when this is dropped
                                       // Don't care if the sender was closed. If it is shutting down, it will close our task as well soon enough
        heartbeat_timer
            .as_mut()
            .reset(Instant::now() + self.heartbeat_interval);
        let leader_state =
            state::VolatileLeader::new(&self.peers, self.persistent_state.lock().await.deref());
        // let persistent_state = self.persistent_state.lock().await;
        for (peer_id, follower_state) in leader_state.follower_states.into_iter() {
            let last_log_index_watch = last_log_index_watch.clone();
            let node = Arc::clone(&self);
            jobs.spawn(async move {
                (
                    peer_id,
                    node.sync_log_to_follower(peer_id, follower_state, last_log_index_watch)
                        .await,
                )
            });
        }

        if let Some(res) = jobs.join_next().await {
            match res {
                Err(_) => {
                    // TODO: Return error.
                    warn!("Join error");
                }
                Ok((peer_id, _)) => {
                    info!("Task to sync logs to peer {peer_id} completed, probably stopped being leader. Exiting now and killing all other tasks");
                    jobs.shutdown().await;
                }
            }
        }
    }

    #[tracing::instrument(skip_all, fields(id = self.node_index))]
    async fn tick_client_forever(
        self: Arc<Self>,
        entries_receiver: watch::Receiver<u64>,
    ) -> Result<Never> {
        // We start out as follower, and we never exit this function in usual case, so we can always
        // assume that using election timeout is fine here.
        let mut timer = Box::pin(tokio::time::sleep(
            self.election_timeout.load(Ordering::SeqCst),
        ));

        let mut state = NodeClientState {
            candidate_state: state::VolatileCandidate { votes_received: 0 },
            jobs: JoinSet::new(),
        };

        loop {
            let unboxed_timer = timer.as_mut();
            tokio::select! {
                () = unboxed_timer, if self.role.load(Ordering::SeqCst) != NodeRole::Leader => {
                    // Update election timer again. If it has elapsed then do election stuff
                    self.update_election_timer(&mut timer).await;
                    if timer.is_elapsed() {
                        match self.role.load(Ordering::SeqCst) {
                            NodeRole::Follower => {
                                self.reset_and_update_election_timer(&mut timer).await;
                                state.candidate_state = self.become_candidate(&mut state).await?;
                            }
                            NodeRole::Candidate => {
                                // in case of a split vote, the election timer may expire when we're still a candidate
                                self.reset_and_update_election_timer(&mut timer).await;
                                state.candidate_state = self.become_candidate(&mut state).await?;
                            }
                            NodeRole::Leader => {
                                // Shouldn't be possible
                            }
                        }
                    } else {
                        // debug!("This tick won't do anything since timer got updated after select");
                    }
                }
                Some(res) = state.jobs.join_next(), if !state.jobs.is_empty() => {
                    match res {
                        Err(join_error) => {
                            if join_error.is_panic() {
                                // FIXME
                                error!("Some task panicked..");
                            }
                        }
                        Ok(TaskResult::VoteResponse(vote_response)) => {
                            if vote_response.vote_granted {
                                trace!("Received vote");
                                if matches!(self.role.load(Ordering::SeqCst), NodeRole::Candidate) {
                                    state.candidate_state.votes_received += 1;
                                    if state.candidate_state.votes_received > (1 + self.peers.len()) / 2 {
                                        info!("Elected as leader woohoo");
                                        // TODO: Start task that sends append entries RPCs in loop to everyone
                                        // That task will have ownership of volatile Leader state and also
                                        // the heartbeat timer
                                        let node = Arc::clone(&self);
                                        let entries_receiver = entries_receiver.clone();
                                        state.jobs.spawn(async move {
                                            node.send_entries_to_followers(entries_receiver.clone()).await;
                                            TaskResult::LeaderExit
                                        });
                                        self.role.store(NodeRole::Leader, Ordering::SeqCst);
                                    }
                                } else {
                                    // This can happen when one response was delayed and we got majority votes before
                                    debug!("Received vote response from peer, however current role is not candidate");
                                }
                            } else {
                                let mut persistent_state = self.persistent_state.lock().await;
                                if vote_response.term > persistent_state.current_term() {
                                    // Our term was stale, update it and abort all
                                    trace!(current_term = persistent_state.current_term(), vote_response.term);
                                    persistent_state.update_current_term(vote_response.term).await?;
                                    state.jobs.shutdown().await;
                                }
                            }
                        }
                        Ok(TaskResult::VoteFail(peer_id)) => {
                            if matches!(self.role.load(Ordering::SeqCst), NodeRole::Candidate) {
                                self.request_vote(&mut state, peer_id).await;
                            }
                        }
                        Ok(TaskResult::LeaderExit) => {
                            // TODO: Handle this
                            info!("Leader task exited, will check and probably start election process again idk");
                        }
                    }
                }
            };
        }
    }

    #[tracing::instrument(skip(self), fields(id = self.node_index))]
    async fn request_vote(&self, state: &mut NodeClientState, peer_id: usize) {
        let peer = self
            .peers
            .get(&peer_id)
            .expect("Expected peer_id to be valid");
        let mut rpc_client = peer.rpc_client.clone();

        let request_votes_request = pb::VoteRequest {
            term: self.persistent_state.lock().await.current_term(),
            candidate_id: self.node_index,
            last_log_index: 0, // TODO
            last_log_term: 0,
        };
        let request = tonic::Request::new(request_votes_request.clone());

        state.jobs.spawn(async move {
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

    #[tracing::instrument(skip(self), fields(id = self.node_index))]
    async fn become_candidate(&self, state: &mut NodeClientState) -> Result<VolatileCandidate> {
        // convert to candidate and request votes.
        // Drop previous RPCs, might be due to a split vote
        trace!("Becoming candidate");
        state.jobs.shutdown().await;
        let candidate_state = state::VolatileCandidate { votes_received: 0 };
        self.role.store(NodeRole::Candidate, Ordering::SeqCst);

        self.persistent_state
            .lock()
            .await
            .increment_current_term_and_vote(self.node_index as i32)
            .await
            .with_context(|| {
                format!(
                    "Node id {}: Error in incrementing term and voting",
                    self.node_index
                )
            })?;

        // FIXME: Don't make this vec. Need to fix borrow checker issues
        let peer_ids: Vec<usize> = self.peers.keys().copied().collect();
        for peer_id in peer_ids.into_iter() {
            self.request_vote(state, peer_id).await;
        }
        Ok(candidate_state)
    }
}

impl PeerNode {
    #[tracing::instrument(fields(id = node_id))]
    async fn from_address(address: Uri, peer_id: usize, node_id: usize) -> Result<PeerNode> {
        let endpoint = Endpoint::new(address.clone()).with_context(|| {
            format!(
                "Node {}: Error creating peer node Endpoint to id {}, address {}",
                node_id, peer_id, address
            )
        })?;
        let channel = endpoint.connect_lazy();
        let rpc_client = RaftClient::new(channel);
        let peer_node: PeerNode = PeerNode {
            _address: address,
            rpc_client,
            node_index: peer_id,
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
