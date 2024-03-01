use std::{collections::HashMap, pin::Pin, sync::Arc};

use rand::distributions::Uniform;
use rand::rngs::SmallRng;
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncWrite},
    task::JoinSet,
    time::{Duration, Instant, Sleep},
};
use tonic::transport::{Channel, Uri};

mod pb {
    tonic::include_proto!("raft");
}

mod macro_util;
mod node;
mod service;
mod state;
pub use node::Config;
use pb::raft_client::RaftClient;

pub trait StateFile: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + 'static + Unpin {}
impl<T> StateFile for T where T: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + 'static + Unpin {}

enum NodeRole {
    Follower,
    Candidate(state::VolatileCandidate),
    Leader(state::VolatileLeader),
}

enum TaskResult {
    // TODO: Rename
    VoteResponse(pb::VoteResponse),
    /// node index to which request failed, to retry
    VoteFail(usize),
    HeartbeatSuccess(usize, pb::AppendEntriesResponse),
    /// node index to which request failed, to retry
    HeartbeatFail(usize),
}

pub struct NodeCommon<SFile: StateFile> {
    persistent_state: tokio::sync::Mutex<state::Persistent<SFile>>,
    node_index: u32,
    role: tokio::sync::Mutex<NodeRole>,
}

/// Raft Node with members used for establishing consensus
pub struct NodeClient<SFile: StateFile> {
    node_common: Arc<NodeCommon<SFile>>,
    common_volatile_state: state::VolatileCommon,

    election_timeout: Duration,
    election_timer_distribution: Uniform<f32>,
    heartbeat_interval: Duration,
    /// This timer is used as heartbeat timer when Leader, election timeout otherwise
    timer: Pin<Box<Sleep>>,
    /// This is used to reset the election timer, and is updated by server on receiving append entries RPCs
    last_leader_message_time: Arc<std::sync::Mutex<Instant>>,

    rng: SmallRng,
    /// map from peer_id to PeerNode
    peers: HashMap<usize, PeerNode>,
    jobs: JoinSet<TaskResult>,
}

pub struct NodeServer<SFile: StateFile> {
    node_common: Arc<NodeCommon<SFile>>,
    last_leader_message_time: Arc<std::sync::Mutex<Instant>>,
}

/// Represents information about a peer node that a particular node has and owns
/// grpc client to the particular peer
pub struct PeerNode {
    _address: Uri,
    rpc_client: RaftClient<Channel>,
    node_index: usize,
    /// Only used by leaders, true if a heartbeat is pending that will be retried
    /// This will ensure we don't queue up more heartbeats than necessary
    pending_heartbeat: bool,
}
