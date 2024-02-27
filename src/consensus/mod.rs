use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
};

use rand::distributions::Uniform;
use rand::rngs::SmallRng;
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncWrite},
    task::JoinSet,
    time::{Duration, Interval, Sleep},
};
use tonic::transport::{Channel, Uri};
use tracing::info;

mod pb {
    tonic::include_proto!("raft");
}

mod macro_util;
mod node;
mod service;
mod state;
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
}

pub struct NodeCommon<SFile: StateFile> {
    persistent_state: state::Persistent<SFile>,
    node_index: u32,
}

/// Raft Node with members used for establishing consensus
pub struct NodeClient<SFile: StateFile> {
    node_common: Arc<NodeCommon<SFile>>,
    common_volatile_state: state::VolatileCommon,
    leader_volatile_state: Option<state::VolatileLeader>,
    role: NodeRole,

    election_timeout: Duration,
    election_timer_distribution: Uniform<f32>,
    heartbeat_interval: Duration,
    /// This timer is used as heartbeat timer when Leader, election timeout otherwise
    timer: Pin<Box<Sleep>>,

    rng: SmallRng,
    /// map from peer_id to PeerNode
    peers: HashMap<usize, PeerNode>,
    jobs: JoinSet<TaskResult>,
}

pub struct NodeServer<SFile: StateFile> {
    node_common: Arc<NodeCommon<SFile>>,
}

/// Represents information about a peer node that a particular node has and owns
/// grpc client to the particular peer
pub struct PeerNode {
    address: Uri,
    rpc_client: RaftClient<Channel>,
    node_index: usize,
}

pub fn hello() {
    info!("Hello hello");
}
