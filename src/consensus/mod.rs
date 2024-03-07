use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use atomic_enum::atomic_enum;
use rand::distributions::Uniform;
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncWrite},
    sync::watch,
    time::Duration,
};
use tonic::transport::{Channel, Uri};

mod pb {
    tonic::include_proto!("raft");
}

mod atomic_util;
mod macro_util;
mod node;
mod service;
mod state;
pub use node::Config;
use pb::raft_client::RaftClient;

use atomic_util::{AtomicDuration, AtomicInstant};

pub trait StateFile: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + 'static + Unpin {}
impl<T> StateFile for T where T: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + 'static + Unpin {}

#[atomic_enum] // TODO: Look if there's anything better available or make something
#[derive(PartialEq)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
enum TaskResult {
    // TODO: Rename
    VoteResponse {
        response: pb::VoteResponse,
        candidate_term: u32,
    },
    /// node index to which request failed, to retry
    VoteFail { peer_id: usize, candidate_term: u32 },
    /// Task that was syncing logs to followers exited
    LeaderExit,
    /// New leader got elected, so stopping leader process
    NewLeaderElected,
}

/// Raft Node with members used for establishing consensus
pub struct Node<SFile: StateFile> {
    persistent_state: tokio::sync::Mutex<state::Persistent<SFile>>,
    node_index: u32,
    listen_addr: SocketAddr,
    common_volatile_state: state::VolatileCommon,

    election_timeout: AtomicDuration,
    election_timer_distribution: Uniform<f32>,
    heartbeat_interval: Duration,
    /// This is used to reset the election timer, and is updated by server on receiving append entries RPCs
    role_informer: watch::Sender<NodeRole>,
    last_leader_message_time: AtomicInstant,

    /// map from peer_id to PeerNode
    peers: HashMap<usize, PeerNode>,
}

pub struct NodeServer<SFile: StateFile> {
    node: Arc<Node<SFile>>,
    _entries_informer: watch::Sender<u64>,
}

/// Represents information about a peer node that a particular node has and owns
/// grpc client to the particular peer
pub struct PeerNode {
    _address: Uri,
    rpc_client: RaftClient<Channel>,
    node_index: usize,
}
