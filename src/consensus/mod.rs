use std::pin::Pin;

use node::{state, PeerNode};
use rand::distributions::Uniform;
use rand::rngs::SmallRng;
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncWrite},
    time::{Interval, Sleep},
};
use tonic::transport::Uri;
use tracing::info;

mod pb {
    tonic::include_proto!("raft");
}

mod node;
mod service;

pub trait StateWriter: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + 'static {}
impl<T> StateWriter for T where T: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + 'static {}

/// Raft Node with members used for establishing consensus
pub struct Node<Writer: StateWriter> {
    persistent_state: state::Persistent<Writer>,
    election_timer: Pin<Box<Sleep>>,
    heartbeat_interval: Pin<Box<Interval>>,

    timer_distribution: Uniform<f32>,
    rng: SmallRng,
    peers: Vec<PeerNode>,
    listen_addr: Uri,
}

pub fn hello() {
    info!("Hello hello");
}
