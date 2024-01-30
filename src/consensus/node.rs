use std::fs::File;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::pin::Pin;

use rand::distributions::{Distribution, Uniform};
use rand::rngs::ThreadRng;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, Instant, Sleep};
use tracing::trace;

type Message = Vec<u8>;

pub trait Writer {
    fn write(msg: Message);
}
pub trait StateMachine {
    fn transition(msg: Message);
}

mod state {
    use super::{Message, PeerNode};

    /// This State is updated on stable storage before responding to RPCs
    pub struct Persistent {
        /// latest term server has seen (initialized to 0 on first boot, increases monotonically)
        current_term: u32,
        /// candidateId that received vote in current term (or None if not voted)
        voted_for: Option<Box<PeerNode>>,
        /// logbook, vector of (message, term); all logs might not be applied
        log: Vec<(Message, u32)>,
    }

    impl Persistent {
        pub fn new() -> Self {
            Persistent {
                current_term: 0,
                voted_for: None,
                log: vec![],
            }
        }
    }
}

/// Raft Node with members used for establishing consensus
pub struct Node {
    persistent_state: state::Persistent,
    election_timer: Pin<Box<Sleep>>,

    timer_distribution: Uniform<f32>,
    rng: ThreadRng,
    config: Config,
}

/// Configuration for Raft Consensus, can be read from any file, currently only RON is supported
#[derive(Debug, Serialize, Deserialize)]
struct Config {
    persistent_state_file: String,
    cluster_members: Vec<SocketAddr>,
    election_timeout_interval: RangeInclusive<f32>,
}

impl Config {
    fn from_file(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
        let config_file = File::open(path)?;
        let config: Config = ron::de::from_reader(config_file)?;
        trace!("Parsed config from file {path}: \n{:#?}", config);
        Ok(config)
    }
}

impl Node {
    pub fn new(config_path: &str) -> Result<Node, Box<dyn std::error::Error>> {
        let config = Config::from_file(config_path)?;

        let distribution = Uniform::from(config.election_timeout_interval.clone());
        let rng = thread_rng();
        let mut node = Node {
            persistent_state: state::Persistent::new(),
            election_timer: Box::pin(tokio::time::sleep(Duration::from_secs(0))),
            timer_distribution: distribution,
            rng,
            config,
        };
        node.reset_election_timer();
        Ok(node)
    }

    fn reset_election_timer(self: &mut Self) {
        let timeout = Duration::from_micros(
            (self.timer_distribution.sample(&mut self.rng) * 1000. as f32).floor() as u64,
        );
        trace!("New election timeout is {} millis", timeout.as_millis());
        self.election_timer.as_mut().reset(Instant::now() + timeout);
    }

    /// Process the next event
    // TODO: Should this be async?
    pub async fn tick(self: &mut Self) {
        // Tick the election timer
        let timer = self.election_timer.as_mut();
        tokio::select! {
            () = timer => {
                trace!("Hey the election timer just expired do something about it");
                self.reset_election_timer();
            }
        };
    }
}

/// Represents information about a peer node that a particular node has
struct PeerNode {}
