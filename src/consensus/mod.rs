use tracing::info;

mod pb {
    tonic::include_proto!("raft");
}

mod node;
mod service;

pub use node::Node;

pub fn hello() {
    info!("Hello hello");
}
