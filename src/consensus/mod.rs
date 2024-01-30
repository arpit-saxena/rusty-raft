use tracing::info;

mod pb {
    tonic::include_proto!("raft");
}

mod node;

pub use node::Node;

pub fn hello() {
    info!("Hello hello");
}
