use tracing::info;

mod pb {
    tonic::include_proto!("raft");
}

pub fn hello() {
    info!("Hello hello");
}