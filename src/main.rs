use raft::log_stuffs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    log_stuffs();

    Ok(())
}
