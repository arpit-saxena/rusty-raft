use tracing::{debug, error, info, trace, warn};

pub mod consensus;

pub fn log_stuffs() {
    consensus::hello();
    trace!("This is a traced message");
    debug!("This is a debug message");
    info!("This is an info message");
    warn!("This is a warn message");
    error!("This is an error message");
}
