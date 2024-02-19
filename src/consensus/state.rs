use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::trace;

use super::PeerNode;

/// This State is updated on stable storage before responding to RPCs
pub struct Persistent<StateFile: super::StateFile> {
    /// latest term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: u32,
    /// candidateId that received vote in current term (or None if not voted)
    voted_for: Option<Box<PeerNode>>,
    /// logbook, vector of (message, term); all logs might not be applied
    // log: Vec<(Message, u32)>,
    /// file like object that will interact with storage to read/write persistent state
    state_file: StateFile,
}

const STATE_MAGIC_BYTES: u64 = 0x6d3d5b9932220a79;
const STATE_FILE_VERSION: u32 = 1;

impl<StateFile: super::StateFile> Persistent<StateFile> {
    pub async fn new(mut state_file: StateFile) -> Result<Self, Box<dyn std::error::Error>> {
        let magic_bytes_present = match state_file.read_u64_le().await {
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    // TODO: File might contain data but less than 8 bytes. What to do with that?
                    trace!("Reached EOF when reading magic bytes from state file, will populate the bytes");
                    false
                } else {
                    return Err(Box::new(e));
                }
            }
            Ok(file_magic_bytes) => {
                if file_magic_bytes == STATE_MAGIC_BYTES {
                    trace!("Found matching magic bytes in state file");
                    true
                } else {
                    return Err(format!("Found bytes {:X?} instead of expected magic bytes {:X?}, check provided state file", file_magic_bytes, STATE_MAGIC_BYTES).into());
                }
            }
        };

        let current_term;
        let voted_for;
        if !magic_bytes_present {
            state_file.write_u64_le(STATE_MAGIC_BYTES).await?;
            state_file.write_u32_le(STATE_FILE_VERSION).await?;
            current_term = 0;
            voted_for = -1;
            state_file.write_u32_le(current_term).await?;
            state_file.write_i32_le(voted_for).await?;
            trace!(
                "Written current_term = 0, voted_for = -1, version = {} to state file",
                STATE_FILE_VERSION
            );
        } else {
            let version = state_file.read_u32_le().await?;
            if version > STATE_FILE_VERSION {
                return Err(format!("Can't read file with version {version}, supported version is till {STATE_FILE_VERSION}").into());
            }
            current_term = state_file.read_u32_le().await?;
            voted_for = state_file.read_i32_le().await?;
            trace!(
                "Read version = {}, current_term = {}, voted_for = {} from state file",
                version,
                current_term,
                voted_for
            );
        }

        // TODO: Convert voted_for to PeerNode reference

        Ok(Persistent {
            current_term,
            voted_for: None,
            // log: vec![],
            state_file,
        })
    }
}
