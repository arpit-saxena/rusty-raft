use std::{fmt::Debug, io::SeekFrom, sync::atomic::Ordering};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::trace;

use super::macro_util::{Atomic, Atomize, GenericByteIO};

/// This State is updated on stable storage before responding to RPCs
pub struct Persistent<StateFile: super::StateFile> {
    /// latest term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: FileData<u32>,
    /// candidateId that received vote in current term (or -1 if not voted)
    voted_for: FileData<i32>, // TODO: Maybe store reference to PeerNode?
    /// logbook, vector of (message, term); all logs might not be applied
    // log: Vec<(Message, u32)>,
    /// file like object that will interact with storage to read/write persistent state
    state_file: tokio::sync::Mutex<StateFile>,
}

/// Volatile state that is stored on all servers
pub struct VolatileCommon {
    /// index of highest log entry known to be committed (initialized to 0, increases monotonically)
    pub commit_index: u64,
    /// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    pub last_applied: u64,
}

/// Volatile per-follower state that is stored only for a leader
pub struct VolatileFollowerState {
    /// index of the next log entry to send to that server (initialized to leader last log index + 1)
    pub next_index: u64,
    /// index of highest log entry known to be replicated on the server (initialized to 0, increases monotonically)
    pub match_index: u64,
}

/// Volatile state that is stored only on leaders
pub struct VolatileLeader {
    pub follower_states: Vec<VolatileFollowerState>,
}

/// Volatile state that is stored only on candidates
pub struct VolatileCandidate {
    /// number of votes received
    pub votes_received: usize,
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
        let operation_performed;
        if !magic_bytes_present {
            state_file.write_u64_le(STATE_MAGIC_BYTES).await?;
            state_file.write_u32_le(STATE_FILE_VERSION).await?;

            current_term = FileData::from_state_file_write(0, &mut state_file).await?;
            state_file.write_u32_le(current_term.get_data()).await?;
            voted_for = FileData::from_state_file_write(-1, &mut state_file).await?;
            state_file.write_i32_le(voted_for.get_data()).await?;
            operation_performed = "write";
        } else {
            let version = state_file.read_u32_le().await?;
            if version > STATE_FILE_VERSION {
                return Err(format!("Can't read file with version {version}, supported version is till {STATE_FILE_VERSION}").into());
            }
            current_term = FileData::from_state_file_read(&mut state_file).await?;
            voted_for = FileData::from_state_file_read(&mut state_file).await?;
            operation_performed = "read";
        }

        trace!(
            "Performed {} operation version = {}, current_term = {:?}, voted_for = {:?} to state file",
            operation_performed,
            STATE_FILE_VERSION,
            current_term,
            voted_for,
        );

        Ok(Persistent {
            current_term,
            voted_for,
            // log: vec![],
            state_file: tokio::sync::Mutex::new(state_file),
        })
    }
    pub fn current_term(&self) -> u32 {
        self.current_term.get_data()
    }
    pub async fn update_current_term(
        &self,
        new_term: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.current_term.write(new_term, &self.state_file).await?;
        Ok(())
    }
}

impl VolatileFollowerState {
    pub fn new() -> VolatileFollowerState {
        VolatileFollowerState {
            next_index: 1, // TODO: Initialize this properly
            match_index: 0,
        }
    }
}

impl VolatileCommon {
    pub fn new() -> VolatileCommon {
        VolatileCommon {
            commit_index: 0,
            last_applied: 0,
        }
    }
}

#[derive(Debug)]
struct FileData<T>
where
    T: Debug + Copy + Atomize,
{
    position: SeekFrom,
    data: Atomic<T>,
}

impl<T> FileData<T>
where
    T: Copy + Debug + Atomize,
{
    /// Construct FileData from given data by writing to the file at current offset
    async fn from_state_file_write<StateFile>(
        data: T,
        state_file: &mut StateFile,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        StateFile: super::StateFile + GenericByteIO<T>,
    {
        let position_from_start = state_file.stream_position().await?;
        state_file.write_little_endian(data).await?;
        Ok(Self {
            position: SeekFrom::Start(position_from_start),
            data: Atomic::from(data),
        })
    }

    /// Construct FileData by reading data from the file at current offset
    async fn from_state_file_read<StateFile>(
        state_file: &mut StateFile,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        StateFile: super::StateFile + GenericByteIO<T>,
    {
        let position_from_start = state_file.stream_position().await?;
        let data = state_file.read_little_endian().await?;
        Ok(Self {
            position: SeekFrom::Start(position_from_start),
            data: Atomic::from(data),
        })
    }

    fn get_data(&self) -> T {
        self.data.load(Ordering::SeqCst)
    }

    async fn write<StateFile>(
        &self,
        data: T,
        state_file_mutex: &tokio::sync::Mutex<StateFile>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        StateFile: super::StateFile + GenericByteIO<T>,
    {
        let mut state_file = state_file_mutex.lock().await;
        let current_position = state_file.stream_position().await?;
        state_file.seek(self.position).await?;
        state_file.write_little_endian(data).await?;
        state_file.flush().await?; // Be really sure this value is written to disk
        self.data.store(data, Ordering::SeqCst);
        state_file.seek(SeekFrom::Start(current_position)).await?;
        Ok(())
    }
}
