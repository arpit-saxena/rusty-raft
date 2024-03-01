use std::{collections::HashMap, fmt::Debug, io::SeekFrom};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::trace;

use super::{macro_util::GenericByteIO, PeerNode};

struct LogEntry {
    index: FileData<u64>,
    term: FileData<u32>,
    length: FileData<u64>,
}

/// This State is updated on stable storage before responding to RPCs
pub struct Persistent<StateFile: super::StateFile> {
    /// latest term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: FileData<u32>,
    /// candidateId that received vote in current term (or -1 if not voted)
    voted_for: FileData<i32>, // TODO: Maybe store reference to PeerNode?
    /// logbook, vector of LogEntry; all logs might not be applied
    log: Vec<LogEntry>,
    /// file like object that will interact with storage to read/write persistent state
    state_file: StateFile,
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
    pub follower_states: HashMap<usize, VolatileFollowerState>,
}

/// Volatile state that is stored only on candidates
pub struct VolatileCandidate {
    /// number of votes received
    pub votes_received: usize,
}

const STATE_MAGIC_BYTES: u64 = 0x6d3d5b9932220a79;
const STATE_FILE_VERSION: u32 = 1;

impl LogEntry {
    async fn from_write<StateFile>(
        index: u64,
        term: u32,
        log: &[u8],
        state_file: &mut StateFile,
    ) -> Result<LogEntry, Box<dyn std::error::Error>>
    where
        StateFile: super::StateFile,
    {
        let index = FileData::from_state_file_write(index, state_file).await?;
        let term = FileData::from_state_file_write(term, state_file).await?;
        let length = FileData::from_state_file_write(log.len() as u64, state_file).await?;
        state_file.write_all(log).await?;
        state_file.flush().await?; // Make sure the log is flushed to the disk
        Ok(LogEntry {
            index,
            term,
            length,
        })
    }

    async fn from_read<StateFile>(
        state_file: &mut StateFile,
    ) -> Result<LogEntry, Box<dyn std::error::Error>>
    where
        StateFile: super::StateFile,
    {
        let index = FileData::from_state_file_read(state_file).await?;
        let term = FileData::from_state_file_read(state_file).await?;
        let length = FileData::from_state_file_read(state_file).await?;
        state_file
            .seek(SeekFrom::Current(length.data as i64))
            .await?;
        Ok(LogEntry {
            index,
            term,
            length,
        })
    }
}

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
        let log;
        if !magic_bytes_present {
            state_file.write_u64_le(STATE_MAGIC_BYTES).await?;
            state_file.write_u32_le(STATE_FILE_VERSION).await?;

            current_term = FileData::from_state_file_write(0, &mut state_file).await?;
            state_file.write_u32_le(current_term.data).await?;
            voted_for = FileData::from_state_file_write(-1, &mut state_file).await?;
            state_file.write_i32_le(voted_for.data).await?;
            log = vec![];
            operation_performed = "write";
            // TODO: Write an end marker
        } else {
            let version = state_file.read_u32_le().await?;
            if version > STATE_FILE_VERSION {
                return Err(format!("Can't read file with version {version}, supported version is till {STATE_FILE_VERSION}").into());
            }
            current_term = FileData::from_state_file_read(&mut state_file).await?;
            voted_for = FileData::from_state_file_read(&mut state_file).await?;
            log = Self::read_log(&mut state_file).await?;
            operation_performed = "read";
        }

        trace!(
            "Performed {} operation version = {}, current_term = {:?}, voted_for = {:?}, logs length = {}, to state file",
            operation_performed,
            STATE_FILE_VERSION,
            current_term,
            voted_for,
            log.len(),
        );

        Ok(Persistent {
            current_term,
            voted_for,
            log,
            state_file,
        })
    }

    async fn read_log(state_file: &mut StateFile) -> Result<Vec<LogEntry>, Box<dyn std::error::Error>> {
        let mut log = Vec::new();
        // FIXME: Assuming if error is returned, we have reached EOF, however there can be other errors as well
        while let Ok(log_entry) = LogEntry::from_read(state_file).await {
            trace!("Read log entry: index = {}, term = {}, length = {}", log_entry.index.data, log_entry.term.data, log_entry.length.data);
            log.push(log_entry);
        }
        Ok(log)
    }

    pub fn current_term(&self) -> u32 {
        self.current_term.data
    }
    pub async fn update_current_term(
        &mut self,
        new_term: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Only update if new term is different from old term.
        if self.current_term.data == new_term {
            return Ok(());
        }

        self.current_term
            .write(new_term, &mut self.state_file)
            .await?;
        // New term, new node to vote for
        self.voted_for.write(-1, &mut self.state_file).await?;
        Ok(())
    }
    pub async fn increment_current_term_and_vote(
        &mut self,
        node_id: i32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.current_term
            .write(self.current_term.data + 1, &mut self.state_file)
            .await?;
        self.voted_for.write(node_id, &mut self.state_file).await?;
        trace!(
            "Incremented current term to {} and voted for {}",
            self.current_term.data,
            node_id
        );
        Ok(())
    }
    pub async fn grant_vote_if_possible(
        &mut self,
        candidate_id: u32,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let candidate_id = candidate_id as i32;
        if self.voted_for.data == -1 {
            self.voted_for
                .write(candidate_id, &mut self.state_file)
                .await?;
            Ok(true)
        } else {
            Ok(self.voted_for.data == candidate_id)
        }
    }
    pub async fn add_log(&mut self, message: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let log_index = self.log.last().map(|record| record.index.data).unwrap_or(0) + 1;
        let term = self.current_term();
        let log_entry =
            LogEntry::from_write(log_index, term, message, &mut self.state_file).await?;
        self.log.push(log_entry);
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

impl VolatileLeader {
    pub fn new(peers: &HashMap<usize, PeerNode>) -> VolatileLeader {
        let mut follower_states = HashMap::new();
        for peer_id in peers.keys() {
            follower_states.insert(*peer_id, VolatileFollowerState::new());
        }
        VolatileLeader { follower_states }
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
    T: Debug + Copy,
{
    position: SeekFrom,
    data: T,
}

impl<T> FileData<T>
where
    T: Copy + Debug,
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
            data,
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
            data,
        })
    }

    async fn write<StateFile>(
        &mut self,
        data: T,
        state_file: &mut StateFile,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        StateFile: super::StateFile + GenericByteIO<T>,
    {
        let current_position = state_file.stream_position().await?;
        state_file.seek(self.position).await?;
        state_file.write_little_endian(data).await?;
        state_file.flush().await?; // Be really sure this value is written to disk
        self.data = data;
        state_file.seek(SeekFrom::Start(current_position)).await?;
        Ok(())
    }
}
