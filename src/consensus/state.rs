use std::{
    collections::HashMap,
    fmt::Debug,
    io::SeekFrom,
    mem::size_of,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::watch,
};
use tonic::transport::Channel;
use tracing::trace;

use super::{macro_util::GenericByteIO, pb::raft_client::RaftClient, PeerNode};

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
    pub commit_index_informer: watch::Sender<u64>,
    /// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    pub last_applied: u64,
}

/// Volatile per-follower state that is stored only for a leader
pub struct VolatileFollowerState {
    /// index of the next log entry to send to that server (initialized to leader last log index + 1)
    pub next_index: u64,
    /// index of highest log entry known to be replicated on the server (initialized to 0, increases monotonically)
    match_index: Arc<AtomicU64>,
    /// rpc client to send entries to follower
    pub rpc_client: RaftClient<Channel>,
    /// Watch receiver that is used to inform if any new log has been added that should be synced to the followers
    pub last_log_index_watch: watch::Receiver<u64>,
    /// watch sender that is used to inform that match_index has been updated
    match_index_notifier: Arc<watch::Sender<()>>,
}

/// Volatile state that is stored only on leaders
pub struct VolatileLeader {
    pub follower_states: HashMap<usize, VolatileFollowerState>,
}

/// Volatile state that is stored only on candidates
#[derive(Debug)]
pub struct VolatileCandidate {
    /// number of votes received
    pub votes_received: usize,
}

const STATE_MAGIC_BYTES: u64 = 0x6d3d5b9932220a79;
const STATE_FILE_VERSION: u32 = 1;

#[derive(Error, Debug)]
pub enum StateError {
    #[error("Given state file's initial bytes don't match magic bytes")]
    MagicBytesNotMatching,

    #[error(
        "State File version {file_version} is more than supported version {supported_version}"
    )]
    VersionNotSupported {
        file_version: u32,
        supported_version: u32,
    },

    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

impl LogEntry {
    async fn from_write<StateFile>(
        index: u64,
        term: u32,
        log: &[u8],
        state_file: &mut StateFile,
    ) -> Result<LogEntry, StateError>
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

    async fn from_read<StateFile>(state_file: &mut StateFile) -> Result<LogEntry, StateError>
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

pub struct AppendLogEntry {
    pub entries: Vec<Vec<u8>>,
    pub prev_log_term: u32,
    pub prev_log_index: u64,
}

impl<StateFile: super::StateFile> Persistent<StateFile> {
    pub async fn new(mut state_file: StateFile) -> Result<Self, StateError> {
        let magic_bytes_present = match state_file.read_u64_le().await {
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    // TODO: File might contain data but less than 8 bytes. What to do with that?
                    trace!("Reached EOF when reading magic bytes from state file, will populate the bytes");
                    false
                } else {
                    return Err(StateError::IOError(e));
                }
            }
            Ok(file_magic_bytes) => {
                if file_magic_bytes == STATE_MAGIC_BYTES {
                    trace!("Found matching magic bytes in state file");
                    true
                } else {
                    trace!("Found bytes {:X?} instead of expected magic bytes {:X?}, check provided state file", file_magic_bytes, STATE_MAGIC_BYTES);
                    return Err(StateError::MagicBytesNotMatching);
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
            voted_for = FileData::from_state_file_write(-1, &mut state_file).await?;
            log = vec![];
            operation_performed = "write";
            // TODO: Write an end marker
        } else {
            let version = state_file.read_u32_le().await?;
            if version > STATE_FILE_VERSION {
                return Err(StateError::VersionNotSupported {
                    file_version: version,
                    supported_version: STATE_FILE_VERSION,
                });
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

    async fn read_log(state_file: &mut StateFile) -> Result<Vec<LogEntry>, StateError> {
        let mut log = Vec::new();
        // FIXME: Assuming if error is returned, we have reached EOF, however there can be other errors as well
        while let Ok(log_entry) = LogEntry::from_read(state_file).await {
            trace!(
                "Read log entry: index = {}, term = {}, length = {}",
                log_entry.index.data,
                log_entry.term.data,
                log_entry.length.data
            );
            log.push(log_entry);
        }
        Ok(log)
    }

    pub fn current_term(&self) -> u32 {
        self.current_term.data
    }
    pub async fn update_current_term(&mut self, new_term: u32) -> Result<(), StateError> {
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
    ) -> Result<(), StateError> {
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
    pub async fn grant_vote_if_possible(&mut self, candidate_id: u32) -> Result<bool, StateError> {
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
    pub async fn add_log(&mut self, message: &[u8], term: u32) -> Result<u64, StateError> {
        let log_index = self.log.last().map(|record| record.index.data).unwrap_or(0) + 1;
        let log_entry =
            LogEntry::from_write(log_index, term, message, &mut self.state_file).await?;
        self.log.push(log_entry);
        Ok(log_index)
    }
    pub fn has_matching_entry(&self, index: u64, term: u32) -> bool {
        // index being 0 implies null entry (present in empty list)
        if index == 0 {
            return true;
        }

        // INVARIANT: Every entry left has index less than current entry's index and
        // term less than or equal to the current entry's term
        for entry in self.log.iter().rev() {
            if entry.index.data < index {
                return false;
            }
            if entry.index.data > index {
                continue;
            }
            // => entry.index.data == index

            if entry.term.data < term {
                return false;
            }
            if entry.term.data > term {
                continue;
            }
            // => entry.term.data == term

            return true;
        }
        false
    }
    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|entry| entry.index.data).unwrap_or(0)
    }
    // Very very inefficient implementation, just for completeness purposes
    pub async fn get_entries_from(&mut self, index: u64) -> Result<AppendLogEntry, StateError> {
        let mut prev_log_index = 0;
        let mut prev_log_term = 0;
        let mut prev_log_i = None;
        // Assume that index is monotonically increasing
        for (i, entry) in self.log.iter().enumerate().rev() {
            if entry.index.data < index {
                prev_log_index = entry.index.data;
                prev_log_term = entry.term.data;
                prev_log_i = Some(i);
                // FIXME: UGH
                break;
            }
        }

        let mut entries = Vec::new();
        let log_i = prev_log_i.map_or(0, |prev_i| prev_i + 1);
        for i in log_i..self.log.len() {
            let log_entry = &self.log[i];
            let current_pos = self.state_file.stream_position().await?;
            let length = log_entry.length.data as usize;
            self.state_file.seek(log_entry.length.position).await?;
            self.state_file
                .seek(SeekFrom::Current(size_of::<u64>() as i64))
                .await?;
            let mut entry = Vec::with_capacity(length);
            self.state_file
                .read_exact(&mut entry.as_mut_slice()[0..length])
                .await?;
            entries.push(entry);
            self.state_file.seek(SeekFrom::Start(current_pos)).await?;
        }

        Ok(AppendLogEntry {
            entries,
            prev_log_term,
            prev_log_index,
        })
    }
}

impl VolatileFollowerState {
    /// Update match_index and notify. Assumes that match index will be monotonically increasing
    pub fn update_match_index(&mut self, match_index: u64) {
        let old_match_index = self.match_index.swap(match_index, Ordering::SeqCst);
        // NOTE: Using >= to account for heartbeats i.e. empty AppendEntries response
        assert!(match_index >= old_match_index);
        if match_index > old_match_index {
            self.match_index_notifier.send_replace(());
        }
    }
}

impl VolatileLeader {
    pub fn new<StateFile: super::StateFile>(
        peers: &HashMap<usize, PeerNode>,
        persistent_state: &Persistent<StateFile>,
        last_log_index_watch: watch::Receiver<u64>,
        match_index_notifier: Arc<watch::Sender<()>>,
    ) -> VolatileLeader {
        let mut follower_states = HashMap::new();
        let next_index = persistent_state.last_log_index() + 1;
        for (peer_id, peer) in peers {
            let rpc_client = peer.rpc_client.clone();
            let last_log_index_watch = last_log_index_watch.clone();
            let match_index_notifier = Arc::clone(&match_index_notifier);
            follower_states.insert(
                *peer_id,
                VolatileFollowerState {
                    next_index,
                    match_index: Arc::new(AtomicU64::new(0)),
                    rpc_client,
                    last_log_index_watch,
                    match_index_notifier,
                },
            );
        }
        VolatileLeader { follower_states }
    }

    pub fn next_indexes(&self) -> Vec<Arc<AtomicU64>> {
        let mut vec = Vec::with_capacity(self.follower_states.len());
        for follower_state in self.follower_states.values() {
            vec.push(Arc::clone(&follower_state.match_index))
        }
        vec
    }
}

impl VolatileCommon {
    pub fn new() -> VolatileCommon {
        let (tx, _) = watch::channel(0_u64);
        VolatileCommon {
            commit_index_informer: tx,
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
    ) -> Result<Self, StateError>
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
    async fn from_state_file_read<StateFile>(state_file: &mut StateFile) -> Result<Self, StateError>
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
    ) -> Result<(), StateError>
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
