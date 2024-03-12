use std::{
    collections::HashMap,
    fmt::Debug,
    io::SeekFrom,
    mem::{size_of, size_of_val},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::watch,
};
use tonic::transport::Channel;
use tracing::{debug, trace, warn};

use super::{
    macro_util::GenericByteIO,
    pb::{self, raft_client::RaftClient},
    PeerNode,
};

struct LogEntry {
    index: usize,
    term: FileData<u32>,
    length: FileData<u64>,
}

struct Log {
    // TODO: To support snapshotting, keep a snapshot, and the log would have a start index
    /// Log entries; all logs might not be applied
    entries: Vec<LogEntry>,
    /// index of last log
    last_log_idx: usize,
    /// log entries start position in file
    start_position: u64,
}

/// This State is updated on stable storage before responding to RPCs
pub struct Persistent<StateFile: super::StateFile> {
    /// latest term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: FileData<u32>,
    /// candidateId that received vote in current term (or -1 if not voted)
    voted_for: FileData<i32>, // TODO: Maybe store reference to PeerNode?
    /// logbook, vector of LogEntry; all logs might not be applied
    log: Log,
    /// file like object that will interact with storage to read/write persistent state
    state_file: StateFile,
}

/// Volatile state that is stored on all servers
pub struct VolatileCommon {
    /// index of highest log entry known to be committed (initialized to 0, increases monotonically)
    pub commit_index_informer: watch::Sender<usize>,
    /// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    pub last_applied: u64,
}

/// Volatile per-follower state that is stored only for a leader
pub struct VolatileFollowerState {
    /// index of the next log entry to send to that server (initialized to leader last log index + 1)
    pub next_index: usize,
    /// index of highest log entry known to be replicated on the server (initialized to 0, increases monotonically)
    match_index: Arc<AtomicUsize>,
    /// rpc client to send entries to follower
    pub rpc_client: RaftClient<Channel>,
    /// Watch receiver that is used to inform if any new log has been added that should be synced to the followers
    pub last_log_index_watch: watch::Receiver<usize>,
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

    #[error("Couldn't add log entry since log with previous index and term was not found")]
    LogNotMatching,

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("Bug: {0}")]
    Bug(String),
}

impl LogEntry {
    async fn write_end_marker<StateFile>(state_file: &mut StateFile) -> Result<(), StateError>
    where
        StateFile: super::StateFile,
    {
        state_file.write_u32_le(0).await?; // term
        state_file.seek(SeekFrom::Current(-4)).await?;
        Ok(())
    }

    fn seek_position_after_self(&self) -> SeekFrom {
        SeekFrom::Start(
            self.length.position_from_start
                + (size_of_val(&self.length.data) as u64)
                + self.length.data,
        )
    }
}

impl Log {
    async fn push<StateFile>(
        &mut self,
        term: u32,
        log: &[u8],
        state_file: &mut StateFile,
    ) -> Result<usize, StateError>
    where
        StateFile: super::StateFile,
    {
        let term = FileData::from_state_file_write(term, state_file).await?;
        let length = FileData::from_state_file_write(log.len() as u64, state_file).await?;
        state_file.write_all(log).await?;
        LogEntry::write_end_marker(state_file).await?;
        state_file.flush().await?; // Make sure the log is flushed to the disk
        let index = self.last_log_idx + 1;
        // trace!("Pushing index {}, term {}, length {}", index, term.data, length.data);
        let entry = LogEntry {
            index,
            term,
            length,
        };
        self.last_log_idx += 1;
        self.entries.push(entry);

        Ok(index)
    }

    async fn from_read<StateFile>(state_file: &mut StateFile) -> Result<Log, StateError>
    where
        StateFile: super::StateFile,
    {
        let mut log_entries = Vec::new();
        let mut index = 1;
        let log_start_position = state_file.stream_position().await?;

        loop {
            let term: FileData<u32> = match FileData::from_state_file_read(state_file).await {
                Ok(term) => {
                    if term.data == 0 {
                        let size_of_term: i64 = size_of_val(&term.data).try_into().unwrap();
                        state_file.seek(SeekFrom::Current(-size_of_term)).await?;
                        return Ok(Log {
                            entries: log_entries,
                            last_log_idx: index - 1,
                            start_position: log_start_position,
                        });
                    }
                    term
                }
                Err(StateError::IOError(e)) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("Encountered EOF while reading log entries, expected null entry.");
                        return Ok(Log {
                            entries: log_entries,
                            last_log_idx: index - 1,
                            start_position: log_start_position,
                        });
                    }
                    return Err(StateError::IOError(e));
                }
                Err(e) => {
                    return Err(e);
                }
            };
            let length = FileData::from_state_file_read(state_file).await?;
            state_file
                .seek(SeekFrom::Current(length.data as i64))
                .await?;

            log_entries.push(LogEntry {
                index,
                term,
                length,
            });
            index += 1;
        }
    }

    async fn new<StateFile>(state_file: &mut StateFile) -> Result<Log, StateError>
    where
        StateFile: super::StateFile,
    {
        let log_start_position = state_file.stream_position().await?;
        LogEntry::write_end_marker(state_file).await?;
        Ok(Log {
            entries: vec![],
            last_log_idx: 0,
            start_position: log_start_position,
        })
    }

    #[tracing::instrument(skip(self, message, state_file))]
    async fn push_last_log_at_idx<StateFile>(
        &mut self,
        message: &[u8],
        term: u32,
        idx: usize,
        state_file: &mut StateFile,
    ) -> Result<(), StateError>
    where
        StateFile: super::StateFile,
    {
        assert!(idx > 0); // idx is 1-based idx
        let idx = idx - 1;
        if idx > self.entries.len() {
            return Err(StateError::Bug(format!(
                "push_last_log_at_idx: Adding log at idx > log length, which is {}",
                self.entries.len()
            )));
        }

        if idx < self.entries.len() {
            debug!("Current length is {}, truncating it", self.entries.len());
        }

        self.entries.truncate(idx);
        let position = self
            .entries
            .last()
            .map_or(SeekFrom::Start(self.start_position), |entry| {
                entry.seek_position_after_self()
            });
        state_file.seek(position).await?;
        self.last_log_idx = self.entries.last().map_or(0, |entry| entry.index);
        self.push(term, message, state_file).await?;
        Ok(())
    }
}

pub struct AppendLogEntry {
    pub entries: Vec<pb::LogEntry>,
    pub prev_log_term: u32,
    pub prev_log_index: usize,
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
            log = Log::new(&mut state_file).await?;
            operation_performed = "write";
            state_file.write_u32_le(0).await?; // term 0, for end marker
            state_file.seek(SeekFrom::Current(-4)).await?; // TODO sigh
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
            log = Log::from_read(&mut state_file).await?;
            operation_performed = "read";
        }

        trace!(
            "Performed {} operation version = {}, current_term = {:?}, voted_for = {:?}, logs length = {}, to state file",
            operation_performed,
            STATE_FILE_VERSION,
            current_term,
            voted_for,
            log.entries.len(),
        );

        Ok(Persistent {
            current_term,
            voted_for,
            log,
            state_file,
        })
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

    pub async fn append_log(&mut self, message: &[u8], term: u32) -> Result<usize, StateError> {
        self.log.push(term, message, &mut self.state_file).await
    }

    pub async fn add_log_entries(
        &mut self,
        messages: &Vec<pb::LogEntry>,
        prev_log_index: usize,
        prev_log_term: u32,
    ) -> Result<(), StateError> {
        if self.has_matching_entry(prev_log_index, prev_log_term) {
            let mut log_idx = prev_log_index + 1;
            for pb::LogEntry { term, entry } in messages {
                // TODO: Add a function in log or edit push, to accept multiple entries
                self.log
                    .push_last_log_at_idx(entry, *term, log_idx, &mut self.state_file)
                    .await?;
                log_idx += 1;
            }
            Ok(())
        } else {
            Err(StateError::LogNotMatching)
        }
    }

    // TODO: Move function to impl Log
    pub fn has_matching_entry(&self, index: usize, term: u32) -> bool {
        // index being 0 implies null entry (present in empty list)
        if index == 0 {
            return true;
        }

        // INVARIANT: Every entry left has index less than current entry's index and
        // term less than or equal to the current entry's term
        for entry in self.log.entries.iter().rev() {
            if entry.index < index {
                return false;
            }
            if entry.index > index {
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

    pub fn last_log_index(&self) -> usize {
        self.log.entries.last().map_or(0, |entry| entry.index)
    }

    pub fn last_log_term(&self) -> u32 {
        self.log.entries.last().map_or(0, |entry| entry.term.data)
    }

    // Very very inefficient implementation, just for completeness purposes
    pub async fn get_entries_from(&mut self, index: usize) -> Result<AppendLogEntry, StateError> {
        let mut prev_log_index = 0;
        let mut prev_log_term = 0;
        let mut prev_log_i = None;
        // Assume that index is monotonically increasing
        for (i, entry) in self.log.entries.iter().enumerate().rev() {
            if entry.index < index {
                prev_log_index = entry.index;
                prev_log_term = entry.term.data;
                prev_log_i = Some(i);
                // FIXME: UGH
                break;
            }
        }

        let mut entries = Vec::new();
        let log_i = prev_log_i.map_or(0, |prev_i| prev_i + 1);
        for i in log_i..self.log.entries.len() {
            let log_entry = &self.log.entries[i];
            let current_pos = self.state_file.stream_position().await?;
            let length = log_entry.length.data as usize;
            self.state_file
                .seek(SeekFrom::Start(log_entry.length.position_from_start))
                .await?;
            self.state_file
                .seek(SeekFrom::Current(size_of::<u64>() as i64))
                .await?;
            let mut log = Vec::with_capacity(length);
            log.resize(length, b'0');
            self.state_file
                .read_exact(&mut log.as_mut_slice()[0..length])
                .await?;
            let entry = pb::LogEntry {
                entry: log,
                term: log_entry.term.data,
            };
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
    pub fn update_match_index(&mut self, match_index: usize) {
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
        last_log_index_watch: watch::Receiver<usize>,
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
                    match_index: Arc::new(AtomicUsize::new(0)),
                    rpc_client,
                    last_log_index_watch,
                    match_index_notifier,
                },
            );
        }
        VolatileLeader { follower_states }
    }

    pub fn next_indexes(&self) -> Vec<Arc<AtomicUsize>> {
        let mut vec = Vec::with_capacity(self.follower_states.len());
        for follower_state in self.follower_states.values() {
            vec.push(Arc::clone(&follower_state.match_index))
        }
        vec
    }
}

impl VolatileCommon {
    pub fn new() -> VolatileCommon {
        let (tx, _) = watch::channel(0);
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
    position_from_start: u64,
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
            position_from_start,
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
            position_from_start,
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
        state_file
            .seek(SeekFrom::Start(self.position_from_start))
            .await?;
        state_file.write_little_endian(data).await?;
        state_file.flush().await?; // Be really sure this value is written to disk
        self.data = data;
        state_file.seek(SeekFrom::Start(current_position)).await?;
        Ok(())
    }
}
