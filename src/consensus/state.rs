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

impl<StateFile: super::StateFile> Persistent<StateFile> {
    pub fn new(state_file: StateFile) -> Self {
        // TODO: Read current_term and voted_for from the file. Update the file if not present

        Persistent {
            current_term: 0,
            voted_for: None,
            // log: vec![],
            state_file,
        }
    }
}
