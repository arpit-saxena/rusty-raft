use super::PeerNode;
use super::StateWriter;

/// This State is updated on stable storage before responding to RPCs
pub struct Persistent<Writer: StateWriter> {
    /// latest term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: u32,
    /// candidateId that received vote in current term (or None if not voted)
    voted_for: Option<Box<PeerNode>>,
    /// logbook, vector of (message, term); all logs might not be applied
    // log: Vec<(Message, u32)>,
    /// writer that will interact with storage to read/write state
    writer: Writer,
}

impl<Writer: StateWriter> Persistent<Writer> {
    pub fn new(writer: Writer) -> Self {
        // TODO: Read current_term and voted_for from the file. Update the file if not present

        Persistent {
            current_term: 0,
            voted_for: None,
            // log: vec![],
            writer,
        }
    }
}
