use super::pb::raft_server::Raft;
use super::pb::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use super::NodeServer;

use tokio::time::Instant;
use tonic::{Request, Response, Status};
use tracing::trace;

impl<StateFile: super::StateFile> NodeServer<StateFile> {
    fn update_last_leader_message_time(&self) -> Result<(), Status> {
        let mut last_leader_message_time = self
            .last_leader_message_time
            .lock()
            .map_err(|_| Status::internal("Poison error"))?;
        *last_leader_message_time = Instant::now();
        Ok(())
    }
}

#[tonic::async_trait]
impl<StateFile: super::StateFile> Raft for NodeServer<StateFile> {
    #[tracing::instrument(skip_all, fields(id = self.node_common.node_index))]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        trace!("Append entries called");
        self.update_last_leader_message_time()?;

        let mut persistent_state = self.node_common.persistent_state.lock().await;
        let request = request.into_inner();

        let success =
            if persistent_state.has_matching_entry(request.prev_log_index, request.prev_log_term) {
                for entry in request.entries {
                    persistent_state.add_log(&entry).await.map_err(|e| Status::unavailable(e.to_string()))?;
                    // TODO: If there's an error here, we probably need to restart the Node
                }
                true
            } else {
                false
            };

        let reply = AppendEntriesResponse {
            term: persistent_state.current_term(),
            success,
        };
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip_all, id = self.node_common.node_index)]
    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let request = request.into_inner();
        let mut persistent_state = self.node_common.persistent_state.lock().await;

        let vote_granted = if request.term < persistent_state.current_term() {
            false
        } else {
            // FIXME: Error handling
            if persistent_state
                .update_current_term(request.term)
                .await
                .is_err()
            {
                return Err(Status::internal("Error in updating current term"));
            }
            // TODO: Check log matching
            match persistent_state
                .grant_vote_if_possible(request.candidate_id)
                .await
            {
                Ok(voted) => voted,
                Err(_) => return Err(Status::internal("Error in granting vote")),
            }
        };

        trace!(
            "request_vote called, vote_granted = {}, candidate_id = {}, term = {}",
            vote_granted,
            request.candidate_id,
            persistent_state.current_term()
        );
        let vote_response = VoteResponse {
            term: persistent_state.current_term(),
            vote_granted,
        };
        Ok(Response::new(vote_response))
    }
}
