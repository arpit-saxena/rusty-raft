use super::pb::raft_server::Raft;
use super::pb::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use super::NodeServer;

use tokio::time::Instant;
use tonic::{Request, Response, Status};
use tracing::trace;

#[tonic::async_trait]
impl<StateFile: super::StateFile> Raft for NodeServer<StateFile> {
    async fn append_entries(
        &self,
        _: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        trace!("Append entries called");
        {
            let mut last_leader_message_time = self
                .last_leader_message_time
                .lock()
                .map_err(|_| Status::internal("Poison error"))?;
            *last_leader_message_time = Instant::now();
        }
        let reply = AppendEntriesResponse {
            term: self
                .node_common
                .persistent_state
                .lock()
                .await
                .current_term(),
            success: true,
        };
        Ok(Response::new(reply))
    }

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
