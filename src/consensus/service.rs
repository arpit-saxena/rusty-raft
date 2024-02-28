use super::pb::raft_server::Raft;
use super::pb::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use super::{NodeClient, NodeServer};

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::trace;

#[tonic::async_trait]
impl<StateFile: super::StateFile> Raft for NodeServer<StateFile> {
    async fn append_entries(
        &self,
        _: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        // let reply = AppendEntriesResponse{term: 0, success: false};
        // Ok(Response::new(reply))
        todo!();
    }

    async fn request_vote(
        &self,
        _request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        trace!("request_vote called");

        let vote_response = VoteResponse {
            term: self
                .node_common
                .persistent_state
                .lock()
                .await
                .current_term(),
            vote_granted: true,
        };
        Ok(Response::new(vote_response))
    }
}
