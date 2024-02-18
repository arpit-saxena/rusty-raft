use super::pb::raft_server::Raft;
use super::pb::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};

use super::{Node, StateWriter};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl<Writer: StateWriter> Raft for Node<Writer> {
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
        todo!()
    }
}
