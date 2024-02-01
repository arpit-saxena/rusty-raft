use super::pb::raft_server::Raft;
use super::pb::{HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse};
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct RaftService {}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn ping(
        &self,
        _: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let reply = HeartbeatResponse {};
        Ok(Response::new(reply))
    }

    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        unimplemented!()
    }
}
