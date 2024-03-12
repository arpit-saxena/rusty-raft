use std::sync::atomic::Ordering;

use super::atomic_util::UpdateIfNew;
use super::pb::raft_server::Raft;
use super::pb::{
    AppendEntriesRequest, AppendEntriesResponse, PerformActionRequest, PerformActionResponse,
    VoteRequest, VoteResponse,
};
use super::state::StateError;
use super::{NodeRole, NodeServer};

use anyhow::Context;
use tokio::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{debug, trace};

impl<StateFile: super::StateFile> NodeServer<StateFile> {
    fn update_last_leader_message_time(&self) -> Result<(), Status> {
        self.node
            .last_leader_message_time
            .store(Instant::now(), Ordering::SeqCst)
            .context(
                "Error in updating last leader message time to INstant::now, some bug in Instant!",
            )
            .map_err(|e| Status::from_error(e.into()))?;
        Ok(())
    }
}

#[tonic::async_trait]
impl<StateFile: super::StateFile> Raft for NodeServer<StateFile> {
    #[tracing::instrument(skip_all, fields(id = self.node.node_index))]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let mut persistent_state = self.node.persistent_state.lock().await;
        let current_term = persistent_state.current_term();
        let request = request.into_inner();

        let success = if request.term < current_term {
            debug!(
                "Rejecting AppendEntries since its term {} is less than our term {}",
                request.term, current_term,
            );
            false
        } else {
            // Revert to follower, if we received entries of newer term or stay a follower
            let _ = self.node.role_informer.send_if_new(NodeRole::Follower);
            self.update_last_leader_message_time()?;
            self.node
                .leader_id
                .store(request.leader_id as i64, Ordering::SeqCst);

            match persistent_state
                .add_log_entries(
                    &request.entries,
                    request.prev_log_index.try_into().unwrap(),
                    request.prev_log_term,
                )
                .await
            {
                Ok(()) => true,
                Err(StateError::LogNotMatching) => false,
                Err(e) => return Err(Status::from_error(Box::new(e))),
            }
        };

        let reply = AppendEntriesResponse {
            term: persistent_state.current_term(),
            success,
        };
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip_all, fields(id = self.node.node_index))]
    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let request = request.into_inner();
        let mut persistent_state = self.node.persistent_state.lock().await;

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

            let our_last_log_term = persistent_state.last_log_term() as i32;
            // FIXME: UGHHGHGHGHGH
            let our_last_log_index = persistent_state.last_log_index() as u64;
            let candidate_log_up_to_date = if request.last_log_term != our_last_log_term {
                request.last_log_term >= our_last_log_term
            } else if request.last_log_index != our_last_log_index {
                request.last_log_index >= our_last_log_index
            } else {
                true
            };
            trace!(
                our_last_log_term,
                our_last_log_index,
                request.last_log_term,
                request.last_log_index
            );

            if !candidate_log_up_to_date {
                false
            } else {
                match persistent_state
                    .grant_vote_if_possible(request.candidate_id)
                    .await
                {
                    Ok(voted) => voted,
                    Err(_) => return Err(Status::internal("Error in granting vote")),
                }
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

    #[tracing::instrument(skip_all, fields(id = self.node.node_index))]
    async fn perform_action(
        &self,
        request: Request<PerformActionRequest>,
    ) -> Result<Response<PerformActionResponse>, Status> {
        let request = request.into_inner();

        trace!("Received perform_action request {:?}", request);

        // FIXME: [URGENT] There's a race condition between getting current term and leader_id. If leader_id updates after we got lock
        // on the mutex, then we'll be committing a log with the wrong term :/
        let mut persistent_state = self.node.persistent_state.lock().await;
        let current_term = persistent_state.current_term();
        let leader_id = self.node.leader_id.load(Ordering::SeqCst) as u32; // TODO: sighhhh
        if leader_id != self.node.node_index {
            let response = PerformActionResponse {
                success: false,
                leader_id,
            };
            return Ok(Response::new(response));
        }

        match persistent_state
            .append_log(&request.action, current_term)
            .await
        {
            Err(e) => {
                return Err(Status::from_error(Box::new(e)));
            }
            Ok(log_index) => {
                std::mem::drop(persistent_state);
                self.last_log_index_informer.send_replace(log_index);

                // Added to log, now just need to wait for commit index to increase and respond
                let mut receiver = self
                    .node
                    .common_volatile_state
                    .commit_index_informer
                    .subscribe();
                if receiver
                    .wait_for(|commit_idx| *commit_idx >= log_index)
                    .await
                    .is_err()
                {
                    Response::new(Status::internal(
                        "commit_index_informer dropped, shouldn't be possible",
                    ));
                }

                let response = PerformActionResponse {
                    success: true,
                    leader_id,
                };
                Ok(Response::new(response))
            }
        }
    }
}
