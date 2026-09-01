use std::io::Error as IoError;
use std::result::Result as StdResult;
use std::time::Duration;

use futures_timer::Delay;
use futures_util::future::Either;
use futures_util::future::select;
use log::{error, warn};
use zenoh::qos::{CongestionControl, Priority};

use super::LeaderHandler;
use super::RaftNode;
use crate::error::{Error, Result};
use crate::util::sleep;
use wedb_raft::types::{
  ForwardRequest, ForwardResponse, ForwardToLeader, NodeId, RequestPayload, decode, encode,
};
use zenoh_raft::async_runtime::watch::WatchReceiver;

const MAX_RETRIES: u32 = 30;
const RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(10);
const RETRY_MAX_INTERVAL: Duration = Duration::from_millis(100);

impl RaftNode {
  #[inline]
  pub(crate) fn current_leader_id(&self) -> Option<NodeId> {
    self.raft().metrics().borrow_watched().current_leader
  }

  pub(crate) async fn get_leader(&self) -> Result<Option<NodeId>> {
    if let Some(leader) = self.current_leader_id() {
      return Ok(Some(leader));
    }

    let delay = Delay::new(Duration::from_millis(500));
    let mut metrics_rx = self.raft().metrics();

    let fut = async {
      loop {
        if let Some(leader) = metrics_rx.borrow_watched().current_leader {
          return Ok(Some(leader));
        }
        if let Err(e) = WatchReceiver::changed(&mut metrics_rx).await {
          let error_msg = format!("Metrics watch error: {e:?}");
          error!("{error_msg}");
          return Err(Error::internal(error_msg));
        }
      }
    };

    futures_util::pin_mut!(fut);
    match select(fut, delay).await {
      Either::Left((res, _)) => res,
      Either::Right(_) => Ok(None),
    }
  }

  #[inline]
  pub(crate) async fn assume_leader(&self) -> StdResult<LeaderHandler<'_>, ForwardToLeader> {
    let current_node_id = *self.raft().node_id();

    let leader_id = match self.current_leader_id() {
      Some(id) => Some(id),
      None => self.get_leader().await.ok().flatten(),
    };

    match leader_id {
      Some(leader_id) => {
        if leader_id == current_node_id {
          Ok(LeaderHandler::new(self))
        } else {
          Err(ForwardToLeader {
            leader_id: Some(leader_id),
            leader_node: None,
          })
        }
      }
      None => Err(ForwardToLeader {
        leader_id: None,
        leader_node: None,
      }),
    }
  }

  pub(crate) async fn exec_or_forward(&self, payload: RequestPayload) -> Result<ForwardResponse> {
    self
      .handle_forward_request(ForwardRequest::new(payload))
      .await
  }

  pub async fn handle_forward_request(&self, request: ForwardRequest) -> Result<ForwardResponse> {
    if request.hop >= 5 {
      return Err(Error::internal(
        "Too many forward hops, possible leader routing loop",
      ));
    }

    for attempt in 0..MAX_RETRIES {
      match self.assume_leader().await {
        Ok(leader) => match Self::dispatch_leader_handler(leader, request.body.clone()).await {
          Ok(response) => return Ok(response),
          Err(e) => {
            if Self::is_retriable_error(&e) && attempt < MAX_RETRIES - 1 {
              let delay = RETRY_INITIAL_INTERVAL * 2u32.saturating_pow(attempt);
              let delay = delay.min(RETRY_MAX_INTERVAL);
              let attempt_num = attempt + 1;
              warn!(
                "handle_forward_request: leader dispatch failed ({e}), retry {attempt_num}/{MAX_RETRIES} after {delay:?}"
              );
              sleep(delay).await;
              continue;
            }
            return Err(e);
          }
        },
        Err(forward_err) => {
          let retry_reason = match forward_err.leader_id {
            Some(leader_id) => {
              let mut fwd = request.clone();
              fwd.hop += 1;
              match self.send_forward_request(leader_id, fwd).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                  if Self::is_retriable_error(&e) {
                    Some(format!("Failed to forward request ({e})"))
                  } else {
                    return Err(e);
                  }
                }
              }
            }
            None => Some("No leader available to forward request".to_string()),
          };

          if let Some(reason) = retry_reason
            && attempt < MAX_RETRIES - 1
          {
            let delay = RETRY_INITIAL_INTERVAL * 2u32.saturating_pow(attempt);
            let delay = delay.min(RETRY_MAX_INTERVAL);
            let attempt_num = attempt + 1;
            warn!("{reason}, retrying {attempt_num}/{MAX_RETRIES}, waiting {delay:?}");
            sleep(delay).await;
            continue;
          }

          return Err(Error::internal(
            "No leader available to forward request after max retries",
          ));
        }
      }
    }

    Err(Error::internal(
      "No leader available to forward request after max retries",
    ))
  }

  pub(crate) async fn send_forward_request(
    &self,
    leader_id: NodeId,
    request: ForwardRequest,
  ) -> Result<ForwardResponse> {
    let data = encode(&request).map_err(|e| Error::internal(format!("encode failed: {e}")))?;
    let key = self.get_or_declare_forward_key(leader_id).await;
    let replies = self
      .session
      .get(&key)
      .congestion_control(CongestionControl::Block)
      .priority(Priority::InteractiveHigh)
      .express(true)
      .payload(data)
      .timeout(Duration::from_secs(5))
      .await
      .map_err(|e| Error::retryable(IoError::other(format!("Zenoh get error: {e}"))))?;

    let reply = replies
      .recv_async()
      .await
      .map_err(|e| Error::retryable(IoError::other(format!("Zenoh recv error: {e}"))))?;

    let sample = reply
      .result()
      .map_err(|e| Error::retryable(IoError::other(format!("Zenoh reply error: {e:?}"))))?;

    let reply_res: StdResult<ForwardResponse, String> =
      decode(sample.payload().to_bytes().as_ref())
        .map_err(|e| Error::internal(format!("decode failed: {e}")))?;
    reply_res.map_err(|e| Error::internal(format!("Leader error: {e}")))
  }

  pub(crate) async fn dispatch_leader_handler(
    leader: LeaderHandler<'_>,
    body: RequestPayload,
  ) -> Result<ForwardResponse> {
    match body {
      RequestPayload::Write(entry) => {
        let result = leader.write(entry).await?;
        Ok(ForwardResponse::Write(result))
      }
      RequestPayload::BatchWrite(req) => {
        leader.batch_write(req).await?;
        Ok(ForwardResponse::BatchWrite(()))
      }
      RequestPayload::Txn(req) => {
        let result = leader.txn(req).await?;
        Ok(ForwardResponse::Txn(result))
      }
      RequestPayload::GetKV(req) => {
        let result = leader.read(req).await?;
        Ok(ForwardResponse::GetKV(result))
      }
      RequestPayload::ScanPrefix(req) => {
        let result = leader.scan_prefix(req).await?;
        Ok(ForwardResponse::ScanPrefix(result))
      }
      RequestPayload::Join(req) => {
        leader.add_node(req).await?;
        Ok(ForwardResponse::Join(()))
      }
      RequestPayload::Leave(req) => {
        leader.remove_node(req).await?;
        Ok(ForwardResponse::Leave(()))
      }
      RequestPayload::GetMembers(req) => {
        let result = leader.get_members(req).await?;
        Ok(ForwardResponse::GetMembers(result))
      }
    }
  }

  fn is_retriable_error(error: &Error) -> bool {
    error.is_retryable()
  }
}
