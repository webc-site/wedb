use std::fmt::Display;
use std::future::{Future, IntoFuture};
use std::io::{Error as IoError, Read, Seek, SeekFrom};
use std::pin::pin;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::Either;
use futures_util::future::select;
use futures_util::stream::{FuturesOrdered, unfold};
use futures_util::{FutureExt, Stream, StreamExt};
use zenoh::key_expr::KeyExpr;
use zenoh::qos::{CongestionControl, Priority};
use zenoh::query::ConsolidationMode;
use zenoh_raft::alias::{LogIdOf, VoteOf};
use zenoh_raft::base::{BoxFuture, BoxStream};
use zenoh_raft::error::{NetworkError, RPCError, Unreachable};
use zenoh_raft::errors::{ReplicationClosed, StreamingError};
use zenoh_raft::network::{RPCOption, RaftNetwork, RaftNetworkFactory, StreamAppendFuture};
use zenoh_raft::raft::{
  AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};

use crate::error::{Error, Result};
use crate::types::{
  ChunkSnapshotRequest, ChunkSnapshotResponse, CompactAppendEntriesRequest,
  CompactAppendEntriesResponse, CompactChunkSnapshotRequest, CompactChunkSnapshotResponse,
  CompactVoteRequest, CompactVoteResponse, Node, NodeId, Snapshot, SnapshotData, TypeConfig,
  decode, encode,
};

use super::keys::raft_keyexpr;

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SNAPSHOT_CHUNK_SIZE: usize = 256 * 1024;
const DEFAULT_MAX_IN_FLIGHT: usize = 8;

#[inline]
fn rpc_err(e: Error) -> RPCError<TypeConfig> {
  RPCError::Network(NetworkError::new(&e))
}

#[inline]
fn stream_unreachable(e: impl Display) -> StreamingError<TypeConfig> {
  let io_err = IoError::other(e.to_string());
  StreamingError::from(Unreachable::new(&io_err))
}

#[inline]
fn append_entries_priority(is_heartbeat: bool) -> Priority {
  if is_heartbeat {
    Priority::RealTime
  } else {
    Priority::InteractiveHigh
  }
}

pub struct NetworkConnection {
  target_id: NodeId,
  session: Arc<zenoh::Session>,
  append_entries_key: KeyExpr<'static>,
  vote_key: KeyExpr<'static>,
  snapshot_key: KeyExpr<'static>,
}

impl NetworkConnection {
  /// 异步创建网络连接并并发预声明 KeyExpr（分配紧凑 numeric ExprId，减少报文头开销）
  pub async fn new_async(target_id: NodeId, session: Arc<zenoh::Session>) -> Self {
    let append_entries_key = raft_keyexpr(target_id, "append_entries");
    let vote_key = raft_keyexpr(target_id, "vote");
    let snapshot_key = raft_keyexpr(target_id, "snapshot");

    // 并发预声明 KeyExpr，减少连接创建时的网络往返等待
    let (append_res, vote_res, snapshot_res) = futures_util::join!(
      IntoFuture::into_future(session.declare_keyexpr(append_entries_key.clone())),
      IntoFuture::into_future(session.declare_keyexpr(vote_key.clone())),
      IntoFuture::into_future(session.declare_keyexpr(snapshot_key.clone())),
    );

    let append_entries_key = append_res.unwrap_or(append_entries_key);
    let vote_key = vote_res.unwrap_or(vote_key);
    let snapshot_key = snapshot_res.unwrap_or(snapshot_key);

    Self {
      target_id,
      session,
      append_entries_key,
      vote_key,
      snapshot_key,
    }
  }

  pub fn target_id(&self) -> NodeId {
    self.target_id
  }

  pub(crate) async fn rpc_session<Req, Resp>(
    session: &zenoh::Session,
    key: &KeyExpr<'_>,
    req: &Req,
    timeout: Duration,
    priority: Priority,
    express: bool,
  ) -> Result<Resp>
  where
    Req: bitcode::Encode,
    Resp: bitcode::DecodeOwned,
  {
    let data = encode(req).map_err(|e| Error::internal(format!("encode failed: {e}")))?;
    let replies = session
      .get(key)
      .congestion_control(CongestionControl::Block)
      .consolidation(ConsolidationMode::None)
      .priority(priority)
      .express(express)
      .payload(data)
      .timeout(timeout)
      .await
      .map_err(|e| Error::Network(format!("Zenoh get error: {e}")))?;

    let reply = replies
      .recv_async()
      .await
      .map_err(|e| Error::Network(format!("Zenoh recv error: {e}")))?;

    let sample = reply
      .result()
      .map_err(|e| Error::Network(format!("Zenoh reply error: {e:?}")))?;

    let resp: Resp = decode(sample.payload().to_bytes().as_ref())
      .map_err(|e| Error::internal(format!("decode failed: {e}")))?;
    Ok(resp)
  }

  async fn send_append_entries(
    session: &zenoh::Session,
    key: &KeyExpr<'_>,
    req: AppendEntriesRequest<TypeConfig>,
    timeout: Duration,
  ) -> Result<AppendEntriesResponse<TypeConfig>> {
    let is_heartbeat = req.entries.is_empty();
    let priority = append_entries_priority(is_heartbeat);
    let compact_req = CompactAppendEntriesRequest::from(req);
    let resp: CompactAppendEntriesResponse =
      Self::rpc_session(session, key, &compact_req, timeout, priority, true).await?;
    Ok(resp.into())
  }

  async fn send_vote(
    session: &zenoh::Session,
    key: &KeyExpr<'_>,
    req: VoteRequest<TypeConfig>,
    timeout: Duration,
  ) -> Result<VoteResponse<TypeConfig>> {
    let compact_req = CompactVoteRequest::from(req);
    let resp: CompactVoteResponse = Self::rpc_session(
      session,
      key,
      &compact_req,
      timeout,
      Priority::RealTime,
      true,
    )
    .await?;
    Ok(resp.into())
  }

  async fn send_chunk_snapshot(
    session: &zenoh::Session,
    key: &KeyExpr<'_>,
    req: ChunkSnapshotRequest,
    timeout: Duration,
  ) -> Result<ChunkSnapshotResponse> {
    let compact_req = CompactChunkSnapshotRequest::from(req);
    let resp: CompactChunkSnapshotResponse =
      Self::rpc_session(session, key, &compact_req, timeout, Priority::Data, false).await?;
    Ok(resp.into())
  }

  async fn append_entries_internal(
    &mut self,
    req: AppendEntriesRequest<TypeConfig>,
    timeout: Duration,
  ) -> Result<AppendEntriesResponse<TypeConfig>> {
    Self::send_append_entries(&self.session, &self.append_entries_key, req, timeout).await
  }

  async fn vote_internal(
    &mut self,
    req: VoteRequest<TypeConfig>,
    timeout: Duration,
  ) -> Result<VoteResponse<TypeConfig>> {
    Self::send_vote(&self.session, &self.vote_key, req, timeout).await
  }

  async fn install_snapshot_internal(
    &mut self,
    req: ChunkSnapshotRequest,
    timeout: Duration,
  ) -> Result<ChunkSnapshotResponse> {
    Self::send_chunk_snapshot(&self.session, &self.snapshot_key, req, timeout).await
  }

  async fn send_snapshot_in_chunks(
    &mut self,
    vote: VoteOf<TypeConfig>,
    snapshot: Snapshot,
    cancel: impl Future<Output = ReplicationClosed> + zenoh_raft::OptionalSend + 'static,
    option: RPCOption,
  ) -> StdResult<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
    let mut cancel = std::pin::pin!(cancel);
    let mut std_file = snapshot.snapshot;

    let end = std_file
      .seek(SeekFrom::End(0))
      .map_err(stream_unreachable)?;

    std_file
      .seek(SeekFrom::Start(0))
      .map_err(stream_unreachable)?;

    let mut offset = 0u64;
    let chunk_size = option
      .snapshot_chunk_size()
      .unwrap_or(DEFAULT_SNAPSHOT_CHUNK_SIZE);
    let timeout = option.soft_ttl().max(DEFAULT_SNAPSHOT_TIMEOUT);

    let snapshot_id = snapshot
      .meta
      .last_log_id
      .map(|id| id.to_string())
      .unwrap_or_else(|| "0".to_string());

    // 预分配复用缓冲区，避免每轮分块重新在堆上申请与释放内存
    let mut chunk_buf = vec![0u8; chunk_size];

    loop {
      if let Some(err) = cancel.as_mut().now_or_never() {
        return Err(err.into());
      }

      let n_read = std_file
        .read(&mut chunk_buf[..chunk_size])
        .map_err(stream_unreachable)?;

      let done = (offset + n_read as u64) >= end;

      let req = ChunkSnapshotRequest {
        vote,
        snapshot_id: snapshot_id.clone(),
        meta: snapshot.meta.clone(),
        offset,
        data: bytes::Bytes::copy_from_slice(&chunk_buf[..n_read]),
        done,
      };

      let rpc_res = match select(
        cancel.as_mut(),
        pin!(self.install_snapshot_internal(req, timeout)),
      )
      .await
      {
        Either::Left((err, _)) => {
          return Err(err.into());
        }
        Either::Right((res, _)) => res,
      };

      match rpc_res {
        Ok(resp) => {
          if resp.vote != vote || done {
            return Ok(SnapshotResponse::new(resp.vote));
          }
        }
        Err(err) => {
          return Err(stream_unreachable(err));
        }
      }

      offset += n_read as u64;
    }
  }
}

type InFlightItem = (
  AppendEntriesResponse<TypeConfig>,
  Option<LogIdOf<TypeConfig>>,
  Option<LogIdOf<TypeConfig>>,
);
type InFlightFuture = BoxFuture<'static, StdResult<InFlightItem, Error>>;

fn spawn_inflight_append(
  session: Arc<zenoh::Session>,
  key: KeyExpr<'static>,
  timeout: Duration,
  req: AppendEntriesRequest<TypeConfig>,
) -> InFlightFuture {
  let prev_log_id = req.prev_log_id;
  let last_log_id = req.entries.last().map(|e| e.log_id).or(prev_log_id);

  Box::pin(async move {
    let resp = NetworkConnection::send_append_entries(&session, &key, req, timeout).await?;
    Ok((resp, prev_log_id, last_log_id))
  })
}

struct PipelineStream<S> {
  input: S,
  in_flight: FuturesOrdered<InFlightFuture>,
  input_exhausted: bool,
  max_in_flight: usize,
  timeout: Duration,
  session: Arc<zenoh::Session>,
  append_entries_key: KeyExpr<'static>,
}

impl<S> PipelineStream<S>
where
  S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Unpin,
{
  fn new(
    input: S,
    session: Arc<zenoh::Session>,
    append_entries_key: KeyExpr<'static>,
    timeout: Duration,
    max_in_flight: usize,
  ) -> Self {
    Self {
      input,
      in_flight: FuturesOrdered::new(),
      input_exhausted: false,
      max_in_flight,
      timeout,
      session,
      append_entries_key,
    }
  }

  #[inline]
  fn push_inflight(&mut self, req: AppendEntriesRequest<TypeConfig>) {
    let fut = spawn_inflight_append(
      self.session.clone(),
      self.append_entries_key.clone(),
      self.timeout,
      req,
    );
    self.in_flight.push_back(fut);
  }

  fn fill_inflight_available(&mut self) {
    while !self.input_exhausted && self.in_flight.len() < self.max_in_flight {
      match self.input.next().now_or_never() {
        Some(Some(req)) => self.push_inflight(req),
        Some(None) => {
          self.input_exhausted = true;
          break;
        }
        None => break,
      }
    }
  }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
  type SnapshotData = SnapshotData;

  async fn append_entries(
    &mut self,
    rpc: AppendEntriesRequest<TypeConfig>,
    option: RPCOption,
  ) -> StdResult<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
    let timeout = option.soft_ttl().max(DEFAULT_RPC_TIMEOUT);
    self
      .append_entries_internal(rpc, timeout)
      .await
      .map_err(rpc_err)
  }

  async fn vote(
    &mut self,
    rpc: VoteRequest<TypeConfig>,
    option: RPCOption,
  ) -> StdResult<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
    let timeout = option.soft_ttl().max(DEFAULT_RPC_TIMEOUT);
    self.vote_internal(rpc, timeout).await.map_err(rpc_err)
  }

  fn stream_append<'s, S>(
    &'s mut self,
    input: S,
    option: RPCOption,
  ) -> StreamAppendFuture<'s, TypeConfig>
  where
    S: Stream<Item = AppendEntriesRequest<TypeConfig>> + zenoh_raft::OptionalSend + Unpin + 'static,
  {
    let fu = async move {
      let timeout = option.soft_ttl().max(DEFAULT_RPC_TIMEOUT);
      let session = self.session.clone();
      let append_entries_key = self.append_entries_key.clone();

      let init_state = PipelineStream::new(
        input,
        session,
        append_entries_key,
        timeout,
        DEFAULT_MAX_IN_FLIGHT,
      );

      let strm = unfold(Some(init_state), move |state| async move {
        let mut state = state?;

        // 当没有在途请求时，等待下一个输入
        if state.in_flight.is_empty() {
          if state.input_exhausted {
            return None;
          }
          match state.input.next().await {
            Some(req) => state.push_inflight(req),
            None => {
              state.input_exhausted = true;
              return None;
            }
          }
        }

        // 尽力填满在途并发窗口
        state.fill_inflight_available();

        if state.in_flight.is_empty() {
          return None;
        }

        let res = match state.in_flight.next().await {
          Some(r) => r,
          None => return None,
        };
        match res {
          Ok((resp, prev_log_id, last_log_id)) => {
            let is_success = resp.is_success();
            let stream_result = resp.into_stream_result(prev_log_id, last_log_id);
            let next_state = if is_success { Some(state) } else { None };
            Some((Ok(stream_result), next_state))
          }
          Err(e) => Some((Err(rpc_err(e)), None)),
        }
      });

      let strm: BoxStream<'s, _> = Box::pin(strm);
      Ok(strm)
    };

    Box::pin(fu)
  }

  async fn full_snapshot(
    &mut self,
    vote: VoteOf<TypeConfig>,
    snapshot: Snapshot,
    cancel: impl Future<Output = ReplicationClosed> + zenoh_raft::OptionalSend + 'static,
    option: RPCOption,
  ) -> StdResult<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
    self
      .send_snapshot_in_chunks(vote, snapshot, cancel, option)
      .await
  }
}

#[derive(Clone)]
pub struct NetworkFactory {
  session: Arc<zenoh::Session>,
}

impl NetworkFactory {
  pub fn new(session: Arc<zenoh::Session>) -> Self {
    Self { session }
  }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
  type Network = NetworkConnection;

  async fn new_client(&mut self, target: NodeId, _node: &Node) -> Self::Network {
    NetworkConnection::new_async(target, self.session.clone()).await
  }
}
