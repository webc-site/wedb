use std::fs::File;
use std::future::Future;
use std::io::{Seek, SeekFrom, Write};
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use papaya::HashMap;
use zenoh::query::Query;
use zenoh_raft::raft::{AppendEntriesRequest, SnapshotResponse, VoteRequest};

use crate::error::Error;
use crate::node::RaftNode;
use wedb_raft::types::{
  ChunkSnapshotRequest, CompactAppendEntriesRequest, CompactAppendEntriesResponse,
  CompactChunkSnapshotRequest, CompactChunkSnapshotResponse, CompactVote, CompactVoteRequest,
  CompactVoteResponse, ForwardRequest, ForwardResponse, Snapshot, SnapshotMeta, TypeConfig, Vote,
  decode, encode,
};

const STALE_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(300);

struct StreamingSnapshot {
  data: File,
  current_offset: u64,
  created_at: Instant,
}

/// 统一的 Zenoh RPC 查询调度辅助函数，封装反序列化、业务执行、结果序列化与响应投递
async fn dispatch_rpc<Req, Resp, F, Fut>(query: &Query, op_name: &'static str, f: F)
where
  Req: bitcode::DecodeOwned,
  Resp: bitcode::Encode,
  F: FnOnce(Req) -> Fut,
  Fut: Future<Output = StdResult<Resp, String>>,
{
  let payload = match query.payload() {
    Some(p) => p.to_bytes(),
    None => {
      log::warn!("Received {op_name} query without payload");
      let _ = query.reply_err("Missing query payload").await;
      return;
    }
  };

  let req: Req = match decode(payload.as_ref()) {
    Ok(r) => r,
    Err(e) => {
      log::error!("Failed to decode {op_name} request: {e}");
      let _ = query
        .reply_err(format!("Decode {op_name} request failed: {e}"))
        .await;
      return;
    }
  };

  match f(req).await {
    Ok(resp) => match encode(&resp) {
      Ok(bytes) => {
        if let Err(e) = query.reply(query.key_expr(), bytes).await {
          log::error!("Failed to reply to {op_name} query: {e}");
        }
      }
      Err(e) => {
        log::error!("Failed to encode {op_name} response: {e}");
        let _ = query
          .reply_err(format!("Encode {op_name} response failed: {e}"))
          .await;
      }
    },
    Err(e) => {
      log::error!("{op_name} failed: {e}");
      let _ = query.reply_err(format!("{op_name} failed: {e}")).await;
    }
  }
}

pub struct RaftServiceImpl {
  node: Arc<RaftNode>,
  /// 使用 papaya 无锁 HashMap 替代 Mutex<HashMap>，消除外层锁竞争
  streaming_snapshots: HashMap<String, Arc<Mutex<StreamingSnapshot>>>,
}

impl RaftServiceImpl {
  pub fn new(node: Arc<RaftNode>) -> Self {
    Self {
      node,
      streaming_snapshots: HashMap::new(),
    }
  }

  /// 处理 Zenoh Query RPC 请求
  pub async fn handle_query(&self, query: Query) {
    let key = query.key_expr().as_str();
    log::debug!(
      "handle_query on node {} received key: {key}",
      self.node.conf.node_id
    );

    if key.ends_with("/forward") {
      self.handle_forward_query(query).await;
    } else if key.ends_with("/append_entries") {
      self.handle_append_entries_query(query).await;
    } else if key.ends_with("/vote") {
      self.handle_vote_query(query).await;
    } else if key.ends_with("/snapshot") {
      self.handle_snapshot_query(query).await;
    } else {
      log::warn!("Received unknown query on key: {key}");
      let _ = query
        .reply_err(format!("Unknown key expression: {key}"))
        .await;
    }
  }

  pub async fn handle_forward_query(&self, query: Query) {
    dispatch_rpc(&query, "Forward", |req: ForwardRequest| async {
      let res = self.node.handle_forward_request(req).await;
      let response_data: StdResult<ForwardResponse, String> = res.map_err(|e| e.to_string());
      Ok(response_data)
    })
    .await;
  }

  pub async fn handle_append_entries_query(&self, query: Query) {
    dispatch_rpc(
      &query,
      "AppendEntries",
      |req: CompactAppendEntriesRequest| async {
        let append_req: AppendEntriesRequest<TypeConfig> = req.into();
        let resp = self
          .node
          .raft()
          .append_entries(append_req)
          .await
          .map_err(|e| e.to_string())?;
        Ok(CompactAppendEntriesResponse::from(&resp))
      },
    )
    .await;
  }

  pub async fn handle_vote_query(&self, query: Query) {
    dispatch_rpc(&query, "Vote", |req: CompactVoteRequest| async {
      let is_pre_vote = req.is_pre_vote;
      let vote_req: VoteRequest<TypeConfig> = req.into();
      let resp = if is_pre_vote {
        self
          .node
          .raft()
          .pre_vote(vote_req)
          .await
          .map_err(|e| e.to_string())?
      } else {
        self
          .node
          .raft()
          .vote(vote_req)
          .await
          .map_err(|e| e.to_string())?
      };
      Ok(CompactVoteResponse::from(&resp))
    })
    .await;
  }

  pub async fn handle_snapshot_query(&self, query: Query) {
    dispatch_rpc(
      &query,
      "Snapshot",
      |req: CompactChunkSnapshotRequest| async {
        let chunk_req = ChunkSnapshotRequest::from(req);
        self
          .process_snapshot_chunk(chunk_req)
          .await
          .map_err(|e| e.to_string())
      },
    )
    .await;
  }

  async fn process_snapshot_chunk(
    &self,
    snapshot_req: ChunkSnapshotRequest,
  ) -> Result<CompactChunkSnapshotResponse, Error> {
    let vote = snapshot_req.vote;
    let snapshot_id = snapshot_req.snapshot_id.clone();
    let snapshot_meta = snapshot_req.meta.clone();
    let offset = snapshot_req.offset;
    let done = snapshot_req.done;

    self.evict_stale();
    let streaming_entry = self.ensure_streaming_entry(&snapshot_id, offset).await?;

    {
      let mut streaming = streaming_entry.lock().unwrap();
      if streaming.current_offset != offset {
        streaming
          .data
          .seek(SeekFrom::Start(offset))
          .map_err(|e| Error::internal(format!("Failed to seek: {e}")))?;
        streaming.current_offset = offset;
      }
      streaming
        .data
        .write_all(&snapshot_req.data)
        .map_err(|e| Error::internal(format!("Failed to write snapshot data: {e}")))?;
      streaming.current_offset += snapshot_req.data.len() as u64;
    }

    if done {
      let removed = {
        let pin = self.streaming_snapshots.pin();
        pin.remove(&snapshot_id).cloned()
      };
      if let Some(entry) = removed {
        let file = match Arc::try_unwrap(entry) {
          Ok(mutex) => mutex.into_inner().unwrap().data,
          Err(arc) => arc
            .lock()
            .unwrap()
            .data
            .try_clone()
            .map_err(|e| Error::internal(format!("Failed to clone snapshot file: {e}")))?,
        };
        let response = self.install_snapshot(vote, snapshot_meta, &file).await?;
        return Ok(CompactChunkSnapshotResponse {
          vote: CompactVote::from(&response.vote),
        });
      }
    }

    let current_vote = self.node.raft().metrics().borrow_watched().vote;
    Ok(CompactChunkSnapshotResponse {
      vote: CompactVote::from(&current_vote),
    })
  }

  /// 清理超时的流式快照会话
  fn evict_stale(&self) {
    let now = Instant::now();
    let pin = self.streaming_snapshots.pin();
    let stale_keys: Vec<String> = pin
      .iter()
      .filter_map(|(snapshot_id, s)| {
        if let Ok(guard) = s.try_lock()
          && now.duration_since(guard.created_at) >= STALE_SNAPSHOT_TIMEOUT
        {
          log::warn!("Evicting stale streaming snapshot session for snapshot_id={snapshot_id}");
          return Some(snapshot_id.clone());
        }
        None
      })
      .collect();
    for key in stale_keys {
      pin.remove(&key);
    }
  }

  async fn ensure_streaming_entry(
    &self,
    snapshot_id: &str,
    offset: u64,
  ) -> Result<Arc<Mutex<StreamingSnapshot>>, Error> {
    {
      let pin = self.streaming_snapshots.pin();
      if let Some(entry) = pin.get(snapshot_id) {
        return Ok(entry.clone());
      }
    }

    if offset != 0 {
      return Err(Error::internal(format!(
        "Snapshot mismatch: expected offset 0 for new snapshot {snapshot_id}, got {offset}"
      )));
    }

    let std_file = self
      .node
      .state_machine()
      .create_snapshot_temp_file(snapshot_id)
      .map_err(|e| Error::internal(format!("Failed to create snapshot temp file: {e}")))?;

    let entry = Arc::new(Mutex::new(StreamingSnapshot {
      data: std_file,
      current_offset: 0,
      created_at: Instant::now(),
    }));

    let pin = self.streaming_snapshots.pin();
    pin.insert(snapshot_id.to_string(), entry.clone());
    Ok(entry)
  }

  async fn install_snapshot(
    &self,
    vote: Vote,
    snapshot_meta: SnapshotMeta,
    file: &File,
  ) -> Result<SnapshotResponse<TypeConfig>, Error> {
    let mut snapshot_file = file
      .try_clone()
      .map_err(|e| Error::internal(format!("Failed to clone snapshot file handle: {e}")))?;
    snapshot_file
      .flush()
      .map_err(|e| Error::internal(format!("Failed to flush file: {e}")))?;
    snapshot_file
      .seek(SeekFrom::Start(0))
      .map_err(|e| Error::internal(format!("Failed to seek to start: {e}")))?;

    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: snapshot_file,
    };

    self
      .node
      .raft()
      .install_full_snapshot(vote, snapshot)
      .await
      .map_err(|e| Error::internal(format!("InstallFullSnapshot failed: {e}")))
  }
}
