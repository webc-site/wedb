use bytes::Bytes;

use crate::types::{SnapshotMeta, Vote};

#[derive(Debug, Clone)]
pub struct ChunkSnapshotRequest {
  pub vote: Vote,
  pub snapshot_id: String,
  pub meta: SnapshotMeta,
  pub offset: u64,
  pub data: Bytes,
  pub done: bool,
}

#[derive(Debug, Clone)]
pub struct ChunkSnapshotResponse {
  pub vote: Vote,
}
