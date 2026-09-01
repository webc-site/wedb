use std::io::Result;

use zenoh_raft::alias::{LogIdOf, VoteOf};

use super::compact::{CompactLogId, CompactVote};
use super::encoder::{decode, encode};
use super::raft_type::TypeConfig;

pub trait RaftCodec: Sized {
  fn encode_to(&self) -> Result<Vec<u8>>;
  fn decode_from(buf: &[u8]) -> Result<Self>;
}

impl RaftCodec for LogIdOf<TypeConfig> {
  fn encode_to(&self) -> Result<Vec<u8>> {
    let compact = CompactLogId::from(self);
    encode(&compact)
  }

  fn decode_from(buf: &[u8]) -> Result<Self> {
    let compact: CompactLogId = decode(buf)?;
    Ok(compact.into())
  }
}

impl RaftCodec for VoteOf<TypeConfig> {
  fn encode_to(&self) -> Result<Vec<u8>> {
    let compact = CompactVote::from(self);
    encode(&compact)
  }

  fn decode_from(buf: &[u8]) -> Result<Self> {
    let compact: CompactVote = decode(buf)?;
    Ok(compact.into())
  }
}
