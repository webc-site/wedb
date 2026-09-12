#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
mod index_ckpt;
mod manager;
mod meta;

pub use error::{Error, Result};
/// 恢复语义一律要求 tail 一致性截断：不带钳制的裸恢复（tail = None）会放行超出
/// 一致性点的索引条目，已收敛为 [`read_index_checkpoint_truncated`] 的显式参数，
/// 杜绝「图省事传 None」的误用面
pub use index_ckpt::{IndexCkptHeader, read_index_checkpoint_truncated, write_index_checkpoint};
pub use manager::{
  CheckpointManager, CprRecover, CprStore, RecoveredCheckpoint, next_token, next_token_above,
  take_index_checkpoint,
};
pub use meta::{
  CheckpointMeta, CheckpointType, CprPhase, FORMAT_VERSION, HlogMeta, INDEX_EXT, INDEX_PREFIX,
  INTEGRITY_FROM_VERSION, IndexMeta, META_EXT, META_PREFIX, StoreMeta, TMP_EXT, index_filename,
  index_tmp_filename, meta_filename, meta_tmp_filename, parse_token, token_to_base32,
};
