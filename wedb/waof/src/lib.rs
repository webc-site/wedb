#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

//! waof 内部分层对齐 C#：wal/（物理层，TsavoriteLog 对标）与 aof/（语义层，
//! GarnetAppendOnlyFile 对标）；外部 API 经根 re-export，路径保持稳定。
//!
//! 与 whlog `HybridLog` 的分工边界（双日志分层，非重复实现）：本 crate wal/ 是
//! **物理 WAL**（先写日志、按事务提交序定水位，对标 C# Tsavorite TsavoriteLog 的
//! CommitAsync 提交流水）；whlog 是**混合日志**（存储引擎主日志，页缓冲 + 纪元驱逐，
//! 对标 C# AllocatorBase 的 AsyncFlushPagesForSnapshot 页刷盘流水）。二者同为环形页
//! 缓冲 + 刷盘流水线形态但职责不重叠，共用的刷盘/截断内核单点在 `wdev::Device`
//! （`flush_range_aligned` 与 `truncate_begin_until`），两侧均经此落盘，不重复下沉。

mod aof;

mod error;

mod wal;

pub use aof::{
  address::{AOF_ADDRESS_BYTES, AofAddress, MAX_SUBLOG_COUNT},
  args::{arg_sequence_len, decode_arg_sequence, decode_arg_slices, encode_arg_sequence},
  entry_type::AofEntryType,
  header::{
    AofChunkHeader, AofHeader, AofHeaderType, AofShardedHeader, AofShardedLogTransactionHeader,
    AofSingleLogTransactionHeader,
  },
};
pub use error::{Error, Result};
pub use wal::{
  commit::{COMMIT_FRAME_TOTAL_LEN, NO_COOKIE, is_commit_frame},
  config::{FsyncPolicy, WalConfig},
  header::{RECORD_HEADER_LEN, WalFrameHeader},
  iterator::WalScanIterator,
  log::{WalLog, WalLogInner},
  record::WalRecord,
  ring_buffer::RingBuffer,
  sequence_number_generator::SequenceNumberGenerator,
};
