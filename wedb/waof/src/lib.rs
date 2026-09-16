#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

//! waof 内部分层对齐 C#：wal/（物理层，TsavoriteLog 对标）与 aof/（语义层，
//! GarnetAppendOnlyFile 对标）；外部 API 经根 re-export，路径保持稳定。

mod aof;

mod error;

mod wal;

pub use aof::{
  address::{AOF_ADDRESS_BYTES, AofAddress, MAX_SUBLOG_COUNT},
  args::{arg_sequence_len, decode_arg_sequence, encode_arg_sequence},
  header::{
    AofChunkHeader, AofHeader, AofHeaderType, AofShardedHeader, AofShardedLogTransactionHeader,
    AofSingleLogTransactionHeader,
  },
};
pub use error::{Error, Result};
pub use wal::{
  config::WalConfig,
  header::{RECORD_HEADER_LEN, RecordHeader},
  iterator::WalScanIterator,
  log::{WalLog, WalLogInner},
  record::WalRecord,
  ring_buffer::RingBuffer,
  sequence_number_generator::SequenceNumberGenerator,
};

/// 初始有效 AOF 地址（头区占位记录之后，64 字节）
pub const FIRST_VALID_AOF_ADDRESS: i64 = 64;
