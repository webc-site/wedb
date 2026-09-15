#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

mod address;
mod args;
mod config;
mod disk_window;
mod error;
mod header;
mod iterator;
mod log;
mod record;
mod ring_buffer;
mod sequence_number_generator;

pub use address::{AOF_ADDRESS_BYTES, AofAddress, MAX_SUBLOG_COUNT};
pub use args::{arg_sequence_len, decode_arg_sequence, encode_arg_sequence};
pub use config::WalConfig;
pub use error::{Error, Result};
pub use header::{
  AofChunkHeader, AofHeader, AofHeaderType, AofShardedHeader, AofShardedLogTransactionHeader,
  AofSingleLogTransactionHeader, RECORD_HEADER_LEN, RecordHeader,
};
pub use iterator::WalScanIterator;
pub use log::{WalLog, WalLogInner};
pub use record::WalRecord;
pub use ring_buffer::RingBuffer;
pub use sequence_number_generator::SequenceNumberGenerator;

/// 初始有效 AOF 地址（头区占位记录之后，64 字节）
pub const FIRST_VALID_AOF_ADDRESS: i64 = 64;
