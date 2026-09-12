#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

pub mod address;
mod config;
mod disk_window;
pub mod entry_type;
mod error;
mod header;
mod iterator;
mod log;
mod record;
mod ring_buffer;

pub use address::{AOF_ADDRESS_BYTES, AofAddress, MAX_SUBLOG_COUNT};
pub use config::WalConfig;
pub use entry_type::AofEntryType;
pub use error::{Error, Result};
pub use header::{RECORD_HEADER_LEN, RecordHeader};
pub use iterator::WalScanIterator;
pub use log::{WalLog, WalLogInner};
pub use record::WalRecord;
pub use ring_buffer::RingBuffer;

/// 初始有效 AOF 地址（头区占位记录之后，64 字节）
pub const FIRST_VALID_AOF_ADDRESS: i64 = 64;
