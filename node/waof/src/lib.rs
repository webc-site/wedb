#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

mod config;
mod disk_window;
mod error;
mod header;
mod iterator;
mod log;
mod record;
mod ring_buffer;

pub use config::WalConfig;
pub use error::{Error, Result};
pub use header::{RECORD_HEADER_LEN, RecordHeader};
pub use iterator::WalScanIterator;
pub use log::{WalLog, WalLogInner};
pub use record::WalRecord;
pub use ring_buffer::RingBuffer;
