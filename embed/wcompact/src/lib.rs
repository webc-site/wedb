#![cfg_attr(docsrs, feature(doc_cfg))]

mod compactor;
pub mod error;
pub mod host;

pub use compactor::{CompactionStats, CompactionType, LogCompactor};
pub use error::{Error, Result};
pub use host::{CompactSession, CompactStore, TTL_VALUE_LEN};
