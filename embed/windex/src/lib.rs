#![cfg_attr(docsrs, feature(doc_cfg))]

mod bucket;
mod entry;
mod error;
mod overflow_pool;
mod table;

pub use bucket::{
  BucketExclusiveGuard, BucketSharedGuard, DATA_ENTRIES, ENTRIES_PER_BUCKET, HashBucket,
  OVERFLOW_INDEX,
};
pub use entry::HashBucketEntry;
pub use error::{Error, Result};
pub use overflow_pool::OverflowPool;
pub use table::{CandidateAddresses, HashEntryInfo, HashIndex, MultiBucketGuard, prefetch_read_l1};
