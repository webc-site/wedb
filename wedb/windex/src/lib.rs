#![cfg_attr(docsrs, feature(doc_cfg))]

mod bucket;
mod buckets;
mod candidate;
mod chain;
mod entry;
mod entry_info;
mod error;
mod overflow_pool;
mod prefetch;
pub mod ram;
pub mod split;
mod table;

pub use bucket::{
  BucketExclusiveGuard, BucketSharedGuard, DATA_ENTRIES, ENTRIES_PER_BUCKET, HashBucket, KeyLatch,
  OVERFLOW_INDEX,
};
pub use buckets::HashBuckets;
pub use candidate::{CandidateAddresses, CandidateAddressesIntoIter};
pub use entry::HashBucketEntry;
pub use entry_info::HashEntryInfo;
pub use error::{Error, Result};
pub use overflow_pool::OverflowPool;
pub use prefetch::{PREFETCH_WINDOW, prefetch_read_l1};
pub use split::{
  CHUNK_BITS, CHUNK_SIZE, SPLIT_COMPLETED, SPLIT_IN_PROGRESS, SPLIT_UNSTARTED, chunk_count,
  chunk_offset_for_hash, split_chunk, split_single_bucket,
};
pub use table::{HashIndex, PrefetchProbe};
