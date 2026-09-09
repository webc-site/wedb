#![allow(clippy::empty_line_after_doc_comments, clippy::empty_line_after_outer_attr, clippy::doc_lazy_continuation)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod address;
mod buffer;
mod config;
mod error;
mod flush;
mod hlog;
mod output;
mod scan;

pub use address::{AddressManager, AddressSnapshot};
pub use buffer::CircularPageBuffer;
pub use config::{
  DEFAULT_INITIAL_ADDRESS, DEFAULT_MUTABLE_FRACTION, DEFAULT_NUM_PAGES, DEFAULT_PAGE_SIZE,
  HybridLogConfig, SECTOR_ALIGNMENT, ro_lag_num_from_fraction,
};
pub use error::{Error, Result};
pub use flush::{PageFlushRange, PendingFlushList};
pub use hlog::{HybridLog, PAD_KEY_LEN};
pub use output::RecordOutput;
pub use scan::ScanIterator;
