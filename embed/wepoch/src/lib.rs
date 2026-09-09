#![cfg_attr(docsrs, feature(doc_cfg))]

mod entry;
mod epoch;
mod error;

pub use entry::{EpochEntry, MAX_USER_WORDS};
pub use epoch::{
  DRAIN_LIST_SIZE, EpochGuard, LightEpoch, Participant, ProtectedScope, current_thread_id,
};
pub use error::{Error, Result};
