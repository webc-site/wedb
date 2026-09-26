#![cfg_attr(docsrs, feature(doc_cfg))]

mod entry;
mod epoch;
mod error;
mod guard;
mod participant;
mod tls;
mod wait;

pub use entry::EpochEntry;
pub use epoch::{DRAIN_LIST_SIZE, LightEpoch};
pub use error::{Error, Result};
pub use guard::EpochSuspendGuard;
pub use participant::{EpochGuard, Participant, ProtectedScope};
pub use wait::{wait_condition_async, wait_condition_sync};
