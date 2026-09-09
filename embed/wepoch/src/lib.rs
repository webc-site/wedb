#![cfg_attr(docsrs, feature(doc_cfg))]

mod entry;
mod epoch;
mod error;
mod participant;
mod tls;

pub use entry::{EpochEntry, MAX_USER_WORDS};
pub use epoch::{DRAIN_LIST_SIZE, LightEpoch};
pub use error::{Error, Result};
pub use participant::{EpochGuard, Participant, ProtectedScope};
pub use wbase::thread::current_thread_id;
