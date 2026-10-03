#![cfg_attr(docsrs, feature(doc_cfg))]

mod entry;
mod epoch;
mod error;
mod guard;
mod participant;
mod tls;
mod wait;

pub use entry::EpochEntry;
#[cfg(not(debug_assertions))]
pub use epoch::LightEpoch;
// DRAIN_LIST_SIZE 生产面仅条目表构造内部消费，不导出；dev/test 构建按
// LightEpoch.TestHooks 同例放行，供 tests/epoch/drain.rs 压力常量取值
#[cfg(debug_assertions)]
pub use epoch::{DRAIN_LIST_SIZE, LightEpoch};
pub use error::{Error, Result};
pub use guard::EpochSuspendGuard;
pub use participant::{EpochGuard, Participant, ProtectedScope};
pub use wait::{wait_condition_async, wait_condition_sync};
