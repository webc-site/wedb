#![cfg_attr(docsrs, feature(doc_cfg))]

mod entry;
mod epoch;
mod error;
mod guard;
mod participant;
mod tls;

pub use entry::EpochEntry;
pub use epoch::{DRAIN_LIST_SIZE, LightEpoch};
pub use error::{Error, Result};
pub use guard::EpochSuspendGuard;
pub use participant::{EpochGuard, Participant, ProtectedScope};

/// 纪元访问接口（严格对标 libs/storage/Tsavorite/cs/src/core/Epochs/IEpochAccessor.cs:IEpochAccessor）
pub trait EpochAccessor {
  /// 尝试挂起纪元保护（若当前受保护返回 true，否则 false）
  fn try_suspend(&self) -> bool;

  /// 恢复纪元保护
  fn resume(&self);
}
