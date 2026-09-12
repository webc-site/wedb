//! storeWrapper 提交标记写入通道（对标 libs/server/StoreWrapper.cs:EnqueueCommit）

use std::sync::Arc;

use parking_lot::Mutex;
use waof::AofEntryType;

/// storeWrapper 提交标记写入面
pub trait StoreCommitFace: Send + Sync {
  /// 追加一条提交标记到 AOF
  fn enqueue_commit(&self, entry_type: AofEntryType, version: i64);
}

/// storeWrapper 提交标记具体通道（消除 dyn）
#[derive(Clone)]
pub enum StoreCommitChannel {
  /// 内存记录器
  Recorder(Arc<Mutex<Vec<(AofEntryType, i64)>>>),
  /// 函数指针回调
  Fn(fn(AofEntryType, i64)),
}

impl StoreCommitFace for StoreCommitChannel {
  fn enqueue_commit(&self, entry_type: AofEntryType, version: i64) {
    match self {
      Self::Recorder(records) => {
        records.lock().push((entry_type, version));
      }
      Self::Fn(f) => f(entry_type, version),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use parking_lot::Mutex;
  use waof::AofEntryType;

  use super::*;

  #[test]
  fn test_store_commit_channel() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let channel = StoreCommitChannel::Recorder(records.clone());
    channel.enqueue_commit(AofEntryType::CheckpointStartCommit, 42);
    channel.enqueue_commit(AofEntryType::CheckpointEndCommit, 42);
    let guard = records.lock();
    assert_eq!(guard.len(), 2);
    assert_eq!(guard[0], (AofEntryType::CheckpointStartCommit, 42));
    assert_eq!(guard[1], (AofEntryType::CheckpointEndCommit, 42));
  }
}
