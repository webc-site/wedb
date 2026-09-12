//! 存储 API（对标 libs/server/Servers/StoreApi.cs:StoreApi）
//!
//! 单机模式下直接委托存储引擎执行 AOF 提交与数据库清理。

use std::{future::Future, sync::Arc};

/// 存储 API 抽象面（C# StoreWrapper.WaitForCommitAsync / CommitAOFAsync / FlushDatabase 的本域投影）
pub trait StoreApiFace: Send + Sync {
  /// 等待 AOF 提交（false = 提交被配置忽略）
  fn wait_for_commit(&self) -> bool;
  /// 提交 AOF（刷盘 + 推进提交地址）
  fn commit_aof(&self, db_id: i64) -> wkv::Result<()>;
  /// 清库（unsafe_truncate_log 为破坏性日志截断）
  fn flush_database(
    &self,
    unsafe_truncate_log: bool,
    db_id: i64,
  ) -> impl Future<Output = wkv::Result<()>> + Send;
}

impl<T: StoreApiFace + ?Sized> StoreApiFace for Arc<T> {
  fn wait_for_commit(&self) -> bool {
    (**self).wait_for_commit()
  }
  fn commit_aof(&self, db_id: i64) -> wkv::Result<()> {
    (**self).commit_aof(db_id)
  }
  fn flush_database(
    &self,
    unsafe_truncate_log: bool,
    db_id: i64,
  ) -> impl Future<Output = wkv::Result<()>> + Send {
    (**self).flush_database(unsafe_truncate_log, db_id)
  }
}

/// 单机存储 API 门面
pub struct StoreApi<S> {
  store: S,
}

impl<S: StoreApiFace> StoreApi<S> {
  /// 构造单机存储 API 门面
  pub fn new(store: S) -> Self {
    Self { store }
  }

  /// 等待 AOF 提交
  pub fn wait_for_commit(&self) -> bool {
    self.store.wait_for_commit()
  }

  /// 提交 AOF（刷盘 + 推进提交地址）
  pub fn commit_aof(&self, db_id: i64) -> wkv::Result<bool> {
    self.store.commit_aof(db_id).map(|()| true)
  }

  /// 清库（删除全部键；unsafe_truncate_log 为破坏性日志截断）
  ///
  /// libs/server/Servers/StoreApi.cs:FlushDB
  pub async fn flush_db(&self, db_id: i64, unsafe_truncate_log: bool) -> wkv::Result<bool> {
    self
      .store
      .flush_database(unsafe_truncate_log, db_id)
      .await
      .map(|()| true)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::{AtomicBool, Ordering};

  use compio::runtime::Runtime;

  use super::*;

  /// 记录提交调用的测试存储面
  struct MockStore {
    waited: AtomicBool,
    committed: AtomicBool,
    flushed: AtomicBool,
  }

  impl MockStore {
    fn new() -> Arc<Self> {
      Arc::new(Self {
        waited: AtomicBool::new(false),
        committed: AtomicBool::new(false),
        flushed: AtomicBool::new(false),
      })
    }
  }

  impl StoreApiFace for MockStore {
    fn wait_for_commit(&self) -> bool {
      self.waited.store(true, Ordering::SeqCst);
      true
    }
    // 满足 StoreApiFace trait 契约保留
    fn commit_aof(&self, _db_id: i64) -> wkv::Result<()> {
      self.committed.store(true, Ordering::SeqCst);
      Ok(())
    }
    // 满足 StoreApiFace trait 契约保留
    async fn flush_database(&self, _unsafe_truncate_log: bool, _db_id: i64) -> wkv::Result<()> {
      self.flushed.store(true, Ordering::SeqCst);
      Ok(())
    }
  }

  #[test]
  fn standalone_commit_and_flush() {
    let store = MockStore::new();
    let api = StoreApi::new(store.clone());

    assert!(api.wait_for_commit());
    assert!(store.waited.load(Ordering::SeqCst));

    assert!(api.commit_aof(0).expect("提交成功"));
    assert!(store.committed.load(Ordering::SeqCst));

    let rt = Runtime::new().expect("运行时");
    rt.block_on(async {
      assert!(api.flush_db(0, false).await.expect("清库成功"));
      assert!(store.flushed.load(Ordering::SeqCst));
    });
  }
}
