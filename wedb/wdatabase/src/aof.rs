//! 数据库 AOF 抽象接口（对标 libs/server/AOF/GarnetAppendOnlyFile.cs）

use std::{future::Future, sync::Arc};

use waof::AofAddress;
use wdev::Device;

use super::garnet_database::GarnetDatabase;

/// 数据库 AOF 日志接口（由具体 AOF 实现注入）
pub trait DatabaseAof<D: Device>: Send + Sync + Sized {
  /// AOF 当前总尺寸（字节）
  fn total_size(&self) -> i64;

  /// 安全写日志尾地址
  fn tail_address(&self) -> i64;

  /// 异步截断至指定 AOF 地址
  fn truncate_until_async<'a>(&'a self, until: &'a AofAddress) -> impl Future<Output = ()> + 'a;

  /// 物理提交刷盘
  fn commit_flush_async(&self) -> impl Future<Output = ()> + '_;

  /// 异步恢复
  fn recover_async(&self) -> impl Future<Output = ()> + '_;

  /// 是否满足截断条件
  fn can_truncate(&self) -> bool;

  /// 是否满足提交条件
  fn can_commit(&self) -> bool;

  /// 等待提交完成（已刷盘 >= 已提交）
  fn wait_for_commit(&self) -> bool;

  /// 异步等待提交落盘（事件驱动无锁挂起）
  fn wait_for_commit_async(&self, until_address: i64) -> impl Future<Output = ()> + '_;

  /// 安全刷 AOF 尾地址
  fn safe_flush_address(&self) -> Option<u64>;

  /// 重放 AOF 条目至指定地址
  fn replay_database_aof<'a>(
    self: Arc<Self>,
    db: &'a GarnetDatabase<D, Self>,
    until: u64,
  ) -> impl Future<Output = wkv::Result<u64>> + 'a;
}

impl<D: Device> DatabaseAof<D> for () {
  #[inline]
  fn total_size(&self) -> i64 {
    0
  }

  #[inline]
  fn tail_address(&self) -> i64 {
    0
  }

  #[inline]
  async fn truncate_until_async<'a>(&'a self, _until: &'a AofAddress) {}

  #[inline]
  async fn commit_flush_async(&self) {}

  #[inline]
  async fn recover_async(&self) {}

  #[inline]
  fn can_truncate(&self) -> bool {
    false
  }

  #[inline]
  fn can_commit(&self) -> bool {
    false
  }

  #[inline]
  fn wait_for_commit(&self) -> bool {
    true
  }

  #[inline]
  async fn wait_for_commit_async(&self, _until_address: i64) {}

  #[inline]
  fn safe_flush_address(&self) -> Option<u64> {
    None
  }

  #[inline]
  async fn replay_database_aof(
    self: Arc<Self>,
    _db: &GarnetDatabase<D, Self>,
    _until: u64,
  ) -> wkv::Result<u64> {
    Ok(0)
  }
}
