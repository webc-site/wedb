//! 数据库 AOF 抽象接口（对标 libs/server/AOF/GarnetAppendOnlyFile.cs）

use std::{future::Future, sync::Arc};

use waof::AofAddress;
use wdev::Device;

use super::garnet_database::GarnetDatabase;

/// 数据库 AOF 日志接口（由具体 AOF 实现注入）
pub trait DatabaseAof<D: Device>: Send + Sync {
  /// AOF 当前总尺寸（字节）
  fn total_size(&self) -> i64;

  /// 安全写日志尾地址
  fn tail_address(&self) -> i64;

  /// 异步截断至指定 AOF 地址
  fn truncate_until_async<'a>(
    &'a self,
    until: &'a AofAddress,
  ) -> impl Future<Output = ()> + 'a;

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

  /// 安全刷 AOF 尾地址
  fn safe_flush_address(&self) -> Option<u64>;

  /// 重放 AOF 条目至指定地址
  fn replay_database_aof<'a>(
    self: Arc<Self>,
    db: &'a GarnetDatabase<D>,
    until: u64,
  ) -> impl Future<Output = wkv::Result<u64>> + 'a;
}
