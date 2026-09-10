//! 主存 pending 完成器（对标 libs/server/Storage/Session/MainStore/CompletePending.cs）

use wdev::Device;

use super::super::storage_session::StorageSession;

impl<'a, D: Device> StorageSession<'a, D> {
  /// 完成当前会话所有挂起的存储操作
  ///
  /// 缺口说明：C# 侧自旋等待 Tsavorite 异步 IO 队列闭环；wkv 的所有读写调用
  /// 均在调用内同步闭环（磁盘候选经内置异步路径完成），跨调用不遗留 pending，
  /// 本方法退化为一致性空操作并返回 true（无遗留挂起）。
  ///
  /// libs/server/Storage/Session/MainStore/CompletePending.cs:CompletePendingForSession
  pub fn complete_pending_for_session(&self) -> bool {
    true
  }
}
