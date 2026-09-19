//! 数据库管理命令的存储执行段（FLUSHDB 清库；SWAPDB 内核在 wkv swap 域）
//!
//! 对标 C# DatabaseManagerBase 清库族（FlushDatabase 单库清空，rust 真身映射在 wkv flush_database）/
//! MultiDatabaseManager.cs:TrySwapDatabases 在 rust 共享存储模型下的执行内核：
//! C# 两库为独立 Tsavorite 实例（交换容器指针即完成 SWAPDB，清库走
//! `Store.Log.ShiftBeginAddress(TailAddress)` 整段截断）；rust 单存储以
//! `[NsVarint][DbVarint][Tag]` 前缀物理隔离各库，清库为 O(1) 虚拟 ID 换号
//! 秒清（旧前缀整体失效，旁路结构由延时 GC 与紧缩异步回收），交换为 wkv
//! `StoreSession::swap_databases` 的全 tag 搬移内核。

use wdev::Device;

use super::super::storage_session::StorageSession;

impl<'a, D: Device> StorageSession<'a, D> {
  /// 删除当前库全部用户键（O(1) 虚拟 ID 换号秒清）
  ///
  /// FLUSHDB / CLUSTER RESET HARD 的清库执行段：wkv O(1) 虚拟 ID 换号秒清
  /// （分配新虚拟号原子替换路由快照，旧号入延时 GC 队列由后台紧缩物理回收，
  /// 绝无逐键扫描），随键 TTL / ETag 旁路记录与集合、对象信封域一并随前缀
  /// 失效；映射变更经 KeyTag::DbMeta 元记录落盘供重启重建。
  ///
  /// 广播归属：RESP 面 FLUSHDB 的清库唯一入口在
  /// [`SingleDatabaseManager::flush_database`](crate::database::SingleDatabaseManager::flush_database)
  /// （换号 + FlushDb 广播条目，慢路径 `StoreGarnetApi::flush_command_slow`
  /// 承接），本函数不复制该漏斗；CLUSTER RESET HARD 经此清库对译 C#
  /// ClusterProvider.cs:FlushDB（`store.Log.ShiftBeginAddress` 节点本地截断，
  /// 无 SafeFlushAOF 广播段）——重置是本节点身份的自清动作，副本/他节点
  /// 不因一次 RESET 换号，故此处按标签只落 DbMeta 映射、不入日志。
  ///
  /// C# 对应面为 DatabaseManagerBase 清库族（结构性映射在 wkv flush_database）
  pub async fn delete_all_user_keys(&self) -> wkv::Result<usize> {
    let ns = self.batch.session.namespace();
    let db = self.batch.session.active_db();
    self.batch.store.flush_database(ns, db).await?;
    // 刷新会话缓存（以拾取最新的 virtual DB ID）
    self.batch.session.set_context(ns, db);
    Ok(0)
  }
}
