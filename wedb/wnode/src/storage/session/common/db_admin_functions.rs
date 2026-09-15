//! 数据库管理命令的存储执行段（FLUSHDB 清库；SWAPDB 内核在 wkv swap 域）
//!
//! 对标 C# DatabaseManagerBase 清库族（FlushDatabase 单库清空，rust 真身映射在 wkv flush_database）/
//! MultiDatabaseManager.cs:TrySwapDatabases 在 rust 共享存储模型下的执行内核：
//! C# 两库为独立 Tsavorite 实例（交换容器指针即完成 SWAPDB，清库走
//! `Store.Log.ShiftBeginAddress(TailAddress)` 整段截断）；rust 单存储以
//! `[NsVarint][DbVarint][Tag]` 前缀物理隔离各库，旁路结构（wbftree 独立
//! 树文件、TTL 记录表）无法随整段截断释放，故清库为逐键完整删除
//! （版本栅栏失效 + 严格删空生命周期），交换为 wkv
//! `StoreSession::swap_databases` 的全 tag 搬移内核。

use gxhash::HashSet as GxHashSet;
use wdev::Device;
use wkv::StoreSession;

use super::super::storage_session::StorageSession;
use crate::storage::session::common::array_key_iteration_functions::{
  TAG_ENVELOPE, TAG_META, TAG_STRING, scan_err,
};

/// 按标签扫描会话当前库用户键（去重；`want_tag = u8::MAX` 收 String + Meta +
/// ObjectEnvelope 三类用户面记录）
///
/// 单一定义供清库（FLUSHDB / 双库清空段）共用：同键多历史版本去重后仅
/// 保留一份，逐键删除以最新态为准（经哈希索引，免疫历史版本重复）
async fn scan_keys_by_tag<D: Device>(
  session: &StoreSession<D>,
  want_tag: u8,
) -> wkv::Result<Vec<Vec<u8>>> {
  let prefix = session.session_prefix();
  let prefix_slice = prefix.as_slice();
  let mut keys: GxHashSet<Vec<u8>> = GxHashSet::default();
  session
    .store
    .hlog()
    .scan(
      session.store.begin_address(),
      session.store.tail_address(),
      |_addr, rec| {
        let key = rec.key();
        if let Some(rest) = key.strip_prefix(prefix_slice)
          && !rec.is_tombstone()
          && let Some((&tag, user_key)) = rest.split_first()
          && (want_tag == u8::MAX || tag == want_tag)
          && matches!(tag, TAG_STRING | TAG_META | TAG_ENVELOPE)
        {
          keys.insert(user_key.to_vec());
        }
        Ok(true)
      },
    )
    .await
    .map_err(scan_err)?;
  Ok(keys.into_iter().collect())
}

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 删除当前库全部用户键（String / Meta / ObjectEnvelope），返回删除键数
  ///
  /// FLUSHDB / FLUSHALL / CLUSTER RESET HARD 的清库执行段。逐键走完整
  /// 异步删除（`StoreSession::delete`：随键 TTL 清理 + 集合 Meta 版本栅栏
  /// 秒删 + wbftree 树文件排空释放 + 对象信封双域删除）。
  ///
  /// C# 对应面为 DatabaseManagerBase 清库族（结构性映射在 wkv flush_database）
  pub async fn delete_all_user_keys(&self) -> wkv::Result<usize> {
    let keys = scan_keys_by_tag(self.batch.session, u8::MAX).await?;
    let mut deleted = 0usize;
    for key in keys {
      if self.batch.delete(&key).await? {
        deleted += 1;
      }
    }
    Ok(deleted)
  }
}
