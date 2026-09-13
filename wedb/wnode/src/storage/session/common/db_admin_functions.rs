//! 数据库管理命令的存储执行段（FLUSHDB / SWAPDB 清库与跨库交换）
//!
//! 对标 libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase /
//! MultiDatabaseManager.cs:TrySwapDatabases 在 rust 共享存储模型下的执行内核：
//! C# 两库为独立 Tsavorite 实例（交换容器指针即完成 SWAPDB，清库走
//! `Store.Log.ShiftBeginAddress(TailAddress)` 整段截断）；rust 单存储以
//! `[NsVarint][DbVarint][Tag]` 前缀物理隔离各库，旁路结构（wbftree 独立
//! 树文件、TTL 记录表）无法随整段截断释放，故清库为逐键完整删除
//! （版本栅栏失效 + 严格删空生命周期），交换为前缀重写搬移。

use std::sync::Arc;

use gxhash::HashSet as GxHashSet;
use wdev::Device;
use wkv::{StoreSession, WedbStore};

use super::super::storage_session::StorageSession;
use crate::storage::session::common::array_key_iteration_functions::{
  TAG_META, TAG_STRING, scan_err,
};

/// 按标签扫描会话当前库用户键（去重；`want_tag = u8::MAX` 收 String + Meta 两类）
///
/// 单一定义供清库（FLUSHDB / 双库清空段）与快照收集（SWAPDB 字符串面）共用：
/// 同键多版本首遇即最新（后写先遇），GxHashSet 去重杜绝旧版本重复入清单
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
          && matches!(tag, TAG_STRING | TAG_META)
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
  /// 删除当前库全部用户键（String / Meta），返回删除键数
  ///
  /// FLUSHDB / FLUSHALL / CLUSTER RESET HARD 的清库执行段。逐键走完整
  /// 异步删除（`StoreSession::delete`：随键 TTL 清理 + 集合 Meta 版本栅栏
  /// 秒删 + wbftree 树文件排空释放）。
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase
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

/// 库键值快照条目：(用户键, 值, 随键 TTL ticks)
type DbSnapshot = Vec<(Vec<u8>, Vec<u8>, Option<i64>)>;

/// 跨库键值交换（SWAPDB 搬移核心，共享存储模型）
///
/// 对标 C# MultiDatabaseManager.TrySwapDatabases 的数据面等价实现：全量
/// 收集两库字符串键值与随键 TTL → 双库逐键完整删除 → 交叉重写并重放
/// TTL（原库 1 数据落库 2，反之亦然）。集合 Meta 键不搬移（其 key_id 为
/// 物理地址衍生，跨库重写会致栅栏寻址错乱，交换中直接删除——与
/// `MultiDatabaseManager::try_swap_databases` 的字符串面口径一致）。
/// 使用独立后台会话（ns/db 前缀显式驱动），不动调用方会话上下文。
///
/// 返回 false = 任一存储操作失败（C# TrySwapDatabases 失败口径）。
///
/// libs/server/Databases/MultiDatabaseManager.cs:TrySwapDatabases
pub async fn swap_db_keys<D: Device>(
  store: &Arc<WedbStore<D>>,
  ns: u64,
  db_id1: i64,
  db_id2: i64,
) -> bool {
  let Ok(session) = store.new_session() else {
    return false;
  };
  let snap1 = collect_db_snapshot(&session, ns, db_id1).await;
  let snap2 = collect_db_snapshot(&session, ns, db_id2).await;
  // 快照在握后清空两库（含 Meta 与旁路子键的完整删除）
  for db_id in [db_id1, db_id2] {
    session.set_context(ns, db_id.max(0) as u64);
    let Ok(keys) = scan_keys_by_tag(&session, u8::MAX).await else {
      return false;
    };
    for key in keys {
      if session.delete(&key).await.is_err() {
        return false;
      }
    }
  }
  // 交叉重写：库 1 快照落库 2，库 2 快照落库 1（C# 交换 GarnetDatabase
  // 容器指针的等价数据面）
  for (db_id, snap) in [(db_id2, snap1), (db_id1, snap2)] {
    session.set_context(ns, db_id.max(0) as u64);
    for (key, value, ttl) in snap {
      if session.upsert(&key, &value).await.is_err() {
        return false;
      }
      if let Some(ticks) = ttl
        && session.put_ttl(&key, ticks).await.is_err()
      {
        return false;
      }
    }
  }
  true
}

/// 收集指定库字符串键值与随键 TTL（C# 数据面交换的收集段）
async fn collect_db_snapshot<D: Device>(
  session: &StoreSession<D>,
  ns: u64,
  db_id: i64,
) -> DbSnapshot {
  session.set_context(ns, db_id.max(0) as u64);
  let Ok(keys) = scan_keys_by_tag(session, TAG_STRING).await else {
    return Vec::new();
  };
  let mut snap = Vec::with_capacity(keys.len());
  for key in keys {
    let Ok(Some(value)) = session.read(&key).await else {
      continue;
    };
    let ttl = session.ttl_of(&key).await.ok().flatten();
    snap.push((key, value, ttl));
  }
  snap
}
