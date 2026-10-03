//! DbMeta 换号记录持久化面（doc/zh/db.md「即时原子提交」写面单点族）
//!
//! 全部走换号写路径单点：固定根域前缀 (ns 0, db 0)，键载荷与值经
//! [`DbMetaRecord`] 单点编解码，与 flush/swap 持久化、GC 墓碑删除及点查装载
//! 同一物理布局（doc/zh/db.md 1.4 与「即时原子提交」段）。

use wdev::Device;
use wval::KeyTag;

use crate::{
  error::Result,
  session::StoreSession,
  vdb::{DbMetaRecord, ROOT_DBMETA_PREFIX},
};

impl<D: Device> StoreSession<D> {
  /// DbMeta 换号批同步提交内核（固定根域前缀，批内按 safe-order 单点落盘）
  ///
  /// 与 [`Self::persist_dbmeta_batch`] 同键布局；返回需降级异步回放的项下标：
  /// 首条记录即遭遇环形页翻转（整批零写入落地），或第 k 条翻页（前 k 条已
  /// 落地），从 k 起其后全部记录一并转异步回放，批内相对顺序与 safe-order
  /// 一致（绝不让旧下标记录越过降级点先行落盘）。引擎硬错误经 `?` 传播：
  /// 此前已落地的批前缀不回滚，崩溃前缀语义保证最坏旧域泄漏，绝不复活撞号
  pub fn try_persist_dbmeta_sync(&self, items: &[Option<DbMetaRecord>]) -> Result<Vec<usize>> {
    let _guard = self.enter_gated();
    let mut degraded = Vec::new();
    for (idx, rec) in items.iter().enumerate() {
      let Some(rec) = rec else { continue };
      // 一旦有记录降级，其后记录直接并入回放序列，保持批内落盘顺序
      if !degraded.is_empty() {
        degraded.push(idx);
        continue;
      }
      match self.try_upsert_tag_sync_unprotected_with_prefix(
        ROOT_DBMETA_PREFIX.as_slice(),
        rec.key().as_slice(),
        KeyTag::DbMeta,
        rec.value().as_slice(),
      )? {
        Ok(_address) => {}
        Err(_page_id) => degraded.push(idx),
      }
    }
    Ok(degraded)
  }

  /// 原子批持久化 DbMeta 换号记录（doc/zh/db.md「即时原子提交」写面单点）
  ///
  /// items 顺序即落盘 safe-order（新映射 → 旧域退役墓碑 → 分配水位 0x05），
  /// 批内不合并不重排。换号事务全程持 [`crate::store::WedbStore::lock_dbmeta`]，杜绝并发
  /// 换号事务的记录流交错。同步快路径遇页翻转即降级异步回放，回放逐项 await
  /// 完成后才向调用方返回，命令应答即含全部记录持久化承诺
  pub async fn persist_dbmeta_batch(&self, items: &[Option<DbMetaRecord>]) -> Result<()> {
    let degraded = self.try_persist_dbmeta_sync(items)?;
    if degraded.is_empty() {
      return Ok(());
    }
    log::warn!(
      "DbMeta 换号批同步快路径翻页，{} 项降级异步回放",
      degraded.len()
    );
    for idx in degraded {
      let Some(rec) = items[idx].as_ref() else {
        continue;
      };
      let rec_k = Self::session_tag_key_with_prefix(
        ROOT_DBMETA_PREFIX.as_slice(),
        KeyTag::DbMeta,
        rec.key().as_slice(),
      );
      self
        .upsert_raw(rec_k.as_slice(), rec.value().as_slice())
        .await?;
    }
    Ok(())
  }

  /// 持久化单条 DbMeta 系统元数据（原子批退化形态，safe-order 仅剩本条）
  pub async fn persist_dbmeta(&self, rec: &DbMetaRecord) -> Result<()> {
    self.persist_dbmeta_batch(&[Some(*rec)]).await
  }

  /// 在 min_tail 或其后持久化单条 DbMeta 系统元数据（跳过原位更新，FLUSHALL 重挂专用）
  pub async fn persist_dbmeta_tail(&self, rec: &DbMetaRecord, min_tail: u64) -> Result<()> {
    let rec_k = Self::session_tag_key_with_prefix(
      ROOT_DBMETA_PREFIX.as_slice(),
      KeyTag::DbMeta,
      rec.key().as_slice(),
    );
    self
      .upsert_raw_tail(rec_k.as_slice(), rec.value().as_slice(), min_tail)
      .await?;
    Ok(())
  }

  /// 物理删除 DbMeta 系统元数据（快路径优先同步删除，翻页/等待时自动回退异步删除）
  ///
  /// 删除对象固定根域前缀（与 persist 批同布局），不随会话活跃上下文漂移
  pub async fn delete_dbmeta(&self, key: &[u8]) -> Result<()> {
    let deleted = {
      let _guard = self.enter_gated();
      self.try_delete_tag_sync_unprotected_with_prefix(
        ROOT_DBMETA_PREFIX.as_slice(),
        key,
        KeyTag::DbMeta,
      )?
    };
    if deleted.is_err() {
      let rec_k =
        Self::session_tag_key_with_prefix(ROOT_DBMETA_PREFIX.as_slice(), KeyTag::DbMeta, key);
      self.delete_raw(&rec_k).await?;
    }
    Ok(())
  }
}
