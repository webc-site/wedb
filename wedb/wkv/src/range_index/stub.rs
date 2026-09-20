use std::{result::Result as StdResult, sync::Arc};

use wbftree::{BfTreeService, RANGE_INDEX_STUB_SIZE, RangeIndexStub};
use wdev::Device;
use whasher::fast_hash;
use wval::{GarnetObjectType, KeyTag, META_VALUE_SIZE, MetaValue};

use super::{
  RangeIndexError, TreeReadGuard, TreeWriteGuard, encode_meta_stub_record, range_index_blocking,
  wait_tree_checkpoint,
};
use crate::{
  error::{Error, Result},
  session::StoreSession,
};

#[inline]
pub fn rebind_stub(stub: &mut RangeIndexStub, tree: &BfTreeService) {
  stub.tree_handle = tree.native_ptr();
  stub.reset_flags();
}

impl<D: Device> StoreSession<D> {
  pub async fn save_bftree_meta_stub(
    &self,
    key: &[u8],
    meta: &MetaValue,
    stub: &RangeIndexStub,
  ) -> Result<()> {
    let meta_k = self.session_meta_key(key);
    let val = encode_meta_stub_record(meta, stub);
    self.upsert_raw(&meta_k, &val).await?;
    Ok(())
  }

  /// 读取任意集合（RangeIndex 或 FlattenedTree 集合）存根及元数据
  pub async fn load_collection_stub(
    &self,
    key: &[u8],
  ) -> Result<Option<(MetaValue, RangeIndexStub)>> {
    let meta_k = self.session_meta_key(key);
    // RENAME 迁移 claim 判定：迁移窗内显式拒绝（MigrationBusy 锁忙/重试语义，
    // 客户端重试即收敛）。禁「视同不存在」穿透——旧键迁移中读旧影尚可容忍，
    // 但穿透会让分层写臂在 dst 信封域物化重建对象、令懒降阶排空正在快照的旧树
    // （换一种已 ACK 丢失形）；本入口是分层路由与降阶臂唯一探测面，读写一致
    // 按迁移忙拒绝。claim 判据取树身份键 = 物理 Meta 键（本函数下方装载同键，
    // 零额外分配），跨库同名键的 claim 按物理域隔离
    if self.store.range_index.migration_claimed(&meta_k) {
      return Err(Error::MigrationBusy);
    }
    self.load_collection_stub_in_window(key).await
  }

  /// [`Self::load_collection_stub`] 的未门禁形态（自迁移安全换入窗内自用）：
  /// 跳过迁移 claim 判定——claim 持有者自身的装载（物化扫描 / 竞态基线读）
  /// 不得被自己的封窗拒绝。门外调用方一律走门禁入口，本函数不对外承担
  /// 封堵契约
  pub async fn load_collection_stub_in_window(
    &self,
    key: &[u8],
  ) -> Result<Option<(MetaValue, RangeIndexStub)>> {
    let meta_k = self.session_meta_key(key);
    let Some(bytes) = self.read_raw(&meta_k).await? else {
      return Ok(None);
    };
    if bytes.len() < META_VALUE_SIZE {
      return Ok(None);
    }
    let meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])?;
    if !self.probe_alive(key).await? {
      return Ok(None);
    }
    let is_alive = meta.is_live();
    if !is_alive {
      return Ok(None);
    }
    if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
      return Ok(None);
    }
    let stub =
      RangeIndexStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])?;
    Ok(Some((meta, stub)))
  }

  /// 读取 RangeIndex 存根及元数据（支持防重入与类型安全检查）
  ///
  /// libs/server/Resp/Parser/RespCommand.cs:IsRangeIndexCommand
  ///
  /// 热路径时间复杂度优化：RI 点操作每次调用本函数，旧实现经 load_meta 读一次
  /// 元记录后再 read_raw 重复读同一记录（2 次主存 I/O）；现改为单次 read_raw
  /// 同帧解析 MetaValue + TTL 守卫 + 类型检查 + 存根解码（1 次主存 I/O），
  /// 语义与 load_meta 口径一致（过期视同不存在）。
  ///
  /// RI 命令打非 RI 键的门禁在此收口（C# 由存储层记录类型判别回 WrongType，
  /// 本函数是 rust 侧 RI 点操作的唯一装载入口，判据为记录自己的物理域事实，
  /// 不吃命令位图）：String 域与集合信封域命中一律 `WrongType`（C#
  /// ValueIsObject 先于类型白名单），非 RangeIndex 的存活元记录同样
  /// `WrongType`，三域皆缺才是索引缺失（`Ok(None)` → 调用方答 no such index）。
  pub async fn load_range_index_stub(
    &self,
    key: &[u8],
  ) -> StdResult<Option<(MetaValue, RangeIndexStub)>, RangeIndexError> {
    let meta_k = self.session_meta_key(key);
    // RENAME 迁移 claim 判定（封堵面单点）：迁移窗内键暂时不可见（与持久墓碑
    // 封堵态等价），读臂按 NotFound 拒绝；写臂（RI.SET/SETBATCH/DEL）None →
    // NotFound 同为显式拒绝且零穿透（NotFound 后无重建路径，杜绝「快照后写入
    // 不进新副本、随旧键排空销毁」的已 ACK 丢失写）。与 load_collection_stub
    // 的迁移忙拒绝非对称：RI 面 None 是终态拒绝，分层面 None 会穿透物化。
    // claim 判据取树身份键 = 物理 Meta 键（下方装载同键，零额外分配）
    if self.store.range_index.migration_claimed(&meta_k) {
      return Ok(None);
    }
    let Some(bytes) = self.read_raw(&meta_k).await? else {
      // 无 RI 元记录：同名普通字符串键 / 集合信封键命中皆 WRONGTYPE，非类型不符
      // 而是压根不是索引
      if self.read(key).await?.is_some() {
        return Err(RangeIndexError::WrongType);
      }
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
      if self.contains_key_raw(&env_k).await? {
        return Err(RangeIndexError::WrongType);
      }
      return Ok(None);
    };
    if bytes.len() < META_VALUE_SIZE {
      return Ok(None);
    }
    let meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])
      .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    // TTL 守卫 (与 load_meta 口径一致)：过期集合视同不存在
    if !self.probe_alive(key).await? {
      return Ok(None);
    }
    if meta.collection_type != GarnetObjectType::RangeIndex {
      return Err(RangeIndexError::WrongType);
    }
    if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
      return Ok(None);
    }
    let stub =
      RangeIndexStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])?;
    Ok(Some((meta, stub)))
  }

  /// 获取在线 BfTree 实例及其条带共享读锁 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:ReadRangeIndex 与 ReadRangeIndexLock)
  ///
  /// 先在无锁/共享锁状态下快速命中（稳态 O(1)：一次 volatile 读 + 一次注册表
  /// 查找），若存根已被刷盘 (IsFlushed = true) 则先通过 RIPROMOTE 重新提升至日志尾部并清除
  /// 刷盘标记；若树未激活则释放读锁后调用 get_or_open_tree（文件 I/O + 快照解析
  /// 属重操作，卸载 compio 阻塞线程，避免慢恢复停摆整核），激活完成后执行
  /// RIRESTORE 存根回写（新句柄写回 + 清 Recovered 位，对标 C# RestoreTree 尾段
  /// 的 RIRESTORE RMW），随后重新获取共享读锁。整个数据读取/修改操作在其 RAII
  /// 读锁保护下安全执行。
  pub async fn acquire_tree_read(
    &self,
    key: &[u8],
    stub: &RangeIndexStub,
  ) -> StdResult<TreeReadGuard<'_>, RangeIndexError> {
    // 树身份键 = 物理 Meta 键（会话域内派生单点，跨库同名键按物理域隔离；
    // 注册表查找、条带锁、惰性恢复、检查点屏障同一判据）
    let id_key = self.session_meta_key(key);
    let key_hash = fast_hash(&id_key);
    let mut current_stub = *stub;
    loop {
      // 屏障异步等待：外层 VersionShift 屏障跨 await 持有，写者挂起让出 reactor
      // （同步忙自旋会在持有窗口霸占 compio worker 造成检查点 I/O 永不收割的死锁）
      wait_tree_checkpoint(&self.store.range_index, &id_key).await?;
      let read_lock = self.store.range_index.read_range_index_lock(key_hash);
      if let Some(tree) = self.store.range_index.get_tree(&id_key) {
        if current_stub.is_flushed() {
          drop(read_lock);
          self.promote_range_index_to_tail(key).await?;
          current_stub.set_flushed(false);
          continue;
        }
        return Ok(TreeReadGuard::new(tree, read_lock));
      }
      drop(read_lock);

      if current_stub.is_flushed() {
        self.promote_range_index_to_tail(key).await?;
        current_stub.set_flushed(false);
        continue;
      }

      // 惰性恢复慢路径：卸载阻塞线程 (条带锁在 manager 内部自取自放，不跨线程边界)
      let mgr = Arc::clone(&self.store.range_index);
      let restore_key = id_key.clone();
      let restore_stub = current_stub;
      let tree = range_index_blocking(move || mgr.get_or_open_tree(&restore_key, &restore_stub))
        .await?
        .map_err(RangeIndexError::from)?;

      // 惰性激活补登换号回收旁表：重启后未经检查点恢复的树首访激活时旁表
      // 尚无记录，若不补登则后续 FLUSHDB 取不到该键，同名重建被 IndexExists 拦截
      self.register_bftree_key(key);

      // RIRESTORE 存根回写：不持任何条带锁 (对标 C# RestoreTree 释放 X 锁后再发
      // RIRESTORE RMW 的分裂设计——锁内发 RMW 会与延迟 OnFlush 自死锁)
      self.restore_range_index_stub(key, &tree).await?;
    }
  }

  /// 获取在线 BfTree 实例及其条带独占写锁（多步写臂互斥面）
  /// 对应 C# RangeIndexManager.AcquireExclusiveForDelete 独占守卫形态；
  /// 语义差异：C# 独占锁仅承载生命周期操作——DEL/
  /// 淘汰/检查点快照/惰性恢复，RI 数据写每命令单树操作走共享锁即可；rust 分层
  /// 集合写臂是「探测 → 树写 → 计数 → meta 回写」多步序列，共享锁下两臂可交错
  /// 互踩（删除结果丢弃、meta 覆写丢更新），故数据写面升格独占，对位 C# 对象域
  /// 同键写经 Tsavorite 记录锁串行（TsavoriteKV.cs RMW InPlaceUpdater 前置记录
  /// X 锁），纯读臂维持共享锁）
  ///
  /// 骨架与 [`acquire_tree_read`] 同型：检查点屏障在无锁状态下异步等待（让出
  /// reactor 不自旋霸占 compio worker）；存根被刷盘先放锁再晋升重试；树未激活
  /// 先放锁再卸载阻塞线程恢复（恢复路径的条带锁在 manager 内部自取自放，不跨
  /// 线程边界，持本锁进阻塞卸载会与其自取的锁互锁）；RIRESTORE 回写不持条带锁
  /// （锁内发 RMW 与延迟 OnFlush 自死锁，分裂设计同读面）。
  pub async fn acquire_tree_write(
    &self,
    key: &[u8],
    stub: &RangeIndexStub,
  ) -> StdResult<TreeWriteGuard<'_>, RangeIndexError> {
    // 树身份键 = 物理 Meta 键（同读面口径：注册表查找、条带锁、惰性恢复、
    // 检查点屏障同一判据，跨库同名键按物理域隔离）
    let id_key = self.session_meta_key(key);
    let key_hash = fast_hash(&id_key);
    let mut current_stub = *stub;
    loop {
      wait_tree_checkpoint(&self.store.range_index, &id_key).await?;
      let write_lock = self
        .store
        .range_index
        .acquire_exclusive_for_delete(key_hash);
      if let Some(tree) = self.store.range_index.get_tree(&id_key) {
        if current_stub.is_flushed() {
          drop(write_lock);
          self.promote_range_index_to_tail(key).await?;
          current_stub.set_flushed(false);
          continue;
        }
        return Ok(TreeWriteGuard::new(tree, write_lock));
      }
      drop(write_lock);

      if current_stub.is_flushed() {
        self.promote_range_index_to_tail(key).await?;
        current_stub.set_flushed(false);
        continue;
      }

      // 惰性恢复慢路径：卸载阻塞线程（条带锁自取自放，不跨线程边界）
      let mgr = Arc::clone(&self.store.range_index);
      let restore_key = id_key.clone();
      let restore_stub = current_stub;
      let tree = range_index_blocking(move || mgr.get_or_open_tree(&restore_key, &restore_stub))
        .await?
        .map_err(RangeIndexError::from)?;

      // 惰性激活补登换号回收旁表（同读面口径）
      self.register_bftree_key(key);

      // RIRESTORE 存根回写：不持任何条带锁（分裂设计同读面）
      self.restore_range_index_stub(key, &tree).await?;
    }
  }
}
