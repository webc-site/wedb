//! 上下文元数据（对标 libs/server/Resp/Vector/VectorManager.ContextMetadata.cs）
//!
//! [`ContextMetadata`] 记录进程级上下文分配状态（在用/清理中/迁移中三张 64 位
//! 位图 + 每上下文的 hash slot 表），持久化到存储的同时在内存保持副本以供快速访问。
//! [`super::vector_manager::VectorManager`] 的本 partial 承接分配/迁移保留/
//! FLUSH 屏蔽/命名空间编解码等管理面方法。

use std::collections::BTreeSet;

use super::vector_manager::{CONTEXT_METADATA_SIZE, CONTEXT_STEP, VectorManager};

/// 上下文分配元数据（160 字节磁盘格式：4×u64 位图 + 64×u16 槽位）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContextMetadata {
  /// 修改版本号（每次变更递增，用于落盘去重）。
  pub version: u64,
  /// 在用位图。
  in_use: u64,
  /// 清理中位图。
  cleaning_up: u64,
  /// 迁移中位图。
  migrating: u64,
  /// 各位的 hash slot 分配表。
  slots: [u16; 64],
}

impl Default for ContextMetadata {
  fn default() -> Self {
    Self {
      version: 0,
      in_use: 0,
      cleaning_up: 0,
      migrating: 0,
      slots: [0; 64],
    }
  }
}

/// 非法 hash slot（迁移保留时尚未确定目标）。
pub const UNKNOWN_HASH_SLOT: u16 = u16::MAX;

impl ContextMetadata {
  /// 是否完全为空（恢复期可剪枝）。
  pub fn is_empty(&self) -> bool {
    self.in_use == 0 && self.migrating == 0 && self.cleaning_up == 0
  }

  /// 位运算辅助：上下文 → 位下标 + 掩码。
  #[inline]
  fn bit(context: u16) -> (u32, u64) {
    let ctx = u64::from(context);
    debug_assert!(ctx % CONTEXT_STEP == 0, "只允许整块上下文，不允许子位");
    debug_assert!((ctx / CONTEXT_STEP) < 64, "上下文超出预期范围");
    let bit_ix = u32::try_from(ctx / CONTEXT_STEP).unwrap_or(0);
    (bit_ix, 1u64 << bit_ix)
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:IsInUse
  #[inline]
  pub fn is_in_use(&self, _allow_zero: bool, context: u16) -> bool {
    let (_, mask) = Self::bit(context);
    self.in_use & mask != 0
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:IsMigrating
  #[inline]
  pub fn is_migrating(&self, _allow_zero: bool, context: u16) -> bool {
    let (_, mask) = Self::bit(context);
    self.migrating & mask != 0
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:IsCleaningUp
  #[inline]
  pub fn is_cleaning_up(&self, _allow_zero: bool, context: u16) -> bool {
    let (_, mask) = Self::bit(context);
    self.cleaning_up & mask == mask
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:GetNamespacesForHashSlots
  ///
  /// 返回在用、未清理且命中目标 hash slot 集合的上下文块全集。
  pub fn get_namespaces_for_hash_slots(&self, hash_slots: &BTreeSet<i32>) -> Option<Vec<u16>> {
    let mut ret = None;
    let mut remaining = self.in_use;
    while remaining != 0 {
      let in_use_ix = remaining.trailing_zeros();
      let in_use_mask = 1u64 << in_use_ix;
      remaining &= !in_use_mask;

      if self.cleaning_up & in_use_mask != 0 {
        // 清理中的上下文无需迁移
        continue;
      }

      let hash_slot = self.slots[in_use_ix as usize];
      if !hash_slots.contains(&(i32::from(hash_slot))) {
        // 在用但不是迁移目标
        continue;
      }

      let ret = ret.get_or_insert_with(Vec::new);
      let ns_start = (CONTEXT_STEP * u64::from(in_use_ix)) as u16;
      for i in 0..CONTEXT_STEP {
        ret.push(ns_start + i as u16);
      }
    }
    ret
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:NextNotInUse
  ///
  /// 返回下一个未占用上下文；全部占用时返回 None。
  /// `allow_zero == false` 时整体上下文 0 保留（仅首块元数据适用）。
  pub fn next_not_in_use(&self, allow_zero: bool) -> Option<u16> {
    let mut ignoring_unusable = self.in_use;
    if !allow_zero {
      ignoring_unusable |= 1;
    }

    let free = !ignoring_unusable;
    if free == 0 {
      return None;
    }
    let bit = free.trailing_zeros();
    if bit >= 64 {
      return None;
    }
    Some((u64::from(bit) * CONTEXT_STEP) as u16)
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:TryReserveForMigration
  ///
  /// 保留 count 个上下文供迁移使用（标记在用 + 迁移中，slot 暂记非法值）。
  pub fn try_reserve_for_migration(&mut self, allow_zero: bool, count: usize) -> Option<Vec<u16>> {
    let mut available_mask = self.in_use;
    if !allow_zero {
      available_mask |= 1;
    }
    let available = (available_mask).count_ones() as usize;
    let free_count = 64 - available;
    if free_count < count {
      return None;
    }

    let mut reserved = Vec::with_capacity(count);
    for _ in 0..count {
      let ctx = self.next_not_in_use(allow_zero)?;
      self.mark_in_use(allow_zero, ctx, UNKNOWN_HASH_SLOT);
      self.mark_migrating(allow_zero, ctx);
      reserved.push(ctx);
    }
    Some(reserved)
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:MarkInUse
  pub fn mark_in_use(&mut self, _allow_zero: bool, context: u16, hash_slot: u16) {
    let (bit_ix, mask) = Self::bit(context);
    debug_assert!(self.in_use & mask == 0, "即将标记的上下文已在使用");
    self.in_use |= mask;
    self.slots[bit_ix as usize] = hash_slot;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:MarkMigrating
  pub fn mark_migrating(&mut self, _allow_zero: bool, context: u16) {
    let (_, mask) = Self::bit(context);
    debug_assert!(self.in_use & mask != 0, "迁移标记要求上下文已在使用");
    debug_assert!(self.migrating & mask == 0, "上下文已处于迁移中");
    self.migrating |= mask;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:MarkMigrationComplete
  pub fn mark_migration_complete(&mut self, _allow_zero: bool, context: u16, hash_slot: u16) {
    let (bit_ix, mask) = Self::bit(context);
    debug_assert!(self.in_use & mask != 0, "应已在使用");
    debug_assert!(self.migrating & mask != 0, "应为迁移目标");
    self.migrating &= !mask;
    self.slots[bit_ix as usize] = hash_slot;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:MarkCleaningUp
  pub fn mark_cleaning_up(&mut self, _allow_zero: bool, context: u16) {
    let (_, mask) = Self::bit(context);
    debug_assert!(self.in_use & mask != 0, "清理标记要求上下文已在使用");
    debug_assert!(self.cleaning_up & mask == 0, "上下文已处于清理中");
    self.cleaning_up |= mask;
    // 若正在迁移则一并终止；slot 保留备用
    self.migrating &= !mask;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:ClearIsCleaningUp
  pub fn clear_is_cleaning_up(&mut self, _allow_zero: bool, context: u16) {
    let (_, mask) = Self::bit(context);
    debug_assert!(self.in_use & mask != 0, "被清理的上下文应在使用中");
    debug_assert!(self.cleaning_up & mask != 0, "未标记清理的上下文不能清除");
    self.cleaning_up &= !mask;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:FinishedCleaningUp
  pub fn finished_cleaning_up(&mut self, _allow_zero: bool, context: u16) {
    let (bit_ix, mask) = Self::bit(context);
    debug_assert!(self.in_use & mask != 0, "清理完成的上下文应在使用中");
    debug_assert!(self.cleaning_up & mask != 0, "清理完成的上下文应已标记");
    self.cleaning_up &= !mask;
    self.in_use &= !mask;
    self.slots[bit_ix as usize] = 0;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:UpdateHashSlot
  pub fn update_hash_slot(&mut self, _allow_zero: bool, context: u16, slot: u16) {
    let (bit_ix, mask) = Self::bit(context);
    debug_assert!(self.in_use & mask != 0, "更新 hash slot 的上下文应在使用中");
    debug_assert!(
      self.cleaning_up & mask == 0,
      "正在清理的上下文不应更新 slot"
    );
    self.slots[bit_ix as usize] = slot;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:GetNeedCleanup
  pub fn get_need_cleanup(&self) -> Option<Vec<u16>> {
    if self.cleaning_up == 0 {
      return None;
    }
    Some(bits_to_contexts(self.cleaning_up))
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:GetMigrating
  pub fn get_migrating(&self) -> Option<Vec<u16>> {
    if self.migrating == 0 {
      return None;
    }
    Some(bits_to_contexts(self.migrating))
  }

  /// 序列化为 160 字节磁盘格式（4×u64 + 64×u16，小端）。
  pub fn to_bytes(&self) -> [u8; CONTEXT_METADATA_SIZE] {
    let mut out = [0u8; CONTEXT_METADATA_SIZE];
    out[0..8].copy_from_slice(&self.version.to_le_bytes());
    out[8..16].copy_from_slice(&self.in_use.to_le_bytes());
    out[16..24].copy_from_slice(&self.cleaning_up.to_le_bytes());
    out[24..32].copy_from_slice(&self.migrating.to_le_bytes());
    for (i, slot) in self.slots.iter().enumerate() {
      out[32 + i * 2..32 + i * 2 + 2].copy_from_slice(&slot.to_le_bytes());
    }
    out
  }

  /// 从磁盘字节反序列化。
  pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
    if bytes.len() != CONTEXT_METADATA_SIZE {
      return None;
    }
    let rd_u64 = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
    let mut slots = [0u16; 64];
    for (i, slot) in slots.iter_mut().enumerate() {
      *slot = u16::from_le_bytes(bytes[32 + i * 2..32 + i * 2 + 2].try_into().unwrap());
    }
    Some(Self {
      version: rd_u64(0),
      in_use: rd_u64(8),
      cleaning_up: rd_u64(16),
      migrating: rd_u64(24),
      slots,
    })
  }
}

/// 位图 → 上下文块起始值列表。
fn bits_to_contexts(bits: u64) -> Vec<u16> {
  let mut ret = Vec::new();
  let mut remaining = bits;
  while remaining != 0 {
    let ix = remaining.trailing_zeros();
    ret.push((u64::from(ix) * CONTEXT_STEP) as u16);
    remaining &= !(1u64 << ix);
  }
  ret
}

impl VectorManager {
  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:DecomposeContext
  ///
  /// 将存储外的上下文拆为 (元数据数组下标， 元数据内上下文值)。
  pub fn decompose_context(context: u64) -> (usize, u16) {
    (
      (context / (64 * CONTEXT_STEP)) as usize,
      (context % (64 * CONTEXT_STEP)) as u16,
    )
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:OffsetForContextMetadata
  ///
  /// 给定元数据数组下标，返回其承载的首个上下文。
  pub fn offset_for_context_metadata(context_metadata_index: usize) -> u64 {
    (context_metadata_index as u64) * 64 * CONTEXT_STEP
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:NextVectorSetContext
  ///
  /// 分配全局唯一的新上下文（必要时扩容元数据数组）。
  pub fn next_vector_set_context(&self, hash_slot: u16) -> Option<u64> {
    let mut metas = self.context_metadatas.lock();
    let mut start_from = 0usize;

    loop {
      for i in start_from..metas.len() {
        let allow_zero = i != 0;
        if let Some(next_free) = metas[i].next_not_in_use(allow_zero) {
          metas[i].mark_in_use(allow_zero, next_free, hash_slot);
          let context = Self::offset_for_context_metadata(i) + u64::from(next_free);
          self.dirty_context_metadatas.lock().insert(i);
          self.persist_context_metadata(&metas);
          return Some(context);
        }
      }

      // 超过 uint.MaxValue 上下文上限（约 830 万 Vector Set）视为错误
      let limit_of_new_allocation =
        Self::offset_for_context_metadata(metas.len()) + 64 * CONTEXT_STEP;
      if limit_of_new_allocation > u64::from(u32::MAX) {
        return None;
      }

      // 全部已满：扩容一个 ContextMetadata
      metas.push(ContextMetadata::default());
      start_from = metas.len() - 1;
      self.dirty_context_metadatas.lock().insert(start_from);
    }
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:AllocateTestContexts
  ///
  /// 测试用途：强制分配 count 个上下文。
  pub fn allocate_test_contexts(&self, count: usize) -> Vec<u64> {
    (0..count)
      .filter_map(|_| self.next_vector_set_context(0))
      .collect()
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:BeginFlush
  ///
  /// FLUSHDB/FLUSHALL 期间阻止新上下文签发与元数据更新；
  /// 返回的 guard 在 drop 时重置元数据状态。
  pub fn begin_flush(&self) -> Option<FlushGuard> {
    if !self.is_enabled {
      return None;
    }
    let mut metas = self.context_metadatas.lock();
    *metas = vec![ContextMetadata::default()];
    drop(metas);
    self.dirty_context_metadatas.lock().clear();
    self.recovered_indexes.lock().clear();
    self.recovered_metadata.lock().clear();
    Some(FlushGuard)
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:ReserveContextsForMigration
  ///
  /// 为迁移保留 count 个上下文（尚未"可见"，但已不可他用）。
  pub fn reserve_contexts_for_migration(&self, count: usize) -> Option<Vec<u64>> {
    debug_assert!(
      count > 0 && count <= 64,
      "单个 ContextMetadata 至多承载 64 个上下文"
    );

    let mut metas = self.context_metadatas.lock();
    let mut start_from = 0usize;

    loop {
      for i in start_from..metas.len() {
        let allow_zero = i != 0;
        if let Some(sub_contexts) = metas[i].try_reserve_for_migration(allow_zero, count) {
          let offset = Self::offset_for_context_metadata(i);
          self.dirty_context_metadatas.lock().insert(i);
          self.persist_context_metadata(&metas);
          return Some(
            sub_contexts
              .iter()
              .map(|c| offset + u64::from(*c))
              .collect(),
          );
        }
      }

      let limit_of_new_allocation =
        Self::offset_for_context_metadata(metas.len()) + 64 * CONTEXT_STEP;
      if limit_of_new_allocation > u64::from(u32::MAX) {
        return None;
      }

      metas.push(ContextMetadata::default());
      start_from = metas.len() - 1;
      self.dirty_context_metadatas.lock().insert(start_from);
    }
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:UpdateContextMetadata
  ///
  /// 将脏元数据刷入持久化承接层（wkv 集成前为域内记录表）。
  pub fn update_context_metadata(&self) {
    let metas = self.context_metadatas.lock();
    self.persist_context_metadata(&metas);
  }

  /// 脏元数据落盘承接（逐条写持久化表并清空脏集）。
  fn persist_context_metadata(&self, metas: &[ContextMetadata]) {
    let mut dirty = self.dirty_context_metadatas.lock();
    let mut store = self.metadata_store.lock();
    for &i in dirty.iter() {
      if let Some(meta) = metas.get(i) {
        store.insert(i as i32, meta.to_bytes());
      }
    }
    dirty.clear();
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:GetNamespacesForHashSlots
  ///
  /// 汇总所有元数据块中命中给定 hash slot 的命名空间（迁移用）。
  pub fn get_namespaces_for_hash_slots(&self, hash_slots: &BTreeSet<i32>) -> BTreeSet<u64> {
    let mut ret = BTreeSet::new();
    let metas = self.context_metadatas.lock();
    for (i, meta) in metas.iter().enumerate() {
      let offset = Self::offset_for_context_metadata(i);
      if let Some(sub) = meta.get_namespaces_for_hash_slots(hash_slots) {
        for item in sub {
          ret.insert(offset + u64::from(item));
        }
      }
    }
    ret
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:ExtractContextFromNamespaces
  ///
  /// 从命名空间字节中还原上下文（1 或 4 字节）。
  pub fn extract_context_from_namespaces(namespace_bytes: &[u8]) -> u64 {
    debug_assert!(
      namespace_bytes.len() == 1 || namespace_bytes.len() == 4,
      "命名空间长度不符"
    );
    match namespace_bytes.len() {
      1 => u64::from(namespace_bytes[0]),
      _ => u64::from(u32::from_le_bytes(
        namespace_bytes[..4].try_into().unwrap_or([0; 4]),
      )),
    }
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:StoreContextInNamespace
  ///
  /// 上下文 → 命名字节（≤255 用单字节，否则 4 字节小端）。
  /// 返回实际占用的字节切片长度。
  pub fn store_context_in_namespace(context: u64, namespace_bytes: &mut [u8]) -> usize {
    debug_assert!(namespace_bytes.len() >= 4, "提供的缓冲区空间不足");
    debug_assert!(
      context > 0 && context <= u64::from(u32::MAX),
      "上下文必须位于 (0, uint.MaxValue]"
    );

    if context <= u64::from(u8::MAX) {
      namespace_bytes[0] = context as u8;
      1
    } else {
      namespace_bytes[..4].copy_from_slice(&(context as u32).to_le_bytes());
      4
    }
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:UpdateHashSlot
  ///
  /// 迁移/重命名后按索引 VALUE 同步上下文的 hash slot 记录。
  pub fn update_hash_slot(&self, old_value: &[u8], new_slot: u16) {
    let Some(index) = super::vector_manager__index::Index::from_bytes(old_value) else {
      return;
    };
    let (context_index, context_value) = Self::decompose_context(index.context);

    let mut metas = self.context_metadatas.lock();
    if let Some(meta) = metas.get_mut(context_index) {
      meta.update_hash_slot(context_index != 0, context_value, new_slot);
      drop(metas);
      self.dirty_context_metadatas.lock().insert(context_index);
      self.update_context_metadata();
    }
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:GetContextState
  ///
  /// 测试用途：检视指定上下文的三态标志。
  pub fn get_context_state(&self, context: u64) -> (bool, bool, bool) {
    let (context_index, context_value) = Self::decompose_context(context);
    let metas = self.context_metadatas.lock();
    let Some(meta) = metas.get(context_index) else {
      return (false, false, false);
    };
    let allow_zero = context_index != 0;
    (
      meta.is_in_use(allow_zero, context_value),
      meta.is_cleaning_up(allow_zero, context_value),
      meta.is_migrating(allow_zero, context_value),
    )
  }
}

/// FLUSH 屏蔽守卫（构造时已重置上下文状态，drop 为无操作）。
///
/// C# 以锁令牌 + Monitor 实现互斥；Rust 侧 [`VectorManager::begin_flush`]
/// 在持锁状态下同步重置全部上下文状态，guard 仅作为作用域标记。
#[derive(Debug)]
pub struct FlushGuard;

#[cfg(test)]
mod tests {
  use super::*;
  use crate::resp::vector::vector_manager::VectorManagerOptions;

  fn meta() -> ContextMetadata {
    ContextMetadata::default()
  }

  #[test]
  fn bitmap_lifecycle() {
    let mut m = meta();
    assert!(m.is_empty());

    // 首块不允许 0 上下文
    assert_eq!(m.next_not_in_use(false), Some(CONTEXT_STEP as u16));
    assert_eq!(m.next_not_in_use(true), Some(0));

    m.mark_in_use(false, 8, 42);
    assert!(m.is_in_use(false, 8));
    assert!(!m.is_empty());
    assert_eq!(m.next_not_in_use(false), Some(16));

    m.mark_cleaning_up(false, 8);
    assert_eq!(m.get_need_cleanup(), Some(vec![8]));
    m.finished_cleaning_up(false, 8);
    assert_eq!(m.get_need_cleanup(), None);
    assert!(!m.is_in_use(false, 8));
    assert!(m.is_empty());
  }

  #[test]
  fn migration_states() {
    let mut m = meta();
    assert!(m.try_reserve_for_migration(false, 2).is_some());
    assert_eq!(m.get_migrating().unwrap().len(), 2);
    assert_eq!(m.get_need_cleanup(), None);

    // 迁移完成 → 写入真实 slot
    m.mark_migration_complete(false, 8, 7);
    assert!(!m.is_migrating(false, 8));
    assert!(m.is_in_use(false, 8));

    // 放弃迁移 → 转入清理
    m.mark_cleaning_up(false, 16);
    assert!(!m.is_migrating(false, 16));
    assert!(m.is_cleaning_up(false, 16));
    m.clear_is_cleaning_up(false, 16);
    assert!(!m.is_cleaning_up(false, 16));
  }

  #[test]
  fn reserve_exhaustion() {
    let mut m = meta();
    // 首块可用位仅 63（0 保留），请求 64 失败
    assert!(m.try_reserve_for_migration(false, 64).is_none());
    assert!(m.try_reserve_for_migration(true, 64).is_some());
    assert_eq!(m.next_not_in_use(true), None);
  }

  #[test]
  fn hash_slot_namespace_filter() {
    let mut m = meta();
    m.mark_in_use(true, 0, 5);
    m.mark_in_use(true, 8, 6);
    let mut targets = BTreeSet::new();
    targets.insert(5);
    let ns = m.get_namespaces_for_hash_slots(&targets).unwrap();
    assert_eq!(ns, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    assert!(m.get_namespaces_for_hash_slots(&BTreeSet::new()).is_none());
  }

  #[test]
  fn binary_format_roundtrip() {
    let mut m = meta();
    m.mark_in_use(true, 8, 1234);
    m.mark_migrating(true, 8);
    let bytes = m.to_bytes();
    assert_eq!(bytes.len(), CONTEXT_METADATA_SIZE);
    assert_eq!(ContextMetadata::from_bytes(&bytes).unwrap(), m);
    assert!(ContextMetadata::from_bytes(&bytes[..100]).is_none());
  }

  #[test]
  fn context_decompose_and_namespace_codec() {
    // 64 * 8 = 512 上下文/块
    assert_eq!(VectorManager::decompose_context(0), (0, 0));
    assert_eq!(VectorManager::decompose_context(512), (1, 0));
    assert_eq!(VectorManager::decompose_context(519), (1, 7));
    assert_eq!(VectorManager::offset_for_context_metadata(2), 1024);

    // 单字节命名空间
    let mut buf = [0u8; 4];
    assert_eq!(VectorManager::store_context_in_namespace(8, &mut buf), 1);
    assert_eq!(VectorManager::extract_context_from_namespaces(&buf[..1]), 8);

    // 4 字节命名空间
    assert_eq!(
      VectorManager::store_context_in_namespace(70000, &mut buf),
      4
    );
    assert_eq!(VectorManager::extract_context_from_namespaces(&buf), 70000);
  }

  #[test]
  fn manager_context_allocation_flow() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });

    // 分配 → 状态在用
    let ctx = manager.next_vector_set_context(3).unwrap();
    assert_ne!(ctx, 0);
    let (in_use, cleanup, migrating) = manager.get_context_state(ctx);
    assert!(in_use && !cleanup && !migrating);

    // 不重复分配
    let ctx2 = manager.next_vector_set_context(3).unwrap();
    assert_ne!(ctx, ctx2);

    // 迁移保留
    let reserved = manager.reserve_contexts_for_migration(2).unwrap();
    assert_eq!(reserved.len(), 2);
    assert!(reserved.iter().all(|c| manager.get_context_state(*c).2));

    // hash slot 更新（以真实分配的上下文构造索引记录；上下文 0 非法）
    let idx = super::super::vector_manager__index::Index {
      context: ctx,
      ..Default::default()
    };
    manager.update_hash_slot(&idx.to_bytes(), 9);
    manager.allocate_test_contexts(3);

    // FLUSH 屏蔽后全部重置
    let guard = manager.begin_flush();
    assert!(guard.is_some());
    assert_eq!(manager.get_context_state(ctx), (false, false, false));
  }

  #[test]
  fn flush_guard_disabled_manager() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: false,
      ..Default::default()
    });
    assert!(manager.begin_flush().is_none());
  }

  #[test]
  fn metadata_persistence_roundtrip() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });
    let ctx = manager.next_vector_set_context(11).unwrap();
    let (context_index, _) = VectorManager::decompose_context(ctx);

    // 落盘承接层应记录该元数据块
    let store = manager.metadata_store.lock();
    let bytes = store.get(&(context_index as i32)).unwrap();
    let meta = ContextMetadata::from_bytes(bytes).unwrap();
    assert!(meta.version > 0);
  }
}
