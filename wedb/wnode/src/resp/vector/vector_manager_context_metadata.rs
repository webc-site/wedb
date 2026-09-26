//! 上下文元数据（对标 libs/server/Resp/Vector/VectorManager.ContextMetadata.cs）
//!
//! [`ContextMetadata`] 记录进程级上下文分配状态（在用/清理中/迁移中三张 64 位
//! 位图 + 每上下文的 hash slot 表），持久化到存储的同时在内存保持副本以供快速访问。
//! [`super::vector_manager::VectorManager`] 的本 partial 承接分配/迁移保留/
//! FLUSH 屏蔽/命名空间编解码等管理面方法。

use std::collections::BTreeSet;

use wbase::{hash_slot::slot_of, map::HashSet};

use super::{
  vector_manager::{CONTEXT_METADATA_SIZE, CONTEXT_STEP, CONTEXTS_PER_METADATA, VectorManager},
  vector_manager_index::Index,
  vector_manager_locking::split_registry_key,
};

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
  pub slots: [u16; CONTEXTS_PER_METADATA as usize],
}

impl Default for ContextMetadata {
  fn default() -> Self {
    Self {
      version: 0,
      in_use: 0,
      cleaning_up: 0,
      migrating: 0,
      slots: [0; CONTEXTS_PER_METADATA as usize],
    }
  }
}

/// 非法 hash slot（迁移保留时尚未确定目标）。
pub const UNKNOWN_HASH_SLOT: u16 = u16::MAX;

/// 一块元数据跨越的上下文编号步长（元数据下标 ↔ 上下文的除/模基数）。
const METADATA_CONTEXT_SPAN: u64 = CONTEXTS_PER_METADATA * CONTEXT_STEP;

impl ContextMetadata {
  /// 是否完全为空（恢复期可剪枝）。
  pub fn is_empty(&self) -> bool {
    self.in_use == 0 && self.migrating == 0 && self.cleaning_up == 0
  }

  /// 位运算辅助：上下文 → 位下标 + 掩码；`allow_zero == false` 时断言零上下文非法
  ///（10 处调用方共用的原逐字断言并入单点）。
  #[inline]
  fn bit(allow_zero: bool, context: u16) -> (u32, u64) {
    debug_assert!(
      allow_zero || context != 0,
      "Zero context not permitted here"
    );
    let ctx = u64::from(context);
    debug_assert!(ctx % CONTEXT_STEP == 0, "只允许整块上下文，不允许子位");
    debug_assert!(
      ctx / CONTEXT_STEP < CONTEXTS_PER_METADATA,
      "上下文超出预期范围"
    );
    let bit_ix = u32::try_from(ctx / CONTEXT_STEP).unwrap_or(0);
    (bit_ix, 1u64 << bit_ix)
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:IsInUse
  #[inline]
  pub fn is_in_use(&self, allow_zero: bool, context: u16) -> bool {
    let (_, mask) = Self::bit(allow_zero, context);
    self.in_use & mask != 0
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:IsMigrating
  #[inline]
  pub fn is_migrating(&self, allow_zero: bool, context: u16) -> bool {
    let (_, mask) = Self::bit(allow_zero, context);
    self.migrating & mask != 0
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:IsCleaningUp
  #[inline]
  pub fn is_cleaning_up(&self, allow_zero: bool, context: u16) -> bool {
    let (_, mask) = Self::bit(allow_zero, context);
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
    if bit >= CONTEXTS_PER_METADATA as u32 {
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
    let free_count = CONTEXTS_PER_METADATA as usize - available;
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
  pub fn mark_in_use(&mut self, allow_zero: bool, context: u16, hash_slot: u16) {
    let (bit_ix, mask) = Self::bit(allow_zero, context);
    debug_assert!(self.in_use & mask == 0, "即将标记的上下文已在使用");
    self.in_use |= mask;
    self.slots[bit_ix as usize] = hash_slot;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:MarkMigrating
  pub fn mark_migrating(&mut self, allow_zero: bool, context: u16) {
    let (_, mask) = Self::bit(allow_zero, context);
    debug_assert!(self.in_use & mask != 0, "迁移标记要求上下文已在使用");
    debug_assert!(self.migrating & mask == 0, "上下文已处于迁移中");
    self.migrating |= mask;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:MarkMigrationComplete
  pub fn mark_migration_complete(&mut self, allow_zero: bool, context: u16, hash_slot: u16) {
    let (bit_ix, mask) = Self::bit(allow_zero, context);
    debug_assert!(self.in_use & mask != 0, "应已在使用");
    debug_assert!(self.migrating & mask != 0, "应为迁移目标");
    self.migrating &= !mask;
    self.slots[bit_ix as usize] = hash_slot;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:MarkCleaningUp
  pub fn mark_cleaning_up(&mut self, allow_zero: bool, context: u16) {
    let (_, mask) = Self::bit(allow_zero, context);
    debug_assert!(self.in_use & mask != 0, "清理标记要求上下文已在使用");
    debug_assert!(self.cleaning_up & mask == 0, "上下文已处于清理中");
    self.cleaning_up |= mask;
    // 若正在迁移则一并终止；slot 保留备用
    self.migrating &= !mask;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:ClearIsCleaningUp
  pub fn clear_is_cleaning_up(&mut self, allow_zero: bool, context: u16) {
    let (_, mask) = Self::bit(allow_zero, context);
    debug_assert!(self.in_use & mask != 0, "被清理的上下文应在使用中");
    debug_assert!(self.cleaning_up & mask != 0, "未标记清理的上下文不能清除");
    self.cleaning_up &= !mask;
    self.version += 1;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:FinishedCleaningUp
  pub fn finished_cleaning_up(&mut self, allow_zero: bool, context: u16) {
    let (_, mask) = Self::bit(allow_zero, context);
    debug_assert!(self.in_use & mask != 0, "清理完成的上下文应在使用中");
    debug_assert!(self.cleaning_up & mask != 0, "未标记清理的上下文不能清除");
    self.cleaning_up &= !mask;
    // 归还段复用 release 单套机制
    self.release(allow_zero, context);
  }

  /// 创建失败臂的槽位取回：in_use 清零 + slot 清零 + version 递增。
  /// C# 无对位（原生 CreateIndex 以 Debug.Assert 声明不可失败，无失败回退
  /// 面）；rust create_index 可失败，失败臂据此取回刚分配的槽位防单调泄漏。
  /// [`Self::finished_cleaning_up`] 的归还段复用本原语。
  pub fn release(&mut self, allow_zero: bool, context: u16) {
    let (bit_ix, mask) = Self::bit(allow_zero, context);
    debug_assert!(self.in_use & mask != 0, "取回的上下文应在使用中");
    self.in_use &= !mask;
    self.slots[bit_ix as usize] = 0;
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
    let mut slots = [0u16; CONTEXTS_PER_METADATA as usize];
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

use wvector::store::StoreCallbacks;

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:DecomposeContext
  ///
  /// 将存储外的上下文拆为 (元数据数组下标， 元数据内上下文值)。
  pub fn decompose_context(context: u64) -> (usize, u16) {
    (
      (context / METADATA_CONTEXT_SPAN) as usize,
      (context % METADATA_CONTEXT_SPAN) as u16,
    )
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:OffsetForContextMetadata
  ///
  /// 给定元数据数组下标，返回其承载的首个上下文。
  pub fn offset_for_context_metadata(context_metadata_index: usize) -> u64 {
    (context_metadata_index as u64) * METADATA_CONTEXT_SPAN
  }

  /// 上下文槽位分配骨架（next/reserve 共用单点）：自 `start_from` 起逐块
  /// 试分配，命中即标脏返回 `(块号, 产出)`；全满先查 u32::MAX 上限再扩一块、
  /// 自新块重扫。分配段为纯同步（标脏 + 内存镜像），脏集写透由调用方在
  /// 守卫外 `.await` 闭环——parking_lot 守卫绝不跨 await
  fn alloc_context_slot<T>(
    metas: &mut Vec<ContextMetadata>,
    dirty: &mut BTreeSet<usize>,
    mut try_alloc: impl FnMut(&mut ContextMetadata, bool) -> Option<T>,
  ) -> Option<(usize, T)> {
    let mut start_from = 0usize;
    loop {
      for (i, meta) in metas.iter_mut().enumerate().skip(start_from) {
        let allow_zero = i != 0;
        if let Some(t) = try_alloc(meta, allow_zero) {
          dirty.insert(i);
          return Some((i, t));
        }
      }

      // 超过 uint.MaxValue 上下文上限（约 830 万 Vector Set）视为错误
      let limit_of_new_allocation =
        Self::offset_for_context_metadata(metas.len()) + METADATA_CONTEXT_SPAN;
      if limit_of_new_allocation > u64::from(u32::MAX) {
        return None;
      }

      // 全部已满：扩容一个 ContextMetadata，自新块重扫
      metas.push(ContextMetadata::default());
      start_from = metas.len() - 1;
      dirty.insert(start_from);
    }
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:NextVectorSetContext
  ///
  /// 分配全局唯一的新上下文（必要时扩容元数据数组）。分配段为纯同步
  ///（标脏 + 内存镜像），脏集写透在守卫外 `.await` 闭环——parking_lot
  /// 守卫绝不跨 await。
  pub async fn next_vector_set_context(&self, hash_slot: u16) -> Option<u64> {
    let context = {
      let mut metas = self.context_metadatas.lock();
      let (i, next_free) = Self::alloc_context_slot(
        &mut metas,
        &mut self.dirty_context_metadatas.lock(),
        |meta, allow_zero| {
          meta.next_not_in_use(allow_zero).inspect(|&nf| {
            meta.mark_in_use(allow_zero, nf, hash_slot);
          })
        },
      )?;
      Self::offset_for_context_metadata(i) + u64::from(next_free)
    };
    self.flush_dirty_context_metadata().await;
    Some(context)
  }

  /// 创建失败臂的上下文回收（[`Self::next_vector_set_context`] 的对偶；
  /// C# CreateIndex 不可失败故无此面对偶）：in_use 位与 slot 清零、version
  /// 递增，写透后槽位立即可复用，杜绝存储抖动期失败单调泄漏。
  pub async fn release_vector_set_context(&self, context: u64) {
    let (context_index, context_value) = Self::decompose_context(context);
    {
      let mut metas = self.context_metadatas.lock();
      if let Some(meta) = metas.get_mut(context_index) {
        meta.release(context_index != 0, context_value);
      }
      self.dirty_context_metadatas.lock().insert(context_index);
    }
    self.flush_dirty_context_metadata().await;
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:ReserveContextsForMigration
  ///
  /// 为迁移保留 count 个上下文（尚未"可见"，但已不可他用）。分配段为纯
  /// 同步（标脏 + 内存镜像），脏集写透在守卫外 `.await` 闭环——parking_lot
  /// 守卫绝不跨 await。
  pub async fn reserve_contexts_for_migration(&self, count: usize) -> Option<Vec<u64>> {
    debug_assert!(
      count > 0 && count <= CONTEXTS_PER_METADATA as usize,
      "单个 ContextMetadata 至多承载 64 个上下文"
    );

    // 执行域兜底自持专用会话（形态同 [`Self::delete_vector_set_of`] 的收口
    // 兜底臂：守卫经 `DomainGuardSend` 持至写透完成）：本口的真实调用面是
    // `CLUSTER RESERVE VECTOR_SET_CONTEXTS`，走连接
    // 线程的集群命令分派（network_cluster_reserve 的挂起化慢臂），不经命令面
    // `StoreGarnetApi::exec` 的整段绑定，而元数据落盘要写透登记表旁路记录
    // （flush_dirty_context_metadata → RegistryPersistence::put 取当前执行域
    // 会话）。已绑定（后台臂/命令段内复用本口）即空操作零开销。上下文元数据
    // 键恒落根域前缀（见 vector_registry_recovery 模块头），落位与所绑会话的
    // 域无关。兜底让位（工厂未装配/会话槽位耗尽）不作门控，真身写透承接层
    // 缺绑自有断言+失败口径
    let _domain = self.ensure_dedicated_session();

    let reserved = {
      let mut metas = self.context_metadatas.lock();
      let (i, sub_contexts) = Self::alloc_context_slot(
        &mut metas,
        &mut self.dirty_context_metadatas.lock(),
        |meta, allow_zero| meta.try_reserve_for_migration(allow_zero, count),
      )?;
      let offset = Self::offset_for_context_metadata(i);
      Some(
        sub_contexts
          .iter()
          .map(|c| offset + u64::from(*c))
          .collect::<Vec<u64>>(),
      )
    };
    if reserved.is_some() {
      self.flush_dirty_context_metadata().await;
    }
    reserved
  }

  /// libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:UpdateContextMetadata
  ///
  /// 将脏元数据刷入持久化承接层（wkv 集成前为域内记录表）。
  pub async fn update_context_metadata(&self) {
    self.flush_dirty_context_metadata().await;
  }

  /// 脏元数据落盘承接（锁内同步收集脏集快照 + 写内存镜像表，随后锁外逐条
  /// 异步写透 KeyTag::VectorRegistry 旁路记录——parking_lot 守卫与
  /// crossbeam pin 守卫均不跨 await；C# 元数据记录驻主存随检查点持久的
  /// rust 等价承接）。
  async fn flush_dirty_context_metadata(&self) {
    let snapshot: Vec<(i32, [u8; CONTEXT_METADATA_SIZE])> = {
      let metas = self.context_metadatas.lock();
      let mut dirty = self.dirty_context_metadatas.lock();
      let snapshot: Vec<(i32, [u8; CONTEXT_METADATA_SIZE])> = dirty
        .iter()
        .filter_map(|&i| metas.get(i).map(|meta| (i as i32, meta.to_bytes())))
        .collect();
      dirty.clear();
      snapshot
    };
    {
      let store = self.metadata_store.pin();
      for (index, bytes) in &snapshot {
        store.insert(*index, *bytes);
      }
    }
    for (index, bytes) in &snapshot {
      self.persist_registry_metadata(*index, bytes).await;
    }
  }

  /// 汇总所有元数据块中命中给定 hash slot 的命名空间（迁移用，委托各 ContextMetadata 扫描）。
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

  /// SWAPDB 换库联动槽位更新（对标 C# NetworkSWAPDB 库级定槽下的槽位翻转对偶）
  ///
  /// 交换两库逻辑槽位后，在用向量上下文的 `slots[]` 盖章联动换号：
  /// - 逻辑库 `db1` 对应槽位 `slot1 = slot_of(ns, db1)`
  /// - 逻辑库 `db2` 对应槽位 `slot2 = slot_of(ns, db2)`
  ///
  /// 遍历属于该租户物理域 `vns` 的在用上下文，若盖章为 `slot1` 则改换为 `slot2`；若为 `slot2` 则改换为 `slot1`。
  /// 更新后将元数据块标记为 dirty 并持久化（`flush_dirty_context_metadata`），
  /// 确保在线改章与后续检查点/恢复/槽迁移发现面口径完全一致。
  pub async fn swap_database_slots(&self, vns: u64, ns: u64, db1: u64, db2: u64) {
    if db1 == db2 {
      return;
    }
    let slot1 = slot_of(ns, db1);
    let slot2 = slot_of(ns, db2);
    if slot1 == slot2 {
      return;
    }

    let _domain = self.ensure_dedicated_session();

    // 收集属于当前租户物理命名空间 vns 的在用上下文
    let mut target_contexts = HashSet::default();
    for (rk, bytes) in self.key_index_registry.pin().iter() {
      let (domain, _) = split_registry_key(rk.as_slice());
      if domain.vns == vns
        && let Some(index) = Index::from_bytes(bytes.as_slice())
      {
        target_contexts.insert(index.context);
      }
    }

    let mut dirty_metas = Vec::new();
    {
      let mut metas = self.context_metadatas.lock();
      for (i, meta) in metas.iter_mut().enumerate() {
        let offset = Self::offset_for_context_metadata(i);
        let mut meta_dirty = false;
        let mut remaining = meta.in_use;
        while remaining != 0 {
          let bit_ix = remaining.trailing_zeros();
          remaining &= !(1u64 << bit_ix);
          let context = offset + u64::from(bit_ix) * CONTEXT_STEP;
          if !target_contexts.is_empty() && !target_contexts.contains(&context) {
            continue;
          }
          if meta.slots[bit_ix as usize] == slot1 {
            meta.slots[bit_ix as usize] = slot2;
            meta_dirty = true;
          } else if meta.slots[bit_ix as usize] == slot2 {
            meta.slots[bit_ix as usize] = slot1;
            meta_dirty = true;
          }
        }
        if meta_dirty {
          dirty_metas.push(i);
        }
      }
    }

    if !dirty_metas.is_empty() {
      {
        let mut dirty = self.dirty_context_metadatas.lock();
        for idx in dirty_metas {
          dirty.insert(idx);
        }
      }
      self.flush_dirty_context_metadata().await;
    }
  }
}
