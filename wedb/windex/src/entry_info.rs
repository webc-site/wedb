use std::sync::atomic::Ordering;

use crate::{
  bucket::{DATA_ENTRIES, HashBucket},
  entry::HashBucketEntry,
};

/// 哈希槽位精确定位句柄（严格对标 C# Garnet HashEntryInfo）
///
/// 封装单趟遍历定位出的哈希桶指针、槽位索引、旧条目与 Tag，
/// 支持无需二次哈希、无需重新扫描桶链的定点原子 CAS（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/HashEntryInfo.cs:TryCAS）。
#[derive(Debug)]
pub struct HashEntryInfo<'a> {
  pub(crate) bucket: &'a HashBucket,
  pub(crate) slot: usize,
  pub(crate) raw: u64,
  pub(crate) tag: u16,
}

impl<'a> HashEntryInfo<'a> {
  /// 是否在哈希表中命中已存在的匹配 Tag 槽位
  #[inline]
  pub fn is_found(&self) -> bool {
    self.raw != 0
  }

  /// 获取匹配槽位当前存储的逻辑地址
  #[inline]
  pub fn address(&self) -> u64 {
    HashBucketEntry::from_raw(self.raw).address()
  }

  /// 直接在已知槽位上尝试原子 CAS 写入新地址（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/HashEntryInfo.cs:TryCAS）
  ///
  /// - 若为新 Key（`!is_found()`，即 `raw == 0`）：执行 CAS 0 -> new_raw
  /// - 若为已有 Key 更新：执行 CAS old_raw -> new_raw
  #[inline]
  pub fn try_cas(&mut self, new_address: u64) -> bool {
    if new_address == HashBucketEntry::INVALID_ADDRESS
      || new_address > HashBucketEntry::ADDRESS_MASK
    {
      return false;
    }
    debug_assert!(self.slot < DATA_ENTRIES, "try_cas 槽位越界");
    let new_entry = HashBucketEntry::new(new_address, self.tag, false);
    let new_raw = new_entry.as_raw();
    if unsafe { self.bucket.entries.get_unchecked(self.slot) }
      .compare_exchange(self.raw, new_raw, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
    {
      self.raw = new_raw;
      true
    } else {
      false
    }
  }

  /// 槽位所有权判定：快照字为空槽（0），或其条目指纹与本句柄 Tag 一致
  ///
  /// 本 crate 的 `HashEntryInfo` 只能改写「归本句柄所有」的槽位，这是 C# 以
  /// Tentative 占位隐含保证的前提（见 [`Self::set_to_current`] 的对照论证）。
  /// Tag 一致即等价于 C# 的「该槽为本键所据」：探针的两类产出天然满足本判定——
  /// 命中槽由 `HashIndex::classify_slot` 以 `matches_tag` 定序，
  /// 空闲槽快照恒为 0。
  #[inline(always)]
  fn owns_slot(&self, raw: u64) -> bool {
    raw == 0 || HashBucketEntry::from_raw(raw).tag() == self.tag
  }

  /// 对标 C# HashEntryInfo.SetToCurrent：重读当前槽位条目
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/HashEntryInfo.cs:SetToCurrent
  /// ephemeral 桶锁成功后调用：定位 Tag 与加锁之间槽位可能已被并发 CAS/脱钩改写，
  /// 重读杜绝陈旧地址（条目被清退时 raw 归零，调用方按未命中处理）。
  ///
  /// 并发前提（C# 对照 TsavoriteBase.cs:FindOrCreateTag 的 `InsertInBucketInternal`）：
  /// C# 的 SetToCurrent 之所以可以直接取槽位当前字作为后续 TryCAS 的期望值，是因为
  /// 该槽此刻必为本操作所据——要么是 `FindTagOrFreeInternal` 命中的本 Tag 已提交条目，
  /// 要么是刚以 `Interlocked.CompareExchange(0, tentative)` 占位成功的新槽（占位字的
  /// Tag 即本键 Tag，且 C# 截断清退显式豁免 `kTempInvalidAddress`，他人无从覆写）。
  /// 本实现取消了 Tentative 两阶段占位（见
  /// [`crate::HashIndex::find_or_create_tag_by_hash_with_min_addr`] 异同注释），空闲槽
  /// 在末尾单次 CAS 落笔前不受任何保护，探针与本刷新之间可被并发写者抢占为他键条目；
  /// 若无条件采纳该 foreign 字，后续 `try_cas` 会以它为期望值成功落槽，把他人条目从索引
  /// 上抹去（该键记录仍完好存于日志，却再无可命中其 Tag 的槽位，读侧永久 NOTFOUND）。
  /// 故刷新仅在 `owns_slot` 成立时改写快照；不成立时维持探针期的旧值，
  /// 由调用方的 CAS 败者重试（对标 C# RETRY_LATER）自然收敛。
  #[inline]
  pub fn set_to_current(&mut self) {
    let raw = unsafe { self.bucket.entries.get_unchecked(self.slot) }.load(Ordering::Acquire);
    if self.owns_slot(raw) {
      self.raw = raw;
    }
  }

  /// 获取槽位所在桶的独占锁 RAII 守卫（ephemeral X-Latch）
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:FindOrCreateTagAndTryEphemeralXLock
  /// 的锁协议本体（LockTable.TryLockExclusive → HashBucket.TryAcquireExclusiveLatch）
  #[inline]
  pub fn lock_exclusive_guard(&self) -> Option<crate::BucketExclusiveGuard<'a>> {
    self.bucket.lock_exclusive_guard()
  }

  /// 获取槽位所在桶的共享锁 RAII 守卫（ephemeral S-Latch）
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:FindTagAndTryEphemeralSLock
  /// 的锁协议本体（LockTable.TryLockShared → HashBucket.TryAcquireSharedLatch）
  #[inline]
  pub fn lock_shared_guard(&self) -> Option<crate::BucketSharedGuard<'a>> {
    self.bucket.lock_shared_guard()
  }

  /// 尝试原子置零当前槽位以实现物理脱钩删除（Record Elision）
  ///
  /// 若当前槽位包含有效记录且未被并发覆写，单指令 CAS 0 脱钩回收物理槽位。
  /// raw 为 0 时必须直接失败：CAS(0 -> 0) 恒成功会误报删除成功。
  #[inline]
  pub fn try_elide(&mut self) -> bool {
    debug_assert!(self.slot < DATA_ENTRIES, "try_elide 槽位越界");
    if !self.is_found() {
      return false;
    }
    if unsafe { self.bucket.entries.get_unchecked(self.slot) }
      .compare_exchange(self.raw, 0, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
    {
      self.raw = 0;
      true
    } else {
      false
    }
  }
}
