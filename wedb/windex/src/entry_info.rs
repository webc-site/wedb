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

  /// 对标 C# HashEntryInfo.SetToCurrent：重读当前槽位条目
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/HashEntryInfo.cs:SetToCurrent
  /// ephemeral 桶锁成功后调用：定位 Tag 与加锁之间槽位可能已被并发 CAS/脱钩改写，
  /// 重读杜绝陈旧地址（条目被清退时 raw 归零，调用方按未命中处理）
  #[inline]
  pub fn set_to_current(&mut self) {
    self.raw = unsafe { self.bucket.entries.get_unchecked(self.slot) }.load(Ordering::Acquire);
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
