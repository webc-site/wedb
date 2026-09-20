use std::sync::atomic::{AtomicU64, Ordering, fence};

use crate::{
  bucket::HashBucket,
  candidate::CandidateAddresses,
  chain::{ChainStep, ChainWalker, SlotScan},
  entry::HashBucketEntry,
  entry_info::HashEntryInfo,
  table::HashIndex,
};

/// HashIndex 查找域（桶探测与链首查找，对位
/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs 的
/// TryFindTag 侧：FindTag 单槽探针、候选地址收集与精确条目定位）
impl HashIndex {
  /// 快速单槽位探针查找（严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTag）
  ///
  /// 一旦遇到第 1 个匹配 tag 且非 tentative、address != 0 的 entry，立即返回其逻辑地址。
  /// 绝大多数情况下（99.9%）哈希桶第 0 或第 1 槽位即命中，完全规避全桶 7 槽位扫描原子加载与候选数组分配开销。
  #[inline]
  pub fn find_tag(&self, key: &[u8]) -> Option<u64> {
    let hash = Self::hash_key(key);
    self.find_tag_by_hash(hash)
  }

  /// 基于预先计算的哈希值进行快速单槽位探针查找
  #[inline]
  pub fn find_tag_by_hash(&self, hash: u64) -> Option<u64> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));
    loop {
      if let Some(addr) = walker.curr.find_tag_address(tag) {
        return Some(addr);
      }
      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End | ChainStep::Cycle => return None,
      }
    }
  }

  /// 快速单槽位探针查找并产出可定点 [`HashEntryInfo::try_cas`] / [`HashEntryInfo::try_elide`]
  /// 的槽位句柄，带已截断死槽位无锁实时清退（清退口径与
  /// [`Self::find_or_create_tag_by_hash_with_min_addr`] 完全一致，唯二差异：
  /// 不记录空闲生槽、链尾永不分配溢出桶）
  ///
  /// 纯查找语义，严格对标 C# InternalDelete 经
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:FindTagAndTryEphemeralXLock
  /// 使用的 `TsavoriteBase.FindTag`：只查不建，键不存在时立即 NOTFOUND 返回，
  /// 绝不为满载链分配溢出桶——`FindOrCreateTag` 的建槽语义仅供 Upsert/RMW 使用。
  /// `hash` 由调用方单次算定并全程复用（C# 同口径：`hei.hash` 单源，
  /// 不设 key 版与 by_hash 版两份并行包装）。
  pub fn find_tag_entry_by_hash_with_min_addr(
    &self,
    hash: u64,
    min_valid_addr: u64,
  ) -> Option<HashEntryInfo<'_>> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));
    loop {
      #[inline(always)]
      fn check_slot<'a>(
        bucket: &'a HashBucket,
        slot: usize,
        tag: u16,
        min_valid_addr: u64,
      ) -> Option<HashEntryInfo<'a>> {
        if let SlotScan::Hit(raw) =
          HashIndex::classify_slot(&bucket.entries[slot], tag, min_valid_addr)
        {
          Some(HashEntryInfo {
            bucket,
            slot,
            raw,
            tag,
          })
        } else {
          None
        }
      }

      if let Some(hei) = check_slot(walker.curr, 0, tag, min_valid_addr) {
        return Some(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 1, tag, min_valid_addr) {
        return Some(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 2, tag, min_valid_addr) {
        return Some(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 3, tag, min_valid_addr) {
        return Some(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 4, tag, min_valid_addr) {
        return Some(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 5, tag, min_valid_addr) {
        return Some(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 6, tag, min_valid_addr) {
        return Some(hei);
      }

      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End | ChainStep::Cycle => return None,
      }
    }
  }

  /// 查询匹配指定 Key 对应 Tag 的所有候选逻辑地址（零堆分配栈小数组，带链步数上限保护）
  #[inline]
  pub fn lookup_candidates(&self, key: &[u8]) -> CandidateAddresses {
    self.lookup_candidates_by_hash(Self::hash_key(key))
  }

  /// 基于预先计算好的哈希值查询候选逻辑地址
  pub fn lookup_candidates_by_hash(&self, hash: u64) -> CandidateAddresses {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    let mut results = CandidateAddresses::new();
    let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));
    let expected_hi = (tag as u64) & HashBucketEntry::TAG_MASK;

    loop {
      #[inline(always)]
      fn check_slot(item: &AtomicU64, expected_hi: u64, results: &mut CandidateAddresses) {
        let raw = item.load(Ordering::Relaxed);
        if raw == 0 {
          return;
        }
        if (raw >> HashBucketEntry::TAG_SHIFT) == expected_hi {
          let addr = raw & HashBucketEntry::ADDRESS_MASK;
          if addr != 0 {
            results.push(addr);
          }
        }
      }

      check_slot(&walker.curr.entries[0], expected_hi, &mut results);
      check_slot(&walker.curr.entries[1], expected_hi, &mut results);
      check_slot(&walker.curr.entries[2], expected_hi, &mut results);
      check_slot(&walker.curr.entries[3], expected_hi, &mut results);
      check_slot(&walker.curr.entries[4], expected_hi, &mut results);
      check_slot(&walker.curr.entries[5], expected_hi, &mut results);
      check_slot(&walker.curr.entries[6], expected_hi, &mut results);

      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End | ChainStep::Cycle => break,
      }
    }

    // 命中屏障：调用方将按候选地址解引用记录内存，fence(Acquire) 与发布方
    // CAS(AcqRel) 构成 release/acquire 同步，整链仅此一次
    if !results.is_empty() {
      fence(Ordering::Acquire);
    }

    results
  }

  /// 单槽位分类内核（[`Self::find_or_create_tag_by_hash_with_min_addr`] 与
  /// [`Self::find_tag_entry_by_hash_with_min_addr`] 的单一事实源，逐槽口径严格对齐
  /// C# libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTagOrFreeInternal）：
  ///
  /// - 空槽 → [`SlotScan::Free`]；
  /// - 已提交条目指向被日志截断回收的地址（address < min_valid_addr，ReadCache 条目
  ///   地址含指示位数值上不落入截断区，显式排除）→ 单指令 CAS 置零原位清退：
  ///   清退成功、或并发清退抢先（actual == 0）→ [`SlotScan::Free`]；清退窗口内槽位
  ///   被并发覆写为目标 Tag 的最新有效条目 → [`SlotScan::Hit`]（CAS 失败路径的
  ///   Acquire failure-ordering 载入自身即同步点，无需额外屏障）；
  /// - 其余匹配目标 Tag 的已提交条目 → [`SlotScan::Hit`]（命中槽 Acquire 复读与
  ///   发布方 CAS(AcqRel) 建立 release/acquire 同步，对标 C# FindTag 的 volatile
  ///   读语义，论证见 `HashBucket::find_tag_address`；调用方解引用命中地址的记录
  ///   数据时可见其全部前置写）；
  /// - 非目标 Tag 的有效条目 → [`SlotScan::Occupied`]。
  pub(crate) fn classify_slot(item: &AtomicU64, tag: u16, min_valid_addr: u64) -> SlotScan {
    let raw = item.load(Ordering::Relaxed);
    if raw == 0 {
      return SlotScan::Free;
    }

    let entry = HashBucketEntry::from_raw(raw);

    // 严格对标 C# libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTagOrFreeInternal：
    // 已提交条目指向被日志截断回收的地址时，单指令 CAS 置零原位清退为生槽，
    // 彻底阻断溢出桶伪分配
    if min_valid_addr > 0
      && !entry.is_tentative()
      && !entry.is_read_cache()
      && entry.address() < min_valid_addr
    {
      return match item.compare_exchange(raw, 0, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => SlotScan::Free,
        Err(actual_raw) => {
          if actual_raw == 0 {
            // 另一线程已抢先将其清退置零，当前槽位已成为空闲槽位
            SlotScan::Free
          } else {
            let actual = HashBucketEntry::from_raw(actual_raw);
            if actual.matches_tag(tag)
              && (actual.is_read_cache() || actual.address() >= min_valid_addr)
            {
              // 另一线程已将该槽位并发覆写为目标 Tag 的最新有效条目，直接命中：
              // actual_raw 经 CAS 的 Acquire failure-ordering 载入，与覆写方
              // CAS(AcqRel) 已构成同步，无需额外屏障
              SlotScan::Hit(actual_raw)
            } else {
              // 该槽已被其他有效条目占用且非目标 Tag，继续沿桶推进检查后续槽位
              SlotScan::Occupied
            }
          }
        }
      };
    }

    if entry.matches_tag(tag) {
      let synced_raw = item.load(Ordering::Acquire);
      let synced_entry = HashBucketEntry::from_raw(synced_raw);
      if synced_entry.matches_tag(tag) {
        SlotScan::Hit(synced_raw)
      } else if synced_raw == 0 {
        SlotScan::Free
      } else {
        SlotScan::Occupied
      }
    } else {
      SlotScan::Occupied
    }
  }

  /// 沿溢出链定位精确匹配 `(tag, address)` 的已提交条目（严格对标 TsavoriteBase FindTag + HashEntryInfo 装载）
  ///
  /// 复用 [`HashBucket::find_entry_by_address`] 单桶定位与步数上限链遍历，产出可定点
  /// [`HashEntryInfo::try_cas`] / [`HashEntryInfo::try_elide`] 的哈希槽位句柄。
  pub(crate) fn find_exact_entry_by_hash(
    &self,
    hash: u64,
    address: u64,
  ) -> Option<HashEntryInfo<'_>> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let mut walker = ChainWalker::new(self.get_bucket((hash as usize) & self.mask));
    loop {
      if let Some((slot, entry)) = walker.curr.find_entry_by_address(tag, address) {
        return Some(HashEntryInfo {
          bucket: walker.curr,
          slot,
          raw: entry.as_raw(),
          tag,
        });
      }
      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End | ChainStep::Cycle => return None,
      }
    }
  }
}
