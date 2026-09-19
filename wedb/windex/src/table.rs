use std::{
  hint::spin_loop,
  result,
  sync::atomic::{AtomicU64, Ordering, fence},
};

use whasher::fast_hash;

use crate::{
  Result,
  bucket::{BucketExclusiveGuard, BucketSharedGuard, HashBucket},
  buckets::HashBuckets,
  chain::{ChainStep, ChainWalker, SlotScan},
  entry::HashBucketEntry,
  error::Error,
  overflow_pool::OverflowPool,
};
pub use crate::{
  candidate::CandidateAddresses,
  entry_info::HashEntryInfo,
  prefetch::{PREFETCH_WINDOW, prefetch_read_l1},
};

/// 批量读单键预取探针（对标 C# ContextReadWithPrefetch 内 `hashes[]` 与 `HashEntryInfo` 的
/// 同源装载：键哈希在预取阶段一次算定，随首地址贯穿整条内存读链，内核与回溯零重算）
#[derive(Clone, Copy)]
pub struct PrefetchProbe {
  /// 键哈希（[`HashIndex::hash_key`] 单次算定，供分桶与全链探针复用）
  pub hash: u64,
  /// [`HashIndex::find_tag_by_hash`] 装载的链首地址
  pub first_addr: Option<u64>,
}

impl PrefetchProbe {
  /// 预取窗口内未填充的槽位（有效长度为调用方本批键数）
  const EMPTY: Self = Self {
    hash: 0,
    first_addr: None,
  };
}

/// 64 字节 Cacheline 对齐无锁哈希索引表
///
/// 仿写 Microsoft Garnet Tsavorite 的核心哈希索引：
/// - 每个主哈希桶大小严格为 64 字节，与 CPU 缓存行对齐
/// - 内部包含 7 个数据槽位与 1 个溢出桶/自旋锁管理槽位
/// - 使用 15 位 Tag 进行常数级哈希碰撞前置过滤
/// - 单次 CAS 无锁并发插入（0 -> 完整条目，无半成品窗口；C# 两阶段 Tentative
///   协议的查重职责由调用方候选地址择新承担，见 `insert_to_bucket` 注释）
/// - 支持 RCU 无锁 CAS 更新与原子置零删除
///
/// # 寻址不变量
///
/// `mask == buckets.len() - 1` 且 `buckets.len()` 恒为 2 的幂（构造时强制校验），
/// 三者一经构造终身绑定不可变：单表实例不可变，在线动态扩容通过 SplitIndex 状态机创建新表并分块迁移。
/// 全部 `get_unchecked` 裸寻址的安全前提均依赖该不变量（`x & mask < len` 恒成立）；
/// 公开字段仅供只读检查，外部改写将破坏该安全前提。
pub struct HashIndex {
  pub buckets: HashBuckets,
  pub overflow_pool: OverflowPool,
  pub size: usize,
  pub mask: usize,
}

impl HashIndex {
  /// 创建指定容量的哈希索引表
  ///
  /// 要求 `num_buckets` 必须是 2 的幂且大于 0。
  pub fn new(num_buckets: usize) -> Result<Self> {
    if num_buckets == 0 || !num_buckets.is_power_of_two() {
      return Err(Error::InvalidBucketCount(num_buckets));
    }

    let buckets = HashBuckets::new(num_buckets)?;

    Ok(Self {
      buckets,
      overflow_pool: OverflowPool::new(),
      size: num_buckets,
      mask: num_buckets - 1,
    })
  }

  /// 清空哈希索引表（全部主桶置零，释放全部溢出桶）
  pub fn clear(&self) {
    self.buckets.clear();
    self.overflow_pool.clear();
  }

  /// 获取指定下标的主哈希桶引用（内部自动按 mask 截断，100% 内存安全且消除分支预测越界检查）
  #[inline(always)]
  pub fn get_bucket(&self, bucket_idx: usize) -> &HashBucket {
    let idx = bucket_idx & self.mask;
    unsafe { self.buckets.get_unchecked(idx) }
  }

  /// 使用 gxhash 高性能哈希函数计算键的 64 位哈希值
  #[inline]
  pub fn hash_key(key: &[u8]) -> u64 {
    fast_hash(key)
  }

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

  /// 向指定下标的主哈希桶直接插入 Tag 与逻辑地址（用于扩容分裂迁移与定向重建）
  ///
  /// 生产导出面唯一的免查重追加写入口：寻找空位或沿溢出链插入，单次 CAS 原子发布完整条目
  /// （无半成品窗口），链遍历以步数上限防死循环。
  /// 注意：本方法不做同 Tag 查重——同一 Tag 重复追加即产生多候选（同键多版本场景），
  /// 需要查重语义的写路径走 [`Self::find_or_create_tag_by_hash_with_min_addr`]
  /// （对标 TsavoriteBase FindOrCreateTag）。
  ///
  /// C# 对照（TsavoriteBase.FindOrCreateTag）：C# 为两阶段协议——先 CAS 装 Tentative 占位，
  /// 两阶段之间夹 FindOtherSlotForThisTagMaybeTentativeInternal 全链同 Tag 查重去并存。
  /// 本实现把同 Tag 查重刻意上移为调用方按候选地址择新解决（见
  /// [`Self::find_or_create_tag_by_hash_with_min_addr`] 异同注释），两阶段之间已无任何逻辑：
  /// 相邻的「CAS 装 Tentative + 平写提交」在可观测性上严格等价于单次 CAS 0 -> 完整条目
  /// （读者经 matches_tag 只会看到空槽或完整条目），故合并为单次原子操作，
  /// 插入热路径少一次原子写且无半成品条目窗口。
  pub fn insert_to_bucket(&self, bucket_idx: usize, tag: u16, address: u64) -> Result<()> {
    if address == HashBucketEntry::INVALID_ADDRESS {
      return Err(Error::InvalidAddress(address));
    }
    if address > HashBucketEntry::ADDRESS_MASK {
      return Err(Error::AddressOverflow(address));
    }

    let idx = bucket_idx & self.mask;

    'retry: loop {
      let mut walker = ChainWalker::new(self.get_bucket(idx));

      loop {
        if let Some(slot) = walker.curr.find_empty_slot() {
          if walker.curr.try_insert(slot, tag, address) {
            return Ok(());
          }
          continue 'retry;
        }

        // 槽位已满：沿溢出链推进，链尾无溢出桶时的分配-CAS 挂载-败者归还
        // 统一由 ChainWalker::advance_or_extend 单点内核完成（与探针写路径共用）
        walker.advance_or_extend(&self.overflow_pool)?;
      }
    }
  }

  /// 单槽位分类内核（[`Self::find_or_create_tag_by_hash_with_min_addr`] 与
  /// [`Self::find_tag_entry_by_hash_with_min_addr`] 的单一事实源，逐槽口径严格对齐
  /// C# TsavoriteBase.FindTagOrFreeInternal）：
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
  fn classify_slot(item: &AtomicU64, tag: u16, min_valid_addr: u64) -> SlotScan {
    let raw = item.load(Ordering::Relaxed);
    if raw == 0 {
      return SlotScan::Free;
    }

    let entry = HashBucketEntry::from_raw(raw);

    // 严格对标 C# TsavoriteBase.cs FindTagOrFreeInternal：
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

  /// 单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOrCreateTag / HashEntryInfo）
  ///
  /// 与 C# `TsavoriteBase.FindOrCreateTag` 的异同：
  /// - 同：单趟链遍历、记录首个空槽位、截断死槽位（address < min_valid_addr）原位 CAS 置零清退复用、
  ///   链尾无空槽时分配并 CAS 挂载新溢出桶（失败方归还冗余桶后沿赢家桶深入遍历，挂载内核
  ///   与免查重追加写入口 `insert_to_bucket` 共用 ChainWalker::advance_or_extend 单点）；
  /// - 异：本实现不做 C# 的"先装 Tentative 占位再全链查重"两阶段协议，而是把最终值的原子 CAS
  ///   留给调用方 `HashEntryInfo::try_cas` 一次完成——读者永远只会看到 0 或完整条目，天然免去
  ///   半成品条目窗口；同 Tag 并发插入可能各占一槽形成多候选，由上层按候选地址择新解决。
  ///
  /// 键形态调用方按 `hash_key(key)` 预哈希后调用本方法（C# 同口径：由调用方传 ref hash 或 key，
  /// 不设 key 版与 by_hash 版两份并行包装）。
  pub fn find_or_create_tag_by_hash_with_min_addr(
    &self,
    hash: u64,
    min_valid_addr: u64,
  ) -> Result<HashEntryInfo<'_>> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));
    let mut first_free: Option<(&HashBucket, usize)> = None;

    loop {
      #[inline(always)]
      fn check_slot<'a>(
        bucket: &'a HashBucket,
        slot: usize,
        tag: u16,
        min_valid_addr: u64,
        first_free: &mut Option<(&'a HashBucket, usize)>,
      ) -> Option<HashEntryInfo<'a>> {
        match HashIndex::classify_slot(&bucket.entries[slot], tag, min_valid_addr) {
          SlotScan::Hit(raw) => Some(HashEntryInfo {
            bucket,
            slot,
            raw,
            tag,
          }),
          SlotScan::Free => {
            first_free.get_or_insert((bucket, slot));
            None
          }
          SlotScan::Occupied => None,
        }
      }

      if let Some(hei) = check_slot(walker.curr, 0, tag, min_valid_addr, &mut first_free) {
        return Ok(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 1, tag, min_valid_addr, &mut first_free) {
        return Ok(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 2, tag, min_valid_addr, &mut first_free) {
        return Ok(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 3, tag, min_valid_addr, &mut first_free) {
        return Ok(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 4, tag, min_valid_addr, &mut first_free) {
        return Ok(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 5, tag, min_valid_addr, &mut first_free) {
        return Ok(hei);
      }
      if let Some(hei) = check_slot(walker.curr, 6, tag, min_valid_addr, &mut first_free) {
        return Ok(hei);
      }

      // 链尾（溢出指针为 0）且全链已见可复用空槽：直接复用首空槽收口，绝不转入扩链内核，
      // 否则会给一条「已满但可复用」的链伪分配一个空溢出桶
      if let Some((free_bucket, slot)) = first_free
        && walker.curr.overflow_index() == 0
      {
        return Ok(HashEntryInfo {
          bucket: free_bucket,
          slot,
          raw: 0,
          tag,
        });
      }

      // 沿链推进一格；链尾无溢出桶时的分配-CAS 挂载-败者归还与免查重追加写入口
      // insert_to_bucket 共用 ChainWalker::advance_or_extend 单点内核
      walker.advance_or_extend(&self.overflow_pool)?;
    }
  }

  /// 原子 CAS 更新逻辑地址（RCU 路径，带链步数上限保护）
  ///
  /// 如果在索引中找到匹配的 `(tag, old_address)` 条目，则原子将其地址替换为 `new_address`。
  #[inline]
  pub fn update_address(&self, key: &[u8], old_address: u64, new_address: u64) -> bool {
    self.update_address_by_hash(Self::hash_key(key), old_address, new_address)
  }

  /// 基于哈希值原子 CAS 更新逻辑地址的内部内核（严格对标 TsavoriteBase FindTag 定位 + HashEntryInfo.TryCAS）
  fn update_address_by_hash(&self, hash: u64, old_address: u64, new_address: u64) -> bool {
    if new_address == HashBucketEntry::INVALID_ADDRESS
      || new_address > HashBucketEntry::ADDRESS_MASK
      || old_address == HashBucketEntry::INVALID_ADDRESS
    {
      return false;
    }
    let Some(mut hei) = self.find_exact_entry_by_hash(hash, old_address) else {
      return false;
    };
    hei.try_cas(new_address)
  }

  /// 原子置零删除指定条目（带链遍历保护）
  #[inline]
  pub fn delete(&self, key: &[u8], address: u64) -> bool {
    self.delete_by_hash(Self::hash_key(key), address)
  }

  /// 基于哈希值原子置零删除指定条目的内部内核（严格对标 TsavoriteBase FindTag 定位 + HashEntryInfo.TryElide 记录脱钩）
  fn delete_by_hash(&self, hash: u64, address: u64) -> bool {
    if address == HashBucketEntry::INVALID_ADDRESS {
      return false;
    }
    let Some(mut hei) = self.find_exact_entry_by_hash(hash, address) else {
      return false;
    };
    hei.try_elide()
  }

  /// 沿溢出链定位精确匹配 `(tag, address)` 的已提交条目（严格对标 TsavoriteBase FindTag + HashEntryInfo 装载）
  ///
  /// 复用 [`HashBucket::find_entry_by_address`] 单桶定位与步数上限链遍历，产出可定点
  /// [`HashEntryInfo::try_cas`] / [`HashEntryInfo::try_elide`] 的哈希槽位句柄。
  fn find_exact_entry_by_hash(&self, hash: u64, address: u64) -> Option<HashEntryInfo<'_>> {
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

  /// 获取指定下标的主桶引用
  #[inline]
  pub fn bucket(&self, bucket_idx: usize) -> &HashBucket {
    self.get_bucket(bucket_idx)
  }

  /// 获取指定键对应的主桶引用（crate 内桶锁寻址辅助，导出面不开放：
  /// 外部一律经 [`Self::bucket`] + [`Self::bucket_index_for_key`] 显式两步定位）
  #[inline]
  fn bucket_for_key(&self, key: &[u8]) -> &HashBucket {
    let hash = Self::hash_key(key);
    self.get_bucket((hash as usize) & self.mask)
  }

  /// 计算哈希值对应的主桶索引下标
  #[inline]
  pub fn bucket_index_for_hash(&self, hash: u64) -> usize {
    (hash as usize) & self.mask
  }

  /// 计算键对应的主桶索引下标
  #[inline]
  pub fn bucket_index_for_key(&self, key: &[u8]) -> usize {
    let hash = Self::hash_key(key);
    (hash as usize) & self.mask
  }

  /// 尝试对键对应的主桶获取共享锁（S-Latch）
  #[inline]
  pub fn try_lock_shared(&self, key: &[u8]) -> bool {
    self.bucket_for_key(key).try_lock_shared()
  }

  /// 释放键对应主桶的共享锁
  #[inline]
  pub fn unlock_shared(&self, key: &[u8]) {
    self.bucket_for_key(key).unlock_shared();
  }

  /// 尝试对键对应的主桶获取独占锁（X-Latch）
  #[inline]
  pub fn try_lock_exclusive(&self, key: &[u8]) -> bool {
    self.bucket_for_key(key).try_lock_exclusive()
  }

  /// 释放键对应主桶的独占锁
  #[inline]
  pub fn unlock_exclusive(&self, key: &[u8]) {
    self.bucket_for_key(key).unlock_exclusive();
  }

  /// 将键对应主桶的独占锁原子降级为共享锁
  #[inline]
  pub fn downgrade(&self, key: &[u8]) {
    self.bucket_for_key(key).downgrade_latch();
  }

  /// 判定键对应的主桶是否存在任意锁占用（对标 Garnet IsLocked，供测试并发与死锁断言）
  #[inline]
  pub fn is_locked(&self, key: &[u8]) -> bool {
    self.bucket_for_key(key).is_latched()
  }

  /// 获取键对应主桶的共享锁 RAII 守卫
  #[inline]
  pub fn lock_shared_guard(&self, key: &[u8]) -> Option<BucketSharedGuard<'_>> {
    self.bucket_for_key(key).lock_shared_guard()
  }

  /// 获取键对应主桶的独占锁 RAII 守卫
  #[inline]
  pub fn lock_exclusive_guard(&self, key: &[u8]) -> Option<BucketExclusiveGuard<'_>> {
    self.bucket_for_key(key).lock_exclusive_guard()
  }

  /// 获取键对应主桶的独占锁 RAII 守卫（带自旋退避）
  #[inline]
  pub fn lock_key_exclusive(&self, key: &[u8]) -> Result<BucketExclusiveGuard<'_>> {
    let bucket = self.bucket_for_key(key);
    for _ in 0..1024 {
      if let Some(guard) = bucket.lock_exclusive_guard() {
        return Ok(guard);
      }
      spin_loop();
    }
    bucket.lock_exclusive_guard().ok_or(Error::LockTimeout)
  }

  /// 单批两级硬件预取内核：产出 [`PrefetchProbe`] 探针数组（严格对照
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:ContextReadWithPrefetch）
  ///
  /// 对标 C# 单批内的两趟预取（窗口 [`PREFETCH_WINDOW`] = C# `PrefetchSize`）：
  /// 1. 第一级：逐键单次算定哈希并预取主桶 cacheline（C# `Sse.Prefetch0(tableAligned +
  ///    (hash & size_mask))`）；每个哈希算定后经 `on_hash` 交回调用方推进在线扩容分块
  ///    （rust 协作式迁移，C# 无此步），失败显式上抛，杜绝半迁移状态下按未迁移桶取探针；
  /// 2. 第二级：逐哈希 [`Self::find_tag_by_hash`] 装载链首地址，命中者交 `prefetch_record`
  ///    预取记录物理内存（C# 同位 `FindTag` + `hlogBase.GetPhysicalAddress`；内存驻留区间
  ///    判定由持有日志的调用方在该回调内完成，索引层不感知日志）。
  ///
  /// `keys` 为本批键（长度不超过窗口，超窗由调用方分块逐批调用，内核按窗口长度截断），
  /// 返回定长 [`PREFETCH_WINDOW`] 探针数组，有效长度为 `keys.len()`。
  #[inline]
  pub fn prefetch_batch_probes<K, E>(
    &self,
    keys: &[K],
    mut on_hash: impl FnMut(u64) -> result::Result<(), E>,
    mut prefetch_record: impl FnMut(u64),
  ) -> result::Result<[PrefetchProbe; PREFETCH_WINDOW], E>
  where
    K: AsRef<[u8]>,
  {
    let count = keys.len().min(PREFETCH_WINDOW);
    let mut probes = [PrefetchProbe::EMPTY; PREFETCH_WINDOW];

    // 1. 第一级预取：哈希桶 cacheline（哈希随探针带出，后续读链零重算）
    for (key, probe) in keys.iter().zip(probes[..count].iter_mut()) {
      let hash = Self::hash_key(key.as_ref());
      on_hash(hash)?;
      prefetch_read_l1(self.get_bucket((hash as usize) & self.mask));
      probe.hash = hash;
    }

    // 2. 第二级预取：链首地址命中即预取记录物理内存
    for probe in probes[..count].iter_mut() {
      probe.first_addr = self.find_tag_by_hash(probe.hash);
      if let Some(addr) = probe.first_addr {
        prefetch_record(addr);
      }
    }

    Ok(probes)
  }
}
