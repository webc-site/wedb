use std::{
  hint::spin_loop,
  sync::atomic::{AtomicU64, Ordering, fence},
  thread::{sleep, yield_now},
  time::Duration,
};

use wbase::backoff::Backoff;
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
  candidate::CandidateAddresses, entry_info::HashEntryInfo, guard::MultiBucketGuard,
  prefetch::prefetch_read_l1,
};

/// 64 字节 Cacheline 对齐无锁哈希索引表
///
/// 仿写 Microsoft Garnet Tsavorite 的核心哈希索引：
/// - 每个主哈希桶大小严格为 64 字节，与 CPU 缓存行对齐
/// - 内部包含 7 个数据槽位与 1 个溢出桶/自旋锁管理槽位
/// - 使用 15 位 Tag 进行常数级哈希碰撞前置过滤
/// - 单次 CAS 无锁并发插入（0 -> 完整条目，无半成品窗口；C# 两阶段 Tentative
///   协议的查重职责由调用方候选地址择新承担，见 `insert_by_hash` 注释）
/// - 支持 RCU 无锁 CAS 更新与原子置零删除
///
/// # 寻址不变量
///
/// `mask == buckets.len() - 1` 且 `buckets.len()` 恒为 2 的幂（构造时强制校验），
/// 三者一经构造终身绑定不可变：索引定容，构造后不支持在线扩容。
/// 全部 `get_unchecked` 裸寻址的安全前提均依赖该不变量（`x & mask < len` 恒成立）；
/// 公开字段仅供只读检查，外部改写将破坏该安全前提。
pub struct HashIndex {
  pub buckets: HashBuckets,
  pub overflow_pool: OverflowPool,
  pub size: usize,
  pub mask: usize,
}

impl HashIndex {
  /// 硬件预取滑动窗口大小（1:1 对标 Garnet Tsavorite PrefetchSize = 12）
  pub const PREFETCH_WINDOW: usize = 12;
  /// 栈上内联加锁条目数上限
  pub const INLINE_LOCK_ENTRIES: usize = 16;
  /// 自旋让步阈值（超过后让出 CPU 时间片）
  pub const SPIN_RETRY_THRESHOLD: usize = 32;
  /// 指数退避自旋幂次上限
  pub const SPIN_LIMIT_MAX_EXP: usize = 5;
  /// 自旋抖动掩码
  pub const SPIN_LIMIT_JITTER_MASK: usize = 0x7;
  /// yield 让核重试预算（越过此值进入睡眠退避段）
  pub const YIELD_RETRY_BUDGET: usize = 1024;
  /// 睡眠退避重试预算（100µs→1ms 封顶，合计约 8-16s 耐心窗口，抵御高负载下 CPU 调度毛刺）
  pub const SLEEP_RETRY_BUDGET: usize = 16384;
  /// 睡眠退避起始等待时间（微秒）
  pub const SLEEP_BASE_MICROS: u64 = 100;
  /// 睡眠退避递增上限（微秒）
  pub const SLEEP_MAX_ADDITIONAL_MICROS: u64 = 900;

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
  /// 的槽位句柄（纯查找语义，严格对标 C# InternalDelete 经
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:FindTagAndTryEphemeralXLock
  /// 使用的 `TsavoriteBase.FindTag`：只查不建，键不存在时立即 NOTFOUND 返回，
  /// 绝不为满载链分配溢出桶——`FindOrCreateTag` 的建槽语义仅供 Upsert/RMW 使用）
  #[inline]
  pub fn find_tag_entry(&self, key: &[u8]) -> Option<HashEntryInfo<'_>> {
    self.find_tag_entry_by_hash_with_min_addr(Self::hash_key(key), 0)
  }

  /// 基于哈希值探针查找并产出槽位句柄，带已截断死槽位无锁实时清退
  /// （清退口径与 [`Self::find_or_create_tag_by_hash_with_min_addr`] 完全一致，
  /// 唯二差异：不记录空闲生槽、链尾永不分配溢出桶）
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

  /// 查询匹配指定 Key 对应 Tag 的所有候选逻辑地址列表（`Vec<u64>` 便捷封装）
  #[inline]
  pub fn lookup(&self, key: &[u8]) -> Vec<u64> {
    self.lookup_candidates(key).to_vec()
  }

  /// 向哈希索引中插入键与对应的逻辑地址
  ///
  /// 寻找空位或沿溢出链插入，单次 CAS 原子发布完整条目（无半成品窗口；语义详见
  /// [`Self::insert_by_hash`] 与其 C# 两阶段协议对照注释），链遍历以步数上限防死循环。
  /// 注意：本方法不做同 Tag 查重——键已存在时会产生多候选（同键多版本场景），需要查重语义的
  /// 调用方请使用 `find_tag_or_insert` / `find_or_create_tag`（对标 TsavoriteBase FindOrCreateTag）。
  #[inline]
  pub fn insert(&self, key: &[u8], address: u64) -> Result<()> {
    self.insert_by_hash(Self::hash_key(key), address)
  }

  /// 基于哈希值插入逻辑地址（并发冲突时自动归还冗余溢出桶，杜绝泄漏）
  ///
  /// 试探性 CAS 被并发竞争者抢占时，从链头重走寻找下一个空槽位（严格对标 TsavoriteBase FindOrCreateTag
  /// 的整链重试协议），避免链头附近留下永久空洞、推高溢出链深度恶化探测复杂度。
  pub fn insert_by_hash(&self, hash: u64, address: u64) -> Result<()> {
    if address == HashBucketEntry::INVALID_ADDRESS {
      return Err(Error::InvalidAddress(address));
    }
    if address > HashBucketEntry::ADDRESS_MASK {
      return Err(Error::AddressOverflow(address));
    }

    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    'retry: loop {
      let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));

      loop {
        // 1. 单次 CAS 空槽位插入最终完整条目（CAS(AcqRel) 自带发布屏障，
        //    内存序论证参见 HashBucket::find_tag_address）
        //
        //    C# 对照（TsavoriteBase.FindOrCreateTag）：C# 为两阶段协议——先 CAS 装
        //    Tentative 占位，两阶段之间夹 FindOtherSlotForThisTagMaybeTentativeInternal
        //    全链同 Tag 查重去并存。本实现把同 Tag 查重刻意上移为调用方按候选地址择新
        //    解决（见 find_or_create_tag_by_hash_with_min_addr 异同注释），两阶段之间已无
        //    任何逻辑：相邻的「CAS 装 Tentative + 平写提交」在可观测性上严格等价于单次
        //    CAS 0 -> 完整条目（读者经 matches_tag 只会看到空槽或完整条目），故合并为
        //    单次原子操作，插入热路径少一次原子写且无半成品条目窗口。
        if let Some(slot) = walker.curr.find_empty_slot() {
          if walker.curr.try_insert(slot, tag, address) {
            return Ok(());
          }
          // 空槽位被并发竞争者抢占：从链头重走，寻找下一个空槽位
          continue 'retry;
        }

        // 2. 当前桶数据槽位已满，沿溢出链推进
        match walker.advance(&self.overflow_pool) {
          ChainStep::Next => {}
          ChainStep::End => {
            // 链尾无溢出桶：分配新桶并 CAS 挂载；并发败者归还冗余桶（对标
            // libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs:Free），此后无论谁挂载成功，下一桶均已就位
            if walker.curr.overflow_index() == 0 {
              let new_idx = self.overflow_pool.allocate()?;
              if !walker.curr.set_overflow_index(new_idx) {
                self.overflow_pool.free(new_idx);
              }
            }
            // 推进进入新桶继续寻找空槽；End 分支理论不可达（上一行已保证溢出指针
            // 非零），纯防御兜底
            match walker.advance(&self.overflow_pool) {
              ChainStep::Next => {}
              ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
              ChainStep::End => return Err(Error::OverflowPoolExhausted),
            }
          }
          ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
        }
      }
    }
  }

  /// 单次遍历执行查找或试探性插入（严格对标 TsavoriteBase FindOrCreateTag）
  ///
  /// 若已存在匹配 tag 且有效非试探的非零地址，直接返回 `Ok((Some(existing_addr), false))`；
  /// 若未找到，则在首个空槽位原子 CAS 插入 `(address, tag)` 并返回 `Ok((None, true))`。
  pub fn find_tag_or_insert_by_hash(&self, hash: u64, address: u64) -> Result<(Option<u64>, bool)> {
    if address == HashBucketEntry::INVALID_ADDRESS {
      return Err(Error::InvalidAddress(address));
    }
    if address > HashBucketEntry::ADDRESS_MASK {
      return Err(Error::AddressOverflow(address));
    }

    // CAS 试探冲突回退走 wbase Backoff 三阶退避（spin→yield→sleep，对标 C# SpinWait 语义）
    let mut backoff = Backoff::new();
    loop {
      let mut hei = self.find_or_create_tag_by_hash(hash)?;
      if hei.is_found() {
        return Ok((Some(hei.address()), false));
      }
      if hei.try_cas(address) {
        return Ok((None, true));
      }
      backoff.snooze();
    }
  }

  /// 单次遍历查找或插入键（对标 TsavoriteBase FindOrCreateTag）
  #[inline]
  pub fn find_tag_or_insert(&self, key: &[u8], address: u64) -> Result<(Option<u64>, bool)> {
    self.find_tag_or_insert_by_hash(Self::hash_key(key), address)
  }

  /// 单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOrCreateTag / HashEntryInfo）
  #[inline]
  pub fn find_or_create_tag(&self, key: &[u8]) -> Result<HashEntryInfo<'_>> {
    self.find_or_create_tag_with_min_addr(key, 0)
  }

  /// 单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位，并基于 min_valid_addr 实时清理清退已截断的死槽位
  /// （严格对标 C# Garnet TsavoriteBase.cs:338-352 FindOrCreateTag & kInvalidAddress CAS 原位清退复用）
  #[inline]
  pub fn find_or_create_tag_with_min_addr(
    &self,
    key: &[u8],
    min_valid_addr: u64,
  ) -> Result<HashEntryInfo<'_>> {
    self.find_or_create_tag_by_hash_with_min_addr(Self::hash_key(key), min_valid_addr)
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

  /// 基于哈希值单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位
  #[inline]
  pub fn find_or_create_tag_by_hash(&self, hash: u64) -> Result<HashEntryInfo<'_>> {
    self.find_or_create_tag_by_hash_with_min_addr(hash, 0)
  }

  /// 基于哈希值单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位
  ///
  /// 与 C# `TsavoriteBase.FindOrCreateTag` 的异同：
  /// - 同：单趟链遍历、记录首个空槽位、截断死槽位（address < min_valid_addr）原位 CAS 置零清退复用、
  ///   链尾无空槽时分配并 CAS 挂载新溢出桶（失败方归还冗余桶后沿赢家桶深入遍历）；
  /// - 异：本实现不做 C# 的"先装 Tentative 占位再全链查重"两阶段协议，而是把最终值的原子 CAS
  ///   留给调用方 `HashEntryInfo::try_cas` 一次完成——读者永远只会看到 0 或完整条目，天然免去
  ///   半成品条目窗口；同 Tag 并发插入可能各占一槽形成多候选，由上层按候选地址择新解决。
  pub fn find_or_create_tag_by_hash_with_min_addr(
    &self,
    hash: u64,
    min_valid_addr: u64,
  ) -> Result<HashEntryInfo<'_>> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));
    let mut first_free: Option<(&HashBucket, usize)> = None;

    'search: loop {
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

      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End => {
          // 链尾重读溢出指针为 0：排除「另一线程刚抢先挂载溢出桶」的竞争窗口
          if walker.curr.overflow_index() == 0 {
            if let Some((free_bucket, slot)) = first_free {
              return Ok(HashEntryInfo {
                bucket: free_bucket,
                slot,
                raw: 0,
                tag,
              });
            }

            // 整条链均无空槽位：分配新溢出桶并 CAS 挂载；
            // 并发败者归还冗余桶后沿赢家桶继续深入遍历
            let new_overflow_idx = self.overflow_pool.allocate()?;
            if walker.curr.set_overflow_index(new_overflow_idx) {
              // 挂载成功：新桶由本线程独占产出（全零），slot 0 即首个空槽
              // SAFETY: new_overflow_idx 由本线程 allocate() 刚产出，恒合法且对应 chunk 已初始化
              let new_bucket = unsafe { self.overflow_pool.get_unchecked(new_overflow_idx) };
              return Ok(HashEntryInfo {
                bucket: new_bucket,
                slot: 0,
                raw: 0,
                tag,
              });
            }
            self.overflow_pool.free(new_overflow_idx);
          }
          // 沿赢家桶深入遍历；End 分支理论不可达（上方已保证溢出指针非零），纯防御兜底
          match walker.advance(&self.overflow_pool) {
            ChainStep::Next => continue 'search,
            ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
            ChainStep::End => return Err(Error::OverflowPoolExhausted),
          }
        }
        ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
      }
    }
  }
  /// 原子 CAS 更新逻辑地址（RCU 路径，带链步数上限保护）
  ///
  /// 如果在索引中找到匹配的 `(tag, old_address)` 条目，则原子将其地址替换为 `new_address`。
  #[inline]
  pub fn update_address(&self, key: &[u8], old_address: u64, new_address: u64) -> bool {
    self.update_address_by_hash(Self::hash_key(key), old_address, new_address)
  }

  /// 基于哈希值原子 CAS 更新逻辑地址（严格对标 TsavoriteBase FindTag 定位 + HashEntryInfo.TryCAS）
  pub fn update_address_by_hash(&self, hash: u64, old_address: u64, new_address: u64) -> bool {
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

  /// 基于哈希值原子置零删除指定条目（严格对标 TsavoriteBase FindTag 定位 + HashEntryInfo.TryElide 记录脱钩）
  pub fn delete_by_hash(&self, hash: u64, address: u64) -> bool {
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

  /// 获取已分配的溢出桶总数
  #[inline]
  pub fn overflow_bucket_count(&self) -> u64 {
    self.overflow_pool.allocated_count()
  }

  /// 获取指定下标的主桶引用
  #[inline]
  pub fn bucket(&self, bucket_idx: usize) -> &HashBucket {
    self.get_bucket(bucket_idx)
  }

  /// 获取指定键对应的主桶引用
  #[inline]
  pub fn bucket_for_key(&self, key: &[u8]) -> &HashBucket {
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

  /// 判定键对应的主桶是否存在任意锁占用（对标 Garnet IsLocked）
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

  /// 哈希批量统一流水线驱动：预热窗口 + 滑动窗口预取逐项回调
  ///
  /// 12 项滑动预取窗口 1:1 对标 Garnet Tsavorite ContextReadWithPrefetch
  /// （PrefetchSize = 12）：先预热前 12 个主桶，随后每处理第 i 项前预取
  /// 第 i+12 项主桶，使预取延迟与遍历耗时重叠。
  fn batch_pipeline(&self, hashes: &[u64], mut query: impl FnMut(&Self, u64)) {
    if hashes.is_empty() {
      return;
    }
    for &hash in &hashes[..Self::PREFETCH_WINDOW.min(hashes.len())] {
      prefetch_read_l1(self.get_bucket((hash as usize) & self.mask));
    }
    for (i, &hash) in hashes.iter().enumerate() {
      if let Some(&next_hash) = hashes.get(i + Self::PREFETCH_WINDOW) {
        prefetch_read_l1(self.get_bucket((next_hash as usize) & self.mask));
      }
      query(self, hash);
    }
  }

  /// 基于预先计算好的哈希值进行流水线批量预取与候选地址查询
  pub fn lookup_candidates_batch_by_hash(
    &self,
    hashes: &[u64],
    results: &mut [CandidateAddresses],
  ) {
    let count = hashes.len().min(results.len());
    let mut idx = 0;
    self.batch_pipeline(&hashes[..count], |index, hash| {
      results[idx] = index.lookup_candidates_by_hash(hash);
      idx += 1;
    });
  }

  /// 基于预先计算好的哈希值进行流水线批量预取与快速单槽位探针查找
  pub fn find_tag_batch_by_hash(&self, hashes: &[u64], results: &mut [Option<u64>]) {
    let count = hashes.len().min(results.len());
    let mut idx = 0;
    self.batch_pipeline(&hashes[..count], |index, hash| {
      results[idx] = index.find_tag_by_hash(hash);
      idx += 1;
    });
  }

  /// 原地切片去重，单次单向遍历，将唯一元素排在前部并返回有效长度（零堆分配，稳定版标准 Rust）
  #[inline]
  fn in_place_dedup_by<T: Copy, F>(slice: &mut [T], mut same_bucket: F) -> usize
  where
    F: FnMut(&T, &T) -> bool,
  {
    if slice.len() <= 1 {
      return slice.len();
    }
    let mut write_idx = 1;
    for read_idx in 1..slice.len() {
      if !same_bucket(&slice[write_idx - 1], &slice[read_idx]) {
        if write_idx != read_idx {
          slice[write_idx] = slice[read_idx];
        }
        write_idx += 1;
      }
    }
    write_idx
  }

  /// 统一多键加锁驱动：桶寻址 -> 全序排序去重 -> 两阶段加锁
  ///
  /// 1. 桶下标恒由 `hash & mask` 截断产出（构造不变量 `mask == buckets.len() - 1`），
  ///    为 [`Self::acquire_unique_locked_entries`] 的 get_unchecked 提供安全前提；
  /// 2. 按桶下标升序排序形成全局加锁全序（杜绝死锁），同桶排他锁优先并去重
  ///    （读写混合时保留最高锁级；纯排他路径该 tie-break 为恒等，语义不变）；
  /// 3. 条目数 <= 16 走栈上内联零分配，超出走堆缓冲。
  fn acquire_bucket_locks<I>(&self, items: I) -> Result<MultiBucketGuard<'_>>
  where
    I: ExactSizeIterator<Item = (usize, bool)>,
  {
    let count = items.len();
    if count == 0 {
      return Ok(MultiBucketGuard::new(self));
    }

    let mut stack_entries = [(0usize, false); Self::INLINE_LOCK_ENTRIES];
    let mut heap_entries;
    let entries: &mut [(usize, bool)] = if count <= Self::INLINE_LOCK_ENTRIES {
      for (slot, e) in stack_entries[..count].iter_mut().zip(items) {
        *slot = e;
      }
      &mut stack_entries[..count]
    } else {
      heap_entries = items.collect::<Vec<_>>();
      &mut heap_entries
    };

    // 桶下标升序全序（防死锁）；同桶排他优先（true 排前），相邻去重保留首个
    // 即保留最高锁级（slice 无 dedup_by——该方法为 Vec 专属，栈/堆统一切片
    // 借用故自行实现）
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
    let deduped_len = Self::in_place_dedup_by(entries, |a, b| a.0 == b.0);

    self.acquire_unique_locked_entries(&entries[..deduped_len])
  }

  /// 基于两阶段锁（2PL）获取多个键的独占锁（严格对标 Garnet OverflowBucketLockTable）
  #[inline]
  pub fn acquire_keys_lock_exclusive(&self, keys: &[&[u8]]) -> Result<MultiBucketGuard<'_>> {
    self.acquire_bucket_locks(keys.iter().map(|k| (self.bucket_index_for_key(k), true)))
  }

  /// 获取多个哈希值对应的桶锁（支持读写混合锁，排他锁优先，对标 C# LockAllKeys）
  #[inline]
  pub fn acquire_hash_locks(&self, items: &[(u64, bool)]) -> Result<MultiBucketGuard<'_>> {
    self.acquire_bucket_locks(
      items
        .iter()
        .map(|&(h, ex)| (self.bucket_index_for_hash(h), ex)),
    )
  }

  /// 统一核心加锁驱动引擎（零堆分配回滚、指数退避防活锁）
  ///
  /// 重试预算分三段：短自旋（32 次）→ yield 让核（[`Self::YIELD_RETRY_BUDGET`]）→
  /// 睡眠退避（[`Self::SLEEP_RETRY_BUDGET`]，100µs→1ms 封顶）后报 [`Error::LockTimeout`]
  fn acquire_unique_locked_entries(
    &self,
    unique_entries: &[(usize, bool)],
  ) -> Result<MultiBucketGuard<'_>> {
    let mut retry_count = 0usize;

    loop {
      let mut locked_count = 0usize;

      // 阶段一：顺次尝试加锁
      for &(b_idx, is_exclusive) in unique_entries {
        // SAFETY: 本函数私有，unique_entries 恒由 acquire_bucket_locks 产出，
        // b_idx 源自 bucket_index_for_key/hash（hash & mask 截断，恒小于
        // buckets.len()），无越界风险，免去检查开销
        let bucket = unsafe { self.buckets.get_unchecked(b_idx) };
        let ok = if is_exclusive {
          bucket.try_lock_exclusive()
        } else {
          bucket.try_lock_shared()
        };

        if ok {
          locked_count += 1;
        } else {
          break;
        }
      }

      // 阶段二：校验是否全量加锁成功
      if locked_count == unique_entries.len() {
        return Ok(MultiBucketGuard::from_slice(self, unique_entries));
      }

      // 阶段三：部分加锁失败，在栈上就地逆序回滚解锁（零堆分配开销！）
      for &(b_idx, is_exclusive) in unique_entries[..locked_count].iter().rev() {
        // SAFETY: 同阶段一，b_idx 恒为 hash & mask 截断后的合法桶下标
        let bucket = unsafe { self.buckets.get_unchecked(b_idx) };
        if is_exclusive {
          bucket.unlock_exclusive();
        } else {
          bucket.unlock_shared();
        }
      }

      retry_count += 1;
      if retry_count >= Self::YIELD_RETRY_BUDGET + Self::SLEEP_RETRY_BUDGET {
        return Err(Error::LockTimeout);
      }

      // 指数退避与自旋抖动（Jitter）：彻底消除对称竞争活锁
      if retry_count < Self::SPIN_RETRY_THRESHOLD {
        let spin_limit = (1usize << retry_count.min(Self::SPIN_LIMIT_MAX_EXP))
          | (retry_count & Self::SPIN_LIMIT_JITTER_MASK);
        for _ in 0..spin_limit {
          spin_loop();
        }
      } else if retry_count < Self::YIELD_RETRY_BUDGET {
        yield_now();
      } else {
        // 耐心等待阶段：临界区可合法跨越慢速 I/O await（如 ZADD 持锁写盘），持有者
        // 被 OS 冻结或 I/O 抖动时纯 yield 预算会瞬间烧穿并产生伪 LockTimeout（并发
        // 回归测试在全量套件高负载下曾偶发）。改为 100µs→1ms 封顶的指数睡眠退避，
        // 给出秒级等待窗口后再判超时
        let elapsed = retry_count - Self::YIELD_RETRY_BUDGET;
        let backoff_us = Self::SLEEP_BASE_MICROS
          .saturating_add(((elapsed >> 3) as u64).min(Self::SLEEP_MAX_ADDITIONAL_MICROS));
        sleep(Duration::from_micros(backoff_us));
      }
    }
  }
}
