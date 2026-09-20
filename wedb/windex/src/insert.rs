use crate::{
  Result,
  bucket::HashBucket,
  chain::{ChainWalker, SlotScan},
  entry::HashBucketEntry,
  entry_info::HashEntryInfo,
  error::Error,
  table::HashIndex,
};

/// HashIndex 修改域（CAS 插入/更新/回收，对位
/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs 的
/// 落槽侧：免查重追加写、FindOrCreateTag 建槽探针与 RCU 地址换新/脱钩删除）
impl HashIndex {
  /// 向指定下标的主哈希桶直接插入 Tag 与逻辑地址（用于扩容分裂迁移与定向重建）
  ///
  /// 生产导出面唯一的免查重追加写入口：寻找空位或沿溢出链插入，单次 CAS 原子发布完整条目
  /// （无半成品窗口），链遍历以步数上限防死循环。
  /// 注意：本方法不做同 Tag 查重——同一 Tag 重复追加即产生多候选（同键多版本场景），
  /// 需要查重语义的写路径走 [`Self::find_or_create_tag_by_hash_with_min_addr`]
  /// （对标 TsavoriteBase FindOrCreateTag）。
  ///
  /// C# 对照（TsavoriteBase.FindOrCreateTag）：C# 为两阶段协议——先 CAS 装 Tentative 占位，
  /// 两阶段之间夹 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOtherSlotForThisTagMaybeTentativeInternal 全链同 Tag 查重去并存。
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
}
