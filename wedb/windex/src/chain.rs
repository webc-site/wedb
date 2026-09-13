use crate::{bucket::HashBucket, overflow_pool::OverflowPool};

/// 溢出链单步推进结果
pub(crate) enum ChainStep {
  /// 成功前进到链上下一个桶
  Next,
  /// 链在此终止（溢出指针为 0）
  End,
  /// 步数超上限判定为链环/数据损坏（正常数据永不出现，纯防御）
  Cycle,
}

/// 溢出链遍历步数上限：溢出桶挂链后永不释放（free 仅回收挂载竞争败者的未挂载桶），
/// 故链长严格受限于池容量上限 MAX_CHUNKS×CHUNK_SIZE = 2^22；超限即判定数据损坏/链环，
/// 立即终止遍历而非死循环
pub(crate) const MAX_CHAIN_STEPS: usize = 1 << 22;

/// 单槽位扫描决策（[`crate::HashIndex::find_or_create_tag_by_hash_with_min_addr`] 与
/// [`crate::HashIndex::find_tag_entry_by_hash_with_min_addr`] 两套 FindTag 探针共用的单一事实源，
/// 严格对标 C# TsavoriteBase.FindTagOrFreeInternal 的逐槽分类）
pub(crate) enum SlotScan {
  /// 槽位承载目标 Tag 的有效条目，携带原始字（含截断清退 CAS 竞争后并发覆写复核命中）
  Hit(u64),
  /// 槽位为空闲生槽（原本为空，或已截断条目被本线程/他人 CAS 清退）
  Free,
  /// 槽位被其他 Tag 的有效条目占用，继续推进
  Occupied,
}

/// 溢出链遍历器（步数上限防御）
///
/// `curr` 沿溢出链逐步前进，`step` 计数达到 `MAX_CHAIN_STEPS` 即终止遍历；
/// 相比 Floyd 龟兔双指针，免去每两步一次的 tortoise 追踪与二次池查找，
/// 常数开销减半且状态更简单，保证损坏数据下所有链遍历安全退出而非死循环。
pub(crate) struct ChainWalker<'a> {
  pub(crate) curr: &'a HashBucket,
  pub(crate) step: usize,
}

impl<'a> ChainWalker<'a> {
  /// 从主桶出发开始遍历
  #[inline]
  pub(crate) fn new(start: &'a HashBucket) -> Self {
    Self {
      curr: start,
      step: 0,
    }
  }

  /// 沿溢出链前进一步
  #[inline]
  pub(crate) fn advance(&mut self, pool: &'a OverflowPool) -> ChainStep {
    let overflow_idx = self.curr.overflow_index();
    if overflow_idx == 0 {
      return ChainStep::End;
    }
    // SAFETY: 溢出链上挂载的索引恒由 OverflowPool::allocate 合法产出
    // （1..=allocated 且对应 chunk 已初始化），跳过 get 的上界检查与 null 兜底
    self.curr = unsafe { pool.get_unchecked(overflow_idx) };
    self.step += 1;
    if self.step >= MAX_CHAIN_STEPS {
      ChainStep::Cycle
    } else {
      ChainStep::Next
    }
  }
}
