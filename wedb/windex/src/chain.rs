use crate::{Result, bucket::HashBucket, error::Error, overflow_pool::OverflowPool};

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
/// 严格对标 C# libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTagOrFreeInternal 的逐槽分类）
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

  /// 写路径链推进内核：沿溢出链前进一步；走到链尾无溢出桶时先扩链挂载，再前进一步
  ///
  /// 溢出/追加的单点入口，与 C# TsavoriteBase 的 FindOrCreateTag 链尾块同构（C# 全仓
  /// 也只有那一处 Allocate → CAS 挂载 → 败者 Free 块，池侧原语为 MallocFixedPageSize 的
  /// Allocate 与 Free）：[`crate::HashIndex::insert_to_bucket`]、
  /// [`crate::HashIndex::find_or_create_tag_by_hash_with_min_addr`] 两条生产写路径一律转调
  /// 本方法，后续新增写路径不得再抄一遍挂载逻辑。
  ///
  /// 并发协议（与收敛前三处逐字等价）：
  ///
  /// 1. [`Self::advance`] 以 Acquire 读溢出指针判尾，判尾后复读一次，排除「另一线程刚
  ///    抢先挂载」的竞争窗口，避免伪分配空溢出桶；
  /// 2. 复读仍为 0 才分配新桶并 CAS 挂载；CAS 败者必须归还槽位，否则池位单调泄漏；
  /// 3. 无论胜负均沿本桶溢出指针前进一桶——本线程新桶与并发赢家桶在指针层面不可区分，
  ///    前进后由调用方在该桶继续寻槽/搜索；
  /// 4. 步数超 [`MAX_CHAIN_STEPS`] 判为链环/数据损坏，挂载后溢出指针仍为 0 判为池耗尽；
  ///    写路径的两类错误在本方法内的 step_result 单点转译，三个调用点不再各抄一遍 match。
  #[inline]
  pub(crate) fn advance_or_extend(&mut self, pool: &'a OverflowPool) -> Result<()> {
    /// 单步结果 → 写路径结果：三条写路径共用的链环/池耗尽错误转译点
    #[inline(always)]
    fn step_result(step: ChainStep) -> Result<()> {
      match step {
        ChainStep::Next => Ok(()),
        ChainStep::Cycle => Err(Error::OverflowCycleDetected),
        ChainStep::End => Err(Error::OverflowPoolExhausted),
      }
    }

    let step = self.advance(pool);
    if !matches!(step, ChainStep::End) {
      return step_result(step);
    }

    // 复读溢出指针：非 0 说明判尾瞬间已有线程抢先挂载，跳过分配直接沿赢家桶推进
    if self.curr.overflow_index() == 0 {
      let new_idx = pool.allocate()?;
      if !self.curr.set_overflow_index(new_idx) {
        // 挂载败者归还冗余桶（对标
        // libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs:Free），
        // 此后无论谁挂载成功，下一桶均已就位
        pool.free(new_idx);
      }
    }

    // 推进进入新桶（或赢家桶）继续寻找空槽；End 理论不可达（上方已保证溢出指针非零），纯防御兜底
    step_result(self.advance(pool))
  }
}
