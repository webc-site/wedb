use parking_lot::Mutex;

use crate::address::AddressManager;

/// 异步页面待刷盘区间（对标 Garnet PageAsyncFlushResult）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageFlushRange {
  /// 起始逻辑地址（包含）
  pub from_address: u64,
  /// 截止逻辑地址（不包含）
  pub until_address: u64,
}

impl PageFlushRange {
  /// 构造新的落盘区间
  #[inline]
  pub const fn new(from_address: u64, until_address: u64) -> Self {
    Self {
      from_address,
      until_address,
    }
  }

  /// 区间总字节数
  #[inline]
  pub const fn len(&self) -> u64 {
    self.until_address.saturating_sub(self.from_address)
  }

  /// 是否为空区间
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.from_address >= self.until_address
  }
}

/// 相交/相邻落盘请求合并与完成跟踪队列（对标 Garnet PendingFlushList.cs 与 PageStatusIndicator）
#[derive(Debug, Default)]
pub struct PendingFlushList {
  list: Mutex<Vec<PageFlushRange>>,
  completed: Mutex<Vec<PageFlushRange>>,
}

impl PendingFlushList {
  /// 创建新的待刷盘合并列表
  pub fn new() -> Self {
    Self {
      list: Mutex::new(Vec::with_capacity(16)),
      completed: Mutex::new(Vec::with_capacity(16)),
    }
  }

  /// 插入待刷盘区间
  /// libs/storage/Tsavorite/cs/src/core/Allocator/PendingFlushList.cs:Add
  pub fn add(&self, range: PageFlushRange) {
    if range.is_empty() {
      return;
    }
    self.list.lock().push(range);
  }

  /// 合并出队所有与本次区间相交或相邻的待刷盘区间，返回覆盖并集（唯一合并出口）
  ///
  /// C# 对位：入队侧 `RemovePreviousAdjacent` 先吸收再 Add，出队侧成功回调链
  /// `RemoveNextAdjacent(FlushedUntilAddress)` 逐条清空
  /// （libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForReadOnly
  /// 与 :AsyncFlushPageCallback）；C# 刷盘失败只记 errorList、从不回填
  /// pendingFlushList（:UnsafeSkipError），队列天然无重叠残留。rust 同步 await
  /// 模型失败经 requeue 按 flushed_until 钳制回填，条目起点恒等于重试区间起点
  /// `page_start(page_id(flushed))`（重叠而非相邻），严格相邻匹配恒不吸收：
  /// 持续失败下队列随重试次数无界增长，恢复后 flushed 越过残留条目 until 即永久
  /// 滞留。故 rust 侧将出队收敛为本单点两级机制：
  /// - 先丢弃已被持久化前缀完全覆盖的陈旧条目（`until_address <= flushed`）——
  ///   该段字节已获持久化承诺，等价 C# 完成回调的链式出队，属跨驱动残留兜底；
  /// - 再贪心吸收所有相交或相邻（含界比较）的条目为并集
  ///   `[min(r.from, range.from), max(r.until, range.until))`，迭代至无匹配——
  ///   并集扩张可能令原本不相交的条目与新范围相接；重试区间单次即收敛停机期
  ///   全部旧条目，队列长度收敛为 0 或 1，恢复后零残留。
  pub fn coalesce(&self, mut range: PageFlushRange, flushed: u64) -> PageFlushRange {
    let mut list = self.list.lock();
    list.retain(|r| r.until_address > flushed);
    while let Some(pos) = list
      .iter()
      .position(|r| r.from_address <= range.until_address && range.from_address <= r.until_address)
    {
      let r = list.swap_remove(pos);
      range.from_address = range.from_address.min(r.from_address);
      range.until_address = range.until_address.max(r.until_address);
    }
    range
  }

  /// 标记某段刷盘区间已成功写入介质，并尽可能连续推进 FlushedUntilAddress
  ///
  /// 严格对标 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftFlushedUntilAddress 与 PageStatusIndicator：
  /// - 若该区间起始紧随当前 flushed_until，则直接推进并级联吸收后续已完成但乱序到达的区间；
  /// - 若该区间与当前 flushed_until 存在空洞（前序区间正在异步落盘中），则暂存至 completed 集合，
  ///   确保 FlushedUntilAddress 永远表示一条绝对连续、无空洞的已落盘逻辑前缀，杜绝覆写未落盘脏页；
  /// - 刷盘以页为粒度，末页推进边界可能越过 tail（页内未写区域），
  ///   故钳制至当前 tail，维持 `flushed_until <= tail` 快照不变式。
  pub fn complete_flush_range(&self, range: PageFlushRange, addrs: &AddressManager) {
    if range.is_empty() {
      return;
    }
    let tail_cap = addrs.tail();
    let mut completed = self.completed.lock();
    let current_flushed = addrs.flushed_until();
    if range.from_address <= current_flushed {
      let mut new_flushed = range.until_address.min(tail_cap).max(current_flushed);
      // completed 保持按 from_address 升序排列，单次线性 retain 级联吸收所有连续已完成区间
      completed.retain(|r| {
        if r.from_address <= new_flushed {
          let next_until = r.until_address.min(tail_cap);
          if next_until > new_flushed {
            new_flushed = next_until;
          }
          false
        } else {
          true
        }
      });
      addrs.shift_flushed_until_address(new_flushed);
    } else {
      // 乱序完成区间：二分插入维持 from_address 升序
      let pos = completed.partition_point(|r| r.from_address < range.from_address);
      completed.insert(pos, range);
    }
  }

  /// 当前待刷盘请求数量
  pub fn len(&self) -> usize {
    self.list.lock().len()
  }

  /// 待刷盘与已完成队列是否均为空
  pub fn is_empty(&self) -> bool {
    self.list.lock().is_empty() && self.completed.lock().is_empty()
  }
}
