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

  /// 是否为空区间
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.from_address >= self.until_address
  }
}

/// 相交/相邻落盘请求合并与完成跟踪队列（对标 Garnet PendingFlushList.cs 与 PageStatusIndicator）
#[derive(Debug)]
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
  /// 模型失败经 requeue 按 flushed_until 钳制回填（地址粒度前缀钳制，见
  /// `hlog::HybridLog::clamp_flush_range`），条目起点恒等于重试区间起点
  /// （重叠而非相邻），严格相邻匹配恒不吸收：
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
  ///
  /// 测试握手：C# PendingFlushList.cs（68 行）无 Length/Count 对应物，本对
  /// 观测面只喂 flush_fault 测试守护 rust 侧 coalesce 单点收敛不变式
  /// （持续失败收敛为单条、恢复后清零），无等价真源可断言队列残留
  #[doc(hidden)]
  pub fn len(&self) -> usize {
    self.list.lock().len()
  }

  /// 待刷盘与已完成队列是否均为空
  ///
  /// 测试握手：同上，仅测试消费，守护「零残留」不变式
  #[doc(hidden)]
  pub fn is_empty(&self) -> bool {
    self.list.lock().is_empty() && self.completed.lock().is_empty()
  }
}

// 保留：clippy new_without_default（默认 warn，门禁 -D warnings 必过）反向
// 要求 pub fn new() 配套 Default；构造点 hlog/mod.rs 用 ::new()，本 impl 属
// 门禁信封面而非自造面
impl Default for PendingFlushList {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

/// 合并内核直测：两类型不再经 crate 根导出（导出面收至类型声明层），
/// 原 whlog/tests/hlog/flush_and_shift.rs 测试 6/6b 移入本模块就近守护
#[cfg(test)]
mod tests {
  use super::{PageFlushRange, PendingFlushList};

  /// 贪心双向区间合并（严格对标 C# PendingFlushList.cs）
  #[test]
  fn coalesce_greedy_bidirectional() {
    let list = PendingFlushList::new();
    assert!(list.is_empty());

    // 插入两个不相邻区间：[100, 200) 与 [300, 400)
    list.add(PageFlushRange::new(100, 200));
    list.add(PageFlushRange::new(300, 400));
    assert_eq!(list.len(), 2);

    // 插入连接桥梁 [200, 300)，coalesce 应双向贪心合并为 [100, 400)
    let merged = list.coalesce(PageFlushRange::new(200, 300), 0);
    assert_eq!(merged, PageFlushRange::new(100, 400));
    assert!(list.is_empty(), "合并后队列中原本的相邻区间应已被取出");
  }

  /// 相交/包含重叠合并与已落盘前缀覆盖条目丢弃
  ///
  /// 对标 C# pendingFlushList 无重叠残留形态（失败从不回填、出队由
  /// RemoveNextAdjacent(FlushedUntilAddress) 链式清空）：rust 回填条目与重试区间
  /// 同起点重叠，判据若只认严格相邻则恒不吸收、队列无界增长且恢复后永久滞留。
  #[test]
  fn coalesce_overlap_and_discard() {
    let list = PendingFlushList::new();

    // 同起点重叠:条目 [100,200) 与重试区间 [100,300) 同 from，必须整体吸收
    list.add(PageFlushRange::new(100, 200));
    assert_eq!(
      list.coalesce(PageFlushRange::new(100, 300), 0),
      PageFlushRange::new(100, 300)
    );
    assert_eq!(list.len(), 0, "同起点重叠条目必须被吸收出队");

    // 反向包含:条目 [100,500) 完全覆盖重试区间 [200,300)
    list.add(PageFlushRange::new(100, 500));
    assert_eq!(
      list.coalesce(PageFlushRange::new(200, 300), 0),
      PageFlushRange::new(100, 500)
    );
    assert_eq!(list.len(), 0, "包含关系条目必须被吸收出队");

    // 局部重叠 + 并集扩张传递吸收: [250,400) 相交并入至 400 后，[400,500) 转为相接再吸收
    list.add(PageFlushRange::new(250, 400));
    list.add(PageFlushRange::new(400, 500));
    assert_eq!(
      list.coalesce(PageFlushRange::new(200, 300), 0),
      PageFlushRange::new(200, 500)
    );
    assert_eq!(list.len(), 0, "扩张后相接的条目必须二次收敛");

    // 已被持久化前缀覆盖的陈旧条目(until <= flushed)直接丢弃，不并入写入区间
    list.add(PageFlushRange::new(100, 200));
    assert_eq!(
      list.coalesce(PageFlushRange::new(500, 600), 200),
      PageFlushRange::new(500, 600)
    );
    assert_eq!(list.len(), 0, "已落盘前缀覆盖条目必须丢弃兜底");

    // 不相交且不相邻的条目原样保留
    list.add(PageFlushRange::new(700, 800));
    assert_eq!(
      list.coalesce(PageFlushRange::new(200, 300), 0),
      PageFlushRange::new(200, 300)
    );
    assert_eq!(list.len(), 1, "无关条目不得被误吸收");
  }
}
