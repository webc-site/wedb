use bitcode::{Decode, Encode};
use parking_lot::Mutex;

use crate::address::AddressManager;

/// 异步页面待刷盘区间（对标 Garnet PageAsyncFlushResult）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
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

/// 连续相邻落盘请求合并与完成跟踪队列（严格对标 Garnet PendingFlushList.cs 与 PageStatusIndicator）
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
  pub fn add(&self, range: PageFlushRange) {
    if range.is_empty() {
      return;
    }
    self.list.lock().push(range);
  }

  /// 移除并返回 until_address 等于指定地址的前相邻区间（对标 libs/storage/Tsavorite/cs/src/core/Allocator/PendingFlushList.cs:RemovePreviousAdjacent）
  pub fn remove_previous_adjacent(&self, address: u64) -> Option<PageFlushRange> {
    let mut list = self.list.lock();
    list
      .iter()
      .position(|r| r.until_address == address)
      .map(|pos| list.swap_remove(pos))
  }

  /// 移除并返回 from_address 等于指定地址的后相邻区间（对标 libs/storage/Tsavorite/cs/src/core/Allocator/PendingFlushList.cs:RemoveNextAdjacent）
  pub fn remove_next_adjacent(&self, address: u64) -> Option<PageFlushRange> {
    let mut list = self.list.lock();
    list
      .iter()
      .position(|r| r.from_address == address)
      .map(|pos| list.swap_remove(pos))
  }

  /// 贪心合并相邻区间并返回合并后的最大连续区间（对标 Garnet 刷盘合并流水线）
  pub fn coalesce(&self, mut range: PageFlushRange) -> PageFlushRange {
    let mut list = self.list.lock();
    // 1. 向前贪心寻找连续相邻区间
    while let Some(pos) = list
      .iter()
      .position(|r| r.until_address == range.from_address)
    {
      range.from_address = list.swap_remove(pos).from_address;
    }
    // 2. 向后贪心寻找连续相邻区间
    while let Some(pos) = list
      .iter()
      .position(|r| r.from_address == range.until_address)
    {
      range.until_address = list.swap_remove(pos).until_address;
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

  /// 清空所有待刷盘与完成跟踪区间
  pub fn clear(&self) {
    self.list.lock().clear();
    self.completed.lock().clear();
  }

  /// 当前待刷盘请求数量
  pub fn len(&self) -> usize {
    self.list.lock().len()
  }

  /// 待刷盘与已完成队列是否均为空
  pub fn is_empty(&self) -> bool {
    self.list.lock().is_empty() && self.completed.lock().is_empty()
  }

  /// 当前暂存的乱序已完成区间数量
  pub fn completed_len(&self) -> usize {
    self.completed.lock().len()
  }
}
