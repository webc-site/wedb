//! 紧缩扫描游标：hlog 页缓冲预取迭代器 + 单次分配可复用记录缓冲

use wdev::Device;
use whlog::{HybridLog, ScanIterator};
use wrecord::RecordRef;

use crate::error::Result;

/// 紧缩扫描游标（hlog 页缓冲预取迭代器 + 单次分配可复用记录缓冲）
///
/// 拉取返回的记录视图借用自内部缓冲，下一次拉取前保持有效；
/// 冷区扫描由迭代器按页预取，整轮紧缩每页至多一次设备 I/O。
pub(super) struct ScanCursor<'a, D: Device> {
  iter: ScanIterator<'a, D>,
  buf: Vec<u8>,
}

impl<'a, D: Device> ScanCursor<'a, D> {
  /// 创建 [from, until) 区间的紧缩扫描游标（缓冲按整页容量预分配，扫描期零增长）
  pub(super) fn new(hlog: &'a HybridLog<D>, from: u64, until: u64) -> Self {
    Self {
      iter: hlog.scan_iter(from, until),
      buf: Vec::with_capacity(hlog.config.page_size),
    }
  }

  /// 拉取下一条记录视图（逻辑地址 + 零拷贝解析视图）
  pub(super) async fn pull(&mut self) -> Result<Option<(u64, RecordRef<'_>)>> {
    match self.iter.next_into(&mut self.buf).await? {
      Some((addr, raw)) => Ok(Some((addr, RecordRef::from_slice(raw)?))),
      None => Ok(None),
    }
  }

  /// 当前游标地址（扫描耗尽时停在记录边界或页首对齐处）
  pub(super) const fn cursor(&self) -> u64 {
    self.iter.current_address()
  }
}
