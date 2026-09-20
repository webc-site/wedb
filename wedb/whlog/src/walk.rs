//! 页内记录链走查单点内核
//!
//! 对标 C# 两条各自独立的页内走查臂的共同步进形态：
//! - libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs:FlushRecordsInRange
//!   （主日志刷盘前 OnFlush 走查，经 [`HybridLog::flush_records_in_range`] 承接）；
//! - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheEvict
//!   （读缓存环形驱逐前哈希链恢复走查，经 [`for_each_record_in_page`] 承接）。
//!
//! 两条臂在 C# 判据并不相同（ReadCacheEvict 对作废记录跳过续步、FlushRecordsInRange
//! 无该臂），故不强拧成一条函数：页内单步解码、终止判据（零头/pad/不可解码/越页界）
//! 与按物理尺寸步进收敛于本文件的 [`next_record`] 一处，页范围簿记（驻留判定、页写锁、
//! 初始页起步偏移、页首地址换算）归分配器层 [`HybridLog::flush_records_in_range`]，
//! 记录语义（过滤、置位、断链恢复）一律外派给调用方闭包。
//! 与地址区间扫描内核 [crate::scan::ScanIterator] 的分工：后者面向跨三区逻辑地址区间、
//! 含磁盘冷读与在途零头自旋，本内核面向驻留页的同步页内走查，二者不互相复刻。

use wdev::Device;
use wrecord::{HEADER_SIZE, RecordHeader};

use crate::hlog::HybridLog;

/// 页内 offset 处单步解码下一条可步进记录，返回 (记录头, 物理尺寸)
///
/// 终止形态（返回 None）：字节不足一头、全零头（`is_null`）、换页填充 pad、
/// 物理尺寸溢出或越出页界。键值区段无需另设越界守卫：kv_size ≤ record_size ≤
/// physical_size 由头编码恒等式（[wrecord::RecordHeader::record_size] 向上对齐 +
/// 非负松弛填充）保证，offset + physical_size ≤ 页界即蕴含键值终点在页内。
/// 物理尺寸恒 ≥ HEADER_SIZE > 0，调用方按返回值步进绝无死循环。
#[inline]
fn next_record(page: &[u8], offset: usize) -> Option<(RecordHeader, usize)> {
  let header = RecordHeader::decode_opt(page.get(offset..)?)?;
  if header.is_pad() || header.is_null() {
    return None;
  }
  let physical_size = header.checked_physical_size()?;
  (offset + physical_size <= page.len()).then_some((header, physical_size))
}

/// 只读走查驻留页内记录链（对标 C# ReadCacheEvict 的页内臂）：自 `from` 起按记录
/// 物理尺寸步进，逐条以 (头, 页内偏移, 键, 值) 调用 `visit`；`visit` 返回 false
/// 即刻终止，终止形态按 [`next_record`] 口径。
///
/// 返回值：`true` 表示走到页尾/终止形态自然收束，`false` 表示 `visit` 提前叫停，
/// 由外层多页调度据此决定是否放弃剩余页。
///
/// 读缓存等自有 `CircularPageBuffer` 的调用方传入整页切片即可复用本单点，
/// 杜绝各消费面复刻解头/步进/终止判据。
pub fn for_each_record_in_page(
  page: &[u8],
  from: usize,
  mut visit: impl FnMut(RecordHeader, usize, &[u8], &[u8]) -> bool,
) -> bool {
  let mut offset = from;
  while let Some((header, physical_size)) = next_record(page, offset) {
    let key_start = offset + HEADER_SIZE;
    let key_end = key_start + header.key_len() as usize;
    let val_end = key_end + header.val_len() as usize;
    if !visit(
      header,
      offset,
      &page[key_start..key_end],
      &page[key_end..val_end],
    ) {
      return false;
    }
    offset += physical_size;
  }
  true
}

/// 可变走查页内记录链（OnFlush 面）：与 [`for_each_record_in_page`] 共用
/// [`next_record`] 单步判据与返回值口径（`true` 自然收束 / `false` 访客叫停），
/// 经不交叠切分向 `visit` 交付可原位改值的记录视图
/// （对标 C# OnFlush 在记录仍驻留内存时就地置 IsFlushed 旗标）。
/// 页写锁的获取与页簿记由 [`HybridLog::flush_records_in_range`] 承担，本函数不公开。
fn for_each_record_in_page_mut(
  page: &mut [u8],
  from: usize,
  mut visit: impl FnMut(RecordHeader, usize, &[u8], &mut [u8]) -> bool,
) -> bool {
  let mut offset = from;
  while let Some((header, physical_size)) = next_record(page, offset) {
    let key_start = offset + HEADER_SIZE;
    let key_end = key_start + header.key_len() as usize;
    let val_end = key_end + header.val_len() as usize;
    let (key_region, val_region) = page.split_at_mut(key_end);
    let stop = !visit(
      header,
      offset,
      &key_region[key_start..key_end],
      &mut val_region[..val_end - key_end],
    );
    offset += physical_size;
    if stop {
      return false;
    }
  }
  true
}

impl<D: Device> HybridLog<D> {
  /// 刷盘前 OnFlush 页范围走查内核（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs:FlushRecordsInRange：
  /// C# 由分配器自走驻留页并经 storeFunctions.CallOnFlush 回调外派语义，rust 同位——
  /// 驻留判定、页写锁、初始页自 page_offset(initial_address) 起步、页首逻辑地址换算
  /// 等页簿记全部收在 whlog，wkv 消费面只提供记录语义闭包，不再钻取 buffer/config）
  ///
  /// 对 `[start_page..=end_page]` 内每个驻留页持页写锁走查页内记录链，逐条以
  /// (头, 记录逻辑地址, 键, 可原位改值的值) 调用 `on_flush`；`on_flush` 返回 false
  /// 提前终止全部走查（错误传播沿用调用方外部捕获模式，本走查自身不落错误）；
  /// 未驻留页直接跳过（刷盘区间之外的页交由其驱逐时按各自上界承接）。
  pub fn flush_records_in_range(
    &self,
    start_page: u64,
    end_page: u64,
    mut on_flush: impl FnMut(RecordHeader, u64, &[u8], &mut [u8]) -> bool,
  ) {
    let init_page = self.config.page_id(self.config.initial_address);
    for p in start_page..=end_page {
      if !self.buffer.is_page_loaded(p) {
        continue;
      }
      let mut guard = self.buffer.write_page(p);
      let page_start = self.config.page_start_address(p);
      let from = if p == init_page {
        self.config.page_offset(self.config.initial_address)
      } else {
        0
      };
      let page: &mut [u8] = &mut guard[..];
      if !for_each_record_in_page_mut(page, from, |header, offset, key, val| {
        on_flush(header, page_start + offset as u64, key, val)
      }) {
        return;
      }
    }
  }
}
