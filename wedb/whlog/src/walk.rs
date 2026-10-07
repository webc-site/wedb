//! 页内记录链走查单点内核
//!
//! 对标 C# 两条各自独立的页内走查臂的共同步进形态：
//! - libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs:FlushRecordsInRange
//!   （主日志刷盘前 OnFlush 走查，经 [`HybridLog::flush_records_in_addr_range`] 承接）；
//! - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheEvict
//!   （读缓存环形驱逐前哈希链恢复走查，经 [`for_each_record_in_page`] 承接）。
//!
//! 两条臂在 C# 判据并不相同（ReadCacheEvict 对作废记录跳过续步、FlushRecordsInRange
//! 无该臂），故不强拧成一条函数：页内单步解码、终止判据（零头/不可解码/越页界）与按物理
//! 尺寸步进收敛于本文件的 [`next_record`] 一处，页范围簿记（驻留判定、页写锁、初始页起步
//! 偏移、页首地址换算）归分配器层 [`HybridLog::flush_records_in_addr_range`]，记录语义
//! （过滤、置位、断链恢复）一律外派给调用方闭包。
//! 与地址区间扫描内核 [crate::scan::ScanIterator] 的分工：后者面向跨三区逻辑地址区间、
//! 含磁盘冷读与在途零头自旋，本内核面向驻留页的同步页内走查，二者不互相复刻；但换页填充
//! pad 的跳步算术 [`wrecord::RecordHeader::pad_extent`] 为两内核共用的唯一真源，pad 按
//! 该值越过本槽留在页内续步，绝不当作页内遍历终点。

use wdev::Device;
use wrecord::{HEADER_SIZE, RecordHeader};

use crate::hlog::HybridLog;

/// 页内 offset 处单步解码下一条可步进记录，返回 (记录头, 记录实际偏移, 物理尺寸)
///
/// 换页填充 pad 与 [crate::scan::ScanIterator] 同一口径：按 [`RecordHeader::pad_extent`]
/// 步进越过本槽、留在页内继续走查后续存活记录，绝不当作页内遍历终点（打 pad 发生在跨页
/// 写入与原位槽位复活处，其后仍可排有存活记录，判终止即漏扫）。全零头（`is_null`）尺寸
/// 不可知，直达页尾兜底。终止形态（返回 None）：字节不足一头、全零头、物理尺寸溢出或越
/// 出页界。键值区段无需另设越界守卫：kv_size ≤ record_size ≤ physical_size 由头编码恒等式
/// （[wrecord::RecordHeader::record_size] 向上对齐 + 非负松弛填充）保证，offset +
/// physical_size ≤ 页界即蕴含键值终点在页内。步进量恒 ≥ HEADER_SIZE > 0，调用方按返回值
/// 步进绝无死循环。
#[inline]
fn next_record(page: &[u8], mut offset: usize) -> Option<(RecordHeader, usize, usize)> {
  loop {
    // from_ptr_atomic 无 Option 语义：先验页内可读完整 16 字节头（越界即终止形态）
    if offset
      .checked_add(HEADER_SIZE)
      .is_none_or(|end| end > page.len())
    {
      return None;
    }
    // SAFETY: 记录 8 字节对齐不变式（RECORD_ALIGNMENT）保证页内记录起点恒对齐（首步由
    // 调用方传页内对齐偏移、后续步进量恒为 8 的倍数），上面已验 [offset, offset+16) 在页
    // 内可读。经 from_ptr_atomic 双字 Acquire 原子读，与写侧 encode_at 的 RDH 字 Release
    // store 配对，使 OnFlush 走查与扫描器共用同一原子读序，消除普通读×无锁裸写的跨线程数据竞争
    let header = unsafe { RecordHeader::from_ptr_atomic(page.as_ptr().add(offset)) };
    if header.is_null() {
      return None;
    }
    let is_pad = header.is_pad();
    let step = match is_pad {
      true => header.pad_extent(),
      false => header.checked_physical_size()?,
    };
    if offset + step > page.len() {
      return None;
    }
    if is_pad {
      offset += step;
      continue;
    }
    return Some((header, offset, step));
  }
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
///
/// 与可变版 [`for_each_record_in_page_mut`]（OnFlush 原位改值臂）系同骨架
/// 双胞：步进/切片换算逐行同构，仅值区可变性不同；骨架变化时两臂必须
/// 同步改，勿增第三胞。
pub fn for_each_record_in_page(
  page: &[u8],
  from: usize,
  mut visit: impl FnMut(RecordHeader, usize, &[u8], &[u8]) -> bool,
) -> bool {
  let mut offset = from;
  while let Some((header, rec_offset, physical_size)) = next_record(page, offset) {
    let key_start = rec_offset + HEADER_SIZE;
    let key_end = key_start + header.key_len() as usize;
    let val_end = key_end + header.val_len() as usize;
    if !visit(
      header,
      rec_offset,
      &page[key_start..key_end],
      &page[key_end..val_end],
    ) {
      return false;
    }
    offset = rec_offset + physical_size;
  }
  true
}

/// 可变走查页内记录链（OnFlush 面）：与 [`for_each_record_in_page`] 共用
/// [`next_record`] 单步判据与返回值口径（`true` 自然收束 / `false` 访客叫停），
/// 经不交叠切分向 `visit` 交付可原位改值的记录视图
/// （对标 C# OnFlush 在记录仍驻留内存时就地置 IsFlushed 旗标）。
/// 页写锁的获取与页簿记由 [`HybridLog::flush_records_in_addr_range`] 承担，本函数不公开；
/// 走查终点由调用方以**页切片长度**交付（`page[..until]`），终止判据与越页界判据同为一个。
fn for_each_record_in_page_mut(
  page: &mut [u8],
  from: usize,
  mut visit: impl FnMut(RecordHeader, usize, &[u8], &mut [u8]) -> bool,
) -> bool {
  let mut offset = from;
  while let Some((header, rec_offset, physical_size)) = next_record(page, offset) {
    let key_start = rec_offset + HEADER_SIZE;
    let key_end = key_start + header.key_len() as usize;
    let val_end = key_end + header.val_len() as usize;
    let (key_region, val_region) = page.split_at_mut(key_end);
    let stop = !visit(
      header,
      rec_offset,
      &key_region[key_start..key_end],
      &mut val_region[..val_end - key_end],
    );
    offset = rec_offset + physical_size;
    if stop {
      return false;
    }
  }
  true
}

impl<D: Device> HybridLog<D> {
  /// 刷盘前 OnFlush 地址区间走查内核（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs:FlushRecordsInRange：
  /// C# 由分配器自走本轮新增已封印的**逻辑地址区间** `[flushStartAddress, flushEndAddress)`
  /// 并经 storeFunctions.CallOnFlush 回调外派语义，rust 同位——驻留判定、页写锁、初始页自
  /// page_offset(initial_address) 起步、页首逻辑地址换算等页簿记全部收在 whlog，wkv 消费面
  /// 只提供记录语义闭包，不再钻取 buffer/config）
  ///
  /// 区间口径是**逻辑地址**而非页：页粒度走查会把本轮未曾落盘的已承诺前缀（`flushed_until`
  /// 以下）整页重走一遍，逐条提交场景下每笔提交都要重扫同一页内此前积累的全部记录，走查
  /// 成本随页内记录数线性放大（O(n²)），实测占每笔提交刷盘成本的 65%~89%，远超设备写本身。
  /// 与同一轮的设备写区间同源，由 [`HybridLog::flush_write_range`] 单点求值。
  ///
  /// 对区间内每个驻留页持页写锁走查页内记录链，逐条以
  /// (头, 记录逻辑地址, 键, 可原位改值的值) 调用 `on_flush`；`on_flush` 返回 false
  /// 提前终止全部走查（错误传播沿用调用方外部捕获模式，本走查自身不落错误）；
  /// 未驻留页直接跳过（其字节交由驱逐路径按各自上界承接）。
  ///
  /// # 契约：`from_addr` 必为记录起始边界
  /// 本内核从该址直接解头步进。生产面下界取自 [`HybridLog::flush_write_range`]，即
  /// `flushed_until`——它恒等于上一轮的封印上界（记录边界）或初始地址；页首地址同样
  /// 是记录边界（跨页写入打 pad，新页必从记录起点起算）。
  pub fn flush_records_in_addr_range(
    &self,
    from_addr: u64,
    until_addr: u64,
    mut on_flush: impl FnMut(RecordHeader, u64, &[u8], &mut [u8]) -> bool,
  ) {
    if from_addr >= until_addr {
      return;
    }
    let init_page = self.config.page_id(self.config.initial_address);
    let first_page = self.config.page_id(from_addr);
    let last_page = self.config.page_id(until_addr - 1);
    for p in first_page..=last_page {
      if !self.buffer.is_page_loaded(p) {
        continue;
      }
      let page_start = self.config.page_start_address(p);
      // 首页自 from_addr 起步（初始页再与 initial_address 的页内偏移取大），末页在
      // until_addr 处收束，中间页走整页；切片长度即走查终点，越切片即终止
      let from = if p == first_page {
        let page_from = self.config.page_offset(from_addr);
        if p == init_page {
          page_from.max(self.config.page_offset(self.config.initial_address))
        } else {
          page_from
        }
      } else {
        0
      };
      let until_off =
        (until_addr.min(page_start + self.config.page_size as u64) - page_start) as usize;
      if from >= until_off {
        continue;
      }
      let mut guard = self.buffer.write_page(p);
      let page: &mut [u8] = &mut guard[..until_off];
      if !for_each_record_in_page_mut(page, from, |header, offset, key, val| {
        on_flush(header, page_start + offset as u64, key, val)
      }) {
        return;
      }
    }
  }
}
