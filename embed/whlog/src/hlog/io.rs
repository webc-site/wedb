use std::sync::atomic::Ordering;

use log::debug;
use wdev::Device;
use wrecord::{HEADER_SIZE, RecordHeader, RecordRef};
use wutil::AlignedBuf;

use super::{
  DISK_READ_CACHE_MASK, DISK_READ_PROBE_LEN, HybridLog, parse_record_from_slice, reject_pad,
};
use crate::{
  config::SECTOR_ALIGNMENT,
  error::{Error, Result},
  flush::PageFlushRange,
  output::RecordOutput,
};

/// 磁盘页内记录头解析：头部读取 + Pad 拒绝 + 页边界校验，返回记录物理尺寸
///
/// 缓存命中页与未命中临时页共用，解析语义与原探针冷读路径逐项一致。
#[inline]
fn parse_disk_page_header(
  page: &[u8],
  offset_in_page: usize,
  addr: u64,
  page_size: usize,
) -> Result<usize> {
  if offset_in_page + HEADER_SIZE > page.len() {
    return Err(Error::RecordCorrupted {
      addr,
      detail: "磁盘读取数据不足记录头大小".into(),
    });
  }
  let header = RecordHeader::from_slice(&page[offset_in_page..offset_in_page + HEADER_SIZE])?;
  reject_pad(header, addr)?;

  let physical_size = header.physical_size();
  if offset_in_page.saturating_add(physical_size) > page_size {
    return Err(Error::RecordCorrupted {
      addr,
      detail: "记录完整内容超出页面容量边界".into(),
    });
  }
  Ok(physical_size)
}

impl<D: Device> HybridLog<D> {
  /// 磁盘冷读准入判定（[Self::read_record] 三区分派与 [Self::read_disk_record]
  /// 前置守卫共用，收敛散落的重复地址换算）：
  /// - `is_on_disk`：addr ∈ `[begin, head)`——已驱逐出内存窗口的正式磁盘区；
  /// - 过渡区：addr ∈ `[head, flushed_until)`——已落盘但 head 尚未越过的并发状态推进窗口。
  #[inline]
  fn disk_readable(&self, addr: u64) -> bool {
    self.addresses.is_on_disk(addr)
      || (addr >= self.addresses.head() && addr < self.addresses.flushed_until())
  }

  /// 同步零拷贝快速探针读取内存驻留记录（无需任何中间 Vec 内存拷贝与堆分配）
  ///
  /// 严格对照 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AllocatorBase.cs 与 InternalRead.cs：闭包直接借用页内物理字节，
  /// 零拷贝零分配。可变区与只读区在 LightEpoch 纪元保护下统一走无锁裸指针直读
  /// （撕裂安全论证与调用方契约详见 [Self::probe_resident]）。
  /// 若记录已不在内存页（在磁盘区或尚未加载），返回 `Ok(None)`，
  /// 调用方可降级走 [Self::read_disk_record] 异步冷读。
  pub fn with_memory_record<R>(
    &self,
    addr: u64,
    f: impl FnOnce(RecordRef<'_>) -> Result<R>,
  ) -> Result<Option<R>> {
    match self.probe_resident(addr)? {
      Some(bytes) => {
        let rec = parse_record_from_slice(
          &bytes,
          self.config.page_offset(addr),
          addr,
          self.config.page_size,
        )?;
        f(rec).map(Some)
      }
      None => Ok(None),
    }
  }

  /// 异步读取指定逻辑地址的记录
  ///
  /// - 若在内存驻留区：不可变区纯指针直读，可变区页读锁保护，返回 `RecordOutput::Memory`；
  /// - 若在磁盘区或已落盘但当前未驻留内存的页面：调用底层 `device.read_range`
  ///   异步读取扇区并解析为 `RecordOutput::Disk`（免纪元保护，不阻塞页回收）。
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadRecordToMemory
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadRecordToMemory
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadRecordToMemory
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadRecordToMemory
  pub async fn read_record(&self, addr: u64) -> Result<RecordOutput> {
    if let Some(bytes) = self.probe_resident(addr)? {
      let offset = self.config.page_offset(addr);
      let rec = parse_record_from_slice(&bytes, offset, addr, self.config.page_size)?;
      let physical_size = rec.physical_size();
      return Ok(RecordOutput::Memory(
        bytes[offset..offset + physical_size].to_vec(),
      ));
    }

    // 磁盘区或已落盘但当前未驻留内存的数据（纯设备 I/O，不触碰内存页缓冲）
    if self.disk_readable(addr) {
      return self.read_disk_record(addr).await;
    }

    if addr < self.addresses.begin() || addr >= self.addresses.tail() {
      Err(Error::AddressOutOfRange {
        addr,
        begin: self.addresses.begin(),
        tail: self.addresses.tail(),
      })
    } else {
      Err(Error::PageNotReady(self.config.page_id(addr)))
    }
  }

  /// 磁盘区记录异步读取（免纪元保护：不触碰内存页缓冲，纯设备 I/O + 池化缓冲内解析）
  ///
  /// 与 [Self::read_record] 磁盘分支共用实现；调用方无需持有纪元守卫即可安全调用，
  /// 冷读 I/O 期间不再阻塞纪元推进与页回收（对标 C# IO 期间 UnsafeSuspendThread 的反面优化）。
  ///
  /// # 连续性装载门槛（sequential-adaptive install）
  /// 直接映射整页磁盘读缓存（[DISK_READ_CACHE_SLOTS] 槽，`page_id % SLOTS` 寻址，
  /// 对标 [crate::scan::ScanIterator] 单页磁盘预取语义）不再"未命中即整页装载"：
  /// 未命中先用 [DISK_READ_PROBE_LEN] 探针冷读（记录 >4KB 时按物理尺寸精确二次读），
  /// 同一页出现**第二次未命中访问**才判定为顺序 / 热点形态，走整页设备读 + 装槽。
  ///
  /// 设计动机：均匀随机负载访问数十万互不相同页，2 槽直接映射命中率≈0，若每次未命中
  /// 都整页设备读（64KB）+ 拷贝，相对 4KB probe 单次 I/O 字节数放大 16 倍（评测实测
  /// 均匀随机读 P50 回退约 15%）。预期行为矩阵（每次点读的设备 I/O 字节数）：
  /// - 均匀随机：每页几乎仅被访问一次 ⇒ 恒 4KB 级 probe，零缓存收益也无字节放大；
  /// - 顺序读：同页第 2 条未命中即整页装载（一次 64KB），其后同页全部缓存命中零 I/O
  ///   （单页摊销 ≈ 4KB + 64KB / 页内记录数）；
  /// - 同页热点：同顺序读——首条 probe、次条装载、其余全命中。
  ///
  /// # 缓存安全性论证（为何整页设备字节可缓存）
  /// - 装载门槛要求整页**均已刷盘且已冻结只读**（`页起点 + page_size <=
  ///   min(flushed_until, read_only)`），两个边界均单调递增，故该条件装载后恒成立：
  ///   - `flushed_until`：页字节已全部落盘；
  ///   - `read_only`：原位更新（`with_mutable_record` 契约 `addr >= read_only`）永远无法
  ///     再触及该页，追加只落在当前尾页 ⇒ 页内存字节已定稿，后续重复刷盘写入的总是
  ///     相同字节 ⇒ 设备字节从此不可变，缓存与设备恒一致（与逐次新鲜 pread 逐字节等价；
  ///     仅刷盘但未滑过只读边界的可变页不装载，避免原位更新 + 重刷造成的陈旧缓存）；
  ///   - 门槛在整页读**之前**评估：两边界单调 ⇒ 读前满足则读后必然仍满足，保守正确；
  /// - 同进程截断（`shift_begin_address` → `truncate_until_address`）仅整段删除历史
  ///   段文件，begin 以下地址再也无法通过前置守卫（`is_on_disk` 要求 `addr >= begin`，
  ///   过渡区要求 `addr >= head >= begin`），陈旧槽位永远不会被命中，无需失效回调；
  ///   64 位逻辑地址单调不复用，无页号 ABA 风险（[Self::last_probe_page] 同理，无需清空）。
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadBlittableRecordToMemory
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadBlittableRecordToMemory
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadBlittableRecordToMemory
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadBlittableRecordToMemory
  pub async fn read_disk_record(&self, addr: u64) -> Result<RecordOutput> {
    if !self.disk_readable(addr) {
      return Err(Error::PageNotReady(self.config.page_id(addr)));
    }

    let page_size = self.config.page_size;
    let page_id = self.config.page_id(addr);
    let offset_in_page = self.config.page_offset(addr);
    let remaining_in_page = page_size - offset_in_page;
    if remaining_in_page < HEADER_SIZE {
      return Err(Error::PadRecord(addr));
    }

    // ---- 命中段：锁内查缓存并拷出记录字节（临界区纯内存操作，绝不跨 .await 持锁） ----
    let slot = (page_id as usize) & DISK_READ_CACHE_MASK;
    {
      let cache = self.disk_read_cache.lock();
      if let Some((p, buf)) = cache[slot].as_ref()
        && *p == page_id
      {
        let page = buf.as_allocated_slice();
        let physical_size = parse_disk_page_header(page, offset_in_page, addr, page_size)?;
        // 经设备缓冲池拷出池化输出（形状与直读路径一致：len == 物理尺寸）后立即解锁
        let record = &page[offset_in_page..offset_in_page + physical_size];
        return Ok(RecordOutput::Disk(
          self.device.pool().get_from_slice(record)?,
        ));
      }
    }

    // ---- 未命中段：连续性装载门槛（sequential-adaptive install，启发式状态见
    //      [HybridLog::last_probe_page]）——swap 记录本次页号，连续两次同页未命中才装载 ----
    let prev_probe_page = self.last_probe_page.swap(page_id, Ordering::Relaxed);
    let page_start = self.config.page_start_address(page_id);
    let page_end = page_start + page_size as u64;
    // 门槛（整页已刷盘且冻结只读）在整页读之前评估：边界单调 ⇒ 读前满足则读后仍满足；
    // 不满足时即便连续两次同页也保守不装载（页字节未定稿，装载有陈旧缓存风险）
    if prev_probe_page != page_id
      || page_end > self.addresses.flushed_until()
      || page_end > self.addresses.read_only()
    {
      // ---- probe 段：4KB 探针冷读（严格对标 C# Garnet InitialIORecordSize）----
      // 首段仅读探针长度解析记录头，记录超出探针时按物理尺寸精确二次读
      // （read_range 对缓冲 I/O 无对齐圆整，二次读无放大），单条记录绝不产生整页读
      let initial_len = DISK_READ_PROBE_LEN.min(remaining_in_page);
      let mut buf = self.device.read_range(addr, initial_len).await?;
      if buf.len() < HEADER_SIZE {
        return Err(Error::RecordCorrupted {
          addr,
          detail: "磁盘读取数据不足记录头大小".into(),
        });
      }
      let header = RecordHeader::from_slice(&buf[..HEADER_SIZE])?;
      reject_pad(header, addr)?;

      let physical_size = header.physical_size();
      if offset_in_page.saturating_add(physical_size) > page_size {
        return Err(Error::RecordCorrupted {
          addr,
          detail: "记录完整内容超出页面容量边界".into(),
        });
      }

      if buf.len() >= physical_size {
        // 记录完整落在探针内：按物理尺寸精确裁剪输出缓冲
        if buf.len() != physical_size {
          buf.set_len(physical_size)?;
        }
      } else {
        // >4KB 记录：按物理尺寸精确二次读
        buf = self.device.read_range(addr, physical_size).await?;
      }
      return Ok(RecordOutput::Disk(buf));
    }

    // ---- 整页装载段：锁外整页设备读（page_size 恒为扇区整数倍，对齐命中零拷贝返回） ----
    let page = self.device.read_range(page_start, page_size).await?;
    let physical_size = parse_disk_page_header(&page, offset_in_page, addr, page_size)?;
    // 记录字节先在锁外拷出（page 为线程私有临时缓冲，无竞争）
    let out = RecordOutput::Disk(
      self
        .device
        .pool()
        .get_from_slice(&page[offset_in_page..offset_in_page + physical_size])?,
    );

    // ---- 装载段：锁内整页写入槽位（拷贝式填充，短临界区，绝不跨 .await 持锁） ----
    // 短读（设备末端不完整页）不可缓存：页字节未满即未定稿
    if page.len() == page_size {
      let mut cache = self.disk_read_cache.lock();
      // 双重校验：并发读可能已在锁间隙装载同页（页面字节不可变，跳过重复填充）
      if cache[slot].as_ref().is_none_or(|(p, _)| *p != page_id) {
        let mut buf = match cache[slot].take() {
          // 已分配槽缓冲：整页覆写换装，驱逐直接映射冲突旧页
          Some((_, buf)) => buf,
          // 惰性分配：该槽首次装载才分配整页容量（SECTOR_ALIGNMENT 对齐）
          None => AlignedBuf::new(page_size, SECTOR_ALIGNMENT).map_err(Error::from)?,
        };
        buf.as_allocated_slice_mut()[..page_size].copy_from_slice(&page);
        cache[slot] = Some((page_id, buf));
      }
    }

    Ok(out)
  }

  /// 异步将指定逻辑页落盘到 Device（移除非必要单页硬件 fsync，对标 Garnet 零阻塞 Direct I/O）
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:WriteAsyncToDeviceForSnapshot
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:WriteAsyncToDeviceForSnapshot
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:WriteAsyncToDeviceForSnapshot
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:WriteAsyncToDeviceForSnapshot
  pub async fn flush_page(&self, page_id: u64) -> Result<()> {
    self.flush_pages_range(page_id, page_id).await
  }

  /// 将 `[from_addr, until_addr)` 地址区间所在页批量落盘（起点页向下对齐、终点页向上对齐）
  ///
  /// [Self::flush_all]（区间 `[flushed_until, tail)`）与 [super::HybridLog::shift_begin_address]
  /// 补刷（区间 `[flushed_until, new_begin)`）共用的地址→页换算收敛点；区间为空时零 I/O 返回。
  pub(crate) async fn flush_addr_range(&self, from_addr: u64, until_addr: u64) -> Result<()> {
    if from_addr >= until_addr {
      return Ok(());
    }
    let start_page = self.config.page_id(from_addr);
    let end_page = self.config.page_id(until_addr.saturating_sub(1));
    self.flush_pages_range(start_page, end_page).await
  }

  /// 异步合并并批量落盘指定逻辑页范围 `[start_page..=end_page]`（对标 Garnet PendingFlushList + Coalesced Direct I/O）
  ///
  /// 通过 [crate::flush::PendingFlushList] 贪心合并相邻待刷盘区间为单次连续写；
  /// 成功后经 `complete_flush_range` 保证 `flushed_until` 永远表示绝对连续、无空洞的
  /// 已落盘逻辑前缀（乱序完成区间暂存级联推进）。
  ///
  /// # 崩溃一致性契约
  /// 本方法仅保证写入设备页缓存/提交队列，硬件持久化屏障由调用方 [Self::sync] 承担
  /// （compio 线程每核模型下 sync 仅覆盖同线程 I/O）；且调用方须保证待刷盘区间内
  /// 不存在在途未完成的记录编码（如先推进 ReadOnlyAddress 并等待纪元排空再刷盘，
  /// 对标 C# 仅刷 SafeReadOnly 以下页面的 OnPagesMarkedReadOnly 驱动策略）。
  ///
  /// 性能设计：
  /// - staging 缓冲以实例字段复用，消除每次刷盘的池借还与归还清零（级联拷贝优化）；
  /// - 写入范围保持页对齐（恢复端按整页粒度读取，文件必须覆盖完整页），跨页合并为单次 I/O；
  /// - 错误路径（中途 PageNotReady / I/O 失败 / 设备短写）必须将合并区间按当前
  ///   `flushed_until` 钳制后回填 `pending_flush`，保证后续刷盘请求仍可与之合并，
  ///   同时杜绝已被持久化前缀覆盖的陈旧区间滞留（吸收后触碰已驱逐页的永久卡死）。
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForSnapshot
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForSnapshot
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForSnapshot
  pub async fn flush_pages_range(&self, start_page: u64, end_page: u64) -> Result<()> {
    if start_page > end_page {
      return Ok(());
    }
    let from_addr = self.config.page_start_address(start_page);
    let until_addr = self.config.page_start_address(end_page.saturating_add(1));

    // 通过 PendingFlushList 贪心合并相邻待刷盘区间
    let merged_range = self
      .pending_flush
      .coalesce(PageFlushRange::new(from_addr, until_addr));

    // 陈旧区间钳制：错误路径回填的待刷区间可能已被后续成功刷盘超越（flushed_until
    // 连续前缀已覆盖其前段甚至全部）。coalesce 相邻吸收会把这类陈旧区间并入本次
    // 写入——其中已滑出内存窗口（head 越过、环形槽位回绕复用）的页不再驻留，原样
    // 拷贝必然 PageNotReady，且错误回填进一步放大区间，最终形成「吸收陈旧区间 →
    // 失败 → 回填更大区间」的永久刷盘卡死。故入口统一钳制到 flushed_until 所在页
    // 起点：该页以下字节已获持久化承诺无需重写（整段覆盖则直接丢弃返回）。
    let merged_range = self.clamp_flush_range(merged_range);
    if merged_range.is_empty() {
      return Ok(());
    }

    let actual_start_page = self.config.page_id(merged_range.from_address);
    let actual_end_page = self
      .config
      .page_id(merged_range.until_address.saturating_sub(1));
    let total_pages = actual_end_page
      .saturating_sub(actual_start_page)
      .saturating_add(1) as usize;
    let total_bytes = merged_range.len() as usize;

    // 复用 staging 缓冲（容量不足时扩容重建；并发刷盘者借空后走独立分配，互不阻塞）
    let mut staging = match self.take_flush_staging(total_bytes) {
      Some(buf) => buf,
      None => AlignedBuf::new(total_bytes, SECTOR_ALIGNMENT).map_err(Error::from)?,
    };
    debug_assert!(staging.capacity() >= total_bytes);
    // SAFETY: 容量不变式由上方 take/分配路径保证（cap >= total_bytes），绝不会失败
    unsafe { staging.set_len_unchecked(total_bytes) };

    // 逐页拷贝：页读锁仅护住单页拷贝（短临界区），await 期间不持任何页锁
    let page_size = self.config.page_size;
    let mut err = None;
    'copy: for (i, p) in (actual_start_page..=actual_end_page).enumerate() {
      if !self.buffer.is_page_loaded(p) {
        err = Some(Error::PageNotReady(p));
        break 'copy;
      }
      let guard = self.buffer.read_page(p);
      let dest_offset = i * page_size;
      staging[dest_offset..dest_offset + page_size].copy_from_slice(&guard);
    }

    if let Some(e) = err {
      self.restore_flush_staging(staging);
      self.requeue_flush_range(merged_range);
      return Err(e);
    }

    // SAFETY（不变式论证）: staging 内容来自 [from, until) 内存页切片，
    // 该区间始于 flushed_until 之下（不可能被页回收清空——页回收要求 head 越过，
    // 而 head <= flushed_until），拷贝完成后即与页内存解耦，await 期间无生命周期风险。
    let (res, buf) = self
      .device
      .write_aligned(merged_range.from_address, staging)
      .await;
    staging = buf;
    // 设备跨段写入慢路径允许返回「短写成功」：未写满的字节绝不能计入持久化前缀，
    // 必须与 I/O 失败同等处理（回填 pending_flush 并报错），否则 flushed_until 会
    // 越过实际未落盘字节，破坏崩溃一致性契约
    let flush_res = match res {
      Ok(n) if n == total_bytes => Ok(()),
      Ok(n) => Err(Error::FlushFailed {
        page_id: actual_start_page,
        detail: format!("设备短写: 已写 {n}/{total_bytes} 字节"),
      }),
      Err(e) => Err(e.into()),
    };
    if let Err(e) = flush_res {
      self.restore_flush_staging(staging);
      self.requeue_flush_range(merged_range);
      return Err(e);
    }

    // 完成记账口径钳制至 tail：写入覆盖完整页（恢复端整页读取契约），
    // 但 flushed_until 记账永不越过 tail，维持 flushed_until <= tail 不变式
    let done_until = merged_range.until_address.min(self.addresses.tail());
    self.pending_flush.complete_flush_range(
      PageFlushRange::new(merged_range.from_address, done_until),
      &self.addresses,
    );
    self.restore_flush_staging(staging);
    debug!(
      "页面范围 [{actual_start_page}..={actual_end_page}] ({total_pages} 页) 成功聚合落盘至偏移 {:#x} ~ {:#x}",
      merged_range.from_address, merged_range.until_address
    );
    Ok(())
  }

  /// 借出容量足够的刷盘 staging 缓冲（None 表示缓存为空或容量不足需调用方自行分配）
  fn take_flush_staging(&self, required: usize) -> Option<AlignedBuf> {
    let mut cache = self.flush_staging.lock();
    match cache.take() {
      Some(buf) if buf.capacity() >= required => Some(buf),
      // 容量不足：丢弃旧缓冲（若入池按池策略清零），由调用方按需新分配
      _ => None,
    }
  }

  /// 将待刷盘区间起点钳制到 flushed_until 所在页起点（低于该起点的字节已获持久化承诺）
  ///
  /// 页粒度钳制保持区间页对齐与扇区对齐写契约；与当前页部分重叠时从页起点整体重写，
  /// 内存页中已落盘部分与设备字节一致（幂等）。整段已被覆盖时返回空区间。
  fn clamp_flush_range(&self, range: PageFlushRange) -> PageFlushRange {
    let floor = self
      .config
      .page_start_address(self.config.page_id(self.addresses.flushed_until()));
    if range.from_address < floor {
      PageFlushRange::new(floor, range.until_address)
    } else {
      range
    }
  }

  /// 错误路径回填待刷盘区间（先按当前 flushed_until 钳制——await 期间并发刷盘可能
  /// 已推进 flushed 覆盖本区间前段，原样回填会让陈旧区间日后被吸收合并触碰已驱逐页）
  fn requeue_flush_range(&self, range: PageFlushRange) {
    let clamped = self.clamp_flush_range(range);
    if !clamped.is_empty() {
      self.pending_flush.add(clamped);
    }
  }

  /// 归还刷盘 staging 缓冲（缓存为空或新缓冲容量更大时收纳复用，否则就地丢弃）
  fn restore_flush_staging(&self, staging: AlignedBuf) {
    let mut cache = self.flush_staging.lock();
    if cache
      .as_ref()
      .is_none_or(|c| staging.capacity() > c.capacity())
    {
      *cache = Some(staging);
    }
  }

  /// 显式触发底层存储设备硬件物理刷盘（对标 Garnet Commit / Checkpoint 同步屏障）
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForRecovery
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForRecovery
  pub async fn sync(&self) -> Result<()> {
    self.device.sync().await.map_err(Error::from)
  }

  /// 刷写所有未落盘脏页至底层设备并返回最新的 FlushedUntilAddress
  pub async fn flush_all(&self) -> Result<u64> {
    let tail = self.addresses.tail();
    let flushed = self.addresses.flushed_until();
    self.flush_addr_range(flushed, tail).await?;
    Ok(self.addresses.flushed_until())
  }

  /// 沿反向指针链遍历历史版本记录（对标 Garnet IterateKeyVersions）
  ///
  /// 从给定起始逻辑地址 `start_addr` 开始沿着 `prev_address` 反向追溯历史版本，
  /// 每条记录调用闭包 `f(addr, record)`，若闭包返回 `Ok(false)` 则提前终止回溯。
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorScan.cs:IterateHashChain
  pub async fn iterate_version_chain<F>(&self, start_addr: u64, mut f: F) -> Result<()>
  where
    F: FnMut(u64, &RecordOutput) -> Result<bool>,
  {
    let mut curr = start_addr;
    let begin = self.addresses.begin();
    while curr >= begin && curr != 0 {
      let record = self.read_record(curr).await?;
      let prev = record.prev_address()?;
      let cont = f(curr, &record)?;
      if !cont || prev == 0 || prev >= curr {
        break;
      }
      curr = prev;
    }
    Ok(())
  }
}
