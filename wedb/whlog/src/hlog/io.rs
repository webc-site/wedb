use std::slice::from_raw_parts;

use log::debug;
use wdev::{Device, FlushError};
use wrecord::{HEADER_SIZE, RecordHeader, RecordRef};

use super::{DISK_READ_PROBE_LEN, HybridLog, parse_record_from_slice, reject_pad};
use crate::{
  error::{Error, Result},
  flush::PageFlushRange,
  output::RecordOutput,
};

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
  /// 严格对照 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs 与 InternalRead.cs：闭包直接借用页内物理字节，
  /// 零拷贝零分配。整个内存驻留区（addr < read_only，含模糊区）在 LightEpoch 纪元保护下
  /// 无锁裸指针直读（撕裂安全由 wrecord 头双原子字发布协议保证，论证见 `Self::probe_resident`）；
  /// 真可变区 `[read_only, tail)` 持页读锁与原位更新写锁互斥。
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

  /// 纪元保护下不可变区（含模糊区）记录纯指针直读（严格对标 C# InternalRead.cs:114-124
  /// 可变区甚至模糊区的无锁 CreateLogRecord 直读 +
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/RecordSource.cs:CreateLogRecord
  /// + libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetPhysicalAddress 纯指针寻址）
  ///
  /// 与 [Self::with_memory_record] 的分工：调用方以一次性边界快照完成三分区判定后，
  /// 本方法不再重复加载 head/tail/read_only 与 page_ids 复验（对齐 C# 日志回溯
  /// 查找中每跳 SetPhysicalAddress 零复验的口径，源见 FindRecord.cs）。页槽位清零
  /// 复用以 SafeHeadAddress 纪元排空为门槛（论证见 `shift_head_address` 文档），
  /// 调用方持纪元守卫期间页数据物理稳定；模糊区 `[safe_read_only, read_only)` 内
  /// 在途原位写的布局撕裂由 wrecord 头 RDH 单原子字发布协议豁免（头解析走
  /// `RecordHeader::from_ptr_atomic` 双字 Acquire 载入），值字节可见性与 C# RMW
  /// 原位更新语义一致（论证见 `probe_resident` 文档第 2/3/4 条）。
  ///
  /// # Safety
  /// 调用方契约：
  /// - 调用线程处于 `LightEpoch` 纪元保护下（如 `EpochGuard` / `Participant`）；
  /// - `addr` 处于不可变区（含模糊区）：`addr >= head` 且 `addr < read_only`
  ///   （两者均为调用方进入本调用**之前**的快照；偏旧快照只会令分区判定更保守、
  ///   本方法不被调用，方向安全）。
  pub unsafe fn with_immutable_record<R>(
    &self,
    addr: u64,
    f: impl FnOnce(RecordRef<'_>) -> Result<R>,
  ) -> Result<R> {
    let offset = self.config.page_offset(addr);
    let rest_len = self.config.page_size - offset;
    // SAFETY: 纪元 + 不可变区双门槛契约见方法文档；切片长度取页内剩余字节，
    // parse_record_from_slice 的页边界校验据此与整页口径等价成立（不弱化）
    let ptr = unsafe { self.get_physical_address(addr) };
    let rest = unsafe { from_raw_parts(ptr, rest_len) };
    let rec = parse_record_from_slice(rest, 0, addr, rest_len)?;
    f(rec)
  }

  /// 异步读取指定逻辑地址的记录
  ///
  /// - 若在内存驻留区：不可变区纯指针直读，可变区页读锁保护，返回 `RecordOutput::Memory`；
  /// - 若在磁盘区或已落盘但当前未驻留内存的页面：调用底层 `device.read_range`
  ///   异步读取扇区并解析为 `RecordOutput::Disk`（免纪元保护，不阻塞页回收）。
  ///
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
  /// 冷读为纯设备直读，引擎内不设任何页缓存（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncGetFromDisk →
  /// AsyncReadRecordToMemory：`bufferPool` 租临时缓冲 + `device.ReadAsync`，回调结束即归还）。
  /// 冷读热点缓存由上层单机制承担——wkv `ReadCache`（对标
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs 与
  /// TryCopyToReadCache.cs）或 `copy_reads_to_tail`（对标 TryCopyToTail.cs），
  /// 见 wedb/wkv/src/session/raw/read.rs 的冷读回填链。
  ///
  /// 单条记录绝不产生整页读：首段仅读探针长度并解析记录头，记录超出探针时按物理尺寸
  /// 精确二次读（对标 C# `IStreamBuffer.DefaultInitialIORecordSize` 探针与
  /// `VerifyRecordFromDiskCallback` 以 `prevLengthToRead` 重发同一记录的口径）。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadBlittableRecordToMemory
  pub async fn read_disk_record(&self, addr: u64) -> Result<RecordOutput> {
    if !self.disk_readable(addr) {
      return Err(Error::PageNotReady(self.config.page_id(addr)));
    }

    let page_size = self.config.page_size;
    let offset_in_page = self.config.page_offset(addr);
    let remaining_in_page = page_size - offset_in_page;
    if remaining_in_page < HEADER_SIZE {
      return Err(Error::PadRecord(addr));
    }

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
      if buf.len() < physical_size {
        return Err(Error::RecordCorrupted {
          addr,
          detail: "磁盘读取数据不足记录物理尺寸".into(),
        });
      }
    }
    Ok(RecordOutput::Disk(buf))
  }

  /// 异步将指定逻辑页落盘到 Device（移除非必要单页硬件 fsync，对标 Garnet 零阻塞 Direct I/O）
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:WriteAsyncToDeviceForSnapshot
  pub async fn flush_page(&self, page_id: u64) -> Result<()> {
    self.flush_pages_range(page_id, page_id).await
  }

  /// 将 `[from_addr, until_addr)` 地址区间所在页批量落盘（起点页向下对齐、终点页向上对齐）
  ///
  /// [Self::flush_all]（区间 `[flushed_until, tail)`）与 [super::HybridLog::shift_begin_address]
  /// 补刷（区间 `[flushed_until, new_begin)`）共用的地址→页换算收敛点；区间为空时零 I/O 返回。
  ///
  /// 读侧上界取调用方的**逻辑**终点 `until_addr`，不取页对齐后的写覆盖终点：整页写入是
  /// 设备与恢复端的粒度要求，而只读封印与持久化承诺只到调用方真正要落盘的位置——C# 同型
  /// （GetFlushPageRange 只做页圆整，FlushedUntilAddress 恒停在已封印区间末）。页粒度上溢
  /// 会把调用方未曾意图落盘的区间封成只读，连带凭空收缩可变区并放大复活/紧缩窗口口径。
  pub(crate) async fn flush_addr_range(&self, from_addr: u64, until_addr: u64) -> Result<()> {
    if from_addr >= until_addr {
      return Ok(());
    }
    let start_page = self.config.page_id(from_addr);
    let end_page = self.config.page_id(until_addr.saturating_sub(1));
    self
      .flush_sealed_page_range(start_page, end_page, until_addr)
      .await
  }

  /// 异步合并并批量落盘指定逻辑页范围 `[start_page..=end_page]`（对标 Garnet PendingFlushList + Coalesced Direct I/O）
  ///
  /// 读侧上界（先封再刷）由内核 [Self::flush_sealed_page_range] 自保，本入口的封印上界
  /// 即页范围终点（与 tail 取小）；契约与不变式见该内核文档。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForSnapshot
  pub async fn flush_pages_range(&self, start_page: u64, end_page: u64) -> Result<()> {
    if start_page > end_page {
      return Ok(());
    }
    let until_addr = self.config.page_start_address(end_page.saturating_add(1));
    self
      .flush_sealed_page_range(start_page, end_page, until_addr)
      .await
  }

  /// 页范围刷盘内核：封 → 排空 → 写 → 记账，读侧上界单点自保
  ///
  /// 通过 [crate::flush::PendingFlushList] 贪心合并相邻待刷盘区间为单次连续写；
  /// 成功后经 `complete_flush_range` 保证 `flushed_until` 永远表示绝对连续、无空洞的
  /// 已落盘逻辑前缀（乱序完成区间暂存级联推进）。
  ///
  /// # 崩溃一致性契约（内核自保，调用方无需自备门槛）
  /// 发起任何设备写入之前，先把 ReadOnlyAddress 推进至本次读侧上界 `seal_bound`（与
  /// tail 取小）并等纪元排空使 SafeReadOnlyAddress 达标，故上界以下必无在途未完成的
  /// 记录编码——追加协议是「CAS 推进 tail 发布物理空间 → 同线程裸写 encode_at」
  /// （`hlog/append.rs`），编码不持页锁，tail 越过页界绝不等于该页内记录已定稿。
  /// 次序对标 C#：`AllocatorBase.cs:1644-1652` 用 `epoch.BumpCurrentEpoch` 包裹
  /// `OnPagesMarkedReadOnly`，后者推进 SafeReadOnlyAddress 完成后才发起
  /// `AsyncFlushPagesForReadOnly`（:1744-1758、:2116-2123，"Called when all threads
  /// have agreed that a page range is sealed"）。持久化前缀记账恒等于该已封印上界，
  /// 零/半截记录不可能被计入 `flushed_until`，杜绝「越界刷 → 前缀越过 → 页不重刷 →
  /// 记录在设备上永久缺失」。硬件持久化屏障（fsync）仍由调用方 [Self::sync] 承担
  /// （compio 线程每核模型下 sync 仅覆盖同线程 I/O）。
  ///
  /// 性能设计：
  /// - 刷盘写内核（池借还、扇区界圆整、尾补零、write_aligned 下发与短写校验）单源在
  ///   [Device::flush_range_aligned]，本函数仅提供页表→连续字节的填充闭包；
  /// - 写入范围保持页对齐（恢复端按整页粒度读取，文件必须覆盖完整页），跨页合并为单次 I/O；
  /// - 错误路径（中途 PageNotReady / I/O 失败 / 设备短写）必须将合并区间按当前
  ///   `flushed_until` 钳制后回填 `pending_flush`，保证后续刷盘请求仍可与之合并，
  ///   同时杜绝已被持久化前缀覆盖的陈旧区间滞留（吸收后触碰已驱逐页的永久卡死）。
  async fn flush_sealed_page_range(
    &self,
    start_page: u64,
    end_page: u64,
    seal_bound: u64,
  ) -> Result<()> {
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

    // 先封再刷：读侧上界 = 调用方逻辑终点与 tail 的较小者，亦即本轮持久化前缀记账
    // 终点。内核在拷贝任何字节之前把只读线推到该上界并等纪元排空，使上界以下的在途
    // 编码全部定稿（契约见本方法文档「崩溃一致性契约」）。coalesce 向上吸收的陈旧区间
    // 不参与封印：其字节仍随整页写入设备，但不计入承诺，后续请求自该页起点整体重写
    let sealed_until = seal_bound.min(self.addresses.tail());
    self.seal_read_only_and_drain(sealed_until).await;

    let actual_start_page = self.config.page_id(merged_range.from_address);
    let actual_end_page = self
      .config
      .page_id(merged_range.until_address.saturating_sub(1));
    let total_pages = actual_end_page
      .saturating_sub(actual_start_page)
      .saturating_add(1) as usize;
    let page_size = self.config.page_size;

    // 刷盘写原语单源下沉：扇区界圆整、设备池借还、尾补零与短写校验全部由
    // Device::flush_range_aligned 内核承担（对标 AllocatorBase.cs:WriteInlinePageAsync），
    // 本函数只提供逐页拷贝的填充闭包——页读锁仅护住单页拷贝（短临界区），
    // 内核在 await 前后均不要求持有任何页锁；页内字节的定稿性由入口处的只读封印
    // 排空屏障承担（页读锁护不住 encode_at 的裸指针写，故不作为刷盘准入手段）
    let fill_res = self
      .device
      .flush_range_aligned(
        merged_range.from_address,
        merged_range.until_address,
        |_start_aligned, buf| {
          for (i, p) in (actual_start_page..=actual_end_page).enumerate() {
            let guard = self.buffer.read_page(p);
            // 锁内双重校验（同 probe_resident 的 TOCTOU 论证）：「锁外预检后加锁」的间隙内
            // 页可被并发驱逐复用——重刷已落盘页（钳制后区间含 flushed_until 所在页）时，
            // 并发刷盘完成可推进 flushed_until/head，令换页者回收该页槽位（清零并标定
            // 新页号），随后锁内读到的将是新页字节，落盘即污染设备已持久化前缀。
            // 页清空/标定均持写锁，读锁持有期间页号与数据稳定，锁内复验
            // 通过即保证整段拷贝期间槽位承载的恒为目标页。
            if !self.buffer.is_page_loaded(p) {
              return Err(Error::PageNotReady(p));
            }
            let dest_offset = i * page_size;
            buf[dest_offset..dest_offset + page_size].copy_from_slice(&guard);
          }
          Ok(())
        },
      )
      .await;

    // 设备跨段写入慢路径允许返回「短写成功」：未写满的字节绝不能计入持久化前缀，
    // 与 I/O 失败、填充失败同等处理（回填 pending_flush 并报错），否则 flushed_until
    // 会越过实际未落盘字节，破坏崩溃一致性契约
    if let Err(err) = fill_res {
      self.requeue_flush_range(merged_range);
      return Err(match err {
        FlushError::Fill(e) => e,
        FlushError::ShortWrite { expected, written } => Error::FlushFailed {
          page_id: actual_start_page,
          detail: format!("设备短写: 已写 {written}/{expected} 字节"),
        },
        FlushError::Device(e) => e.into(),
      });
    }

    // 完成记账上界恒为本轮已封印上界：写入覆盖完整页（恢复端整页读取契约），
    // 但持久化前缀承诺绝不越过安全只读线，维持 `flushed_until <= safe_read_only <= tail`
    // 不变式；上界以上的字节本轮不作承诺，后续刷盘自该页起点整体重写
    // （[Self::clamp_flush_range] 的下界页粒度钳制保证其必然被重新覆盖，幂等）
    self.pending_flush.complete_flush_range(
      PageFlushRange::new(merged_range.from_address, sealed_until),
      &self.addresses,
    );
    self.flush_event.notify(usize::MAX);
    debug!(
      "页面范围 [{actual_start_page}..={actual_end_page}] ({total_pages} 页) 成功聚合落盘至偏移 {:#x} ~ {:#x}（已封印上界 {sealed_until:#x}）",
      merged_range.from_address, merged_range.until_address
    );
    Ok(())
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

  /// 显式触发底层存储设备硬件物理刷盘（对标 Garnet Commit / Checkpoint 同步屏障）
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForRecovery
  pub async fn sync(&self) -> Result<()> {
    self.device.sync().await.map_err(Error::from)
  }

  /// 刷写所有未落盘脏页至底层设备并返回最新的 FlushedUntilAddress
  /// （若 flushed_until 已追平 tail 则 0 I/O 直接短路返回）
  pub async fn flush_all(&self) -> Result<u64> {
    let tail = self.addresses.tail();
    self.flush_until_async(tail).await
  }

  /// 异步刷盘至指定逻辑地址（如果正在落盘，绝不重复物理 flush，而是挂起等待合并完成）
  pub async fn flush_until_async(&self, target: u64) -> Result<u64> {
    loop {
      let flushed = self.addresses.flushed_until();
      if flushed >= target {
        return Ok(flushed);
      }

      let listener = self.flush_event.listen();
      let flushed = self.addresses.flushed_until();
      if flushed >= target {
        return Ok(flushed);
      }

      self.flush_addr_range(flushed, target).await?;

      let new_flushed = self.addresses.flushed_until();
      if new_flushed >= target {
        return Ok(new_flushed);
      }

      listener.await;
    }
  }
}
