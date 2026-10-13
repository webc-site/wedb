use std::slice::from_raw_parts;

use log::debug;
use wdev::{Device, FlushError};
use wrecord::{HEADER_SIZE, RecordHeader, RecordRef};

use super::{
  DISK_READ_PROBE_LEN, HybridLog, RECORD_EXCEEDS_PAGE_DETAIL, parse_record_from_slice, reject_pad,
};
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

  /// 同步零拷贝借用内存驻留记录的 value 切片（[Self::with_memory_record] 的
  /// 借值特化：闭包直接消费 `&[u8]`，无需返回值携 Result）
  ///
  /// 服务对象信封写回成功后的 AOF 镜像：镜像值与记录共享同一字节（对齐 C#
  /// WriteLogUpsert 从 srcLogRecord 取值入账），杜绝为镜像再物化整值缓冲
  pub fn with_record_value<R>(&self, addr: u64, f: impl FnOnce(&[u8]) -> R) -> Result<Option<R>> {
    match self.probe_resident(addr)? {
      Some(bytes) => {
        let rec = parse_record_from_slice(
          &bytes,
          self.config.page_offset(addr),
          addr,
          self.config.page_size,
        )?;
        Ok(Some(f(rec.value())))
      }
      None => Ok(None),
    }
  }

  /// 驻留槽位纯头窥视（不做键值尺寸校验、不拒 Pad、零拷贝），复活池密封不变式
  /// 的唯一头级内省口
  ///
  /// [Self::with_memory_record] / [Self::read_record] 读路径对 Pad 一律
  /// `reject_pad` 拒读；而池中合法驻留形态（复活分裂切出块）只问 RecordInfo
  /// 标志位（SEALED）——对标 C# FreeRecordPool 出入池两侧对 RecordInfo.IsSealed
  /// 的直接断言口径（Helpers.cs:128/:151），头判读无需键值布局参与。
  /// 地址未驻留（磁盘区 / 页未加载）返回 `Ok(None)`。
  ///
  /// 测试握手：生产零调用，保留仅供 wkv 复活池「归池必密封」不变式的
  /// 头级断言（Pad 拒读路径无法承接该内省，无等价生产通道）
  #[doc(hidden)]
  pub fn peek_memory_header(&self, addr: u64) -> Result<Option<RecordHeader>> {
    match self.probe_resident(addr)? {
      Some(bytes) => {
        let offset = self.config.page_offset(addr);
        Ok(
          offset
            .checked_add(HEADER_SIZE)
            .filter(|&end| end <= self.config.page_size.min(bytes.len()))
            .and_then(|_| RecordHeader::decode_opt(&bytes[offset..])),
        )
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
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ReadAsync
  /// libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteAllocatorImpl.cs:ReadAsync
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
    //
    // 探针上界另与「持久化前缀在本址之后的剩余字节」取小：设备上的日志字节恒等于
    // flushed_until（缓冲 I/O 尾零头精确写、无 pad；Direct I/O 至多一个尾扇区补零），
    // 页尾未封印字节在设备上不存在，越界探针会短读成
    // UnexpectedEof（对标 C# 设备读可返回短计数的传输计数契约，见 wdev IDevice 文档）
    let flushed_remaining = self.addresses.flushed_until().saturating_sub(addr) as usize;
    let initial_len = DISK_READ_PROBE_LEN
      .min(remaining_in_page)
      .min(flushed_remaining);
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
        detail: RECORD_EXCEEDS_PAGE_DETAIL.into(),
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
  ///
  /// 页→地址换算仅此一处薄适配；测试握手：生产零调用，保留仅供测试以单页粒度驱动
  /// [Self::flush_addr_range] 内核（生产面由 [Self::flush_all] 与
  /// [super::HybridLog::shift_begin_address] 补刷直接按地址区间进入）
  #[doc(hidden)]
  pub async fn flush_page(&self, page_id: u64) -> Result<()> {
    self
      .flush_addr_range(
        self.config.page_start_address(page_id),
        self.config.page_start_address(page_id.saturating_add(1)),
      )
      .await
  }

  /// 本轮刷盘的读写区间单点求值（OnFlush 走查与设备写的共同界）
  ///
  /// 严格对标 C# `OnPagesMarkedReadOnlyWorker` 的
  /// `[LastIssuedFlushedUntilAddress, OngoingFlushedUntilAddress)`（
  /// libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs:OnPagesMarkedReadOnlyWorker）：
  /// 下界恒为已获持久化承诺的前缀末 `flushed_until`（其以下字节设备上已有，重走查重写
  /// 皆属多余），上界=调用方请求终点与日志 tail 的较小者（tail 以上尚无定稿字节可落盘）。
  /// 刷盘内核 [Self::flush_sealed_page_range] 在 coalesce 之后按同一口径再钳一次：并发
  /// 刷盘可在求值与落笔之间推进 `flushed_until`，该处钳制是兜底而非第二套换算——二者
  /// 同由 `flushed_until` / `tail` 两个真源导出（`clamp_flush_range`）。
  /// 空区间（前缀已覆盖请求终点）由调用方按 [`PageFlushRange::is_empty`] 判定后零 I/O 跳过。
  pub fn flush_write_range(&self, until_addr: u64) -> PageFlushRange {
    PageFlushRange::new(
      self.addresses.flushed_until(),
      until_addr.min(self.addresses.tail()),
    )
  }

  /// 将 `[from_addr, until_addr)` 地址区间批量落盘（逻辑地址粒度，跨页合并为单次连续写）
  ///
  /// [Self::flush_all]（区间 `[flushed_until, tail)`）、[super::HybridLog::shift_begin_address]
  /// 补刷（区间 `[flushed_until, new_begin)`）与 wkv 组提交/驱逐路径共用的地址→页换算收敛点；
  /// 区间为空时零 I/O 返回。wkv 侧的 OnFlush 走查与本写入必须同区间，界一律取自
  /// [Self::flush_write_range]（其下界与本函数的 `from_addr` 同为 `flushed_until`），
  /// 杜绝在消费面复刻钳制口径。
  ///
  /// 读侧上界取调用方的**逻辑**终点 `until_addr`：只读封印与持久化承诺只到调用方真正
  /// 要落盘的位置——C# 同型（FlushedUntilAddress 恒停在已封印区间末）。逻辑终点以下的
  /// 字节按 C# `WriteInlinePageAsync`「Write only required bytes within the page」的口径
  /// 精确落盘，页粒度上溢既不会把调用方未曾意图落盘的区间封成只读（连带凭空收缩可变区、
  /// 放大复活/紧缩窗口口径），也不会让未封印字节上设备。
  pub async fn flush_addr_range(&self, from_addr: u64, until_addr: u64) -> Result<()> {
    if from_addr >= until_addr {
      return Ok(());
    }
    let start_page = self.config.page_id(from_addr);
    let end_page = self.config.page_id(until_addr.saturating_sub(1));
    self
      .flush_sealed_page_range(start_page, end_page, until_addr)
      .await
  }

  /// 页范围刷盘内核：封 → 排空 → 写 → 记账，读侧上界单点自保
  ///
  /// C# 分配器页写抽象的统一落点：
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:WriteAsync
  /// libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteAllocatorImpl.cs:WriteAsync
  ///
  /// 通过 [crate::flush::PendingFlushList] 合并相交或相邻的待刷盘区间为单次连续写；
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
  /// # 刷盘写序（在途写互斥，对标 AllocatorBase.cs:AsyncFlushPagesForReadOnly
  /// :2210-2214 部分页片段串行契约）
  /// 封印排空之后、第二次钳制之前入 [`HybridLog::flush_gate`] 异步闸，守卫存活至
  /// 函数末尾，覆盖拷贝 → 写入 → 错误回填/短路 → 记账 → 唤醒全部尾段。不变式：
  /// 同一时刻至多一个设备写在途。本内核的写区间是 `[flushed_until, sealed_until)`
  /// 的**单调递增前缀**（下界=已承诺前缀末，上界=本轮已封印上界），故 C# 注释所警
  /// 「相邻片段末扇区（未完）覆写下一片段首扇区（已成）」的竞态在本实现根本不成环——
  /// 后写者的下界恒为先写者的上界，扇区圆整只令交界处至多一个扇区幂等重写（同字节
  /// 重写：该扇区以下为已封印定稿内容）。对位 C# 以 PendingFlush +
  /// AsyncFlushPageCallback 回调链实现的「同页写恒串行」；rust 多驱动（组提交/驱逐/
  /// 紧缩补刷）并发直入本内核，收敛为单把异步闸等价承接。闸内不等纪元（封印排空在
  /// 闸外完成）、不等 flush_event（notify 非阻塞），无死锁环。
  ///
  /// 性能设计：
  /// - 刷盘写内核（池借还、扇区界圆整、尾补零、write_aligned 下发与短写校验）单源在
  ///   [Device::flush_range_aligned]，本函数仅提供页表→连续字节的填充闭包；
  /// - **写区间保持逻辑粒度，不向上取整到页**（对标 AllocatorBase.cs:WriteInlinePageAsync
  ///   :629-636「Write only required bytes within the page」）：单笔提交的落盘量等于
  ///   本次新增已封印字节（缓冲 I/O 下精确到字节；Direct I/O 下至多一个尾扇区
  ///   补零）——页容量按内存预算自适应可推导至 16MB，
  ///   整页形态下逐条提交的写放大为该页幅（1000 条单写 = 16GiB），实测吞吐由设备写带宽
  ///   独占。设备上的日志字节恒等于持久化前缀（`flushed_until` 本身，缓冲 I/O 无
  ///   尾 pad），读侧
  ///   （[crate::ScanIterator::cold_read_page]、[HybridLog::read_disk_record]、
  ///   [HybridLog::recover] 装载）一律以 `flushed_until` 封顶请求长度，页尾未落盘字节
  ///   按零承接（内存页尾本身恒零，见 `CircularPageBuffer` 的清零不变式，读侧零头/Pad
  ///   判定与整页写零形态等价）；
  /// - 跨页合并仍为单次连续 I/O（[crate::flush::PendingFlushList] 按地址相邻/相交吸收）；
  /// - 错误路径（中途 PageNotReady / I/O 失败 / 设备短写）必须将合并区间按当前
  ///   `flushed_until` 钳制后回填 `pending_flush`，后续重试区间与之同起点相交，
  ///   恒被 coalesce 单次吸收收敛（杜绝持续失败下队列无界增长与恢复后永久滞留）。
  async fn flush_sealed_page_range(
    &self,
    start_page: u64,
    end_page: u64,
    seal_bound: u64,
  ) -> Result<()> {
    let from_addr = self.config.page_start_address(start_page);
    let until_addr = self.config.page_start_address(end_page.saturating_add(1));

    // 通过 PendingFlushList 合并所有与本次区间相交或相邻的待刷盘区间为单次连续写
    let merged_range = self.pending_flush.coalesce(
      PageFlushRange::new(from_addr, until_addr),
      self.addresses.flushed_until(),
    );

    // 陈旧区间钳制：错误路径回填的待刷区间可能已被后续成功刷盘部分超越（flushed_until
    // 连续前缀已覆盖其前段，终点仍在 flushed 之上）。coalesce 相交吸收会把这类区间并入
    // 本次写入——其中已滑出内存窗口（head 越过、环形槽位回绕复用）的页不再驻留，原样
    // 拷贝必然 PageNotReady，且错误回填进一步放大区间，最终形成「吸收陈旧区间 →
    // 失败 → 回填更大区间」的永久刷盘卡死。故入口统一钳制到 flushed_until：该址以下字节
    // 已获持久化承诺无需重写（整段覆盖则直接丢弃返回）。
    let merged_range = self.clamp_flush_range(merged_range);
    if merged_range.is_empty() {
      return Ok(());
    }

    // 先封再刷：读侧上界 = 调用方逻辑终点与 tail 的较小者，亦即本轮持久化前缀记账
    // 终点，同时就是本轮的设备写终点。内核在拷贝任何字节之前把只读线推到该上界并等
    // 纪元排空，使上界以下的在途编码全部定稿（契约见本方法文档「崩溃一致性契约」）。
    // coalesce 吸收的陈旧区间中越过本上界的部分本轮不写：tail 以下的字节退回 pending
    // 队列，由后续封印到更高位置的刷盘轮次承接；tail 以上属页粒度上溢，无可写字节
    // （[Self::defer_flush_tail]）。
    let sealed_until = seal_bound.min(self.addresses.tail());
    self.seal_read_only_and_drain(sealed_until).await;

    // 刷盘写序闸（不变式见方法文档「刷盘写序」）：封印排空在闸外，持闸后不再等待
    // 纪元与 flush_event；守卫存活至函数末尾，覆盖拷贝→写入→回填/短路→记账→唤醒
    let _gate = self.flush_gate.lock().await;

    // await 纪元排空期间并发任务可能已推进落盘，重新按最新 flushed_until 钳制
    let merged_range = self.clamp_flush_range(merged_range);
    if merged_range.is_empty() {
      return Ok(());
    }
    let deferred_tail = self.defer_flush_tail(&merged_range, sealed_until);

    let write_from = merged_range.from_address;
    let write_to = sealed_until;
    let first_page = self.config.page_id(write_from);
    let last_page = self.config.page_id(write_to.saturating_sub(1));
    let total_pages = last_page.saturating_sub(first_page).saturating_add(1) as usize;
    let page_size = self.config.page_size;

    // 刷盘写原语单源下沉：扇区界圆整、设备池借还、尾补零与短写校验一律由
    // Device::flush_range_aligned 承担（对标 AllocatorBase.cs:WriteInlinePageAsync），
    // 本函数只提供逐页**字节交叠区**的填充闭包——页读锁仅护住本页拷贝片段（短临界区），
    // 内核在 await 前后均不要求持有任何页锁；页内字节的定稿性由入口处的只读封印
    // 排空屏障承担（页读锁护不住 encode_at 的裸指针写，故不作为刷盘准入手段）
    let fill_res = self
      .device
      .flush_range_aligned(write_from, write_to, |start_aligned, buf| {
        for p in first_page..=last_page {
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
          let page_start = self.config.page_start_address(p);
          // 本页与本轮写区间的交叠：下界可能被扇区圆整拉低到本页内更早字节（幂等重写
          // 已承诺前缀的交界扇区），上界恒为本轮已封印上界
          let seg_from = page_start.max(start_aligned);
          let seg_to = page_start.saturating_add(page_size as u64).min(write_to);
          if seg_from >= seg_to {
            continue;
          }
          let dst = (seg_from - start_aligned) as usize..(seg_to - start_aligned) as usize;
          let src = (seg_from - page_start) as usize..(seg_to - page_start) as usize;
          buf[dst].copy_from_slice(&guard[src]);
        }
        Ok(())
      })
      .await;

    // 设备跨段写入慢路径允许返回「短写成功」：未写满的字节绝不能计入持久化前缀，
    // 与 I/O 失败、填充失败同等处理（回填 pending_flush 并报错），否则 flushed_until
    // 会越过实际未落盘字节，破坏崩溃一致性契约。
    // 特殊处理：若报错原因系页槽位未就绪（PageNotReady），且当前 flushed_until 已完全
    // 覆盖本区间，说明本区间已被并发任务完成刷盘并将旧页槽位安全回收复用，直接视作落盘完成。
    if let Err(err) = fill_res {
      let current_flushed = self.addresses.flushed_until();
      if current_flushed >= write_to {
        // 本轮区间已被并发刷盘覆盖，本区间视作落盘完成；越过本上界的尾段仍属未写
        // 字节，必须退回队列（否则该段既不在队列也不在设备上，前缀永不能越过）
        if let Some(tail_range) = deferred_tail {
          self.pending_flush.add(tail_range);
        }
        return Ok(());
      }
      self.requeue_flush_range(PageFlushRange::new(write_from, write_to));
      if let Some(tail_range) = deferred_tail {
        self.pending_flush.add(tail_range);
      }
      return Err(match err {
        FlushError::Fill(e) => e,
        FlushError::ShortWrite { expected, written } => Error::FlushFailed {
          page_id: first_page,
          detail: format!("设备短写: 已写 {written}/{expected} 字节"),
        },
        FlushError::Device(e) => e.into(),
      });
    }

    // 持久化前缀记账恒等于本轮已封印上界，且设备写区间与之同终点（缓冲 I/O 尾零头
    // 精确写；Direct I/O 尾随至多一个扇区由内核补零），维持
    // `flushed_until <= safe_read_only <= tail` 不变式；
    // 上界以上的字节本轮既不落盘也不作承诺，回填的陈旧尾段区间留待后续轮次
    self
      .pending_flush
      .complete_flush_range(PageFlushRange::new(write_from, write_to), &self.addresses);
    if let Some(tail_range) = deferred_tail {
      self.pending_flush.add(tail_range);
    }
    self.flush_event.notify(usize::MAX);
    debug!(
      "页面范围 [{first_page}..={last_page}] ({total_pages} 页) 成功聚合落盘至偏移 {:#x} ~ {:#x}（已封印上界 {sealed_until:#x}）",
      write_from, write_to
    );
    Ok(())
  }

  /// 把合并区间中越过本轮封印上界的尾段退回 pending 队列
  ///
  /// coalesce 是**出队**动作：被吸收的陈旧条目若终点高于本轮可承诺上界（其字节须等
  /// 封印推进到该处才落盘），本轮写不到它，必须原样退回队列，否则该段字节既不在队列
  /// 里也不在设备上，`flushed_until` 永不能越过——恢复端按前缀读取即永久滞留空洞。
  ///
  /// 尾段终点钳到日志 tail：合并区间带页粒度上溢（`until_addr` 是终点页的页尾地址），
  /// 而 partial 页写口径下 tail 以上的字节本轮不写、设备上也不存在，退回队列即成
  /// 「起点已达 tail、永无后续轮次覆盖」的死残留（`complete_flush_range` 把前缀恒
  /// 钳在 tail，队列条目起点只会随下一轮从 flushed_until 重新相交吸收）。尾段不存在
  /// （多数路径：合并终点恒 ≤ 本轮上界，或上溢段全在 tail 以上）时返回 `None`。
  fn defer_flush_tail(&self, merged: &PageFlushRange, sealed_until: u64) -> Option<PageFlushRange> {
    let end = merged.until_address.min(self.addresses.tail());
    if sealed_until < end {
      Some(PageFlushRange::new(sealed_until, end))
    } else {
      None
    }
  }

  /// 将待刷盘区间起点钳制到 `flushed_until`（低于该址的字节已获持久化承诺）
  ///
  /// 钳制粒度是**地址**而非页：设备写区间取逻辑前缀（对标 WriteInlinePageAsync 的
  /// partial 页只写 required bytes），下界页圆整会把已承诺字节重新拉回写覆盖范围，
  /// 逐条提交即退化成整页重写。扇区交界的少量重叠由 [Device::flush_range_aligned]
  /// 的圆整与闸内串行保证幂等。
  ///
  /// 整段已被持久化前缀覆盖时返回空区间。
  fn clamp_flush_range(&self, range: PageFlushRange) -> PageFlushRange {
    let flushed_until = self.addresses.flushed_until();
    if range.until_address <= flushed_until {
      return PageFlushRange::new(0, 0);
    }
    if range.from_address < flushed_until {
      PageFlushRange::new(flushed_until, range.until_address)
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
  ///
  /// 测试握手：生产零调用——生产侧硬件屏障单点在 wkv/src/store/flush.rs 直调
  /// `device.sync()`（设备级屏障，不经本包装，双机制面各司其职）；保留仅供
  /// 测试以 HybridLog 入口驱动同一设备屏障
  #[doc(hidden)]
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
  pub(crate) async fn flush_until_async(&self, target: u64) -> Result<u64> {
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
