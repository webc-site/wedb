use std::sync::atomic::{AtomicU64, Ordering};

use wbase::{backoff::Backoff, pool::AlignedBuf};
use wdev::Device;
use wrecord::{HEADER_SIZE, RDH_WORD_OFFSET, RecordHeader, RecordRef};

use crate::{
  address::AddressSnapshot,
  error::{Error, Result},
  hlog::{HybridLog, PageBytes, parse_record_from_slice},
  output::RecordOutput,
};

/// 扫描单条记录的零拷贝视图：逻辑地址、记录引用与整条物理字节切片
pub struct ScanItem<'a> {
  /// 记录起始逻辑地址
  pub addr: u64,
  /// 零拷贝记录引用（借用自页缓冲或磁盘预取页）
  pub rec: RecordRef<'a>,
  /// 整条记录的物理字节切片（含松弛填充，逻辑长度 = physical_size）
  pub bytes: &'a [u8],
}

/// [`ScanIterator::deliver`] 单轮迭代处置：交付闭包结果，或游标已推进需回循环头
enum Step<R> {
  /// 记录成功解析并交付闭包消费
  Delivered(R),
  /// Pad 越过 / 撕裂复核 / 自旋成功 / 跳页兜底：游标已推进，回循环头
  Next,
}

/// [`ScanIterator::consume_resident`] 内存驻留区访问处置：在 [`Step`] 之上多一档
/// 「锁内复验失败且已落盘」——磁盘回退读必须在纪元守卫之外发起（免滞留纪元推进），
/// 故上抛给 [`ScanIterator::next_ref`] 于守卫段外续接设备读取
enum Resident<R> {
  /// 记录成功解析并交付闭包消费
  Delivered(R),
  /// 游标已推进，回循环头
  Next,
  /// 页未驻留且已落盘：守卫外回退设备冷读
  DiskFallback,
}

/// 在途零头自旋重试预算（对标 C# TsavoriteLogScanIterator.cs:781-790 的
/// `Thread.SpinWait(100)` 后复查 SafeTailAddress 语义）：
/// append 协议「CAS 预占 tail → `encode_at` 首拍发布在途 extent 头」使预留槽位仅在
/// CAS 胜出到该单条 store 之间短暂呈全零（个位指令级），
/// 该预算覆盖此窗与高负载下线程调度抖动窗口。退避阶段机复用 wbase::backoff 单一真源
/// （纯自旋后逐轮让核，wait_busy 忙等绝不睡眠阻塞 reactor），
/// 自旋耗尽仍为零才按恢复清洗零区/页尾空洞跳页兜底，保证有穷终止——extent 头或完整头
/// 已可见的在途槽位走 Pad 精确步进按物理尺寸推进，不占用本预算。
const ZERO_HEADER_SPIN_BUDGET: u32 = 32768;

/// 判定页内 offset 处 16 字节记录头是否全零（CAS 后未及发布 extent 头的窗口 / 恢复清洗零区）
///
/// 在途两阶段发布协议落地后，全零不再是「已预占未编码」的稳定形态（稳定形态为 Pad 形态
/// extent 头，由 Pad 精确步进按物理尺寸越过），本判定只覆盖 CAS 胜出至 extent 头单字
/// store 之间的窗口，以及恢复清洗写出的真正零区。
///
/// 对齐字采用 Acquire 原子载入，与生产者 RDH 字的 Release store 构成 synchronizes-with，
/// 杜绝编译器寄存器缓存与乱序读，在自旋重试热循环中提供最高效实时的内存可见性。
#[inline(always)]
fn is_zero_header(bytes: &[u8], offset: usize) -> bool {
  if offset
    .checked_add(HEADER_SIZE)
    .is_none_or(|end| end > bytes.len())
  {
    true
  } else {
    let ptr = unsafe { bytes.as_ptr().add(offset) };
    if (ptr as usize).is_multiple_of(8) {
      let w0 = unsafe { (&*(ptr as *const AtomicU64)).load(Ordering::Acquire) };
      let w1 =
        unsafe { (&*(ptr.add(RDH_WORD_OFFSET) as *const AtomicU64)).load(Ordering::Acquire) };
      (w0 | w1) == 0
    } else {
      RecordHeader::is_zero_slice(&bytes[offset..])
    }
  }
}

/// 逻辑日志扫描迭代器（支持磁盘冷数据与内存热数据混合连续扫描）
///
/// 严格对标 Microsoft Garnet Tsavorite 中的 ScanIteratorBase 与 TsavoriteLogScanIterator：
/// - 逐条按记录物理尺寸前进：换页填充（Pad 记录）与在途 extent 头解出槽位尺寸后精确
///   步进、同页后续记录继续扫出；仅尺寸不可知的形态（极小子头残片、恢复清洗零区、
///   自旋耗尽的 CAS→extent 微窗）直达下一页开头；
/// - 针对磁盘冷数据区启用单页缓冲预取（SinglePageBuffering），消除同页内重复磁盘 I/O；
/// - 针对内存驻留区实现零拷贝直读（只读区无锁裸读；可变区页读锁，撕裂保护）。
///
/// 快照式可变区判定：[Self::new] 构造时一次性快照 `safe_read_only`（纪元排空边界）
/// 供 [AddressSnapshot::region_mutable] 贯穿使用，消除逐记录的重复 Acquire 加载。
/// 无锁裸读门槛刻意取 safe_read_only 而非 read_only（fuzzy region，对照 C#
/// InternalRMW.cs:199 的 `[SafeReadOnlyAddress, ReadOnlyAddress)` 模糊区）：read_only
/// 推进（换页 `maybe_advance_read_only` / `shift_head_address` 连带强推 / checkpoint）
/// 不取页锁，原位更新者在页写锁内复验「可变区」后落笔的窗口内记录可被并发封入
/// 只读区；C# 靠 RDH 单 8 字节原子字发布令并发扫描者无撕裂，本实现 16 字节头跨两个
/// 8 字节半字不可原子发布（撕裂论证见 `HybridLog::probe_resident`），唯纪元排空边界
/// 能保证复验于封区之前的原位写必已完成（写侧调用方持 epoch 守卫）。fuzzy 区
/// `[safe_read_only, read_only)` 记录走保守页读锁路径。快照方向安全性：safe_read_only
/// 单调递增，快照只允许偏旧，偏旧使 `effective_ro` 偏低，只会把只读区误判为可变区而
/// 走保守读锁路径，绝不会把可变区误判为只读区（那将走无锁裸读路径遭遇在途编码撕裂）。
///
/// 刻意不快照的边界（正确性优先的取舍）：`head` 与 `flushed_until` 保持逐迭代新鲜加载——
/// `flushed_until` 快照偏旧会把「已落盘且已滑出内存窗口」的记录误判为「尚未落盘」而整页跳过，
/// 导致扫描丢记录（这是丢数据方向的误判，而非保守方向），故该处不能采用 dev 侧的构造期全快照优化。
/// `begin_address` 同属逐迭代新鲜加载面：[Self::new] 的构造期快照仅作游标起点钳制，
/// 主循环每轮复验并钳位最新 begin（对标 C# LoadPageIfNeeded 的 BeginAddress 动态钳位，
/// SpanByteScanIterator.cs:135-136）——并发截断抬升水位后，滞后排位会向已 unlink 的
/// 历史段发起设备读（IO 错误）或越界产出已截断的陈旧记录，绝不可沿用单次快照。
///
/// # 内存驻留区纪元素件（内联自扫描器自身）
/// 只读区无锁裸读（`PageBytes::Raw`）要求读取线程处于 `LightEpoch` 保护下（同
/// `HybridLog::probe_resident` 契约）：页槽位回收的前置条件是 `safe_head` 经纪元排空
/// 越过旧页，持守卫期间页内存绝不会被清空复用。本扫描器在 [ScanIterator::next_ref]
/// 单点闭环该契约——内存驻留区同步访问（分支 3 无锁直读、分支 4 页读锁）前获取
/// TLS 纪元守卫（`LightEpoch::protected_scope`，对标 C#
/// `TsavoriteLogScanIterator.cs:239/334/398/485` GetNext 首尾的 `epoch.Resume()` /
/// `epoch.Suspend()` 分段），覆盖页槽位寻址、记录切片解析与闭包 `f` 消费拷贝全程，
/// 出段即释放；磁盘冷读（分支 1 与分支 5 的 `device.read_range`）绝不持守卫，
/// 避免阻塞异步 I/O 滞留纪元推进。由此 wcompact、ttl_sweep、hlog_scan、
/// vector_cleanup 等全部上层调用方自动满足契约，调用侧零改动。未持守卫时最坏情形
/// 为并发驱逐窗口内读到撕裂字节（解析报错或按 Pad 跳过，绝无悬垂 UB——页内存随
/// 实例存活），修复前该窗口可被并发页回收命中造成跳页漏扫与键址错配（数据丢失）。
///
/// # 在途尾部与 C# SafeTailAddress / SkipOnScan 的对照
/// C# 主日志扫描器对「已分配未写完」的槽位有两道等效闸门：一是
/// `TsavoriteLogScanIterator` 默认钳制在 `SafeTailAddress`（commit 提交协议推进），
/// `scanUncommitted` 模式触界时 `Thread.SpinWait(100)` 复查（TsavoriteLogScanIterator.cs:779-790）；
/// 二是分配器写侧先把新记录头写成 Sealed（RecordInfo.cs:WriteInfo「Otherwise, Scan could
/// return partial records」），扫描器据 `SkipOnScan` 跳过该记录但按 `allocatedSize`
/// 继续推进游标（SpanByteScanIterator.cs:GetNext「跳记录不跳页」）。
///
/// 本实现 compio 调用方驱动模型无逐记录提交标记，也无 epoch 用户字（SafeTail 钳制
/// 不可移植），故只落地第二道闸门：append 于 tail CAS 胜出后先单字发布只含槽位尺寸的
/// Pad 形态 extent 头（`HybridLog::encode_at` 首拍），扫描器解出 Pad 即按 `physical_size`
/// 精确越过该槽并继续扫出同页后续记录——本轮唯一漏扫的是 extent 头之后仍在编码的那条
/// 记录本身，后续扫描轮次自愈（与 C# SkipOnScan 同一口径）。第一道闸门以
/// `ZERO_HEADER_SPIN_BUDGET` 有界自旋等效覆盖 CAS 胜出至 extent 头落笔的个位指令窗，
/// 耗尽仍全零（恢复清洗零区/残片）才跳页兜底。因此与热追加并发的扫描仍具最终一致尽力
/// 语义，但漏扫范围由「整页后续记录」收窄为「单条在途记录」；需要强一致快照的调用方
/// （checkpoint/恢复）须先冻结写入（shift_read_only_to_tail + 纪元排空，同 flush
/// 崩溃一致性契约）。
///
/// # 页读失败原子性（对标上游 d20d63993 修复 ScanIteratorBase.BufferAndLoad）
/// C# 的 `BufferAndLoad` 经 `nextLoadedPages` 预占 frame 后把页读挂到
/// `BumpCurrentEpoch` 延迟执行，同步失败会留下「frame 被占而 loadedPages 停在 -1」
/// 的中间态（CAS 死循环 / 等待者永久挂起 / 异常逃逸进无关线程的 drain pass），上游
/// 以 `FailFrameLoad` + interlocked latch 修复。本实现结构性不存在该缺陷类：
/// - 无 frame 预占状态机（无 nextLoadedPages/loadedPages/pendingDrainCallbacks 等
///   对应物），页读为调用方驱动 `read_range(...).await`，失败仅带一道截断竞态复核
///   （错误臂新鲜复验 begin，见 [`Self::cold_read_page`]）：begin 已越过游标的并发
///   删段竞态经钳位跃迁收敛续扫，其余错误原样传播给扫描调用方，无「已预占但永不
///   完成」的中间态；
/// - 读不依赖纪元延迟执行（无 BumpCurrentEpoch(Action) 挂载点），不存在「异常
///   逃逸进无关线程 drain pass」的通道；
/// - 单页磁盘缓存先 `take()` 后读（见 `Self::next_ref` 分支 1），读失败时缓存
///   已出列且不回填，绝不残留指向失败页的毒化缓存；
/// - 迭代器游标仅在成功消费后推进（`Self::advance`），失败重入自动重试当前页。
///
/// 回归测试见 `tests/hlog/flaky_device.rs`（对标 C# test.hlog/FlakyDeviceTests.cs）。
pub struct ScanIterator<'a, D: Device> {
  hlog: &'a HybridLog<D>,
  curr_addr: u64,
  end_addr: u64,
  /// 构造期快照的 SafeReadOnlyAddress（纪元排空边界，单调递增，偏旧只走保守读锁
  /// 路径，fuzzy region 论证见结构体文档）
  safe_read_only: u64,
  /// 缓存的磁盘页面数据及对应的逻辑页号，避免同一页内多次重复磁盘 I/O
  disk_page_cache: Option<(u64, AlignedBuf)>,
  /// 上一次按 Pad 处理的逻辑地址（在途零头复核标记）：parse 双原子字判零与
  /// 后续复读之间存在生产者编码（extent 头 → key → val → RecordInfo 字原子 store →
  /// RDH 字原子 Release store）恰好落入的撕裂窗口，首次 PadRecord 复读非零时
  /// 回循环头复核而非直接跳页；同址二次 PadRecord 即为持久形态（真 Pad 头 /
  /// 页尾残片 / 恢复清洗零区），按跳页兜底。有穷性：扫描游标单调前进，
  /// 同址至多复核一轮。
  pad_seen: Option<u64>,
}

impl<'a, D: Device> ScanIterator<'a, D> {
  /// 创建新的扫描迭代器
  pub fn new(hlog: &'a HybridLog<D>, begin_addr: u64, end_addr: u64) -> Self {
    let begin = hlog.addresses.begin_address.load(Ordering::Acquire);
    let safe_read_only = hlog
      .addresses
      .safe_read_only_address
      .load(Ordering::Acquire);
    Self {
      hlog,
      curr_addr: begin_addr.max(begin),
      end_addr,
      safe_read_only,
      disk_page_cache: None,
      pad_seen: None,
    }
  }

  /// 当前扫描游标逻辑地址
  #[inline]
  pub const fn current_address(&self) -> u64 {
    self.curr_addr
  }

  /// 异步拉取下一条记录并以零拷贝 `ScanItem` 交付闭包消费
  ///
  /// 单一遍历引擎：推模式 [HybridLog::scan]、拉模式 [Self::next] 与缓冲复用变体
  /// [Self::next_into] 均构建于此，三区（磁盘冷读 / 只读直读 / 可变读锁）分派、
  /// Pad 与子头残片跳页、提前终止逻辑全部收敛在本方法内闭环。
  ///
  /// 纪元素件单点闭环：内存驻留区（分支 3/4 与分支 5 前置探测）的页槽位访问、
  /// 记录切片解析与 `f` 消费拷贝全程持 TLS 纪元守卫，出段即释放；分支 1 与分支 5
  /// 的设备冷读绝不持守卫，避免阻塞异步 I/O 滞留纪元推进（契约与 C# 对标段见
  /// [ScanIterator] 结构体文档「内存驻留区纪元素件」）。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteIterator.cs:GetNext
  /// （推进游标 + 经迭代器视图交付当前记录； SpanByteScanIterator.cs:GetNext 的
  /// 「跳记录不跳页」推进协议同承于此）。C# 迭代器/回调接口的统一落点：
  /// libs/storage/Tsavorite/cs/src/core/Allocator/ITsavoriteScanIterator.cs:GetNext
  /// libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteScanIterator.cs:GetNext
  /// libs/storage/Tsavorite/cs/src/core/Allocator/IScanIteratorFunctions.cs:Reader
  /// libs/storage/Tsavorite/cs/src/core/Allocator/IStreamingSnapshotIteratorFunctions.cs:Reader
  pub async fn next_ref<R>(
    &mut self,
    f: impl FnOnce(ScanItem<'_>) -> Result<R>,
  ) -> Result<Option<R>> {
    // 闭包经 Option 承接：每轮迭代仅交付路径 take 一次，Pad/跳页轮次原样留给下一轮
    let mut f = Some(f);
    let tail = self.hlog.addresses.tail_address.load(Ordering::Acquire);
    let effective_end = self.end_addr.min(tail);

    while self.curr_addr < effective_end {
      // 动态复验并钳位 BeginAddress（对标 C# LoadPageIfNeeded 的唯一 begin 钳位点，
      // SpanByteScanIterator.cs:135-136 / ObjectScanIterator.cs:136-137
      // `if (currentAddress < hlogBase.BeginAddress && !assumeInMemory)
      //     currentAddress = hlogBase.BeginAddress;`，本迭代器恒为混合三区、
      // 无 assumeInMemory 档，故钳位臂无条件生效）：构造期单次快照在并发
      // shift_begin_address 抬升截断线后即过期，滞后的游标既可能落入磁盘分支向
      // 已被 truncate_until_address 物理 unlink 的历史段发起设备读（IO 错误经
      // `?` 上抛中断整个扫描），也可能在删段受检查点窗钳制延后时越界产出逻辑上
      // 已截断的陈旧记录，破坏 BeginAddress 之下不可见契约。钳位于每轮循环首部、
      // 页号计算与任何读取之前：落后即整体跃迁至新鲜 begin，磁盘预取缓存与在途
      // 复核标记可能指向截断区一并清退；跃迁后 `continue` 由 while 谓词以新游标
      // 复判，越过 effective_end 即干净终止返回 Ok(None)（对标 C# GetNext 的
      // `currentAddress >= stopAddress` return false 终结分支）。判定单点取
      // AddressManager::begin（对 begin_address 原子的 Acquire 加载唯一 accessor），
      // 不开第二套水位。读错误臂的截断竞态复核（[`Self::cold_read_page`]）同样
      // 落经 [`Self::clamp_to_begin`] 单点，钳位口径全方法唯一。
      let begin = self.hlog.addresses.begin();
      if self.curr_addr < begin {
        self.clamp_to_begin(begin);
        continue;
      }

      let curr_addr = self.curr_addr;
      let page_id = self.hlog.config.page_id(curr_addr);
      let offset = self.hlog.config.page_offset(curr_addr);
      let page_size = self.hlog.config.page_size;

      // 若页内剩余空间不足以容纳记录头，说明已达页尾残片（如 0xFF 填充），跳至下一页开头
      if offset + HEADER_SIZE > page_size {
        self.skip_to_next_page(page_id);
        continue;
      }

      let head = self.hlog.addresses.head_address.load(Ordering::Acquire);
      if curr_addr < head {
        // 1. 磁盘区数据：取出单页预取缓存（未命中则整页冷读）——纯设备 I/O，免守卫
        let bytes = match self.disk_page_cache.take() {
          Some((cached_page, buf)) if cached_page == page_id => PageBytes::Disk(buf),
          _ => {
            let Some(buf) = self.cold_read_page(page_id, curr_addr).await? else {
              continue;
            };
            PageBytes::Disk(buf)
          }
        };
        match self.deliver(bytes, page_id, offset, curr_addr, &mut f)? {
          Step::Delivered(out) => return Ok(Some(out)),
          Step::Next => continue,
        }
      }

      // 2. 越过磁盘区，释放磁盘预取缓存以回收内存
      self.disk_page_cache = None;

      // 3./4. 内存驻留区同步访问段：进入前获取纪元守卫，块尾（记录解析与 `f`
      //       消费拷贝完成）即释放；分支 5 的设备回退读排在块外，守卫绝不横跨 await。
      //       对标 C# TsavoriteLogScanIterator.cs:239-296 GetNext 的
      //       epoch.Resume()/epoch.Suspend() 分段。
      let resident = {
        let _epoch_scope = self.hlog.epoch.protected_scope();
        self.consume_resident(page_id, offset, curr_addr, head, tail, &mut f)?
      };
      match resident {
        Resident::Delivered(out) => return Ok(Some(out)),
        Resident::Next => continue,
        Resident::DiskFallback => {
          // 5. 已落盘但当前未驻留内存（并发状态推进过渡期）：纪元守卫已随上块
          //    退出释放，回退走底层设备读取（纯设备 I/O，免守卫）
          let Some(buf) = self.cold_read_page(page_id, curr_addr).await? else {
            continue;
          };
          let bytes = PageBytes::Disk(buf);
          match self.deliver(bytes, page_id, offset, curr_addr, &mut f)? {
            Step::Delivered(out) => return Ok(Some(out)),
            Step::Next => continue,
          }
        }
      }
    }

    Ok(None)
  }

  /// 磁盘冷读单页（分支 1 未命中与分支 5 回退的公共臂）带截断竞态复核
  ///
  /// `read_range` Err 时新鲜复验 begin（对标同仓读路径补偿范式
  /// wkv/src/session/raw/read.rs:read_from_disk 对 `read_disk_record` 错误的
  /// `cur < begin_address` 复核）：读发起后并发 `shift_begin_address` /
  /// `release_history_until` 抬线并物理 unlink 本页所在段（读免纪元守卫，不在
  /// 截断屏障的在途封口之列），错误属删段竞态而非设备故障——游标落后即经
  /// [`Self::clamp_to_begin`] 跃迁新鲜 begin 并回 `None` 交调用方 `continue`
  /// 循环头（复用循环头既有钳位路径，不新开第二套口径；钳位后读的页所在段
  /// 含 begin 恒存活，见 `Device::truncate_until_address` 的段界保留语义），
  /// 收敛续扫而非以 [`wdev::Error::SegmentNotFound`] 中断整场。begin 未越过
  /// 游标的错误属真实设备故障，原样上抛保持透明传播。
  async fn cold_read_page(&mut self, page_id: u64, curr_addr: u64) -> Result<Option<AlignedBuf>> {
    match self
      .hlog
      .device
      .read_range(
        self.hlog.config.page_start_address(page_id),
        self.hlog.config.page_size,
      )
      .await
    {
      Ok(buf) => Ok(Some(buf)),
      Err(e) => {
        let begin = self.hlog.addresses.begin();
        if curr_addr < begin {
          self.clamp_to_begin(begin);
          Ok(None)
        } else {
          Err(Error::from(e))
        }
      }
    }
  }

  /// 钳位跃迁单点：游标整体跃迁至新鲜 begin，一并清退可能指向截断区的磁盘预取
  /// 缓存与在途复核标记（循环头钳位与 [`Self::cold_read_page`] 错误臂共用，
  /// 全迭代器唯一写点）
  #[inline]
  fn clamp_to_begin(&mut self, begin: u64) {
    self.curr_addr = begin;
    self.disk_page_cache = None;
    self.pad_seen = None;
  }

  /// 内存驻留区消费内核：分支 3（只读区无锁直读）/ 分支 4（可变区页读锁）分派 +
  /// [`Self::deliver`] 解析交付。调用点必须处于纪元守卫内（单点由 [`Self::next_ref`]
  /// 闭环，`try_read_page_unlocked` 的纪元前置条件据此满足）。
  fn consume_resident<R>(
    &mut self,
    page_id: u64,
    offset: usize,
    curr_addr: u64,
    head: u64,
    tail: u64,
    f: &mut Option<impl FnOnce(ScanItem<'_>) -> Result<R>>,
  ) -> Result<Resident<R>> {
    let bytes = if !AddressSnapshot::region_mutable(curr_addr, head, self.safe_read_only, tail)
      && let Some(page_slice) = unsafe { self.hlog.buffer.try_read_page_unlocked(page_id) }
      && curr_addr >= self.hlog.addresses.head()
    {
      // 3. 不可变只读区：无锁纯指针直读（守卫持有多穷临界区内 safe_head 经纪元
      //    排空无法越过本内存页，页槽位恒不被回收清零/复用）
      PageBytes::Raw(page_slice)
    } else {
      // 4. 可变区或无锁直读未命中的页：先取页读锁，再在锁内校验页号与 head——
      //    「先校验后加锁」存在 TOCTOU：校验通过到加锁之间页可被换页驱逐复用，
      //    锁内读到的将是新页数据被按旧偏移误解析。页清空/标定均持写锁，
      //    读锁持有期间页号稳定，锁内双重校验通过即保证读取期间槽位恒为目标页
      let guard = self.hlog.buffer.read_page(page_id);
      if curr_addr >= self.hlog.addresses.head() && self.hlog.buffer.is_page_loaded(page_id) {
        PageBytes::Locked(guard)
      } else {
        drop(guard);
        // flushed_until 须在锁内复验失败后新鲜加载再决策（同 r1 flush 拷贝的
        // TOCTOU 论证）：预检快照在「加载 → 加锁 → 复验」窗口内可被并发刷盘
        // 完成 + head 推进 + 换页回收超越，陈旧值会把已落盘且已滑出内存窗口
        // 的页误判为尚未落盘而整页跳过（丢数据方向误判，见结构体文档
        // 「刻意不快照的边界」）；新鲜值下本分支恒走设备回退（head <= flushed
        // 不变式保证 head 之下字节必已落盘，同 C# BufferAndLoad 永不跳页），
        // skip 仅作不变式被外力破坏时的防御性兜底
        if curr_addr < self.hlog.addresses.flushed_until() {
          return Ok(Resident::DiskFallback);
        }
        self.skip_to_next_page(page_id);
        return Ok(Resident::Next);
      }
    };
    match self.deliver(bytes, page_id, offset, curr_addr, f)? {
      Step::Delivered(out) => Ok(Resident::Delivered(out)),
      Step::Next => Ok(Resident::Next),
    }
  }

  /// 单页字节形态上的记录解析与交付内核（三区分派后的公共尾）：磁盘形态免守卫；
  /// 内存形态由调用点保证处于纪元守卫内直至本方法返回（`f` 闭包为同步零拷贝消费，
  /// 绝不横跨 await）
  fn deliver<R>(
    &mut self,
    bytes: PageBytes<'_>,
    page_id: u64,
    offset: usize,
    curr_addr: u64,
    f: &mut Option<impl FnOnce(ScanItem<'_>) -> Result<R>>,
  ) -> Result<Step<R>> {
    let page_size = self.hlog.config.page_size;
    match parse_record_from_slice(&bytes, offset, curr_addr, page_size) {
      Ok(rec) => {
        let physical_size = rec.physical_size();
        // 交付路径唯一 take 点：take 后本方法即返回，同轮迭代绝不二次进入
        let consume = f.take().expect("扫描闭包仅经交付路径消费一次");
        let out = consume(ScanItem {
          addr: curr_addr,
          rec,
          bytes: &bytes[offset..offset + physical_size],
        })?;
        // 磁盘页消费完毕后回填预取缓存（同页后续记录零 I/O）
        if let PageBytes::Disk(buf) = bytes {
          self.disk_page_cache = Some((page_id, buf));
        }
        self.advance(curr_addr, physical_size, page_id);
        Ok(Step::Delivered(out))
      }
      Err(Error::PadRecord(_)) => {
        // 本臂头部只解一次，三类形态各归一条处置（复核标记承接跨读撕裂）：
        //
        // 1. 尺寸可解的 Pad（真换页填充 / 在途 extent 头）：按 `HEADER_SIZE + val_len`
        //    精确越过本槽，游标留在页内，同页后续记录继续扫出——对标 C# 扫描器据
        //    `SkipOnScan` 跳记录不跳页（SpanByteScanIterator.cs:GetNext 先
        //    `nextAddress = currentAddress + allocatedSize` 再 `continue`）。
        //
        // 2. 尺寸不可解但本读已见完整头（生产者编码恰落在 parse 与本读之间）：撕裂窗口
        //    而非持久形态，置复核标记回循环头以 parse 同视角原址重读。严禁按「非 Pad」
        //    跳页——在途 extent 头协议下该窗口每轮并发追加必现，跳页即连带漏扫同页
        //    后续已提交记录（本票根因的残余形态）。
        //
        // 3. 全零头 / 页尾子头残片（尺寸不可知）：全零是「tail CAS 胜出至 extent 头
        //    落笔微窗」与「恢复清洗零区」共用形态，对标 C# TsavoriteLogScanIterator.cs
        //    779-790 scanUncommitted 的 SafeTailAddress 缓存 + Thread.SpinWait(100)
        //    复查——本实现 compio 调用方驱动模型无逐记录提交标记，等效内联为有界自旋
        //    等待编码，编码即原址重试。磁盘页字节定稿绝无在途窗口，跳过自旋；自旋耗尽
        //    仍为零的恢复清洗零区与子头残片按跳页兜底。
        //
        // 有穷性：1 与 3 的兜底直接推进游标；2 与自旋成功路径同址至多复核一轮
        // （pad_seen 命中即降级为 3 的跳页兜底），游标单调前进，绝无死循环。
        let header = bytes.get(offset..).and_then(RecordHeader::decode_opt);
        if let Some(pad) = header.filter(|h| h.is_pad()) {
          let step = pad.pad_extent().min(page_size - offset);
          self.pad_seen = None;
          if offset + step < page_size {
            if let PageBytes::Disk(buf) = bytes {
              self.disk_page_cache = Some((page_id, buf));
            }
            self.curr_addr = curr_addr + step as u64;
          } else {
            // 槽位恰填满页尾（extent 头覆盖至 page_size）：页内已无空间容纳后继
            self.skip_to_next_page(page_id);
          }
          return Ok(Step::Next);
        }

        let revisit = self.pad_seen == Some(curr_addr);
        if !matches!(bytes, PageBytes::Disk(_)) && !revisit {
          if header.is_some_and(|h| !h.is_null()) {
            self.pad_seen = Some(curr_addr);
            return Ok(Step::Next);
          }
          let mut backoff = Backoff::new();
          while backoff.step_count() < ZERO_HEADER_SPIN_BUDGET && is_zero_header(&bytes, offset) {
            // 阶段动作转调 wbase 真源的忙等面（Sleep 深睡钳制为让核）；预算上限
            // 即睡眠阶段边界，本循环实际可达阶段只有 Spin / Yield
            backoff.stage().wait_busy();
            backoff.advance();
          }
          if !is_zero_header(&bytes, offset) {
            // 记录已在在途窗口内完成编码：置复核标记，回到循环头原址至多复核一轮
            self.pad_seen = Some(curr_addr);
            return Ok(Step::Next);
          }
        }

        // 持久形态兜底（全零空洞 / 页尾子头残片 / 复核后仍未定型）：尺寸不可知，
        // 跳至下一页开头，复核标记就此清退
        self.pad_seen = None;
        self.skip_to_next_page(page_id);
        Ok(Step::Next)
      }
      Err(e) => Err(e),
    }
  }

  /// 异步拉取下一条有效记录（返回 (逻辑地址, 拥有所有权的 [RecordOutput])）
  pub async fn next(&mut self) -> Result<Option<(u64, RecordOutput)>> {
    self
      .next_ref(|item| Ok((item.addr, RecordOutput::Memory(item.bytes.to_vec()))))
      .await
  }

  /// 异步拉取下一条有效记录并拷入调用方缓冲（复用缓冲容量，逐条扫描零堆分配）
  ///
  /// - 返回 `(逻辑地址, 记录字节切片)`，切片借用自 `buf`（在下一次调用前保持有效）；
  /// - 与 [Self::next] 语义一致，但由调用方提供可复用缓冲，
  ///   消除拉模式扫描每条记录 `to_vec` 的堆分配开销。
  pub async fn next_into<'b>(
    &'b mut self,
    buf: &'b mut Vec<u8>,
  ) -> Result<Option<(u64, &'b [u8])>> {
    let addr = self
      .next_ref(|item| {
        buf.clear();
        buf.extend_from_slice(item.bytes);
        Ok(item.addr)
      })
      .await?;
    Ok(addr.map(|a| (a, buf.as_slice())))
  }

  /// 跳至指定页的下一页开头
  #[inline]
  fn skip_to_next_page(&mut self, page_id: u64) {
    self.curr_addr = self.hlog.config.page_start_address(page_id + 1);
  }

  /// 消费成功后推进游标（记录恰抵页尾时直达下一页开头）
  #[inline]
  fn advance(&mut self, curr_addr: u64, total_size: usize, page_id: u64) {
    if self.hlog.config.page_offset(curr_addr) + total_size == self.hlog.config.page_size {
      self.skip_to_next_page(page_id);
    } else {
      self.curr_addr = curr_addr + total_size as u64;
    }
  }
}

impl<D: Device> HybridLog<D> {
  /// 构造连续逻辑地址扫描迭代器（对标 Garnet Pull-based Scan）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:Scan
  /// （LogAccessor 扫描入口：`[begin, end)` 区间 + 双页缓冲默认口径，rust 单一
  /// 分配器下即本单点）
  ///
  /// # 警告：最终一致尽力语义（非强一致快照）
  ///
  /// **扫描终点为裸 tail，与热追加并发的扫描具最终一致尽力语义：仍在编码的在途记录
  /// 本轮按 extent 头跳记录跳过（同页后续记录恒被扫出）、后续扫描轮次自愈；扫描窗口内
  /// 新追加的记录亦可能落在视野之外。需要强一致快照的调用方（checkpoint/恢复/
  /// 审计）必须先冻结写入再扫描——`shift_read_only_to_tail` + 纪元排空（同
  /// flush 崩溃一致性契约），语义边界详见 [ScanIterator] 文档。**
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorScan.cs:MemoryPageScan
  pub fn scan_iter(&self, begin_addr: u64, end_addr: u64) -> ScanIterator<'_, D> {
    ScanIterator::new(self, begin_addr, end_addr)
  }

  /// 推模式连续逻辑地址扫描（对标 Garnet Push-based Scan，单条记录零堆分配借用）
  ///
  /// - 遍历区间 `[begin_addr, min(end_addr, tail))` 内的所有有效记录；
  /// - 跨页自动跳过 Pad 记录、极小子头残片与全零填充直达下一页开头；
  /// - 若用户闭包 `f` 返回 `Ok(false)`，立即提前终止扫描。
  ///
  /// C# 分配器扫描入口的统一落点：
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorScan.cs:Scan
  /// libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteAllocatorImpl.cs:Scan
  ///
  /// # 警告：最终一致尽力语义（非强一致快照）
  ///
  /// **终点为裸 tail、在途记录按 extent 头跳记录越过（同页后续记录恒被扫出）：与热追加
  /// 并发时仍可能漏扫本轮在途记录（后续轮次自愈）。需要强一致快照的调用方必须先冻结写入：
  /// `shift_read_only_to_tail` + 纪元排空，再调用本方法；
  /// 语义边界详见 [ScanIterator] 与 [Self::scan_iter] 文档。**
  pub async fn scan<F>(&self, begin_addr: u64, end_addr: u64, mut f: F) -> Result<()>
  where
    F: FnMut(u64, RecordRef<'_>) -> Result<bool>,
  {
    let mut it = self.scan_iter(begin_addr, end_addr);
    while let Some(cont) = it.next_ref(|item| f(item.addr, item.rec)).await? {
      if !cont {
        break;
      }
    }
    Ok(())
  }

  /// SCAN 游标有效性校验：`cursor` 是否落在记录起始字节（对标 C#
  /// SpanByteScanIterator.SnapCursorToLogicalAddress 的对齐校验语义）
  ///
  /// whlog 页内记录链自页首（页 0 自 DEFAULT_INITIAL_ADDRESS 保留区尾）顺序
  /// 排布、8 字节对齐，记录起始字节集合恰为记录链步进锚点，故判据表达为：
  /// 自 `max(游标所在页页首, begin_address)`（begin 之下链已物理截断，起点
  /// 钳制与 C# ScanLookup 的 BeginAddress 钳制同口径）按记录链步进至
  /// `min(cursor, tail)`，耗尽时恰停在 `cursor` 即有效。`cursor >= tail` 恒
  /// false（对标 C# InitializeGetNextAndAcquireEpoch 的
  /// `currentAddress >= stopAddress` 终结分支）。
  ///
  /// 与 C# 的刻意偏差：C# 对落在记录中间的游标自页首步进「回退对齐」后续扫；
  /// 本实现复用 [Self::scan_iter] 步进链做纯校验，不对齐一律判 false，由调用
  /// 方（wnode scan_cursor）终结遍历回 (0, 空)——Redis SCAN 本无快照保证，
  /// 漏扫方向可容忍，且免去为回退对齐重写三区分派逻辑。已登记
  /// doc/zh/deviations.md 第 20 条 c，勿按 C# 改回。`cursor == 0` 由调用
  /// 方分流（从头扫语义），不在校验域。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteScanIterator.cs:SnapCursorToLogicalAddress
  pub async fn validate_cursor(&self, cursor: u64) -> Result<bool> {
    if cursor >= self.addresses.tail_address.load(Ordering::Acquire) {
      return Ok(false);
    }
    let begin = self.addresses.begin_address.load(Ordering::Acquire);
    let start = self
      .config
      .page_start_address(self.config.page_id(cursor))
      .max(begin);
    let mut it = self.scan_iter(start, cursor);
    while it.next_ref(|_| Ok(true)).await?.is_some() {}
    Ok(it.current_address() == cursor)
  }
}
