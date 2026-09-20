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
/// 对齐字采用 Acquire 原子载入，与生产者 release fence 形成内存屏障配对，
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
///
/// # 调用方契约（无锁直读路径）
/// 只读区无锁裸读（`PageBytes::Raw`）要求调用线程处于 `LightEpoch` 保护下（同
/// `HybridLog::probe_resident` 契约）：页槽位回收的前置条件是 `safe_head` 经纪元排空
/// 越过旧页，持守卫期间页内存绝不会被清空复用；未持守卫时最坏情形为并发驱逐窗口内
/// 读到撕裂字节（解析报错或按 Pad 跳过，绝无悬垂 UB——页内存随实例存活），与 C#
/// 扫描器在内存区读取须持纪元的语义一致。
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
///   对应物），页读为调用方驱动 `read_range(...).await`，失败经 `?` 原地传播给
///   扫描调用方，无「已预占但永不完成」的中间态；
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
  /// 后续复读之间存在生产者编码（extent 头 → key → val → RecordInfo 字 →
  /// Release 屏障 → RDH 字）恰好落入的撕裂窗口，首次 PadRecord 复读非零时
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
  pub async fn next_ref<R>(
    &mut self,
    f: impl FnOnce(ScanItem<'_>) -> Result<R>,
  ) -> Result<Option<R>> {
    let tail = self.hlog.addresses.tail_address.load(Ordering::Acquire);
    let effective_end = self.end_addr.min(tail);

    while self.curr_addr < effective_end {
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
      let bytes = if curr_addr < head {
        // 1. 磁盘区数据：取出单页预取缓存（未命中则整页冷读）
        match self.disk_page_cache.take() {
          Some((cached_page, buf)) if cached_page == page_id => PageBytes::Disk(buf),
          _ => PageBytes::Disk(
            self
              .hlog
              .device
              .read_range(self.hlog.config.page_start_address(page_id), page_size)
              .await?,
          ),
        }
      } else {
        // 2. 越过磁盘区，释放磁盘预取缓存以回收内存
        self.disk_page_cache = None;

        if !AddressSnapshot::region_mutable(curr_addr, head, self.safe_read_only, tail)
          && let Some(page_slice) = unsafe { self.hlog.buffer.try_read_page_unlocked(page_id) }
          && curr_addr >= self.hlog.addresses.head()
        {
          // 3. 不可变只读区：无锁纯指针直读
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
              // 5. 已落盘但当前未驻留内存（并发状态推进过渡期）：回退走底层设备读取
              PageBytes::Disk(
                self
                  .hlog
                  .device
                  .read_range(self.hlog.config.page_start_address(page_id), page_size)
                  .await?,
              )
            } else {
              self.skip_to_next_page(page_id);
              continue;
            }
          }
        }
      };

      match parse_record_from_slice(&bytes, offset, curr_addr, page_size) {
        Ok(rec) => {
          let physical_size = rec.physical_size();
          let out = f(ScanItem {
            addr: curr_addr,
            rec,
            bytes: &bytes[offset..offset + physical_size],
          })?;
          // 磁盘页消费完毕后回填预取缓存（同页后续记录零 I/O）
          if let PageBytes::Disk(buf) = bytes {
            self.disk_page_cache = Some((page_id, buf));
          }
          self.advance(curr_addr, physical_size, page_id);
          return Ok(Some(out));
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
            let step = (HEADER_SIZE + pad.val_len() as usize).min(page_size - offset);
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
            continue;
          }

          let revisit = self.pad_seen == Some(curr_addr);
          if !matches!(bytes, PageBytes::Disk(_)) && !revisit {
            if header.is_some_and(|h| !h.is_null()) {
              self.pad_seen = Some(curr_addr);
              continue;
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
              continue;
            }
          }

          // 持久形态兜底（全零空洞 / 页尾子头残片 / 复核后仍未定型）：尺寸不可知，
          // 跳至下一页开头，复核标记就此清退
          self.pad_seen = None;
          self.skip_to_next_page(page_id);
        }
        Err(e) => return Err(e),
      }
    }

    Ok(None)
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
  /// 漏扫方向可容忍，且免去为回退对齐重写三区分派逻辑。`cursor == 0` 由调用
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
