use std::{hint::spin_loop, sync::atomic::Ordering, thread::yield_now};

use wbase::AlignedBuf;
use wdev::Device;
use wrecord::{HEADER_SIZE, RecordHeader, RecordRef};

use crate::{
  address::AddressManager,
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
/// append 协议「先 CAS 预占 tail 后编码」使预留槽位在编码完成前短暂呈全零，
/// 该预算覆盖常规编码窗口（百纳秒级）；期间每 64 次自旋让步一次 CPU，
/// 自旋耗尽仍为零才按恢复清洗零区/页尾空洞跳页兜底，保证有穷终止。
const ZERO_HEADER_SPIN_BUDGET: usize = 16384;

/// 自旋让步间隔（每 64 次自旋让出一次 CPU 时间片，防高负载下烧核）
const ZERO_HEADER_SPIN_YIELD_INTERVAL: usize = 64;

/// 判定页内 offset 处 16 字节记录头是否全零（在途预留槽位 / 恢复清洗零区的物理形态）
///
/// 采用 [RecordHeader::is_zero_slice] 双 64 位整数直接融合成单条比较，
/// 在 16384 次自旋重试热循环中彻底消除切片越界与结构体构造开销。
#[inline(always)]
fn is_zero_header(bytes: &[u8], offset: usize) -> bool {
  if offset >= bytes.len() {
    true
  } else {
    RecordHeader::is_zero_slice(&bytes[offset..])
  }
}

/// 逻辑日志扫描迭代器（支持磁盘冷数据与内存热数据混合连续扫描）
///
/// 严格对标 Microsoft Garnet Tsavorite 中的 ScanIteratorBase 与 TsavoriteLogScanIterator：
/// - 自动跳过换页填充（Pad 记录、全零字节或极小子头残片），直达下一页开头；
/// - 针对磁盘冷数据区启用单页缓冲预取（SinglePageBuffering），消除同页内重复磁盘 I/O；
/// - 针对内存驻留区实现零拷贝直读（只读区无锁裸读；可变区页读锁，撕裂保护）。
///
/// 快照式可变区判定：[Self::new] 构造时一次性快照 `read_only` 边界供
/// [AddressManager::is_mutable_snapshot] 贯穿使用（快照只允许偏旧；`read_only` 单调递增，
/// 偏旧只会把可变区误判为只读区而走保守读锁路径，绝不会把只读区误判为可变区，
/// 判定方向天然安全），消除逐记录的重复 Acquire 加载。
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
/// # 在途尾部与 C# SafeTailAddress 的对照
/// C# `TsavoriteLogScanIterator` 默认钳制在 `SafeTailAddress`（commit 提交协议推进），
/// `scanUncommitted` 模式触界时 `Thread.SpinWait(100)` 复查（TsavoriteLogScanIterator.cs:779-790）。
/// 本实现 compio 调用方驱动模型无逐记录提交标记，扫描终点为裸 tail：触达「已预占
/// 未编码」在途零头时以 `ZERO_HEADER_SPIN_BUDGET` 有界自旋等效 C# SpinWait 复查，
/// 耗尽仍为零（恢复清洗区）才跳页。因此与热追加并发的扫描具最终一致尽力语义——
/// 自旋耗尽的极端在途记录本轮漏扫、后续扫描轮次自愈（GC/过期清理均为周期性扫描）；
/// 需要强一致快照的调用方（checkpoint/恢复）须先冻结写入（shift_read_only_to_tail +
/// 纪元排空，同 flush 崩溃一致性契约）。
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
/// - 单页磁盘缓存先 `take()` 后读（见 [Self::next_ref] 分支 1），读失败时缓存
///   已出列且不回填，绝不残留指向失败页的毒化缓存；
/// - 迭代器游标仅在成功消费后推进（[Self::advance]），失败重入自动重试当前页。
///
/// 回归测试见 `tests/hlog/flaky_device.rs`（对标 C# test.hlog/FlakyDeviceTests.cs）。
pub struct ScanIterator<'a, D: Device> {
  hlog: &'a HybridLog<D>,
  curr_addr: u64,
  end_addr: u64,
  /// 构造期快照的 ReadOnlyAddress（单调递增，偏旧只走保守读锁路径，见结构体文档）
  read_only: u64,
  /// 缓存的磁盘页面数据及对应的逻辑页号，避免同一页内多次重复磁盘 I/O
  disk_page_cache: Option<(u64, AlignedBuf)>,
}

impl<'a, D: Device> ScanIterator<'a, D> {
  /// 创建新的扫描迭代器
  pub fn new(hlog: &'a HybridLog<D>, begin_addr: u64, end_addr: u64) -> Self {
    let begin = hlog.addresses.begin_address.load(Ordering::Acquire);
    let read_only = hlog.addresses.read_only_address.load(Ordering::Acquire);
    Self {
      hlog,
      curr_addr: begin_addr.max(begin),
      end_addr,
      read_only,
      disk_page_cache: None,
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

        let flushed_until = self.hlog.addresses.flushed_until();
        if !AddressManager::is_mutable_snapshot(curr_addr, head, self.read_only, tail)
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
            if curr_addr < flushed_until {
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
          // 全零头两态判别（对标 C# TsavoriteLogScanIterator.cs:779-790 scanUncommitted
          // 模式的 SafeTailAddress 缓存 + Thread.SpinWait(100) 复查语义）：append 协议
          // 先 CAS 预占 tail 再编码（AllocatorBase TryAllocate 先发布后写协议），预留槽位
          // 在编码完成前呈全零，与恢复清洗零区物理不可分。C# 由 SafeTailAddress 提交
          // 协议在扫描器之外隔离该窗口、自旋等待生产者提交；本实现 compio 调用方驱动
          // 模型无逐记录提交标记，等效内联为对在途零头的有界自旋——编码完成后原址
          // 重试，避免把同页后续已完整编码的记录一并跳页漏扫。磁盘页字节 immutable
          // （addr < head 必已落盘定稿，见 read_disk_record 缓存安全性论证）绝无在途
          // 窗口，直接跳过自旋；自旋耗尽仍为零的恢复清洗零区维持跳页兜底。
          if !matches!(bytes, PageBytes::Disk(_)) && is_zero_header(&bytes, offset) {
            let mut spins = 0usize;
            while is_zero_header(&bytes, offset) {
              if spins >= ZERO_HEADER_SPIN_BUDGET {
                break;
              }
              spins += 1;
              if spins.is_multiple_of(ZERO_HEADER_SPIN_YIELD_INTERVAL) {
                yield_now();
              } else {
                spin_loop();
              }
            }
            if !is_zero_header(&bytes, offset) {
              // 记录已在在途窗口内完成编码：丢弃本轮解析，重取新鲜 tail 原址重试
              continue;
            }
          }

          // 换页填充 / 恢复清洗零区 / 页尾残片处理：
          // - PAD 头（复活槽位中段亦可能出现，见 revivify_record_at）按 16 + val_len
          //   精确越过填充区，避免误跳同页后续记录；
          // - 零区与子头残片仅出现于页尾或恢复清洗区，直达下一页开头。
          let pad_step = bytes
            .get(offset..)
            .and_then(RecordHeader::decode_opt)
            .filter(RecordHeader::is_pad)
            .map(|h| (HEADER_SIZE + h.val_len as usize).min(page_size - offset));
          match pad_step {
            Some(step) if offset + step < page_size => {
              if let PageBytes::Disk(buf) = bytes {
                self.disk_page_cache = Some((page_id, buf));
              }
              self.curr_addr = curr_addr + step as u64;
            }
            _ => self.skip_to_next_page(page_id),
          }
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
  /// **扫描终点为裸 tail，与热追加并发的扫描具最终一致尽力语义：极端在途记录
  /// （"已预占未编码"零头自旋耗尽）本轮可能漏扫、后续扫描轮次自愈；扫描窗口内
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
  /// **终点为裸 tail、在途零头有界自旋：与热追加并发时可能漏扫极端在途记录
  /// （后续轮次自愈）。需要强一致快照的调用方必须先冻结写入：
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
}
