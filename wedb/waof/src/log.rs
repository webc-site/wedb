use std::{
  hint::spin_loop,
  ops::Deref,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering, fence},
  },
  thread::yield_now,
};

use async_lock::Mutex as AsyncLockMutex;
use crossfire::{MAsyncTx, mpsc::Array};
use wbase::{
  align::{align_down, align_up},
  backoff::{Backoff, BackoffStage},
  group_commit::{Enter, GroupCommitPipeline, GroupCommitStep},
  pool::AlignedBuf,
  thread::current_thread_id,
};
use wdev::{self, Device, Error as DeviceError};

use super::{
  config::WalConfig,
  disk_window::DiskWindow,
  error::{Error, Result},
  header::{RECORD_HEADER_LEN, RecordHeader},
  iterator::WalScanIterator,
  ring_buffer::RingBuffer,
};

/// 流水线中断错误消息（Follower 侧统一映射，避免雷同字符串散落）
const PIPELINE_BROKEN: &str = "Commit pipeline broken";

/// 复制流推流唤醒信号发送端类型（容量 1 有界通道的发送端；
/// 满即折叠去重——多帧写入只留一个未处理信号）
pub type ReplicationWakeTx = MAsyncTx<Array<()>>;

/// 恢复/扫描共用的滑动窗口分块大小
pub(crate) const RECOVER_CHUNK_SIZE: usize = 64 * 1024;

/// WAL 引擎共享内部状态
pub struct WalLogInner<D: Device> {
  /// 起始有效逻辑地址（该地址之前的段已被物理截断）
  pub begin_address: AtomicU64,
  /// 当前末尾逻辑地址（下一个待分配写入的地址）
  pub tail_address: AtomicU64,
  /// 已刷入底层设备的逻辑地址
  pub flushed_until_address: AtomicU64,
  /// 已成功提交并持久化的逻辑地址
  pub committed_until_address: AtomicU64,
  /// 内存环形写缓冲区
  pub ring_buffer: RingBuffer,
  /// 底层块存储设备句柄
  pub device: Arc<D>,
  /// WAL 配置参数
  pub config: WalConfig,
  /// 并发写入在途追踪槽位
  pub inflight_slots: Box<[AtomicU64]>,
  /// 提交刷盘互斥锁
  pub commit_lock: async_lock::Mutex<()>,
  /// 提交落盘流水线（支持并发合并 Group Commit）
  pub commit_pipeline: GroupCommitPipeline,
  /// 复制流推流唤醒信号发送端（宿主经 [`WalLog::set_replication_wake`] 注入；
  /// 帧数据不流经信号——推流端从环形缓冲按地址拉取，`safe_tail_address()`
  /// 即安全可读面）
  pub replication_wake: OnceLock<ReplicationWakeTx>,
}

impl<D: Device> WalLogInner<D> {
  /// 计算当前安全可读/可刷盘的尾部逻辑地址（所有小于该地址的并发写入均已落盘到环形内存）
  #[inline]
  pub fn safe_tail_address(&self) -> u64 {
    let tail = self.tail_address.load(Ordering::Acquire);
    fence(Ordering::SeqCst);
    self
      .inflight_slots
      .iter()
      .fold(tail, |min, slot| min.min(slot.load(Ordering::Acquire)))
  }

  /// 执行物理段写入与 fdatasync
  pub(crate) async fn flush_and_sync_range(&self, flushed: u64, safe_tail: u64) -> Result<u64> {
    if safe_tail <= flushed {
      return Ok(self.committed_until_address.load(Ordering::Acquire));
    }

    let sector_size = self.device.sector_size() as u64;
    let start_aligned = align_down(flushed, sector_size);
    let end_aligned = align_up(safe_tail, sector_size);

    let write_buf = self.ring_buffer.copy_range_with_padding(
      start_aligned,
      safe_tail,
      end_aligned,
      self.device.pool(),
    )?;

    let expected_len = write_buf.len();
    let (res, _) = self.device.write_aligned(start_aligned, write_buf).await;
    let written_len = res?;
    if written_len != expected_len {
      return Err(Error::ShortWrite {
        expected: expected_len,
        written: written_len,
      });
    }

    if let Err(e) = self.device.sync_data().await {
      return Err(e.into());
    }

    self
      .flushed_until_address
      .store(safe_tail, Ordering::Release);
    self
      .committed_until_address
      .store(safe_tail, Ordering::Release);

    Ok(safe_tail)
  }
}

/// WAL 引擎（对标 C# TsavoriteLog）
///
/// 持久性边界：无独立 commit 元数据记录，`recover` 以最后一条完整记录为已提交。
/// 已 `enqueue` 但未 `commit` 的记录，若被其他写入者并发 `commit` 的刷盘区间
/// 覆盖，崩溃恢复后会被视为已提交——要求精确提交持久性边界的调用方（如
/// 一致性协议日志）须以扫描终点自行界定提交范围
pub struct WalLog<D: Device> {
  pub(crate) inner: Arc<WalLogInner<D>>,
}

impl<D: Device> Clone for WalLog<D> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: Arc::clone(&self.inner),
    }
  }
}

impl<D: Device> Deref for WalLog<D> {
  type Target = WalLogInner<D>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl<D: Device> WalLog<D> {
  /// 创建新的 WAL 日志实例
  pub fn new(device: Arc<D>, config: WalConfig) -> Result<Self> {
    // 环形缓冲区对齐口径取设备扇区大小（单一真源，见 WalConfig 文档）
    let sector_size = device.sector_size();
    let ring_buffer = RingBuffer::new(config.buffer_size, sector_size)?;
    let slot_count = config.inflight_slots.max(1);
    let slots = (0..slot_count)
      .map(|_| AtomicU64::new(u64::MAX))
      .collect::<Box<[_]>>();

    let start_seg = device.start_segment() as u64;
    let seg_size = device.segment_size().unwrap_or(0);
    // 日志首地址取段边界。刻意差异：C# Garnet AOF 保留 kFirstValidAofAddress = 64
    // 头区（GarnetAppendOnlyFile.cs，复制恢复以 64 作"副本非空"哨兵），此处无保留区，
    // 空日志判据为 begin == tail，磁盘格式差异见 header 模块
    let begin_addr = start_seg * seg_size;

    Ok(Self {
      inner: Arc::new(WalLogInner {
        begin_address: AtomicU64::new(begin_addr),
        tail_address: AtomicU64::new(begin_addr),
        flushed_until_address: AtomicU64::new(begin_addr),
        committed_until_address: AtomicU64::new(begin_addr),
        ring_buffer,
        device,
        config,
        inflight_slots: slots,
        commit_lock: AsyncLockMutex::new(()),
        commit_pipeline: GroupCommitPipeline::new(),
        replication_wake: OnceLock::new(),
      }),
    })
  }

  /// 注入复制流推流端口（须在产生任何写入之前调用；重复注入返回 false）
  /// 注册推流唤醒信号发送端（一次注入；重复注入返回 false）
  ///
  /// 唤醒语义：每条记录入队完成后 `try_send(())` 一次（容量 1，满即折叠），
  /// 推流端被唤醒后从 [`Self::safe_tail_address`] 按地址序拉取新帧——推流序
  /// 与 AOF 地址序原子一致（同一环形缓冲的线性化序），并发写入下从侧应用序
  /// 与主侧重启重放序永不发散
  pub fn set_replication_wake(&self, tx: ReplicationWakeTx) -> bool {
    self.replication_wake.set(tx).is_ok()
  }

  /// 打开或恢复已有 WAL 日志实例，自动扫描磁盘段文件恢复有效位点
  pub async fn open(device: Arc<D>, config: WalConfig) -> Result<Self> {
    let log = Self::new(device, config)?;
    log.recover().await?;
    Ok(log)
  }

  /// 扫描恢复已有设备上的数据位点
  ///
  /// WARNING: 须在日志静默（无并发 enqueue/commit/truncate）后调用，对标 C# RecoverAsync
  /// （其同样要求恢复先于任何写入；并发恢复会与在途写入竞争位点原子量）
  ///
  /// 恢复策略（对标 libs/server/AOF/Recover/AofRecover.cs:Recover 的报错 vs 截断取舍）：
  /// - EOF/残缺头/校验和失败/全零填充 → 保守截断至最后一条完整记录（自动容错）；
  /// - 其他底层 I/O 错误（段缺失、介质错误等异常）→ 显式上抛（快速失败），
  ///   绝不静默清空位点伪装成空日志；
  /// - 对照差异：C# 的恢复位点取自检查点元数据（恢复后重放至 CommittedUntil），
  ///   本实现无检查点依赖，通过 CRC 记录链扫描自同步定位尾部，属刻意架构差异
  pub async fn recover(&self) -> Result<u64> {
    let guard = self.commit_lock.lock().await;

    // 1. 先触发底层设备的段文件元数据扫描与恢复（如 SegmentedDevice 恢复 start_segment/end_segment）
    self.device.recover()?;

    let start_seg = self.device.start_segment() as u64;
    let seg_size = self.device.segment_size().unwrap_or(0);
    let begin_addr = start_seg * seg_size;
    self.begin_address.store(begin_addr, Ordering::Release);

    let mut cur = begin_addr;

    // 2. 物理截断后的段首可能落在跨段记录的残缺负载中部，需先帧同步定位首条完整记录
    if start_seg > 0
      && seg_size > 0
      && let Some(sync_addr) = self.frame_sync(cur).await?
    {
      cur = sync_addr;
    }

    // 3. 基于 64KB 磁盘块滑动窗口批量预读流式恢复主记录链，消除每条记录 2 次单独 I/O
    let mut disk_win = DiskWindow::new();

    loop {
      if !disk_win.covers(cur, RECORD_HEADER_LEN) {
        let buf = match self
          .fetch_tail(cur, RECOVER_CHUNK_SIZE, RECORD_HEADER_LEN)
          .await
        {
          Ok(buf) => buf,
          // 可恢复链正常终止于 EOF（末尾残缺头不足 8 字节）
          Err(Error::Device(DeviceError::UnexpectedEof { .. })) => break,
          Err(e) => return Err(e),
        };
        disk_win.replace(cur, buf);
      }

      let rel_off = (cur - disk_win.offset()) as usize;
      let Some(header) = RecordHeader::decode_opt(&disk_win.slice()[rel_off..]) else {
        break;
      };

      // 全零头 = 扇区填充或崩溃残缺尾部（空记录携带哨兵 CRC，绝不呈现全零头），链在此终止
      let entry_len = header.payload_len();
      if header.is_zero() || entry_len > self.config.buffer_size {
        break;
      }

      // 校验当前记录：负载完整位于预读窗内则零 I/O 直验，越窗时回退单次设备读取
      match self
        .verify_candidate(cur, header, disk_win.slice(), rel_off)
        .await
      {
        Ok(true) => cur += (RECORD_HEADER_LEN + entry_len) as u64,
        Ok(false) => break,
        Err(e) => return Err(e),
      }
    }

    self.tail_address.store(cur, Ordering::Release);
    self.flushed_until_address.store(cur, Ordering::Release);
    self.committed_until_address.store(cur, Ordering::Release);

    for slot in self.inflight_slots.iter() {
      slot.store(u64::MAX, Ordering::Release);
    }

    let preload_start = cur
      .saturating_sub(self.config.buffer_size as u64)
      .max(self.begin_address.load(Ordering::Acquire));
    if cur > preload_start {
      let preload_len = (cur - preload_start) as usize;
      let data = self.device.read_range(preload_start, preload_len).await?;
      self.ring_buffer.write_bytes(preload_start, data.as_slice());
    }

    drop(guard);
    Ok(cur)
  }

  /// 批量读取：遇文件尾部 UnexpectedEof 时按实际可得字节数自适应降级（结果不短于 min_len）
  async fn fetch_tail(
    &self,
    offset: u64,
    requested_len: usize,
    min_len: usize,
  ) -> Result<AlignedBuf> {
    match self.device.read_range(offset, requested_len).await {
      Ok(buf) => Ok(buf),
      Err(DeviceError::UnexpectedEof { actual, .. }) if actual >= min_len => {
        Ok(self.device.read_range(offset, actual).await?)
      }
      Err(e) => Err(e.into()),
    }
  }

  /// 段首帧同步：滑动窗口逐字节探测，定位第一条可校验记录的起始地址
  ///
  /// truncate 物理删除历史段后重启，恢复出的段首可能落在跨段记录的残缺负载中部，
  /// 常规扫描会在段首误判损坏而将位点清零。逐字节探测规则：
  /// - 非零头：负载须通过 CRC 校验（含携带哨兵 CRC 的空记录）；
  /// - 全零头：残缺尾部或填充零，逐字节跳过继续探测。
  ///
  /// 探测不设段界、持续滑窗前移直至定位同步点或设备 EOF：残缺负载可覆盖
  /// 多个完整段（单条记录长度可超过段大小），且首条边界记录自身介质损坏时
  /// 下一条合法边界可能落在更深处，任何固定上界都有误判空日志、丢弃其后
  /// 全部合法记录的风险（同步点必然命中 CRC，探测代价有界于日志长度）。
  ///
  /// 成功时前移 begin_address 至同步点并返回该地址；
  /// 全程无合法记录返回 None（保守按空日志处理）；异常 I/O 错误原样上抛
  async fn frame_sync(&self, seg_start: u64) -> Result<Option<u64>> {
    let cap = self.config.buffer_size;
    let mut win_start = seg_start;

    loop {
      let probe = match self
        .fetch_tail(win_start, RECOVER_CHUNK_SIZE, RECORD_HEADER_LEN)
        .await
      {
        Ok(probe) => probe,
        // 日志末尾不足 8 字节的残缺头部：再无可完整解码的记录
        Err(Error::Device(DeviceError::UnexpectedEof { .. })) => break,
        Err(e) => return Err(e),
      };
      let slice = probe.as_slice();
      let mut off = 0;
      while let Some((chunk, _)) = slice[off..].split_first_chunk::<RECORD_HEADER_LEN>() {
        let packed = u64::from_le_bytes(*chunk);
        let entry_len = packed as u32;

        // 全零头（填充/残缺）或负载超限的伪头：前移 1 字节继续探测（单指令极速过滤）
        if packed == 0 || (entry_len as usize) > cap {
          off += 1;
          continue;
        }

        let hdr = RecordHeader {
          entry_len,
          crc32: (packed >> 32) as u32,
        };

        if self
          .verify_candidate(win_start + off as u64, hdr, slice, off)
          .await?
        {
          let sync_addr = win_start + off as u64;
          self.begin_address.store(sync_addr, Ordering::Release);
          return Ok(Some(sync_addr));
        }
        off += 1;
      }
      // 预留 8 字节重叠，避免横跨窗口的记录头漏检
      win_start += (slice.len() as u64)
        .saturating_sub(RECORD_HEADER_LEN as u64)
        .max(1);
    }
    Ok(None)
  }

  /// 校验窗口内候选记录负载的 CRC（负载越出窗口时回退单次设备读取）
  ///
  /// 返回 false 表示校验未通过或负载未完整写入（残缺尾部，EOF）；
  /// 其他底层 I/O 异常原样上抛，绝不静默当作链终止
  async fn verify_candidate(
    &self,
    hdr_addr: u64,
    hdr: RecordHeader,
    slice: &[u8],
    off: usize,
  ) -> Result<bool> {
    let payload_end = off + RECORD_HEADER_LEN + hdr.payload_len();
    if payload_end <= slice.len() {
      let payload = unsafe { slice.get_unchecked(off + RECORD_HEADER_LEN..payload_end) };
      return Ok(hdr.verify(payload).is_ok());
    }
    match self
      .device
      .read_range(hdr_addr + RECORD_HEADER_LEN as u64, hdr.payload_len())
      .await
    {
      Ok(payload) => Ok(hdr.verify(payload.as_slice()).is_ok()),
      // 残缺尾部：负载数据未完整写入，按不可校验处理
      Err(DeviceError::UnexpectedEof { .. }) => Ok(false),
      Err(e) => Err(e.into()),
    }
  }

  /// 将数据追加到 WAL 内存缓冲区，返回起始逻辑地址（支持多线程并发无锁预占地址）
  #[inline]
  pub fn enqueue(&self, payload: &[u8]) -> Result<u64> {
    self.enqueue_parts(&[payload])
  }

  /// 将多个负载部件按序追加为单条 WAL 记录（scatter-write，零整包拼接），返回起始逻辑地址
  ///
  /// 产出的记录帧与 [`Self::enqueue`] 预拼整包后写入逐字节一致（记录头经
  /// `RecordHeader::for_payload_parts` 分段累加，CRC32 线性等价），调用方免去
  /// 预拼整包 Vec 的整量拷贝；恢复侧扫描路径零改动
  pub fn enqueue_parts(&self, parts: &[&[u8]]) -> Result<u64> {
    // u64 口径计算记录总长，规避 32 位平台上 +RECORD_HEADER_LEN 的 usize 溢出
    let payload_len: usize = parts.iter().map(|part| part.len()).sum();
    let record_len = RECORD_HEADER_LEN as u64 + payload_len as u64;
    self.check_record_len(record_len)?;

    // 预先计算记录头与 CRC32，避免在持有在途槽位期间耗费 CPU 算力拖慢并发提交
    let header = RecordHeader::for_payload_parts(parts);
    self.enqueue_with_parts(header.to_bytes(), parts)
  }

  /// 原样写入完整记录帧（8 字节记录头 + 负载），返回起始逻辑地址
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:UnsafeTryEnqueueRaw：不重算记录头，帧字节逐字进日志。服务于
  /// 复制从节点对主节点记录的保真落盘——只要主从帧序列一致且起始位点一致，
  /// 预占地址序列即逐条一致；调用方须以返回地址校验主从位点同步。
  /// 帧头须自洽（非全零且 entry_len 与负载长度一致），错位帧会使恢复链
  /// 在后续记录处校验失败而截断，故入口即拒
  pub fn enqueue_raw(&self, frame: &[u8]) -> Result<u64> {
    let Some((header, payload)) = frame.split_first_chunk::<RECORD_HEADER_LEN>() else {
      return Err(Error::InvalidRecordHeader);
    };
    let parsed = RecordHeader::from_bytes(header);
    if parsed.is_zero() || parsed.entry_len as usize != payload.len() {
      return Err(Error::InvalidRecordHeader);
    }
    self.check_record_len(frame.len() as u64)?;
    self.enqueue_with_parts(*header, &[payload])
  }

  /// 单条记录总长上限：负载以 u32 编码长度，且须完整落入环形缓冲区
  fn check_record_len(&self, record_len: u64) -> Result<()> {
    let header_limit = u32::MAX as u64 + RECORD_HEADER_LEN as u64;
    if record_len > header_limit {
      return Err(Error::RecordTooLarge {
        len: record_len,
        limit: header_limit,
      });
    }
    if record_len > self.config.buffer_size as u64 {
      return Err(Error::RecordTooLarge {
        len: record_len,
        limit: self.config.buffer_size as u64,
      });
    }
    Ok(())
  }

  /// 入队公共路径：注册在途槽位 → CAS 预占地址 → 分部件写入环形缓冲 → 释放槽位
  fn enqueue_with_parts(
    &self,
    header_bytes: [u8; RECORD_HEADER_LEN],
    parts: &[&[u8]],
  ) -> Result<u64> {
    let payload_len: usize = parts.iter().map(|part| part.len()).sum();
    // 1. 注册在途槽位（发布下界，防止 commit 提前刷盘未就绪内存）
    let (slot_idx, current_tail) = self.acquire_inflight_slot();

    // 2. CAS 预占逻辑地址范围（槽位值由 reserve_address 全程维护为当前预占下界；
    //    BufferFull 失败路径由 reserve_address 内部释放槽位）
    let reserved_addr = self.reserve_address(
      RECORD_HEADER_LEN as u64 + payload_len as u64,
      current_tail,
      slot_idx,
    )?;

    // 3. 分部件写入记录头与负载数据到环形缓冲区（单次寻址快路径，零整包拼接）
    // 此刻槽位值 == reserved_addr（CAS 成功路径中尾地址未再变化），
    // 故 safe_tail 至多覆盖到 reserved_addr，绝不越过尚未写入的本记录
    self
      .ring_buffer
      .write_record_parts(reserved_addr, &header_bytes, parts);

    // 4. 推流唤醒信号：帧已写入环形缓冲（safe_tail 语义保证信号被消费时
    // 记录必然完整可读），容量 1 折叠去重——推流端被唤醒后按地址序拉取，
    // 推流序与 AOF 地址序原子一致
    if let Some(tx) = self.replication_wake.get() {
      let _ = tx.try_send(());
    }

    // 5. 释放当前在途槽位（标记为已完成写入）
    unsafe { self.inflight_slots.get_unchecked(slot_idx) }.store(u64::MAX, Ordering::Release);

    Ok(reserved_addr)
  }

  /// 获取一个在途槽位并写入当前 tail 作为安全下界（根据线程 ID 亲和优先分配槽位，thread-per-core 零竞争）
  fn acquire_inflight_slot(&self) -> (usize, u64) {
    let slots_len = self.inflight_slots.len();
    let start = (current_thread_id() as usize) % slots_len;
    let mut backoff = Backoff::new();
    loop {
      let current_tail = self.tail_address.load(Ordering::Acquire);
      let mut idx = start;
      for _ in 0..slots_len {
        let slot = unsafe { self.inflight_slots.get_unchecked(idx) };
        if slot
          .compare_exchange_weak(u64::MAX, current_tail, Ordering::AcqRel, Ordering::Relaxed)
          .is_ok()
        {
          return (idx, current_tail);
        }
        idx += 1;
        if idx == slots_len {
          idx = 0;
        }
      }
      // 槽位让出为纯内存事件，排除 Sleep 深睡分支：Spin 阶段自旋、
      // Yield 及以后让核重试（阶段机复用 wbase::backoff 单一真源）
      match backoff.stage() {
        BackoffStage::Spin => spin_loop(),
        _ => yield_now(),
      }
      backoff.advance();
    }
  }

  /// CAS 循环预占地址空间
  ///
  /// 不变式：槽位值全程维护为当前 CAS 目标下界（初始/acquire 阶段为读取的 tail，
  /// 失败重试后为最新 tail），保证 safe_tail 永不越过本写入者尚未完成的记录起点
  fn reserve_address(
    &self,
    record_len: u64,
    mut current_tail: u64,
    slot_idx: usize,
  ) -> Result<u64> {
    let sector_size = self.device.sector_size() as u64;
    let buf_cap = self.config.buffer_size as u64;
    loop {
      let flushed = self.flushed_until_address.load(Ordering::Acquire);
      let start_aligned = align_down(flushed, sector_size);
      let required_end = current_tail.saturating_add(record_len);
      if required_end.saturating_sub(start_aligned) > buf_cap {
        unsafe { self.inflight_slots.get_unchecked(slot_idx) }.store(u64::MAX, Ordering::Release);
        return Err(Error::BufferFull {
          available: buf_cap.saturating_sub(current_tail.saturating_sub(start_aligned)),
          requested: record_len,
        });
      }

      match self.tail_address.compare_exchange_weak(
        current_tail,
        required_end,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(reserved_addr) => return Ok(reserved_addr),
        Err(actual) => {
          current_tail = actual;
          unsafe { self.inflight_slots.get_unchecked(slot_idx) }
            .store(current_tail, Ordering::Release);
        }
      }
    }
  }

  /// 异步将内存页面刷到底层分段设备，更新 flushed_until_address 和 committed_until_address
  pub async fn commit(&self) -> Result<u64> {
    let target = self.safe_tail_address();
    self.commit_to(target).await
  }

  /// 提交并持久化至指定逻辑地址（严格对标 Garnet Group Commit 流水线合并模式）
  pub async fn commit_to(&self, target: u64) -> Result<u64> {
    // 1. 快速短路（0 I/O）：目标水位已被硬件 sync 持久化覆盖
    let committed = self.committed_until_address.load(Ordering::Acquire);
    if target <= committed {
      return Ok(committed);
    }

    // 2. 状态机协商：判定成为 Leader 还是 Follower
    match self
      .commit_pipeline
      .enter(target, || self.committed_until_address.load(Ordering::Acquire))
    {
      Enter::Done(committed) => return Ok(committed),
      Enter::Follow(rx) => {
        // 3. Follower 分支：挂起等待 Leader 批量唤醒（0 重复物理 I/O）
        return self
          .commit_pipeline
          .wait(rx, target, || {
            self.committed_until_address.load(Ordering::Acquire)
          })
          .await
          .map_err(|_| Error::PipelineBroken(PIPELINE_BROKEN.into()));
      }
      // 升级为 Leader，接管物理刷盘管道
      Enter::Lead => {}
    }

    // 4. Leader 级联写盘主循环（Cascade Loop）：持提交锁与 reset/truncate/recover 串行
    let _commit_guard = self.commit_lock.lock().await;
    self
      .commit_pipeline
      .run_leader(WalCommitStep { wal: &self.inner })
      .await
  }

  /// 高速提交栅栏（Fast Commit Barrier）：等待指定逻辑地址提交落盘（0 表示等待当前尾地址）
  pub async fn wait_for_commit(&self, target_addr: u64) -> Result<u64> {
    let target = if target_addr == 0 {
      self.tail_address.load(Ordering::Acquire)
    } else {
      target_addr
    };
    self.commit_to(target).await
  }

  /// 安全初始化或重设 WAL 日志有效地址范围（对标 C# TsavoriteLog.SafeInitialize / Initialize）
  ///
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:SafeInitialize
  pub fn safe_initialize(&self, begin_address: u64, committed_until_address: u64) {
    let end = committed_until_address.max(begin_address);
    self.begin_address.store(begin_address, Ordering::Release);
    self.tail_address.store(end, Ordering::Release);
    self.flushed_until_address.store(end, Ordering::Release);
    self.committed_until_address.store(end, Ordering::Release);
    for slot in self.inflight_slots.iter() {
      slot.store(u64::MAX, Ordering::Release);
    }
  }

  /// 追加写入并等待提交持久化（对照 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:EnqueueAndWaitForCommitAsync）
  pub async fn enqueue_and_wait_for_commit(&self, payload: &[u8]) -> Result<u64> {
    let addr = self.enqueue(payload)?;
    // u64 口径计算记录末端，规避 32 位平台上 +RECORD_HEADER_LEN 的 usize 溢出
    let end_addr = addr + RECORD_HEADER_LEN as u64 + payload.len() as u64;
    self.wait_for_commit(end_addr).await?;
    Ok(addr)
  }

  /// 推进起始有效地址，并调用底层设备物理截断清理旧段文件
  ///
  /// 持有提交锁与 commit 互斥：防止物理删段与在途刷盘并发，段文件被删除后又被幽灵重建
  pub async fn truncate(&self, until_address: u64) -> Result<()> {
    let guard = self.commit_lock.lock().await;
    let committed = self.committed_until_address.load(Ordering::Acquire);
    let safe_until = until_address.min(committed);
    self.begin_address.fetch_max(safe_until, Ordering::SeqCst);
    self.device.truncate_until_address(safe_until).await?;
    drop(guard);
    Ok(())
  }

  /// 创建指定范围的 WAL 记录扫描迭代器
  pub fn scan(&self, from: u64, to: u64) -> WalScanIterator<D> {
    let begin = self.begin_address.load(Ordering::Acquire);
    let start_addr = from.max(begin);
    WalScanIterator::new(Arc::clone(&self.inner), start_addr, to)
  }

  /// 获取日志当前有效数据总大小（tail_address - begin_address）
  #[inline]
  pub fn total_size(&self) -> u64 {
    self.tail_address().saturating_sub(self.begin_address())
  }

  /// 重置 WAL 日志至初始空状态
  ///
  /// # 危险
  ///
  /// reset 后磁盘历史数据仍在：reset 仅回退内存位点，不物理清零磁盘。若 reset 后
  /// 未将新日志刷满 [begin, align_up(tail)) 前缀即发生崩溃，恢复扫描可能越过新尾部
  /// 复活旧记录（与 C# TsavoriteLog.Reset 后未打检查点即崩溃的恢复语义一致）。
  /// 调用方要么 reset 后立即 truncate 物理清理，要么接受该复活窗口。
  ///
  /// WARNING: 须在日志静默（无并发读写）后调用，对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:Reset。
  pub async fn reset(&self) -> Result<()> {
    log::warn!(
      "WAL reset：磁盘历史数据未清零，崩溃后恢复可能复活旧记录（begin={begin:#x}）",
      begin = self.begin_address.load(Ordering::Acquire),
    );
    let guard = self.commit_lock.lock().await;
    let start_seg = self.device.start_segment() as u64;
    let seg_size = self.device.segment_size().unwrap_or(0);
    let begin_addr = start_seg * seg_size;

    self.begin_address.store(begin_addr, Ordering::Release);
    self.tail_address.store(begin_addr, Ordering::Release);
    self
      .flushed_until_address
      .store(begin_addr, Ordering::Release);
    self
      .committed_until_address
      .store(begin_addr, Ordering::Release);

    for slot in self.inflight_slots.iter() {
      slot.store(u64::MAX, Ordering::Release);
    }
    // 与 commit 一致采用 fdatasync 快速刷盘
    self.device.sync_data().await?;
    drop(guard);
    Ok(())
  }

  /// 扫描当前所有已提交的记录
  #[inline]
  pub fn scan_committed(&self) -> WalScanIterator<D> {
    self.scan(
      self.begin_address.load(Ordering::Acquire),
      self.committed_until_address.load(Ordering::Acquire),
    )
  }

  /// 扫描当前所有已写入（包含未提交内存）的记录
  #[inline]
  pub fn scan_all(&self) -> WalScanIterator<D> {
    self.scan(
      self.begin_address.load(Ordering::Acquire),
      self.tail_address.load(Ordering::Acquire),
    )
  }

  /// 获取起始有效地址
  #[inline]
  pub fn begin_address(&self) -> u64 {
    self.begin_address.load(Ordering::Acquire)
  }

  /// 获取当前尾部逻辑地址
  #[inline]
  pub fn tail_address(&self) -> u64 {
    self.tail_address.load(Ordering::Acquire)
  }

  /// 获取已刷盘的逻辑地址
  #[inline]
  pub fn flushed_until_address(&self) -> u64 {
    self.flushed_until_address.load(Ordering::Acquire)
  }

  /// 获取已提交的逻辑地址
  #[inline]
  pub fn committed_until_address(&self) -> u64 {
    self.committed_until_address.load(Ordering::Acquire)
  }

  /// 获取底层设备引用
  #[inline]
  pub fn device(&self) -> &Arc<D> {
    &self.device
  }

  /// 获取配置引用
  #[inline]
  pub fn config(&self) -> &WalConfig {
    &self.config
  }
}

/// WAL 提交步进器：批次目标取安全尾地址（在途写入下界），水位取已提交位点，
/// 物理持久化复用环形缓冲刷盘 + fdatasync 单点实现
struct WalCommitStep<'a, D: Device> {
  wal: &'a WalLogInner<D>,
}

impl<D: Device> GroupCommitStep for WalCommitStep<'_, D> {
  type Error = Error;

  #[inline]
  fn tail(&self) -> u64 {
    self.wal.safe_tail_address()
  }

  #[inline]
  fn watermark(&self) -> u64 {
    self.wal.committed_until_address.load(Ordering::Acquire)
  }

  async fn step(&self, target: u64) -> Result<u64> {
    let flushed = self.wal.flushed_until_address.load(Ordering::Acquire);
    if target > flushed {
      self.wal.flush_and_sync_range(flushed, target).await
    } else {
      Ok(self.wal.committed_until_address.load(Ordering::Acquire))
    }
  }
}
