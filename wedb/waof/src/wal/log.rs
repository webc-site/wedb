use std::{
  future::ready,
  ops::Deref,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicI64, AtomicU64, Ordering, fence},
  },
};

use async_lock::Mutex as AsyncLockMutex;
use crossfire::{MAsyncTx, mpsc::Array};
use wbase::{group_commit::GroupCommitPipeline, pool::AlignedBuf};
use wdev::{self, Device, Error as DeviceError};

use super::{
  commit,
  config::WalConfig,
  disk_window::DiskWindow,
  header::{RECORD_HEADER_LEN, WalFrameHeader},
  iterator::WalScanIterator,
  record::WalRecord,
  ring_buffer::RingBuffer,
};
use crate::error::{Error, Result};

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
  /// 待随批写出的提交 cookie（宿主经 [`WalLog::set_pending_cookie`] 更新，
  /// Leader 写 commit 元数据帧时采样；[`commit::NO_COOKIE`] = 无序列号）
  pub pending_cookie: AtomicI64,
  /// 已写出的最后 commit 元数据帧尾地址（帧游标：级联循环防重复写帧；
  /// truncate/reset 时同步收敛）
  pub last_commit_frame: AtomicU64,
  /// 恢复收敛出的最后一次提交 cookie（[`commit::NO_COOKIE`] = 无 commit 帧）
  pub recovered_cookie: AtomicI64,
  /// 恢复收敛出的最后 commit 帧的 begin 快照
  pub recovered_committed_begin: AtomicU64,
  /// 恢复保守截尾的损坏帧地址（0 = 本次恢复无截断，观测面见
  /// [`WalLog::recover_truncation`]）
  pub recover_truncated_at: AtomicU64,
  /// 恢复保守截尾丢弃的字节数（损坏点所在段残余 + 其后各段整段）
  pub recover_dropped_bytes: AtomicU64,
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
}

/// WAL 引擎（对标 C# TsavoriteLog）
///
/// 持久性边界：commit 时随批尾写 commit 元数据帧（begin + cookie，对标
/// TsavoriteLog.cs:TryEnqueueCommitRecord，见 [`super::commit`]），`recover`
/// 扫至最后 commit 帧收敛提交上界，帧后的未提交记录不被动转正。设备上无任何
/// commit 帧（旧形态日志或截断后）时回退保守兼容语义：以最后一条完整记录为
/// 已提交。要求更宽提交边界的调用方（如一致性协议日志）仍可以扫描终点自行
/// 界定
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
    // 日志首地址取段边界。刻意差异：C# Garnet AOF 的 kFirstValidAofAddress = 64
    // 源自 TsavoriteLog 设备头区（commit 元数据写设备头，复制恢复以 64 作
    // "副本非空"哨兵）；本实现 commit 元数据为随批尾帧（见模块文档），无头区，
    // 空日志判据统一为 begin == tail，磁盘格式差异见 header 模块
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
        pending_cookie: AtomicI64::new(commit::NO_COOKIE),
        last_commit_frame: AtomicU64::new(begin_addr),
        recovered_cookie: AtomicI64::new(commit::NO_COOKIE),
        recovered_committed_begin: AtomicU64::new(begin_addr),
        recover_truncated_at: AtomicU64::new(0),
        recover_dropped_bytes: AtomicU64::new(0),
      }),
    })
  }

  /// 注册复制流推流唤醒信号发送端（须在产生任何写入之前调用；一次注入，
  /// 重复注入返回 false）
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

    // 截断观测归零（每次恢复独立统计）
    self.recover_truncated_at.store(0, Ordering::Release);
    self.recover_dropped_bytes.store(0, Ordering::Release);

    // 1. 先触发底层设备的段文件元数据扫描与恢复（如 SegmentedDevice 恢复 start_segment/end_segment）
    self.device.recover()?;

    let start_seg = self.device.start_segment() as u64;
    let seg_size = self.device.segment_size().unwrap_or(0);
    let begin_addr = start_seg * seg_size;
    self.begin_address.store(begin_addr, Ordering::Release);

    let mut cur = begin_addr;
    // 最后一个合法 commit 元数据帧（meta + 帧尾地址 = 该批提交上界）
    let mut last_commit: Option<(commit::CommitMeta, u64)> = None;

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
      let Some(header) = WalFrameHeader::decode_opt(&disk_win.slice()[rel_off..]) else {
        self
          .note_recover_truncation(cur, "残缺帧头（不足 8 字节）")
          .await;
        break;
      };

      // 全零头 = 扇区填充或崩溃残缺尾部（空记录携带哨兵 CRC，绝不呈现全零头），链在此终止
      let entry_len = header.payload_len();
      if header.is_zero() || entry_len > self.config.buffer_size {
        let reason = if header.is_zero() {
          "全零帧头（扇区填充或崩溃残缺尾部）"
        } else {
          "帧头负载长度超限（伪头）"
        };
        self.note_recover_truncation(cur, reason).await;
        break;
      }

      // 校验当前记录：负载完整位于预读窗内则零 I/O 直验，越窗时回退单次设备读取
      match self
        .verify_candidate(cur, header, disk_win.slice(), rel_off)
        .await
      {
        Ok(true) => {
          // commit 帧识别：判定本体单点收敛于 commit::is_commit_frame（长度与
          // 魔数判据同一处，见 [`commit::is_commit_frame`]）；此处仅保留
          // 「负载恒 24B」常量比较作 I/O 快速过滤——非 24B 的正常数据条目
          // 零额外读取；帧尾即该批提交上界
          if entry_len == commit::COMMIT_FRAME_PAYLOAD_LEN {
            let payload = self
              .read_recover_payload(cur, &header, disk_win.slice(), rel_off)
              .await?;
            if commit::is_commit_frame(&payload)
              && let Some(meta) = commit::decode_payload(&payload)
            {
              last_commit = Some((meta, cur + commit::COMMIT_FRAME_TOTAL_LEN));
            }
          }
          cur += (RECORD_HEADER_LEN + entry_len) as u64;
        }
        Ok(false) => {
          self
            .note_recover_truncation(cur, "帧负载 CRC 校验失败或负载未完整写入")
            .await;
          break;
        }
        Err(e) => return Err(e),
      }
    }

    // 提交上界收敛：扫至最后 commit 帧（对标 C# RestoreLatestAsync 的
    // commit 元数据装载）；无帧日志回退「最后一条完整记录即已提交」
    let committed = last_commit.as_ref().map_or(cur, |(_, end)| (*end).min(cur));
    self
      .committed_until_address
      .store(committed, Ordering::Release);
    if let Some((meta, _)) = last_commit {
      self.recovered_cookie.store(meta.cookie, Ordering::Release);
      self
        .recovered_committed_begin
        .store(meta.begin, Ordering::Release);
    }
    self.last_commit_frame.store(committed, Ordering::Release);

    self.tail_address.store(cur, Ordering::Release);
    self.flushed_until_address.store(cur, Ordering::Release);

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

  /// 记录恢复保守截尾观测：填充截断统计并留 warn 日志（损坏地址、截断点、
  /// 丢弃字节数），供上层按配置选择拒绝恢复而非静默容忍
  ///
  /// 对标差异登记（FastAofTruncate 两态）：C# 在 FastAofTruncate=false 时对
  /// 副本数据缺口显式拒绝恢复（GarnetAppendOnlyFile.cs:DataLossCheck 消费面）、
  /// =true 时容忍截断；本实现维持容忍截断（自动取最后完整记录）为最终决策，
  /// 截断事实经 [`WalLog::recover_truncation`] 统计与 warn 日志暴露，上层拒绝
  /// 开关不做（刻意差异，见恢复策略文档）
  async fn note_recover_truncation(&self, corrupt_addr: u64, reason: &str) {
    let dropped = self.dropped_bytes_after(corrupt_addr);
    // 尾部判定：截断点之后无非零数据（对齐填充/残缺尾）属崩溃常态，静默
    // 放行不留痕；仅当丢弃了非零数据（疑似中段介质损坏/伪头）才告警统计。
    // 边界登记：整段被清零的介质损坏与填充零不可区分，保守按常态放行
    if !self.has_nonzero_after(corrupt_addr, dropped).await {
      return;
    }
    self
      .recover_truncated_at
      .store(corrupt_addr, Ordering::Release);
    self.recover_dropped_bytes.store(dropped, Ordering::Release);
    log::warn!(
      "WAL 恢复保守截尾：{reason}，损坏帧地址={corrupt_addr:#x}，截断点={corrupt_addr:#x}（tail 收敛于此），丢弃其后 {dropped} 字节",
    );
  }

  /// 探测截断点之后的数据窗口是否含非零字节（纯零填充 = 崩溃常态尾部）
  ///
  /// 探测上限 1MB：更长区间的纯零填充与介质清零不可区分，保守按常态放行
  ///
  /// 返回 false 有两种放行语义，须区分留痕、勿当噪音删除：
  /// - 数据二义放行：读到全零窗口，与整段被清零的介质损坏不可区分，按崩溃常态静默放行；
  /// - 探测失败放行：探测读自身遭遇设备错误，无法判定窗口内容，亦按常态放行但必须
  ///   warn 留痕——设备读错误是设备面信号，与数据面的零/非零二义无关，静默吞没会让
  ///   恢复期介质异常整体不可观测（对标 C#
  ///   libs/server/AOF/Recover/AofRecover.cs 内联恢复驱动 RecoverReplayDriver 异常面对
  ///   IOException 的 LogError；本仓维持「恢复不阻断」故不上抛，仅补齐观测）。
  async fn has_nonzero_after(&self, addr: u64, len: u64) -> bool {
    let probe_len = len.min(1024 * 1024) as usize;
    if probe_len == 0 {
      return false;
    }
    match self.device.read_range(addr, probe_len).await {
      Ok(buf) => buf.as_slice().iter().any(|&b| b != 0),
      Err(e) => {
        log::warn!(
          "WAL 恢复截断探测读失败，按常态放行：探测地址={addr:#x}，探测长度={probe_len}，错误={e}",
        );
        false
      }
    }
  }

  /// 统计损坏点之后的可丢弃字节数（所在段文件残余 + 其后各段整段；尽力
  /// 口径，段元数据不可得时段按 0 计）
  fn dropped_bytes_after(&self, addr: u64) -> u64 {
    let dev = &*self.device;
    let Some(seg_size) = dev.segment_size() else {
      // 单文件设备：逻辑地址即文件内偏移
      let size = dev.get_file_size(dev.start_segment()).unwrap_or(0);
      return size.saturating_sub(addr);
    };
    if seg_size == 0 {
      return 0;
    }
    let seg = addr / seg_size;
    let mut dropped = dev
      .get_file_size(seg as u32)
      .unwrap_or(0)
      .saturating_sub(addr % seg_size);
    let end = dev.end_segment().unwrap_or(seg as u32);
    for next in (seg + 1)..=(end as u64) {
      dropped += dev.get_file_size(next as u32).unwrap_or(0);
    }
    dropped
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

        let hdr = WalFrameHeader {
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
    hdr: WalFrameHeader,
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

  /// 读取恢复扫描候选记录的完整负载（负载在预读窗内零 I/O 直取，越窗回退
  /// 单次设备读取；供 commit 帧识别使用）
  async fn read_recover_payload(
    &self,
    hdr_addr: u64,
    hdr: &WalFrameHeader,
    slice: &[u8],
    off: usize,
  ) -> Result<Vec<u8>> {
    let payload_end = off + RECORD_HEADER_LEN + hdr.payload_len();
    if payload_end <= slice.len() {
      return Ok(unsafe { slice.get_unchecked(off + RECORD_HEADER_LEN..payload_end) }.to_vec());
    }
    Ok(
      self
        .device
        .read_range(hdr_addr + RECORD_HEADER_LEN as u64, hdr.payload_len())
        .await?
        .as_slice()
        .to_vec(),
    )
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

  /// 推进起始有效地址，并调用底层设备物理截断清理旧段文件
  ///
  /// 持有提交锁与 commit 互斥：防止物理删段与在途刷盘并发，段文件被删除后又被幽灵重建
  pub async fn truncate(&self, until_address: u64) -> Result<()> {
    let guard = self.commit_lock.lock().await;
    let committed = self.committed_until_address.load(Ordering::Acquire);
    // 安全上界钳制（只截已获持久承诺的字节）是 waof 侧语义参数，先于内核完成
    let safe_until = until_address.min(committed);
    // 截断编排内核单源下沉（Device::truncate_begin_until，对标 AllocatorBase.cs:
    // ShiftBeginAddress 的「先推进 begin → 物理截断」次序）：waof 无纪元排空
    // 屏障，注入 ready 空屏障；本调用与全部刷盘在 commit_lock 内串行，无可观测
    // 交错差异。帧游标钳至截断点（被物理删除段内的 commit 帧失效，后续 commit
    // 重写新帧）在设备删段返回后完成
    self
      .device
      .truncate_begin_until(&self.begin_address, safe_until, ready(()))
      .await?;
    self
      .last_commit_frame
      .fetch_min(safe_until, Ordering::AcqRel);
    drop(guard);
    Ok(())
  }

  /// 创建指定范围的 WAL 记录扫描迭代器
  pub fn scan(&self, from: u64, to: u64) -> WalScanIterator<D> {
    let begin = self.begin_address.load(Ordering::Acquire);
    let start_addr = from.max(begin);
    WalScanIterator::new(Arc::clone(&self.inner), start_addr, to)
  }

  /// 内存窗口同步记录扫描（直构 WalRecord，零二次堆分配）
  pub fn scan_memory_records(
    &self,
    from: u64,
    to: u64,
    mut f: impl FnMut(&WalRecord) -> bool,
  ) -> bool {
    let cap = self.ring_buffer.capacity() as u64;
    let mem_base = self.tail_address();
    let start = from.max(self.begin_address());
    if start < mem_base.saturating_sub(cap) {
      return false;
    }
    let scan_end = to.min(self.safe_tail_address());
    let mut cur = start;
    while cur + (RECORD_HEADER_LEN as u64) <= scan_end {
      let header = self.ring_buffer.read_header(cur);
      let entry_len = header.payload_len();
      if header.is_zero() || entry_len > self.config().buffer_size {
        break;
      }
      let next_addr = cur + (RECORD_HEADER_LEN as u64) + (entry_len as u64);
      if next_addr > scan_end {
        break;
      }
      let payload = self
        .ring_buffer
        .read_vec(cur + RECORD_HEADER_LEN as u64, entry_len);
      if header.verify(&payload).is_err() {
        break;
      }
      let rec = WalRecord {
        address: cur,
        next_address: next_addr,
        header,
        payload,
      };
      if !f(&rec) {
        break;
      }
      cur = next_addr;
    }
    true
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
    self.last_commit_frame.store(begin_addr, Ordering::Release);
    self
      .pending_cookie
      .store(commit::NO_COOKIE, Ordering::Release);

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

  /// 更新待随批写出的提交 cookie（宿主提交入口调用；[`commit::NO_COOKIE`] = 无序列号）
  #[inline]
  pub fn set_pending_cookie(&self, cookie: i64) {
    self.pending_cookie.store(cookie, Ordering::Release);
  }

  /// 恢复收敛出的最后一次提交 cookie（[`commit::NO_COOKIE`] = 无 commit 帧）
  #[inline]
  pub fn recovered_cookie(&self) -> i64 {
    self.recovered_cookie.load(Ordering::Acquire)
  }

  /// 恢复收敛出的最后 commit 帧的 begin 快照
  #[inline]
  pub fn recovered_committed_begin(&self) -> u64 {
    self.recovered_committed_begin.load(Ordering::Acquire)
  }

  /// 恢复保守截尾观测（Some((损坏帧地址, 丢弃字节数)) = 本次恢复发生截尾；
  /// 供上层按配置选择拒绝恢复，观测面见 [`Self::note_recover_truncation`]）
  #[inline]
  pub fn recover_truncation(&self) -> Option<(u64, u64)> {
    let at = self.recover_truncated_at.load(Ordering::Acquire);
    (at != 0).then(|| (at, self.recover_dropped_bytes.load(Ordering::Acquire)))
  }

  /// 获取配置引用
  #[inline]
  pub fn config(&self) -> &WalConfig {
    &self.config
  }
}
