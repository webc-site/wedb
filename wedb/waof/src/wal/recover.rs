//! WAL 恢复域:扫描驱动器与恢复私有件(log.rs 整体迁入,恢复算法与 C# 锚点注释零改动)

use std::sync::atomic::Ordering;

use wbase::pool::AlignedBuf;
use wdev::{Device, Error as DeviceError};

use super::{
  commit,
  disk_window::DiskWindow,
  header::{RECORD_HEADER_LEN, WalFrameHeader},
  log::{RECOVER_CHUNK_SIZE, WalLog, log_begin_address},
};
use crate::error::{Error, Result};

impl<D: Device> WalLog<D> {
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
  ///
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:RecoverAsync
  pub async fn recover(&self) -> Result<u64> {
    let guard = self.commit_lock.lock().await;

    // 截断观测归零（每次恢复独立统计）
    self.recover_truncated_at.store(0, Ordering::Release);
    self.recover_dropped_bytes.store(0, Ordering::Release);

    // 1. 先触发底层设备的段文件元数据扫描与恢复（如 SegmentedDevice 恢复 start_segment/end_segment）
    self.device.recover()?;

    // 起始位点与 reset/构造同取一个派生单点（见 [`log_begin_address`]）；恢复路径
    // 只写「扫描所得」的尾/提交位点，不复用 reset 的写序
    let begin_addr = log_begin_address(&*self.device);
    self.begin_address.store(begin_addr, Ordering::Release);

    let mut cur = begin_addr;
    // 最后一个合法 commit 元数据帧（meta + 帧尾地址 = 该批提交上界）
    let mut last_commit: Option<(commit::CommitMeta, u64)> = None;

    // 2. 物理截断后的段首可能落在跨段记录的残缺负载中部，需先帧同步定位首条完整记录
    // （仅分段设备且段号已前移时；段号与段大小口径同 [`log_begin_address`] 的派生源）
    if self.device.start_segment() > 0
      && self.device.segment_size().unwrap_or(0) > 0
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
      match self.verify_candidate(cur, header, &disk_win, rel_off).await {
        Ok(true) => {
          // commit 帧识别：判定本体单点收敛于 commit::is_commit_frame（长度与
          // 魔数判据同一处，见 [`commit::is_commit_frame`]）；此处仅保留
          // 「负载恒 24B」常量比较作 I/O 快速过滤——非 24B 的正常数据条目
          // 零额外读取；帧尾即该批提交上界
          if entry_len == commit::COMMIT_FRAME_PAYLOAD_LEN {
            let payload = self
              .read_recover_payload(cur, &header, &disk_win, rel_off)
              .await?;
            let payload = payload.as_slice();
            if commit::is_commit_frame(payload)
              && let Some(meta) = commit::decode_payload(payload)
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
    //
    // 位点写序与 [`WalLog::reset`] 的差异为刻意：本路径写「扫描所得」的
    // committed/tail/flushed 与 recovered_* 观测（reset 不反向改写 recovered_*，
    // 那是「本进程曾恢复过」的事实记录）；pending_cookie 亦不重置（刻意——
    // 它只是 host 于每次 commit 前注入的一次性游标，生产唯一写入点
    // wnode/src/aof/waof_sublog.rs:commit_flush_async 先 set 后 commit，恢复期
    // 复位反而会吞掉 host 在恢复与下一批之间已注入的 cookie）
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

    self.reset_inflight_slots();

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
  /// 副本数据缺口显式拒绝恢复（libs/server/AOF/GarnetAppendOnlyFile.cs:DataLossCheck 消费面）、
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
    // 预读窗复用扫描侧同一容器：探针与越窗回退口径全程单点
    let mut win = DiskWindow::new();

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
      win.replace(win_start, probe);
      let slice = win.slice();
      let mut off = 0;
      while let Some(hdr) = WalFrameHeader::decode_opt(&slice[off..]) {
        // 全零头（填充/残缺）或负载超限的伪头：前移 1 字节继续探测（单指令极速过滤）
        if hdr.is_zero() || hdr.payload_len() > cap {
          off += 1;
          continue;
        }

        if self
          .verify_candidate(win_start + off as u64, hdr, &win, off)
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
    win: &DiskWindow,
    off: usize,
  ) -> Result<bool> {
    if let Some(payload) = win.payload(hdr_addr, &hdr, off) {
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

  /// 读取恢复扫描候选记录的完整负载（负载在预读窗内零 I/O 直借，越窗回退单次
  /// 设备读取并把回读缓冲留在调用栈上；供 commit 帧识别使用，全程零堆分配）
  async fn read_recover_payload<'a>(
    &self,
    hdr_addr: u64,
    hdr: &WalFrameHeader,
    win: &'a DiskWindow,
    off: usize,
  ) -> Result<RecoveredPayload<'a>> {
    if let Some(payload) = win.payload(hdr_addr, hdr, off) {
      return Ok(RecoveredPayload::Window(payload));
    }
    Ok(RecoveredPayload::Device(
      self
        .device
        .read_range(hdr_addr + RECORD_HEADER_LEN as u64, hdr.payload_len())
        .await?,
    ))
  }
}

/// 恢复扫描负载的取用形态：窗内直借预读窗切片，越窗承载单次设备回读的缓冲
///
/// 两种形态统一经 [`RecoveredPayload::as_slice`] 出口交给只需 `&[u8]` 的 commit
/// 判据，取代此前把窗内负载整段复制为堆上 Vec 的逐记录分配
enum RecoveredPayload<'a> {
  /// 负载完整落在预读窗内：零 I/O 借用切片，生命周期随窗口
  Window(&'a [u8]),
  /// 负载越出预读窗：一次设备读取的回读缓冲，随本值析构
  Device(AlignedBuf),
}

impl RecoveredPayload<'_> {
  #[inline]
  fn as_slice(&self) -> &[u8] {
    match self {
      Self::Window(payload) => payload,
      Self::Device(buf) => buf.as_slice(),
    }
  }
}
