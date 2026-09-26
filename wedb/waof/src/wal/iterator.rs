use std::sync::{Arc, atomic::Ordering};

use wbase::pool::AlignedBuf;
use wdev::{Device, Error as DeviceError};

use super::{
  disk_window::DiskWindow,
  header::{RECORD_HEADER_LEN, WalFrameHeader},
  log::{RECOVER_CHUNK_SIZE, WalLogInner},
  record::{WalFrame, WalRecord},
  ring_buffer::{MemFrame, MemFrameAssembled},
};
use crate::error::{Error, Result};

/// 内存环解码失败原因归一（read_record / read_frame 两路共享处置语义）
enum MemFail {
  Invalid,
  CrcMismatch(Error),
}

/// 单步扫描上下文快照（next / next_frame 共享前置产物）
struct ScanStep {
  /// 截断 begin 位点（跳越后落点帧的边界判据用）
  begin_addr: u64,
  /// 本轮是否因 cur < begin 被并发截断越过而跳跃至截断点
  jumped_over: bool,
  /// 本记录起始位点
  record_addr: u64,
  available_end: u64,
  mem_base: u64,
}

/// WAL 记录迭代扫描器，透明支持跨内存与磁盘分段文件顺序读取
pub struct WalScanIterator<D: Device> {
  inner: Arc<WalLogInner<D>>,
  cur_address: u64,
  end_address: u64,
  disk_win: DiskWindow,
  /// 因未提交数据被环形覆写而跳过的记录数（见 `read_record` 内论证）
  overwritten_skips: u64,
}

impl<D: Device> WalScanIterator<D> {
  pub(crate) fn new(inner: Arc<WalLogInner<D>>, start_address: u64, end_address: u64) -> Self {
    Self {
      inner,
      cur_address: start_address,
      end_address,
      disk_win: DiskWindow::new(),
      overwritten_skips: 0,
    }
  }

  /// 获取当前迭代游标逻辑地址
  #[inline]
  pub fn current_address(&self) -> u64 {
    self.cur_address
  }

  /// 获取因未提交数据被环形覆写而跳过的记录数
  ///
  /// `scan` 遇到覆写残迹时必须终止顺序扫描（记录头已被覆写，长度不可知，
  /// 无法定位下一条记录边界），该终止以 `Ok(None)` 呈现，与正常扫完不可区分；
  /// 本计数使调用方得以区分"扫完"与"中途覆写丢失"：非零即代表迭代提前终止，
  /// 且丢失区间磁盘无权威副本（未落盘即被覆写，见 `read_record` 论证），无法回读恢复。
  #[inline]
  pub fn overwritten_skips(&self) -> u64 {
    self.overwritten_skips
  }

  /// 扫描前置单源（next / next_frame 共用）：游标越界判 EOF、迭代中并发截断推进
  /// begin 位点时平滑跳跃（对齐 C# ScanBehindBeginAddress 语义）、可用终点折叠。
  /// 返回 None 表示本轮迭代耗尽（两调用方一致以 `Ok(None)` 终止）
  fn scan_step(&mut self) -> Option<ScanStep> {
    if self.cur_address >= self.end_address {
      return None;
    }

    let begin_addr = self.inner.begin_address.load(Ordering::Acquire);
    // 被截断越过的慢读者标记：本轮因 cur < begin 跳跃至截断点，落点帧的透传
    // 判据须区别对待（见下跳帧终止分支）
    let jumped_over = self.cur_address < begin_addr;
    if jumped_over {
      self.cur_address = begin_addr;
    }

    // 不变式：flushed ≤ safe_tail 恒成立（在途槽位下界 ≥ 注册时 tail ≥ 任意历史 flushed）。
    // 终点不超过 flushed 时（scan_committed 常态）直接采用终点，
    // 免除每条记录 O(在途槽位数) 的 safe_tail 折叠扫描
    let (available_end, mem_base) =
      if self.end_address <= self.inner.flushed_until_address.load(Ordering::Acquire) {
        (
          self.end_address,
          self.inner.tail_address.load(Ordering::Acquire),
        )
      } else {
        let safe_tail = self.inner.safe_tail_address();
        (self.end_address.min(safe_tail), safe_tail)
      };
    if self.cur_address + (RECORD_HEADER_LEN as u64) > available_end {
      return None;
    }

    Some(ScanStep {
      begin_addr,
      jumped_over,
      record_addr: self.cur_address,
      available_end,
      mem_base,
    })
  }

  /// 异步顺序获取下一条 WAL 记录
  ///
  /// C# 顺序读 API 的统一落点（ReadAsync 支持以 nextAddress 续读，GetNext 为
  /// 扫描迭代器拉取原语，rust 单遍历引擎一处承担）：
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:ReadAsync
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogScanIterator.cs:GetNext
  /// libs/storage/Tsavorite/cs/src/core/Allocator/TsavoriteLogAllocatorImpl.cs:ReadAsync
  pub async fn next(&mut self) -> Result<Option<WalRecord>> {
    loop {
      let Some(step) = self.scan_step() else {
        return Ok(None);
      };

      let Some((header, payload, next_addr)) = self
        .read_record(step.record_addr, step.available_end, step.mem_base)
        .await?
      else {
        return Ok(None);
      };

      // 尾截断 × 截断点前起扫的终止条件缺口：慢读者被并发截断越过 begin 后跳跃至
      // 截断点，truncate 钳帧游标、其后 commit 于该位点补写 commit 元数据帧。非跳越
      // 读者的物理扫描面刻意透传该帧（消费层自滤），但被截断越过的读者不得透传此
      // 截断边界帧——对标 C# TsavoriteLogScanIterator.cs:GetNext 命中 commit record
      // 即「跳帧后返回 false」：跳过边界帧续读，其后无权威数据则自然耗尽为 None。
      if step.jumped_over
        && step.record_addr == step.begin_addr
        && super::commit::is_commit_frame(&payload)
      {
        self.cur_address = next_addr;
        continue;
      }

      self.cur_address = next_addr;
      return Ok(Some(WalRecord {
        address: step.record_addr,
        next_address: next_addr,
        header,
        payload,
      }));
    }
  }

  /// 异步顺序获取下一条完整 WAL 记录帧（8B 记录头 + 负载直组，一次分配零二次重拷）
  ///
  /// 对标 C# TsavoriteLogScanIterator.cs:GetNext / BulkConsumeAllAsync 直传帧消费面：
  /// 供复制推流泵等数据面热路径直接获取包含 8B 头 + 负载的完整帧，
  /// 消除原 WalRecord + reconstruct_frame 路径下的二次堆分配与二次内存拷贝。
  pub async fn next_frame(&mut self) -> Result<Option<WalFrame>> {
    loop {
      let Some(step) = self.scan_step() else {
        return Ok(None);
      };

      let Some((frame, next_addr)) = self
        .read_frame(step.record_addr, step.available_end, step.mem_base)
        .await?
      else {
        return Ok(None);
      };

      // 被截断越过的读者不得透传截断边界处补写的 commit 帧（完整论证见 next 同位判据）
      if step.jumped_over
        && step.record_addr == step.begin_addr
        && super::commit::is_commit_frame(&frame[RECORD_HEADER_LEN..])
      {
        self.cur_address = next_addr;
        continue;
      }

      self.cur_address = next_addr;
      return Ok(Some(WalFrame {
        address: step.record_addr,
        next_address: next_addr,
        frame,
      }));
    }
  }

  /// 读取单条记录：优先内存环形缓冲直读，失败时按位点状态回退磁盘权威数据
  ///
  /// 内存臂的单帧解码（读头 → 守卫 → 推进 → 校验）单源化于
  /// [`super::ring_buffer::RingBuffer::decode_frame`]（对标 C#
  /// TsavoriteLog 的 Scan 单趟解码），失败处置共享 `mem_decode_failed`，
  /// 本入口只保留落盘回退终止语义
  async fn read_record(
    &mut self,
    record_addr: u64,
    available_end: u64,
    mem_base: u64,
  ) -> Result<Option<(WalFrameHeader, Vec<u8>, u64)>> {
    let cap = self.inner.ring_buffer.capacity() as u64;
    // 若记录位点仍在环形缓冲窗口内（record_addr >= mem_base - cap），优先从内存解码；
    // 越出窗口则直落设备权威读取
    if record_addr >= mem_base.saturating_sub(cap) {
      let fail = match self
        .inner
        .ring_buffer
        .decode_frame(record_addr, available_end)
      {
        MemFrame::Valid {
          header,
          payload,
          next_addr,
        } => return Ok(Some((header, payload, next_addr))),
        MemFrame::Invalid => MemFail::Invalid,
        MemFrame::CrcMismatch(e) => MemFail::CrcMismatch(e),
      };
      return self
        .mem_decode_failed(record_addr, available_end, cap, fail, false)
        .await;
    }
    self
      .read_from_device(record_addr, available_end, false)
      .await
  }

  /// 读取单条完整帧：优先内存环形缓冲直读，失败时按位点状态回退磁盘权威数据
  ///
  /// 处置语义与 `read_record` 单源共享，`whole_frame` 位仅改变回退产物的
  /// 缓冲形态（完整帧 vs 纯负载），判序与终止条件逐字一致
  async fn read_frame(
    &mut self,
    record_addr: u64,
    available_end: u64,
    mem_base: u64,
  ) -> Result<Option<(Vec<u8>, u64)>> {
    let cap = self.inner.ring_buffer.capacity() as u64;
    if record_addr >= mem_base.saturating_sub(cap) {
      let fail = match self
        .inner
        .ring_buffer
        .decode_frame_assembled(record_addr, available_end)
      {
        MemFrameAssembled::Valid { frame, next_addr } => return Ok(Some((frame, next_addr))),
        MemFrameAssembled::Invalid => MemFail::Invalid,
        MemFrameAssembled::CrcMismatch(e) => MemFail::CrcMismatch(e),
      };
      return Ok(
        self
          .mem_decode_failed(record_addr, available_end, cap, fail, true)
          .await?
          .map(|(_, frame, next_addr)| (frame, next_addr)),
      );
    }
    Ok(
      self
        .read_from_device(record_addr, available_end, true)
        .await?
        .map(|(_, frame, next_addr)| (frame, next_addr)),
    )
  }

  /// 内存环解码失败的统一处置（read_record / read_frame 两路共享单源）：
  /// - Invalid：已落盘则回退设备权威数据；未落盘属位错/覆写残迹竞态，落日志平滑终止
  /// - CrcMismatch：已落盘则回退设备（含并发提交刚完成的情形）；未落盘则区分
  ///   被环形回绕挤出内存窗而遭覆写（显式报告后终止）与真实内存数据损坏（上抛）
  ///
  /// `whole_frame` 透传给设备回退臂决定产物形态
  async fn mem_decode_failed(
    &mut self,
    record_addr: u64,
    available_end: u64,
    cap: u64,
    fail: MemFail,
    whole_frame: bool,
  ) -> Result<Option<(WalFrameHeader, Vec<u8>, u64)>> {
    let flushed = self.inner.flushed_until_address.load(Ordering::Acquire);
    match fail {
      MemFail::Invalid => {
        if record_addr < flushed {
          return self
            .read_from_device(record_addr, available_end, whole_frame)
            .await;
        }
        // 未落盘即无效：位错/覆写残迹竞态下「扫描提前终止」的唯一定位锚，
        // debug 级避免热路径噪声
        log::debug!("扫描在 {record_addr} 遇未落盘无效帧（flushed_until={flushed}），平滑终止");
        Ok(None)
      }
      MemFail::CrcMismatch(e) => {
        if record_addr < flushed {
          // 已落盘则回退磁盘权威数据（含并发提交刚完成的情形）
          return self
            .read_from_device(record_addr, available_end, whole_frame)
            .await;
        }
        // 未落盘却校验失败：若已被环形回绕挤出内存窗（并发写入推进 tail
        // 越过一个容量，数据遭覆写淘汰）则显式报告后终止。
        // 仍在窗内失败属真实内存数据损坏，上抛
        let tail_now = self.inner.tail_address.load(Ordering::Acquire);
        if record_addr < tail_now.saturating_sub(cap) {
          self.overwritten_skips += 1;
          // 显式报告而非静默丢失：该区间尚未落盘（磁盘上只有上一代日志残留），
          // 从设备回读只会读到错误数据——缺数据优于错数据。
          // 对照 C# TsavoriteLog：C# 采用页式管理，页淘汰（SealPageAndFlush）前
          // 必然先刷盘，未提交数据绝不会被覆写，故 C# 扫描无此分支；
          // 本实现的环形缓冲 + 异步取消等极端位点竞争下存在该理论窗口，
          // 无法恢复数据，只能计数 + 日志显式报告（见 `overwritten_skips` 文档）
          log::warn!(
            "扫描在地址 {record_addr:#x} 遇到被环形覆写的未提交记录（tail={tail_now:#x}, \
             容量={cap}），迭代提前终止，跳过 {skipped} 条",
            skipped = self.overwritten_skips,
          );
          return Ok(None);
        }
        Err(e)
      }
    }
  }

  /// 拉取磁盘窗口数据；迭代期间段文件被并发物理删除且 begin 位点已越过本记录时
  /// 返回 None（平滑终止，对齐 C# 扫描器遇截断不抛错的语义）
  async fn fetch_window(&self, offset: u64, len: usize) -> Result<Option<AlignedBuf>> {
    match self.inner.device.read_range(offset, len).await {
      Ok(buf) => Ok(Some(buf)),
      Err(DeviceError::SegmentNotFound(_))
        if offset < self.inner.begin_address.load(Ordering::Acquire) =>
      {
        Ok(None)
      }
      Err(e) => Err(e.into()),
    }
  }

  /// 从磁盘读取单条记录（含 64KB 滑动窗口预读缓存）
  ///
  /// `whole_frame`：true 返回含 8B 头的完整帧缓冲（复制推流直传面，零二次重拷），
  /// false 仅返回校验后的负载；帧头两路统一返回，帧路调用方弃之
  async fn read_from_device(
    &mut self,
    record_addr: u64,
    available_end: u64,
    whole_frame: bool,
  ) -> Result<Option<(WalFrameHeader, Vec<u8>, u64)>> {
    // 预读不得越过 flushed 边界：已落盘数据长度仅保证覆盖到 align_up(flushed)，
    // 越界预读在存在未提交内存尾部时可能触发 UnexpectedEof 使合法记录读取失败
    let limit = available_end.min(self.inner.flushed_until_address.load(Ordering::Acquire));
    if record_addr + (RECORD_HEADER_LEN as u64) > limit {
      return Ok(None);
    }

    // 缓存的磁盘块未能覆盖记录头时重新拉取
    if !self.disk_win.covers(record_addr, RECORD_HEADER_LEN) {
      let fetch_len = ((limit - record_addr) as usize).clamp(RECORD_HEADER_LEN, RECOVER_CHUNK_SIZE);
      let Some(buf) = self.fetch_window(record_addr, fetch_len).await? else {
        return Ok(None);
      };
      self.disk_win.replace(record_addr, buf);
    }

    let rel_off = (record_addr - self.disk_win.offset()) as usize;
    let Some(header) = WalFrameHeader::decode_opt(&self.disk_win.slice()[rel_off..]) else {
      return Ok(None);
    };
    let entry_len = header.payload_len();
    let next_addr = record_addr + (RECORD_HEADER_LEN as u64) + (entry_len as u64);
    if next_addr > limit {
      return Ok(None);
    }

    let total_rec_len = RECORD_HEADER_LEN + entry_len;
    // 负载完整位于缓存块内则零 I/O 直取（窗内偏移 + 头长 + 负载长的判据单点在
    // DiskWindow::payload），越窗则拉取覆盖完整记录的窗口
    let data = if let Some(p) = self.disk_win.payload(record_addr, &header, rel_off) {
      header.verify(p)?;
      if whole_frame {
        self.disk_win.slice()[rel_off..rel_off + total_rec_len].to_vec()
      } else {
        p.to_vec()
      }
    } else {
      let fetch_len = ((limit - record_addr) as usize)
        .clamp(total_rec_len, total_rec_len.max(RECOVER_CHUNK_SIZE));
      let Some(full_buf) = self.fetch_window(record_addr, fetch_len).await? else {
        return Ok(None);
      };
      let slice = full_buf.as_slice();
      let Some(p) = slice.get(RECORD_HEADER_LEN..total_rec_len) else {
        return Ok(None);
      };
      header.verify(p)?;
      let out = if whole_frame {
        slice[..total_rec_len].to_vec()
      } else {
        p.to_vec()
      };
      self.disk_win.replace(record_addr, full_buf);
      out
    };

    Ok(Some((header, data, next_addr)))
  }
}
