use std::sync::{Arc, atomic::Ordering};

use wdev::{Device, Error as DeviceError};

use super::{
  disk_window::DiskWindow,
  error::Result,
  header::{RECORD_HEADER_LEN, RecordHeader},
  log::{RECOVER_CHUNK_SIZE, WalLogInner},
  record::WalRecord,
};

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

  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogScanIterator.cs:Ended
  ///
  /// 检查迭代是否已到达末尾
  #[inline]
  pub fn is_ended(&self) -> bool {
    self.cur_address >= self.end_address
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

  /// 异步顺序获取下一条 WAL 记录
  pub async fn next(&mut self) -> Result<Option<WalRecord>> {
    if self.cur_address >= self.end_address {
      return Ok(None);
    }

    // 迭代中并发截断推进 begin 位点时平滑跳跃（对齐 C# ScanBehindBeginAddress 语义）
    let begin_addr = self.inner.begin_address.load(Ordering::Acquire);
    if self.cur_address < begin_addr {
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
      return Ok(None);
    }

    let record_addr = self.cur_address;
    let Some((header, payload)) = self
      .read_record(record_addr, available_end, mem_base)
      .await?
    else {
      return Ok(None);
    };

    self.cur_address = record_addr + (RECORD_HEADER_LEN as u64) + (header.payload_len() as u64);
    Ok(Some(WalRecord {
      address: record_addr,
      next_address: self.cur_address,
      header,
      payload,
    }))
  }

  /// 读取单条记录：优先内存环形缓冲直读，失败时按位点状态回退磁盘权威数据
  async fn read_record(
    &mut self,
    record_addr: u64,
    available_end: u64,
    mem_base: u64,
  ) -> Result<Option<(RecordHeader, Vec<u8>)>> {
    let cap = self.inner.ring_buffer.capacity() as u64;
    // 内存路径的回退判定不以快照为准——快照可能落后于并发 commit，
    // 已提交落盘的记录其内存槽位随时可能被生产者复用覆写，判定时须现场重读位点
    if record_addr >= mem_base.saturating_sub(cap) {
      let header = self.inner.ring_buffer.read_header(record_addr);
      let entry_len = header.payload_len();
      let next_addr = record_addr + (RECORD_HEADER_LEN as u64) + (entry_len as u64);

      if entry_len > self.inner.config.buffer_size || next_addr > available_end {
        // 头部长度异常（伪头/覆写残迹）或记录超出快照边界：
        // 已落盘则回退磁盘权威数据，否则视为当前不可得，平滑终止
        if record_addr < self.inner.flushed_until_address.load(Ordering::Acquire) {
          return self
            .read_record_from_device(record_addr, available_end)
            .await;
        }
        return Ok(None);
      }

      let payload = self
        .inner
        .ring_buffer
        .read_vec(record_addr + RECORD_HEADER_LEN as u64, entry_len);
      match header.verify(&payload) {
        Ok(()) => return Ok(Some((header, payload))),
        Err(e) => {
          if record_addr >= self.inner.flushed_until_address.load(Ordering::Acquire) {
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
            return Err(e);
          }
          // 已落盘则继续 fall through 回退磁盘权威数据（含并发提交刚完成的情形）
        }
      }
    }
    self
      .read_record_from_device(record_addr, available_end)
      .await
  }

  /// 从磁盘读取单条记录（含 64KB 滑动窗口预读缓存）
  async fn read_record_from_device(
    &mut self,
    record_addr: u64,
    available_end: u64,
  ) -> Result<Option<(RecordHeader, Vec<u8>)>> {
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
    let Some(header) = RecordHeader::decode_opt(&self.disk_win.slice()[rel_off..]) else {
      return Ok(None);
    };
    let entry_len = header.payload_len();
    let next_addr = record_addr + (RECORD_HEADER_LEN as u64) + (entry_len as u64);
    if next_addr > limit {
      return Ok(None);
    }

    let total_rec_len = RECORD_HEADER_LEN + entry_len;
    // 负载完整位于缓存块内则零 I/O 直取，否则拉取覆盖完整记录的窗口
    let payload = if rel_off + total_rec_len <= self.disk_win.slice().len() {
      let p = unsafe {
        self
          .disk_win
          .slice()
          .get_unchecked(rel_off + RECORD_HEADER_LEN..rel_off + total_rec_len)
      };
      header.verify(p)?;
      p.to_vec()
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
      let payload_vec = p.to_vec();
      self.disk_win.replace(record_addr, full_buf);
      payload_vec
    };

    Ok(Some((header, payload)))
  }

  /// 拉取磁盘窗口数据；迭代期间段文件被并发物理删除且 begin 位点已越过本记录时
  /// 返回 None（平滑终止，对齐 C# 扫描器遇截断不抛错的语义）
  async fn fetch_window(&self, offset: u64, len: usize) -> Result<Option<wram::AlignedBuf>> {
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

  /// 一次性收集剩余的所有记录
  pub async fn collect_all(&mut self) -> Result<Vec<WalRecord>> {
    let mut records = Vec::new();
    while let Some(rec) = self.next().await? {
      records.push(rec);
    }
    Ok(records)
  }
}
