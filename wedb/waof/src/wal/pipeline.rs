//! WAL 入队流水线（对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:Enqueue
//! 族）：预校验 → 注册在途槽位 → CAS 预占地址 →
//! 分部件写环形缓冲 → 释放槽位 → 推流唤醒。

use std::{iter::once, sync::atomic::Ordering};

use wbase::{align::sector_bounds, backoff::Backoff, thread::current_thread_id};
use wdev::Device;

use super::{
  header::{RECORD_HEADER_LEN, WalFrameHeader},
  log::WalLog,
};
use crate::error::{Error, Result};

impl<D: Device> WalLog<D> {
  /// 将数据追加到 WAL 内存缓冲区，返回起始逻辑地址（支持多线程并发无锁预占地址）
  #[inline]
  pub fn enqueue(&self, payload: &[u8]) -> Result<u64> {
    self.enqueue_parts(&[payload])
  }

  /// 将多个负载部件按序追加为单条 WAL 记录（scatter-write，零整包拼接），返回起始逻辑地址
  ///
  /// 产出的记录帧与 [`Self::enqueue`] 预拼整包后写入逐字节一致（记录头经
  /// `WalFrameHeader::for_payload_parts` 分段累加，CRC32 线性等价），调用方免去
  /// 预拼整包 Vec 的整量拷贝；恢复侧扫描路径零改动
  pub fn enqueue_parts(&self, parts: &[&[u8]]) -> Result<u64> {
    // u64 口径计算记录总长，规避 32 位平台上 +RECORD_HEADER_LEN 的 usize 溢出
    let payload_len: u64 = parts.iter().map(|part| part.len() as u64).sum();
    self.check_record_len(RECORD_HEADER_LEN as u64 + payload_len)?;

    // 预先计算记录头与 CRC32，避免在持有在途槽位期间耗费 CPU 算力拖慢并发提交
    let header = WalFrameHeader::for_payload_parts(parts);
    self.enqueue_reserved(
      RECORD_HEADER_LEN as u64 + payload_len,
      once((header, parts)),
    )
  }

  /// 原样写入完整记录帧（8 字节记录头 + 负载），返回起始逻辑地址
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:UnsafeTryEnqueueRaw：不重算记录头，帧字节逐字进日志。服务于
  /// 复制从节点对主节点记录的保真落盘——只要主从帧序列一致且起始位点一致，
  /// 预占地址序列即逐条一致；调用方须以返回地址校验主从位点同步。
  /// 帧头须自洽（非全零且 entry_len 与负载长度一致），错位帧会使恢复链
  /// 在后续记录处校验失败而截断，故入口即拒
  pub fn enqueue_raw(&self, frame: &[u8]) -> Result<u64> {
    let Some((header_bytes, payload)) = frame.split_first_chunk::<RECORD_HEADER_LEN>() else {
      return Err(Error::InvalidRecordHeader);
    };
    let parsed = WalFrameHeader::from_bytes(header_bytes);
    if parsed.is_zero() || parsed.entry_len as usize != payload.len() {
      return Err(Error::InvalidRecordHeader);
    }
    self.check_record_len(frame.len() as u64)?;
    self.enqueue_reserved(frame.len() as u64, once((parsed, &[payload][..])))
  }

  /// 单次预留连续落盘多条帧（分块记录原子入队），返回首帧起始逻辑地址。
  ///
  /// 全部帧一次地址预留后依序写入，帧间地址连续，绝不与并发写入者插花
  ///（对标 C# libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.Chunked.cs:EnqueueChunkedSpan
  /// 的 BeginInflightEnqueue 单在途操作）。
  ///
  /// 刻意差异：C# 段间关联靠 AofChunkHeader.objectId（读取器按 objectId
  /// 聚合）；本实现分块布局续帧为纯数据（无帧头复现），正确性完全依赖
  /// 帧地址连续，故必须经本 API 一次落盘。总预留须完整落入环形窗口，
  /// 超出即 RecordTooLarge 整体拒绝（单帧路径受同一窗口约束）
  pub fn enqueue_frames(&self, frames: &[&[&[u8]]]) -> Result<u64> {
    // 一遍预扫：单帧与总长校验统一收敛 check_record_len 单点判定，帧长以
    // u64 口径一次算定（规避 32 位平台 usize 溢出），沿链传递免重复计算
    let mut total_len = 0u64;
    for parts in frames {
      let payload_len: u64 = parts.iter().map(|part| part.len() as u64).sum();
      let frame_len = RECORD_HEADER_LEN as u64 + payload_len;
      self.check_record_len(frame_len)?;
      total_len += frame_len;
    }
    self.check_record_len(total_len)?;
    // 帧头随写入循环按帧就地编码（对标 C# TsavoriteLog.Chunked.cs:
    // EnqueueChunkedSpan 的 ChunkHeaderWriter 写时编码），免整段 headers
    // Vec 的每批堆分配
    self.enqueue_reserved(
      total_len,
      frames
        .iter()
        .map(|parts| (WalFrameHeader::for_payload_parts(parts), *parts)),
    )
  }

  /// 单条记录总长上限：负载以 u32 编码长度，且须完整落入环形缓冲区
  ///
  /// 单帧与分块多帧路径共用的单点校验
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

  /// 帧序列入队核心：注册在途槽位 → CAS 一次预占 total_len 连续地址 →
  /// 依序写入全部帧 → 释放槽位 → 推流唤醒（单点骨架，单帧与分块多帧共用）
  ///
  /// 不变式：槽位值全程维护为当前预占下界（reserve_address 内部维护），
  /// safe_tail 永不越过本写入者尚未完成的首帧起点
  fn enqueue_reserved<'p, I>(&self, total_len: u64, frames: I) -> Result<u64>
  where
    I: IntoIterator<Item = (WalFrameHeader, &'p [&'p [u8]])>,
  {
    // 1. 注册在途槽位（发布下界，防止 commit 提前刷盘未就绪内存）
    let (slot_idx, current_tail) = self.acquire_inflight_slot();

    // 2. CAS 预占逻辑地址范围（槽位值由 reserve_address 全程维护为当前预占下界；
    //    BufferFull 失败路径由 reserve_address 内部释放槽位）
    let reserved_addr = self.reserve_address(total_len, current_tail, slot_idx)?;

    // 3. 依序写入全部帧（帧间地址连续；此刻槽位值 == reserved_addr（CAS
    //    成功路径中尾地址未再变化），故 safe_tail 至多覆盖到 reserved_addr，
    //    绝不越过尚未写入的首帧）；帧头按帧就地编码，帧长直取记录头
    //    entry_len，免沿链重复累加
    let mut addr = reserved_addr;
    for (header, parts) in frames {
      let frame_len = RECORD_HEADER_LEN as u64 + header.payload_len() as u64;
      self
        .ring_buffer
        .write_record_parts(addr, &header.to_bytes(), parts);
      addr += frame_len;
    }
    debug_assert_eq!(
      addr,
      reserved_addr + total_len,
      "帧序列实际写入长度须与预留总长一致"
    );

    // 4. 释放当前在途槽位（标记为已完成写入）——先于推流唤醒：thread-per-core
    //    多核下 pump 可在信号抵达的下一刻于另一核运行，若信号在先，其读到的
    //    safe_tail 下界仍折掉本写入者槽位，扫描扑空后回 recv 深度挂起，而该帧
    //    信号已被消费、槽位释放后不再有信号，末批记录写入静默后无限期滞留。
    //    槽位 Release 存储先于 try_send，经信号通道同步边保证 pump 被唤醒读
    //    safe_tail 时本记录必然完整入面
    unsafe { self.inflight_slots.get_unchecked(slot_idx) }.store(u64::MAX, Ordering::Release);

    // 5. 推流唤醒信号：全部帧已写入环形缓冲且在途槽位已释放（safe_tail 语义
    //    保证信号被消费时记录必然完整可读），容量 1 折叠去重——推流端被唤醒后
    //    按地址序拉取，推流序与 AOF 地址序原子一致。折叠不丢推进：任一写入者
    //    释放槽位后必尝试发信号，被折叠时通道已有待处理信号，其对应扫描发生的
    //    时刻只会更晚，safe_tail 重读必然覆盖此前全部已释放记录
    if let Some(tx) = self.replication_wake.get() {
      let _ = tx.try_send(());
    }

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
      // 槽位让出为纯内存事件，阶段动作转调 wbase::backoff 真源的忙等面
      // （Spin 自旋、其后让核，Sleep 深睡钳制为让核，绝不阻塞 reactor 线程）
      backoff.stage().wait_busy();
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
      let required_end = current_tail.saturating_add(record_len);
      // 扇区界圆整唯一实现点为 wbase::align::sector_bounds（刷盘内核与窗口预算共用）
      let start_aligned = sector_bounds(flushed, required_end, sector_size).0;
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
}
