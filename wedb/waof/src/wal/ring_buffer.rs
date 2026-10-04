use std::{
  alloc::{Layout, alloc_zeroed, dealloc},
  ptr::{NonNull, copy_nonoverlapping, read_unaligned},
};

use wbase::{
  align::{MIN_SECTOR_SIZE, is_valid_sector_size},
  error::Error as WbaseError,
};

use super::header::{RECORD_HEADER_LEN, WalFrameHeader};
use crate::error::{Error, Result};

/// 环形内存单帧解码判定码
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:Scan
///
/// 对标 C# Scan 单趟解码语义：读定长头 → 记录长度守卫（零长填充/非法长度）→
/// 按 recordSize 推进游标 → 校验和验证。守卫谓词与 next_addr 推进单源化于此，
/// 环形内存两处扫描入口（[`super::log::WalLog::scan_memory_records`] 的回调
/// break 终止、[`super::iterator::WalScanIterator`] 的落盘回退）只保留各自
/// 终止语义，不再各自复刻解码步骤
pub(crate) enum MemFrame {
  /// 合法帧：帧头、负载与帧尾地址（下一条记录起始）
  Valid {
    header: WalFrameHeader,
    payload: Vec<u8>,
    next_addr: u64,
  },
  /// 负载 CRC 校验失败：环形覆写残迹或内存数据损坏（携带校验错误原值，
  /// 供调用方按位点状态决定上抛或回退）
  CrcMismatch(Error),
  /// 帧头守卫未过：全零头（扇区填充/崩溃残缺尾，合法记录头绝不全零）、
  /// 负载长度超限（伪头）或帧尾越出可读上界（记录未完整写入/越过调用方
  /// 快照边界）
  Invalid,
}

/// 环形内存单帧直组解码判定码（8B 头 + 负载一次性分配直组）
pub(crate) enum MemFrameAssembled {
  /// 合法帧：完整帧数据（8B 记录头 + 负载）与帧尾地址
  Valid { frame: Vec<u8>, next_addr: u64 },
  /// 负载 CRC 校验失败
  CrcMismatch(Error),
  /// 帧头守卫未过
  Invalid,
}

/// 内存环形写缓冲区
pub struct RingBuffer {
  ptr: NonNull<u8>,
  capacity: usize,
  layout: Layout,
  mask: u64,
}

// SAFETY: 结构独占持有 `new` 中 `alloc_zeroed` 分配的整块堆内存，ptr/capacity/layout/mask 自构造后不再
// 变更，成员全为裸值、不含 `Rc`/`Cell`/thread-local 等非线程安全状态，跨线程移交即移交唯一所有权。
unsafe impl Send for RingBuffer {}
// SAFETY: 方法全为 `&self` 下的界内字节搬运（不改结构字段、不构造越界或悬垂引用），类型自身不含非线程
// 安全状态，故共享引用下的内存安全成立；同一地址区间的读写竞态由上层 WalLog 的水位契约排除——写侧只落在
// `tail_address` 预留的新区域，读侧只在 `safe_tail_address`/`committed_until_address` 以下取样并串行于 `commit_lock`
unsafe impl Sync for RingBuffer {}

/// 帧头守卫与 next_addr 推进单点（decode_frame / decode_frame_assembled 两趟
/// 解码守卫谓词同源，对标 C# TsavoriteLog.cs:Scan 的 GetLength 后零长/非法长度
/// 守卫）：全零头（填充/崩溃残缺尾）或负载超限伪头，或帧尾越过 `read_limit`
/// 一律 `None`；合法返回 `(帧尾 next_addr, 负载长度)`
#[inline]
fn frame_next_addr(
  header: &WalFrameHeader,
  addr: u64,
  read_limit: u64,
  capacity: usize,
) -> Option<(u64, usize)> {
  let entry_len = header.payload_len();
  if header.is_zero() || entry_len > capacity {
    return None;
  }
  let next_addr = addr + (RECORD_HEADER_LEN as u64) + (entry_len as u64);
  (next_addr <= read_limit).then_some((next_addr, entry_len))
}

impl RingBuffer {
  /// 创建指定容量与对齐大小的环形缓冲区
  pub fn new(capacity: usize, align: usize) -> Result<Self> {
    if !is_valid_sector_size(align) {
      return Err(WbaseError::InvalidAlignment(align, MIN_SECTOR_SIZE).into());
    }
    if capacity == 0 || !capacity.is_multiple_of(align) {
      return Err(Error::Mem(WbaseError::InvalidAlignment(capacity, align)));
    }
    // capacity/align 均已前置校验（align 合法扇区、capacity 为 align 整数倍），
    // from_size_align 失败面仅剩上取整溢出 → Overflow
    let layout = Layout::from_size_align(capacity, align).map_err(|_| WbaseError::Overflow)?;
    // SAFETY: capacity 非零且为 align 的整数倍、align 经 `is_valid_sector_size` 校验，layout 尺寸与对齐均合法
    let raw = unsafe { alloc_zeroed(layout) };
    let ptr = NonNull::new(raw).ok_or(WbaseError::AllocFailed(layout))?;
    let mask = if capacity.is_power_of_two() {
      (capacity - 1) as u64
    } else {
      0
    };
    Ok(Self {
      ptr,
      capacity,
      layout,
      mask,
    })
  }

  /// 获取缓冲区总容量
  #[inline]
  pub fn capacity(&self) -> usize {
    self.capacity
  }

  /// 计算逻辑偏移在环形缓冲区内的物理偏移（当容量为 2 的幂时走位运算快路径，免除 64 位除法取模）
  #[inline(always)]
  fn ring_offset(&self, logical_offset: u64) -> usize {
    if self.mask != 0 {
      (logical_offset & self.mask) as usize
    } else {
      (logical_offset % (self.capacity as u64)) as usize
    }
  }

  /// 读取 8 字节定长记录头（快速路径直接单次 64 位无拷贝加载）
  #[inline]
  fn read_header(&self, logical_offset: u64) -> WalFrameHeader {
    let cap = self.capacity;
    let ring_off = self.ring_offset(logical_offset);
    let raw = self.ptr.as_ptr();
    if ring_off + RECORD_HEADER_LEN <= cap {
      // SAFETY: `ring_off + RECORD_HEADER_LEN <= cap` 已判定，读取区间严格落在分配的 cap 字节内；
      // `read_unaligned` 对 `[u8; RECORD_HEADER_LEN]` 无对齐要求且按值返回，不借用底层内存
      let bytes = unsafe { read_unaligned(raw.add(ring_off) as *const [u8; RECORD_HEADER_LEN]) };
      WalFrameHeader::from_bytes(&bytes)
    } else {
      let mut bytes = [0u8; RECORD_HEADER_LEN];
      self.read_bytes(logical_offset, &mut bytes);
      WalFrameHeader::from_bytes(&bytes)
    }
  }

  /// 单趟解码环形内存中 addr 处的单帧（对标 C#
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:Scan 的
  /// 单次遍历解码：读定长头 → 长度守卫 → 推进游标 → 校验和验证）
  ///
  /// - `addr`：帧头起始逻辑地址；
  /// - `read_limit`：本帧可读上界，帧尾 next_addr 越界即判 [`MemFrame::Invalid`]；
  /// - 负载长度守卫取缓冲区自身容量（构造时即 `config.buffer_size`，单一真源）。
  ///
  /// 守卫谓词与 next_addr 推进在此单源化，调用方只消费判定码落实各自终止语义
  #[inline]
  pub(crate) fn decode_frame(&self, addr: u64, read_limit: u64) -> MemFrame {
    let header = self.read_header(addr);
    let Some((next_addr, entry_len)) = frame_next_addr(&header, addr, read_limit, self.capacity)
    else {
      return MemFrame::Invalid;
    };
    let payload = self.read_vec(addr + RECORD_HEADER_LEN as u64, entry_len);
    if let Err(e) = header.verify(&payload) {
      return MemFrame::CrcMismatch(e);
    }
    MemFrame::Valid {
      header,
      payload,
      next_addr,
    }
  }

  /// 环形→线性拷贝单点：自 `logical_offset` 起读 `len` 字节到连续目的指针 `dest`，
  /// 回绕边界自动拆两段（`[ring_off, cap)` + `[0, part2)`）。
  ///
  /// # Safety
  /// - `len <= self.capacity`（构造与守卫已保证单帧不超容量）；
  /// - `dest` 对 `[dest, dest + len)` 有效且与环形缓冲不重叠。
  #[inline]
  unsafe fn copy_ring_to(&self, logical_offset: u64, dest: *mut u8, len: usize) {
    let cap = self.capacity;
    let ring_off = self.ring_offset(logical_offset);
    let raw = self.ptr.as_ptr();
    if ring_off + len <= cap {
      // SAFETY: 调用方保证 len <= capacity 且 ring_off + len <= cap，源区间界内；dest 界内由 Safety 前置
      unsafe { copy_nonoverlapping(raw.add(ring_off), dest, len) };
    } else {
      let part1 = cap - ring_off;
      let part2 = len - part1;
      // SAFETY: 回绕两段源 [ring_off, cap) 与 [0, part2) 均界内且相加恰为 len；dest 界内由 Safety 前置
      unsafe {
        copy_nonoverlapping(raw.add(ring_off), dest, part1);
        copy_nonoverlapping(raw, dest.add(part1), part2);
      }
    }
  }

  /// 线性→环形拷贝单点：把连续源指针 `src` 的 `len` 字节写入自 `logical_offset`
  /// 起的环形区，回绕边界自动拆两段。[`Self::copy_ring_to`] 的写向对偶。
  ///
  /// # Safety
  /// - `len <= self.capacity`；
  /// - `src` 对 `[src, src + len)` 有效且与环形缓冲不重叠。
  #[inline]
  unsafe fn copy_to_ring(&self, logical_offset: u64, src: *const u8, len: usize) {
    let cap = self.capacity;
    let ring_off = self.ring_offset(logical_offset);
    let raw = self.ptr.as_ptr();
    if ring_off + len <= cap {
      // SAFETY: len <= capacity 且 ring_off + len <= cap，目的区间界内；src 界内由 Safety 前置
      unsafe { copy_nonoverlapping(src, raw.add(ring_off), len) };
    } else {
      let part1 = cap - ring_off;
      let part2 = len - part1;
      // SAFETY: 回绕两段目的 [ring_off, cap) 与 [0, part2) 均界内且相加恰为 len；src 界内由 Safety 前置
      unsafe {
        copy_nonoverlapping(src, raw.add(ring_off), part1);
        copy_nonoverlapping(src.add(part1), raw, part2);
      }
    }
  }

  /// 从指定逻辑地址读取数据并分配为 `Vec<u8>`（避免先零填充再拷贝）
  #[inline]
  fn read_vec(&self, logical_offset: u64, len: usize) -> Vec<u8> {
    if len == 0 {
      return Vec::new();
    }
    debug_assert!(len <= self.capacity);
    let mut vec = Vec::with_capacity(len);
    let dest = vec.as_mut_ptr();
    // SAFETY: len <= capacity，dest 为 with_capacity(len) 的未初始化尾部，拷贝恰好写满 len 字节后才 set_len，
    // 不暴露未初始化内存；dest 与环形缓冲不重叠
    unsafe {
      self.copy_ring_to(logical_offset, dest, len);
      vec.set_len(len);
    }
    vec
  }

  /// 从指定逻辑地址一次性分配 8B 记录头 + 负载的完整帧缓冲并直接拷贝，消除二次分配
  #[inline]
  fn read_frame_vec(
    &self,
    header: &WalFrameHeader,
    payload_offset: u64,
    payload_len: usize,
  ) -> Vec<u8> {
    let total_len = RECORD_HEADER_LEN + payload_len;
    debug_assert!(payload_len <= self.capacity);
    let mut vec = Vec::with_capacity(total_len);
    let dest = vec.as_mut_ptr();
    // SAFETY: total_len = RECORD_HEADER_LEN + payload_len，vec 分配了对应容量；dest 前 8 字节就地写入
    // header 序列化字节；payload_dest 偏移 RECORD_HEADER_LEN 经 copy_ring_to 单次/分两段回绕拷贝；
    // 拷贝完毕后 set_len(total_len)，不暴露未初始化内存
    unsafe {
      let hdr_bytes = header.to_bytes();
      copy_nonoverlapping(hdr_bytes.as_ptr(), dest, RECORD_HEADER_LEN);
      self.copy_ring_to(payload_offset, dest.add(RECORD_HEADER_LEN), payload_len);
      vec.set_len(total_len);
    }
    vec
  }

  /// 单趟解码环形内存中 addr 处的完整帧（对标 C# Scan 单趟解码：头守卫校验通过后
  /// 一次分配头+负载缓冲，消除先拆解再拼帧的二次分配重拷）
  #[inline]
  pub(crate) fn decode_frame_assembled(&self, addr: u64, read_limit: u64) -> MemFrameAssembled {
    let header = self.read_header(addr);
    let Some((next_addr, entry_len)) = frame_next_addr(&header, addr, read_limit, self.capacity)
    else {
      return MemFrameAssembled::Invalid;
    };
    let frame = self.read_frame_vec(&header, addr + RECORD_HEADER_LEN as u64, entry_len);
    if let Err(e) = header.verify(&frame[RECORD_HEADER_LEN..]) {
      return MemFrameAssembled::CrcMismatch(e);
    }
    MemFrameAssembled::Valid { frame, next_addr }
  }

  /// 分部件写入完整 WAL 记录（头 + 逐负载部件，scatter-write 零整包拼接）：
  /// 非回绕边界单次寻址顺序拷贝各部件，回绕边界逐部件交给 [`Self::write_bytes`] 自动回绕。
  /// 产出页与整包顺序写入逐字节一致
  #[inline]
  pub fn write_record_parts(
    &self,
    logical_offset: u64,
    header: &[u8; RECORD_HEADER_LEN],
    parts: &[&[u8]],
  ) {
    let payload_len: usize = parts.iter().map(|part| part.len()).sum();
    let total_len = RECORD_HEADER_LEN + payload_len;
    debug_assert!(total_len <= self.capacity);
    let cap = self.capacity;
    let ring_off = self.ring_offset(logical_offset);
    let raw = self.ptr.as_ptr();
    if ring_off + total_len <= cap {
      // SAFETY: total_len <= capacity 且 ring_off + total_len <= cap，头 + 各非空部件依声明长度顺序写入，
      // 写入终点恰为 ring_off + total_len 不越分配界；部件指针即其 `&[u8]` 自身界内
      unsafe {
        let mut dest = raw.add(ring_off);
        copy_nonoverlapping(header.as_ptr(), dest, RECORD_HEADER_LEN);
        dest = dest.add(RECORD_HEADER_LEN);
        for part in parts {
          if part.is_empty() {
            continue;
          }
          copy_nonoverlapping(part.as_ptr(), dest, part.len());
          dest = dest.add(part.len());
        }
      }
    } else {
      self.write_bytes(logical_offset, header);
      let mut offset = logical_offset + RECORD_HEADER_LEN as u64;
      for part in parts {
        if part.is_empty() {
          continue;
        }
        self.write_bytes(offset, part);
        offset += part.len() as u64;
      }
    }
  }

  /// 向指定逻辑地址写入切片数据（支持自动环形回绕）
  #[inline]
  pub fn write_bytes(&self, logical_offset: u64, data: &[u8]) {
    let len = data.len();
    if len == 0 {
      return;
    }
    debug_assert!(len <= self.capacity);
    // SAFETY: len <= capacity，data 与环形缓冲不重叠（data 属调用方、环形属本结构独占堆块）
    unsafe { self.copy_to_ring(logical_offset, data.as_ptr(), len) };
  }

  /// 从指定逻辑地址读取数据到切片中（支持自动环形回绕）
  #[inline]
  pub fn read_bytes(&self, logical_offset: u64, dest: &mut [u8]) {
    let len = dest.len();
    if len == 0 {
      return;
    }
    debug_assert!(len <= self.capacity);
    // SAFETY: len <= capacity，dest 与环形缓冲不重叠且 dest.as_mut_ptr() 对 [0, len) 界内
    unsafe { self.copy_ring_to(logical_offset, dest.as_mut_ptr(), len) };
  }
}

impl Drop for RingBuffer {
  fn drop(&mut self) {
    // SAFETY: capacity 恒 > 0（`new` 已拒零容量），ptr 与 layout 同 `alloc_zeroed` 时一致且构造后未变；
    // Drop 由 `&mut self` 独占、每实例仅一次，释放后无残留引用
    unsafe {
      dealloc(self.ptr.as_ptr(), self.layout);
    }
  }
}
