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

impl RingBuffer {
  /// 创建指定容量与对齐大小的环形缓冲区
  pub fn new(capacity: usize, align: usize) -> Result<Self> {
    if !is_valid_sector_size(align) {
      return Err(WbaseError::InvalidAlignment(align, MIN_SECTOR_SIZE).into());
    }
    if capacity == 0 || !capacity.is_multiple_of(align) {
      return Err(Error::Mem(WbaseError::InvalidAlignment(capacity, align)));
    }
    let layout = Layout::from_size_align(capacity, align).map_err(WbaseError::from)?;
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
  pub fn ring_offset(&self, logical_offset: u64) -> usize {
    if self.mask != 0 {
      (logical_offset & self.mask) as usize
    } else {
      (logical_offset % (self.capacity as u64)) as usize
    }
  }

  /// 读取 8 字节定长记录头（快速路径直接单次 64 位无拷贝加载）
  #[inline]
  pub fn read_header(&self, logical_offset: u64) -> WalFrameHeader {
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

  /// 从指定逻辑地址读取数据并分配为 `Vec<u8>`（避免先零填充再拷贝）
  #[inline]
  pub fn read_vec(&self, logical_offset: u64, len: usize) -> Vec<u8> {
    if len == 0 {
      return Vec::new();
    }
    debug_assert!(len <= self.capacity);
    let mut vec = Vec::with_capacity(len);
    let cap = self.capacity;
    let ring_off = self.ring_offset(logical_offset);
    let raw = self.ptr.as_ptr();
    let dest = vec.as_mut_ptr();
    if ring_off + len <= cap {
      // SAFETY: len <= capacity 且 ring_off + len <= cap，源区间 [ring_off, ring_off + len) 界内；目的为
      // `with_capacity(len)` 的未初始化尾部，恰好写满 len 字节后才 set_len，不暴露未初始化内存
      unsafe {
        copy_nonoverlapping(raw.add(ring_off), dest, len);
        vec.set_len(len);
      }
    } else {
      let part1 = cap - ring_off;
      let part2 = len - part1;
      // SAFETY: 回绕分支 part1 = cap - ring_off、part2 = len - part1，两段源区间 [ring_off, cap) 与 [0, part2)
      // 均在分配界内且相加恰为 len；目的 Vec 容量 len，两段写满后才 set_len
      unsafe {
        copy_nonoverlapping(raw.add(ring_off), dest, part1);
        copy_nonoverlapping(raw, dest.add(part1), part2);
        vec.set_len(len);
      }
    }
    vec
  }

  /// 高性能写入完整 WAL 记录（头 + 负载）：针对 99.9% 的非回绕边界场景走单次寻址与单次连续拷贝
  #[inline]
  pub fn write_record(
    &self,
    logical_offset: u64,
    header: &[u8; RECORD_HEADER_LEN],
    payload: &[u8],
  ) {
    self.write_record_parts(logical_offset, header, &[payload]);
  }

  /// 分部件写入完整 WAL 记录（头 + 逐负载部件，scatter-write 零整包拼接）：
  /// 非回绕边界单次寻址顺序拷贝各部件，回绕边界逐部件交给 [`Self::write_bytes`] 自动回绕。
  /// 产出页与 [`Self::write_record`] 整包写入逐字节一致
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
    let cap = self.capacity;
    let ring_off = self.ring_offset(logical_offset);
    let raw = self.ptr.as_ptr();
    if ring_off + len <= cap {
      // SAFETY: len <= capacity 且 ring_off + len <= cap，源切片与目的区间 [ring_off, ring_off + len) 均界内，
      // 源属调用方、目的属本结构独占堆块，二者不重叠
      unsafe {
        copy_nonoverlapping(data.as_ptr(), raw.add(ring_off), len);
      }
    } else {
      let part1 = cap - ring_off;
      let part2 = len - part1;
      // SAFETY: 回绕分支 part1 = cap - ring_off、part2 = len - part1 各自界内（[ring_off, cap) 与 [0, part2)），
      // 源切片以同一切点分片，两段合计恰为 len
      unsafe {
        copy_nonoverlapping(data.as_ptr(), raw.add(ring_off), part1);
        copy_nonoverlapping(data.as_ptr().add(part1), raw, part2);
      }
    }
  }

  /// 从指定逻辑地址读取数据到切片中（支持自动环形回绕）
  #[inline]
  pub fn read_bytes(&self, logical_offset: u64, dest: &mut [u8]) {
    let len = dest.len();
    if len == 0 {
      return;
    }
    debug_assert!(len <= self.capacity);
    let cap = self.capacity;
    let ring_off = self.ring_offset(logical_offset);
    let raw = self.ptr.as_ptr();
    if ring_off + len <= cap {
      // SAFETY: len <= capacity 且 ring_off + len <= cap，源区间与目的 dest[..len] 均严格界内且不重叠
      unsafe {
        copy_nonoverlapping(raw.add(ring_off), dest.as_mut_ptr(), len);
      }
    } else {
      let part1 = cap - ring_off;
      let part2 = len - part1;
      // SAFETY: 回绕分支两段源 [ring_off, cap) 与 [0, part2) 界内，目的以同一切点分片，合计恰为 dest 全长
      unsafe {
        copy_nonoverlapping(raw.add(ring_off), dest.as_mut_ptr(), part1);
        copy_nonoverlapping(raw, dest.as_mut_ptr().add(part1), part2);
      }
    }
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
