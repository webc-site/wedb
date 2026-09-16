use std::{
  alloc::{Layout, alloc_zeroed, dealloc},
  ptr::{NonNull, copy_nonoverlapping, read_unaligned},
  sync::Arc,
};

use wbase::{
  align::{MIN_SECTOR_SIZE, is_valid_sector_size},
  error::Error as WbaseError,
  pool::{AlignedBuf, BufferPool},
};

use super::header::{RECORD_HEADER_LEN, RecordHeader};
use crate::error::{Error, Result};

/// 内存环形写缓冲区
pub struct RingBuffer {
  ptr: NonNull<u8>,
  capacity: usize,
  layout: Layout,
  mask: u64,
}

unsafe impl Send for RingBuffer {}
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
  pub fn read_header(&self, logical_offset: u64) -> RecordHeader {
    let cap = self.capacity;
    let ring_off = self.ring_offset(logical_offset);
    let raw = self.ptr.as_ptr();
    if ring_off + RECORD_HEADER_LEN <= cap {
      let bytes = unsafe { read_unaligned(raw.add(ring_off) as *const [u8; RECORD_HEADER_LEN]) };
      RecordHeader::from_bytes(&bytes)
    } else {
      let mut bytes = [0u8; RECORD_HEADER_LEN];
      self.read_bytes(logical_offset, &mut bytes);
      RecordHeader::from_bytes(&bytes)
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
      unsafe {
        copy_nonoverlapping(raw.add(ring_off), dest, len);
        vec.set_len(len);
      }
    } else {
      let part1 = cap - ring_off;
      let part2 = len - part1;
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
      unsafe {
        copy_nonoverlapping(data.as_ptr(), raw.add(ring_off), len);
      }
    } else {
      let part1 = cap - ring_off;
      let part2 = len - part1;
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
      unsafe {
        copy_nonoverlapping(raw.add(ring_off), dest.as_mut_ptr(), len);
      }
    } else {
      let part1 = cap - ring_off;
      let part2 = len - part1;
      unsafe {
        copy_nonoverlapping(raw.add(ring_off), dest.as_mut_ptr(), part1);
        copy_nonoverlapping(raw, dest.as_mut_ptr().add(part1), part2);
      }
    }
  }

  /// 基于缓冲池将有效逻辑地址区间 [from, to) 的数据拷贝到大小对齐至 aligned_end 的 AlignedBuf 中，
  /// 并将尾部 [to, aligned_end) 填充 0。落盘完成并在 drop 时自动归还入池。
  pub fn copy_range_with_padding(
    &self,
    from: u64,
    to: u64,
    aligned_end: u64,
    pool: &Arc<BufferPool>,
  ) -> Result<AlignedBuf> {
    let total_len = (aligned_end - from) as usize;
    let mut buf = pool.get(total_len)?;
    self.fill_padded_slice(from, to, total_len, &mut buf)?;
    Ok(buf)
  }

  /// 内部辅助：将有效逻辑区间数据读取到缓冲区并对齐尾部填充 0
  #[inline]
  fn fill_padded_slice(
    &self,
    from: u64,
    to: u64,
    total_len: usize,
    buf: &mut AlignedBuf,
  ) -> Result<()> {
    let valid_len = (to - from) as usize;
    buf.set_len(total_len)?;
    let (valid_part, pad_part) = buf.as_mut_slice().split_at_mut(valid_len);
    if !valid_part.is_empty() {
      self.read_bytes(from, valid_part);
    }
    pad_part.fill(0);
    Ok(())
  }
}

impl Drop for RingBuffer {
  fn drop(&mut self) {
    unsafe {
      dealloc(self.ptr.as_ptr(), self.layout);
    }
  }
}
