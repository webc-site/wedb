use core::{
  ptr::copy_nonoverlapping,
  sync::atomic::{Ordering, fence},
};

use log::trace;

use crate::{
  error::{Error, Result},
  header::{HEADER_SIZE, KEY_LEN_BITS, RecordHeader, align_record_size},
};

/// 计算指定键长和值长下记录的对齐逻辑字节数（头 + 键 + 值，向上对齐到
/// [crate::RECORD_ALIGNMENT] 边界，差值为隐式对齐填充）
///
/// 对标 C# RecordDataHeader.GetAlignedComponentSum：记录分配尺寸由对齐后的组件和构成，
/// 保证页内每条记录头 8 字节对齐（RDH 原子字单次原子发布的前提）。
#[inline]
pub const fn record_size(key_len: usize, val_len: usize) -> usize {
  align_record_size(HEADER_SIZE.saturating_add(key_len).saturating_add(val_len))
}

/// 安全计算指定键长和值长下头 + 键 + 值的未对齐区段字节数，若溢出 usize 则返回 None
///
/// 值数据的精确结束边界（不含隐式对齐填充），对标 C# GetUnalignedComponentSum 口径。
#[inline]
pub const fn checked_record_size(key_len: usize, val_len: usize) -> Option<usize> {
  let Some(s) = HEADER_SIZE.checked_add(key_len) else {
    return None;
  };
  s.checked_add(val_len)
}

/// 最大可表示的键长度（24 位键长位段上限：16MB - 1）
pub const MAX_KEY_LEN: usize = (1usize << KEY_LEN_BITS) - 1;

/// 校验键值长度并构造记录头（返回头与记录对齐逻辑总大小）
///
/// 地址 48 位有效性由 [RecordHeader::new] 内部统一校验，此处不再重复检查。
#[inline]
fn build_header(
  prev_addr: u64,
  key: &[u8],
  val: &[u8],
  is_tombstone: bool,
) -> Result<(RecordHeader, usize)> {
  if key.len() > MAX_KEY_LEN {
    return Err(Error::KeyLengthOverflow(key.len()));
  }
  if val.len() > u32::MAX as usize {
    return Err(Error::ValueLengthOverflow(val.len()));
  }

  let header = RecordHeader::new(prev_addr, key.len() as u32, val.len() as u32, is_tombstone)?;
  let total_size = record_size(key.len(), val.len());
  Ok((header, total_size))
}

/// 将 16 字节头 + 键 + 值连续写入目标裸指针（供切片编码与向量编码共享的底层写入实现）
///
/// # Safety
/// 调用方必须保证 `ptr` 起始的 `HEADER_SIZE + key.len() + val.len()` 字节可写，
/// 且写入区间与 key/val 指向的内存互不重叠。
#[inline]
unsafe fn write_record_unchecked(ptr: *mut u8, header: &RecordHeader, key: &[u8], val: &[u8]) {
  let hdr_bytes = header.to_bytes();
  unsafe {
    copy_nonoverlapping(key.as_ptr(), ptr.add(HEADER_SIZE), key.len());
    copy_nonoverlapping(val.as_ptr(), ptr.add(HEADER_SIZE + key.len()), val.len());
    fence(Ordering::Release);
    copy_nonoverlapping(hdr_bytes.as_ptr(), ptr, HEADER_SIZE);
  }
}

/// 将键值对及元数据编码写入目标字节切片
///
/// 返回写入的字节总数（即记录对齐逻辑大小）。
/// 若目标切片容量不足，返回 `Error::BufferTooShort`。
/// 若键长超出 24 位上限、值长超出 `u32` 上限，或地址超出 48 位，返回相应错误。
pub fn encode_to_slice(
  dst: &mut [u8],
  prev_addr: u64,
  key: &[u8],
  val: &[u8],
  is_tombstone: bool,
) -> Result<usize> {
  let (header, total_size) = build_header(prev_addr, key, val, is_tombstone)?;
  let Some(buf) = dst.get_mut(..total_size) else {
    return Err(Error::BufferTooShort {
      expected: total_size,
      actual: dst.len(),
    });
  };

  let key_len = header.key_len();
  let val_len = header.val_len();
  trace!(
    "编码记录: prev_addr={prev_addr:#x}, key_len={key_len}, val_len={val_len}, is_tombstone={is_tombstone}, total_size={total_size}"
  );

  // SAFETY: buf 已由 get_mut 预先校验长度为 total_size = 对齐(HEADER_SIZE + key.len() + val.len())，
  // dst 独占借用保证与 key/val 互不重叠。
  unsafe { write_record_unchecked(buf.as_mut_ptr(), &header, key, val) };

  Ok(total_size)
}

/// 尝试将键值对及元数据编码为全新分配的 `Vec<u8>`（单次精准容量堆分配）
///
/// 若键长超出 24 位上限、值长超出 `u32` 上限，或地址超出 48 位，返回相应错误。
/// （`try_` 前缀表明可能失败，与 wval 编解码器的 `try_` 系方法命名约定一致）
pub fn try_encode_to_vec(
  prev_addr: u64,
  key: &[u8],
  val: &[u8],
  is_tombstone: bool,
) -> Result<Vec<u8>> {
  let (header, total_size) = build_header(prev_addr, key, val, is_tombstone)?;

  let mut buf = Vec::with_capacity(total_size);
  // SAFETY: buf 已预留 total_size = 对齐(HEADER_SIZE + key.len() + val.len()) 字节空间，
  // 各写入区间互不重叠；逻辑区段之后至 total_size 的隐式对齐填充先清零，
  // 保证 set_len 暴露的全部字节均已初始化。
  unsafe {
    write_record_unchecked(buf.as_mut_ptr(), &header, key, val);
    let kv_end = HEADER_SIZE + key.len() + val.len();
    buf
      .as_mut_ptr()
      .add(kv_end)
      .write_bytes(0, total_size - kv_end);
    buf.set_len(total_size);
  }
  Ok(buf)
}
