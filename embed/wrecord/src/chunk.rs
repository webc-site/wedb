use core::{iter::FusedIterator, ptr::copy_nonoverlapping};

use crate::error::{Error, Result};

/// 集合/哈希分块条目长度前缀字节数（4 字节大端整数）
pub const CHUNK_LEN_PREFIX_SIZE: usize = 4;

/// 集合/哈希分块索引编解码器（4 字节大端长度前缀 + 字节载荷）
#[derive(Debug, Clone, Copy)]
pub struct ChunkCodec;

impl ChunkCodec {
  /// 尝试编码条目列表至指定缓冲区（若单个条目超出 u32 上限则返回错误）
  pub fn try_encode(items: &[&[u8]], buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();
    let mut total_len: usize = 0;
    for item in items {
      if item.len() > u32::MAX as usize {
        return Err(Error::KeyLengthOverflow(item.len()));
      }
      total_len = match total_len.checked_add(CHUNK_LEN_PREFIX_SIZE + item.len()) {
        Some(l) => l,
        None => return Err(Error::RecordSizeOverflow),
      };
    }
    buf.reserve(total_len);
    // SAFETY: reserve 保证了底层容量 >= total_len，写入严格在预留空间内推进并在完成后单次 set_len
    unsafe {
      let mut ptr = buf.as_mut_ptr();
      for item in items {
        let len_bytes = (item.len() as u32).to_be_bytes();
        copy_nonoverlapping(len_bytes.as_ptr(), ptr, CHUNK_LEN_PREFIX_SIZE);
        ptr = ptr.add(CHUNK_LEN_PREFIX_SIZE);
        if !item.is_empty() {
          copy_nonoverlapping(item.as_ptr(), ptr, item.len());
          ptr = ptr.add(item.len());
        }
      }
      buf.set_len(total_len);
    }
    Ok(())
  }

  /// 编码条目列表至指定缓冲区（单次预分配容量 + 零冗余校验指针写入）
  #[inline]
  pub fn encode(items: &[&[u8]], buf: &mut Vec<u8>) {
    Self::try_encode(items, buf).expect("chunk item length overflow");
  }

  /// 编码条目列表并返回新建 Vec 缓冲区（单次精准容量堆分配）
  #[must_use]
  #[inline]
  pub fn encode_to_vec(items: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    Self::encode(items, &mut buf);
    buf
  }

  /// 尝试编码条目列表并返回新建 Vec 缓冲区
  #[inline]
  pub fn try_encode_to_vec(items: &[&[u8]]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    Self::try_encode(items, &mut buf)?;
    Ok(buf)
  }

  /// 追加单个条目至现有分块缓冲区末尾（单次预留与单次长度提交）
  #[inline]
  pub fn append(item: &[u8], buf: &mut Vec<u8>) -> Result<()> {
    if item.len() > u32::MAX as usize {
      return Err(Error::KeyLengthOverflow(item.len()));
    }
    let old_len = buf.len();
    let needed = match CHUNK_LEN_PREFIX_SIZE.checked_add(item.len()) {
      Some(n) => n,
      None => return Err(Error::RecordSizeOverflow),
    };
    buf.reserve(needed);
    let len_bytes = (item.len() as u32).to_be_bytes();
    // SAFETY: reserve 确保容量 >= old_len + needed，原位写入后单次更新长度
    unsafe {
      let ptr = buf.as_mut_ptr().add(old_len);
      copy_nonoverlapping(len_bytes.as_ptr(), ptr, CHUNK_LEN_PREFIX_SIZE);
      if !item.is_empty() {
        copy_nonoverlapping(item.as_ptr(), ptr.add(CHUNK_LEN_PREFIX_SIZE), item.len());
      }
      buf.set_len(old_len + needed);
    }
    Ok(())
  }

  /// 零拷贝流式迭代分块中的所有条目
  #[inline]
  pub fn iter(slice: &[u8]) -> Result<ChunkIter<'_>> {
    let mut remaining = slice;
    let mut count = 0;
    while let Some((len_bytes, rest)) = remaining.split_first_chunk::<CHUNK_LEN_PREFIX_SIZE>() {
      let len = u32::from_be_bytes(*len_bytes) as usize;
      let Some((_, next_rest)) = rest.split_at_checked(len) else {
        return Err(Error::BufferTooShort {
          expected: len,
          actual: rest.len(),
        });
      };
      remaining = next_rest;
      count += 1;
    }
    if !remaining.is_empty() {
      return Err(Error::BufferTooShort {
        expected: CHUNK_LEN_PREFIX_SIZE,
        actual: remaining.len(),
      });
    }

    Ok(ChunkIter { slice, count })
  }
}

/// 分块条目只读流式零拷贝迭代器
#[derive(Clone, Copy, Debug, Default)]
pub struct ChunkIter<'a> {
  slice: &'a [u8],
  count: usize,
}

impl<'a> ChunkIter<'a> {
  /// 获取底层剩余切片只读借用
  #[must_use]
  #[inline(always)]
  pub const fn as_slice(&self) -> &'a [u8] {
    self.slice
  }

  /// 剩余条目总数
  #[must_use]
  #[inline(always)]
  pub const fn remaining_count(&self) -> usize {
    self.count
  }
}

impl<'a> Iterator for ChunkIter<'a> {
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    let (len_bytes, rest) = self.slice.split_first_chunk::<CHUNK_LEN_PREFIX_SIZE>()?;
    let len = u32::from_be_bytes(*len_bytes) as usize;
    let (item, remaining) = rest.split_at_checked(len)?;
    self.slice = remaining;
    self.count = self.count.saturating_sub(1);
    Some(item)
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    (self.count, Some(self.count))
  }
}

impl ExactSizeIterator for ChunkIter<'_> {
  #[inline]
  fn len(&self) -> usize {
    self.count
  }
}

impl FusedIterator for ChunkIter<'_> {}

#[cfg(test)]
mod tests {
  use super::{CHUNK_LEN_PREFIX_SIZE, ChunkCodec};

  /// 编码/追加/迭代全链路往返一致性
  #[test]
  fn chunk_codec_roundtrip() {
    let items: [&[u8]; 4] = [b"", b"alpha", &[0u8, 255, 7], b"omega"];

    let mut buf = Vec::new();
    ChunkCodec::encode(&items, &mut buf);
    assert_eq!(
      buf.len(),
      items
        .iter()
        .map(|i| CHUNK_LEN_PREFIX_SIZE + i.len())
        .sum::<usize>()
    );
    assert_eq!(ChunkCodec::encode_to_vec(&items), buf);
    assert_eq!(ChunkCodec::try_encode_to_vec(&items).unwrap(), buf);

    let mut try_buf = Vec::new();
    ChunkCodec::try_encode(&items, &mut try_buf).unwrap();
    assert_eq!(try_buf, buf);

    // 逐条 append 与一次性 encode 产物一致
    let mut app = Vec::new();
    for it in items {
      ChunkCodec::append(it, &mut app).unwrap();
    }
    assert_eq!(app, buf);

    // 零拷贝流式迭代逐条还原与状态检查
    let mut iter = ChunkCodec::iter(&buf).unwrap();
    assert_eq!(iter.remaining_count(), 4);
    assert_eq!(iter.as_slice(), buf.as_slice());
    assert_eq!(iter.len(), 4);
    let first = iter.next().unwrap();
    assert_eq!(first, items[0]);
    assert_eq!(iter.remaining_count(), 3);
    assert_eq!(iter.len(), 3);

    let rest_items: Vec<&[u8]> = iter.collect();
    assert_eq!(rest_items.as_slice(), &items[1..]);
  }

  /// 空载荷、损坏长度前缀与逐字节截断防御
  #[test]
  fn chunk_codec_boundary_defense() {
    // 空 entries -> 空缓冲、零条目
    let empty: [&[u8]; 0] = [];
    let buf = ChunkCodec::encode_to_vec(&empty);
    assert!(buf.is_empty());
    assert_eq!(ChunkCodec::iter(&buf).unwrap().count(), 0);

    // 长度前缀声明越界：声明 10 字节但载荷仅 3 字节
    let mut corrupt = Vec::new();
    corrupt.extend_from_slice(&10u32.to_be_bytes());
    corrupt.extend_from_slice(b"abc");
    assert!(ChunkCodec::iter(&corrupt).is_err());

    // 尾部残缺前缀（不足 4 字节）
    assert!(ChunkCodec::iter(&[0, 0, 0]).is_err());

    // 逐字节截断探测：完整双条目编码的每段前缀要么报错、要么恰好解出完整条目
    let items = [b"hello".as_slice(), b"world".as_slice()];
    let full = ChunkCodec::encode_to_vec(&items);
    let first_end = CHUNK_LEN_PREFIX_SIZE + items[0].len();
    for len in 0..=full.len() {
      if len == full.len() {
        let iter = ChunkCodec::iter(&full[..len]).unwrap();
        assert_eq!(iter.len(), 2, "len={len} 条目数不符");
        assert_eq!(iter.count(), 2, "len={len} 迭代产出不符");
      } else if len == first_end {
        let iter = ChunkCodec::iter(&full[..len]).unwrap();
        assert_eq!(iter.len(), 1, "len={len} 条目数不符");
        assert_eq!(iter.count(), 1, "len={len} 迭代产出不符");
      } else if len == 0 {
        let iter = ChunkCodec::iter(&full[..len]).unwrap();
        assert_eq!(iter.len(), 0, "len={len} 条目数不符");
        assert_eq!(iter.count(), 0, "len={len} 迭代产出不符");
      } else {
        assert!(
          ChunkCodec::iter(&full[..len]).is_err(),
          "len={len} 残缺前缀必须报错"
        );
      }
    }
  }
}
