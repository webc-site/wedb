use core::{iter::FusedIterator, ops::Deref};

use wbase::simd::fast_key_eq;
use whasher::{HashSet, hash_set_with_capacity};

use crate::error::{Error, Result};

/// 哈希字段值零拷贝切片视图
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldValueRef<'a> {
  /// 字段对应的值切片
  pub value: &'a [u8],
  /// 可选毫秒级绝对过期时间戳
  pub expire_at_ms: Option<u64>,
}

impl<'a> Deref for FieldValueRef<'a> {
  type Target = [u8];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.value
  }
}

impl<'a> AsRef<[u8]> for FieldValueRef<'a> {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    self.value
  }
}

/// 哈希完整条目零拷贝切片视图
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashEntryRef<'a> {
  /// 字段名切片
  pub field: &'a [u8],
  /// 字段值切片
  pub value: &'a [u8],
  /// 可选毫秒级绝对过期时间戳
  pub expire_at_ms: Option<u64>,
}

/// 紧凑哈希只读迭代器（零堆分配、零拷贝）
#[derive(Debug, Clone)]
pub struct CompactHashIter<'a> {
  slice: &'a [u8],
  offset: usize,
  remaining: usize,
}

impl<'a> Iterator for CompactHashIter<'a> {
  type Item = HashEntryRef<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.remaining == 0 || self.offset >= self.slice.len() {
      return None;
    }

    let (entry_len, entry) = CompactHashCodec::parse_entry(self.slice, self.offset).ok()?;
    self.offset += entry_len;
    self.remaining -= 1;
    Some(entry)
  }

  #[inline(always)]
  fn size_hint(&self) -> (usize, Option<usize>) {
    (self.remaining, Some(self.remaining))
  }
}

impl<'a> ExactSizeIterator for CompactHashIter<'a> {
  #[inline(always)]
  fn len(&self) -> usize {
    self.remaining
  }
}

impl<'a> FusedIterator for CompactHashIter<'a> {}

/// 紧凑哈希元素总数前缀字节数（2 字节大端整数）
pub const COMPACT_HASH_COUNT_SIZE: usize = 2;
/// 紧凑哈希字段与值长度前缀字节数（2 字节大端整数）
pub const COMPACT_HASH_LEN_SIZE: usize = 2;
/// 紧凑哈希过期时间标记字节数（1 字节）
pub const COMPACT_HASH_EXPIRE_FLAG_SIZE: usize = 1;
/// 紧凑哈希绝对过期时间戳字节数（8 字节大端整数）
pub const COMPACT_HASH_EXPIRE_TIME_SIZE: usize = 8;

/// 紧凑哈希无状态纯静态编解码器
pub struct CompactHashCodec;

impl CompactHashCodec {
  /// 从只读切片解析元素总数（const fn，单次模式匹配零越界检查）
  #[inline]
  pub const fn count(slice: &[u8]) -> Result<usize> {
    match slice {
      [b0, b1, ..] => Ok(u16::from_be_bytes([*b0, *b1]) as usize),
      _ => Err(Error::BufferTooShort {
        expected: COMPACT_HASH_COUNT_SIZE,
        actual: slice.len(),
      }),
    }
  }

  /// 内部辅助：解析指定偏移处的单个 entry
  /// 返回 `(entry_total_len, HashEntryRef)`
  #[inline]
  pub(crate) fn parse_entry(slice: &[u8], offset: usize) -> Result<(usize, HashEntryRef<'_>)> {
    if offset + COMPACT_HASH_LEN_SIZE > slice.len() {
      return Err(Error::BufferTooShort {
        expected: offset + COMPACT_HASH_LEN_SIZE,
        actual: slice.len(),
      });
    }
    let f_len = u16::from_be_bytes([slice[offset], slice[offset + 1]]) as usize;
    let f_start = offset + COMPACT_HASH_LEN_SIZE;
    let f_end = f_start + f_len;

    if f_end + COMPACT_HASH_LEN_SIZE > slice.len() {
      return Err(Error::BufferTooShort {
        expected: f_end + COMPACT_HASH_LEN_SIZE,
        actual: slice.len(),
      });
    }
    let v_len = u16::from_be_bytes([slice[f_end], slice[f_end + 1]]) as usize;
    let v_start = f_end + COMPACT_HASH_LEN_SIZE;
    let v_end = v_start + v_len;

    if v_end >= slice.len() {
      return Err(Error::BufferTooShort {
        expected: v_end + 1,
        actual: slice.len(),
      });
    }
    let expire_flag = slice[v_end];
    let (entry_len, expire_at_ms) = if expire_flag != 0 {
      let exp_start = v_end + COMPACT_HASH_EXPIRE_FLAG_SIZE;
      let exp_end = exp_start + COMPACT_HASH_EXPIRE_TIME_SIZE;
      if exp_end > slice.len() {
        return Err(Error::BufferTooShort {
          expected: exp_end,
          actual: slice.len(),
        });
      }
      let exp = match slice[exp_start..exp_end].first_chunk::<COMPACT_HASH_EXPIRE_TIME_SIZE>() {
        Some(b) => u64::from_be_bytes(*b),
        None => {
          return Err(Error::BufferTooShort {
            expected: exp_end,
            actual: slice.len(),
          });
        }
      };
      (exp_end - offset, Some(exp))
    } else {
      (v_end + COMPACT_HASH_EXPIRE_FLAG_SIZE - offset, None)
    };

    Ok((
      entry_len,
      HashEntryRef {
        field: &slice[f_start..f_end],
        value: &slice[v_start..v_end],
        expire_at_ms,
      },
    ))
  }

  /// 内部辅助：将单个 entry 编码写入目标字节切片（零额外堆分配）
  #[inline]
  pub(crate) fn write_entry_bytes(
    dst: &mut [u8],
    field: &[u8],
    value: &[u8],
    expire_at_ms: Option<u64>,
  ) {
    let mut cur = 0;
    dst[cur..cur + COMPACT_HASH_LEN_SIZE].copy_from_slice(&(field.len() as u16).to_be_bytes());
    cur += COMPACT_HASH_LEN_SIZE;
    dst[cur..cur + field.len()].copy_from_slice(field);
    cur += field.len();
    dst[cur..cur + COMPACT_HASH_LEN_SIZE].copy_from_slice(&(value.len() as u16).to_be_bytes());
    cur += COMPACT_HASH_LEN_SIZE;
    dst[cur..cur + value.len()].copy_from_slice(value);
    cur += value.len();
    if let Some(exp) = expire_at_ms {
      dst[cur] = 1;
      cur += COMPACT_HASH_EXPIRE_FLAG_SIZE;
      dst[cur..cur + COMPACT_HASH_EXPIRE_TIME_SIZE].copy_from_slice(&exp.to_be_bytes());
    } else {
      dst[cur] = 0;
    }
  }

  /// 零堆分配查找字段，返回值及过期时间视图（单指令长度预筛与快速跳过，提升 CPU 缓存局部性）
  #[inline]
  pub fn find<'a>(slice: &'a [u8], field: &[u8]) -> Option<FieldValueRef<'a>> {
    let count = Self::count(slice).ok()?;
    let mut rest = slice.get(COMPACT_HASH_COUNT_SIZE..)?;
    let target_len = field.len();

    for _ in 0..count {
      let (len_bytes, after_f_len) = rest.split_first_chunk::<COMPACT_HASH_LEN_SIZE>()?;
      let f_len = u16::from_be_bytes(*len_bytes) as usize;
      let (f_bytes, after_field) = after_f_len.split_at_checked(f_len)?;

      if f_len == target_len && fast_key_eq(f_bytes, field) {
        let (v_len_bytes, after_v_len) =
          after_field.split_first_chunk::<COMPACT_HASH_LEN_SIZE>()?;
        let v_len = u16::from_be_bytes(*v_len_bytes) as usize;
        let (val, after_val) = after_v_len.split_at_checked(v_len)?;
        let (&flag, after_flag) = after_val.split_first()?;
        let expire_at_ms = if flag != 0 {
          let (exp_bytes, _) = after_flag.split_first_chunk::<COMPACT_HASH_EXPIRE_TIME_SIZE>()?;
          Some(u64::from_be_bytes(*exp_bytes))
        } else {
          None
        };
        return Some(FieldValueRef {
          value: val,
          expire_at_ms,
        });
      }

      // 快速路径跳过未命中条目：仅解析值长度与过期标记即可跳转下一条目
      let (v_len_bytes, after_v_len) = after_field.split_first_chunk::<COMPACT_HASH_LEN_SIZE>()?;
      let v_len = u16::from_be_bytes(*v_len_bytes) as usize;
      let (_, after_val) = after_v_len.split_at_checked(v_len)?;
      let (&flag, after_flag) = after_val.split_first()?;
      let exp_size = if flag != 0 {
        COMPACT_HASH_EXPIRE_TIME_SIZE
      } else {
        0
      };
      let (_, next_rest) = after_flag.split_at_checked(exp_size)?;
      rest = next_rest;
    }

    None
  }

  /// 零堆分配查找字段值切片（简化接口）
  #[inline(always)]
  pub fn find_field<'a>(slice: &'a [u8], field: &[u8]) -> Option<&'a [u8]> {
    Self::find(slice, field).map(|r| r.value)
  }

  /// 追加或更新字段（已存在则原地/变长更新返回 false，新插入返回 true）
  pub fn set_field(
    buf: &mut Vec<u8>,
    field: &[u8],
    value: &[u8],
    expire_at_ms: Option<u64>,
  ) -> Result<bool> {
    if field.len() > u16::MAX as usize {
      return Err(Error::KeyLengthOverflow(field.len()));
    }
    if value.len() > u16::MAX as usize {
      return Err(Error::ValueLengthOverflow(value.len()));
    }

    if buf.is_empty() {
      buf.extend_from_slice(&0u16.to_be_bytes());
    } else if buf.len() < COMPACT_HASH_COUNT_SIZE {
      return Err(Error::BufferTooShort {
        expected: COMPACT_HASH_COUNT_SIZE,
        actual: buf.len(),
      });
    }

    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let mut offset = COMPACT_HASH_COUNT_SIZE;
    let mut found = None;

    for _ in 0..count {
      let (entry_len, entry) = Self::parse_entry(buf, offset)?;
      if entry.field.len() == field.len() && fast_key_eq(entry.field, field) {
        found = Some((offset, offset + entry_len));
        break;
      }
      offset += entry_len;
    }

    // 预计算新 entry 的序列化字节
    let new_entry_len = COMPACT_HASH_LEN_SIZE
      + field.len()
      + COMPACT_HASH_LEN_SIZE
      + value.len()
      + COMPACT_HASH_EXPIRE_FLAG_SIZE
      + if expire_at_ms.is_some() {
        COMPACT_HASH_EXPIRE_TIME_SIZE
      } else {
        0
      };

    if let Some((start, end)) = found {
      // 字段已存在：更新
      let old_len = end - start;
      let old_buf_len = buf.len();
      if new_entry_len > old_len {
        let diff = new_entry_len - old_len;
        buf.reserve(diff);
        buf.resize(old_buf_len + diff, 0);
        buf.copy_within(end..old_buf_len, start + new_entry_len);
      } else if new_entry_len < old_len {
        let diff = old_len - new_entry_len;
        buf.copy_within(end..old_buf_len, start + new_entry_len);
        buf.truncate(old_buf_len - diff);
      }
      Self::write_entry_bytes(
        &mut buf[start..start + new_entry_len],
        field,
        value,
        expire_at_ms,
      );
      Ok(false)
    } else {
      // 字段不存在：追加
      if count >= u16::MAX as usize {
        return Err(Error::CompactCountOverflow(count + 1));
      }

      let new_count = (count + 1) as u16;
      buf[0..COMPACT_HASH_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());

      let old_buf_len = buf.len();
      buf.reserve(new_entry_len);
      buf.resize(old_buf_len + new_entry_len, 0);
      Self::write_entry_bytes(
        &mut buf[old_buf_len..old_buf_len + new_entry_len],
        field,
        value,
        expire_at_ms,
      );
      Ok(true)
    }
  }

  /// 删除字段（存在并成功删除返回 true，不存在返回 false）
  pub fn delete_field(buf: &mut Vec<u8>, field: &[u8]) -> Result<bool> {
    if buf.len() < COMPACT_HASH_COUNT_SIZE {
      return Ok(false);
    }
    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let mut offset = COMPACT_HASH_COUNT_SIZE;

    for _ in 0..count {
      let (entry_len, entry) = Self::parse_entry(buf, offset)?;
      if entry.field.len() == field.len() && fast_key_eq(entry.field, field) {
        buf.copy_within(offset + entry_len.., offset);
        buf.truncate(buf.len() - entry_len);
        let new_count = (count - 1) as u16;
        buf[0..COMPACT_HASH_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
        return Ok(true);
      }
      offset += entry_len;
    }

    Ok(false)
  }

  /// 原地单次遍历压缩物理空间并淘汰已过期条目（零额外堆分配、O(N) 线性时间复杂度、CPU 缓存友好）
  /// 返回清除的过期条目数
  pub fn purge_expired(buf: &mut Vec<u8>, now: u64) -> Result<usize> {
    if buf.len() < COMPACT_HASH_COUNT_SIZE {
      return Ok(0);
    }
    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let mut offset = COMPACT_HASH_COUNT_SIZE;
    let mut write_offset = COMPACT_HASH_COUNT_SIZE;
    let mut purged = 0;
    let mut new_count = 0u16;

    for _ in 0..count {
      let (entry_len, entry) = Self::parse_entry(buf, offset)?;
      let is_expired = entry.expire_at_ms.is_some_and(|exp| exp <= now);
      if is_expired {
        purged += 1;
      } else {
        if write_offset != offset {
          buf.copy_within(offset..offset + entry_len, write_offset);
        }
        write_offset += entry_len;
        new_count += 1;
      }
      offset += entry_len;
    }

    if purged > 0 {
      buf.truncate(write_offset);
      buf[0..COMPACT_HASH_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
    }

    Ok(purged)
  }

  /// 原地单遍融合清理：一次扫描同时完成过期条目淘汰与多字段删除
  ///
  /// 与 [`Self::purge_expired`] 同型的写指针原地压缩（零额外堆分配、O(N) 线性、
  /// CPU 缓存友好），将 purge + N 次 delete_field 的重复全量扫描合并为单遍；
  /// 返回 `(清除的过期条目数, 实际删除的字段数)`，条目头计数随压缩同步修正。
  pub fn purge_and_delete(buf: &mut Vec<u8>, fields: &[&[u8]], now: u64) -> Result<(usize, usize)> {
    if buf.len() < COMPACT_HASH_COUNT_SIZE {
      return Ok((0, 0));
    }
    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let mut targets: HashSet<&[u8]> = hash_set_with_capacity(fields.len());
    for &f in fields {
      targets.insert(f);
    }

    let mut offset = COMPACT_HASH_COUNT_SIZE;
    let mut write_offset = COMPACT_HASH_COUNT_SIZE;
    let mut purged = 0;
    let mut deleted = 0;
    let mut new_count = 0u16;

    for _ in 0..count {
      let (entry_len, entry) = Self::parse_entry(buf, offset)?;
      let expired = entry.expire_at_ms.is_some_and(|exp| exp <= now);
      if expired {
        purged += 1;
      } else if targets.remove(entry.field) {
        deleted += 1;
      } else {
        if write_offset != offset {
          buf.copy_within(offset..offset + entry_len, write_offset);
        }
        write_offset += entry_len;
        new_count += 1;
      }
      offset += entry_len;
    }

    if purged + deleted > 0 {
      buf.truncate(write_offset);
      buf[0..COMPACT_HASH_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
    }
    Ok((purged, deleted))
  }

  /// 获取全量字段流式迭代器
  #[inline]
  pub fn iter_fields(slice: &[u8]) -> CompactHashIter<'_> {
    let count = if slice.len() >= COMPACT_HASH_COUNT_SIZE {
      u16::from_be_bytes([slice[0], slice[1]]) as usize
    } else {
      0
    };
    CompactHashIter {
      slice,
      offset: COMPACT_HASH_COUNT_SIZE,
      remaining: count,
    }
  }

  /// 获取全量字段流式迭代器（等价于 `iter_fields`）
  #[inline(always)]
  pub fn iter(slice: &[u8]) -> CompactHashIter<'_> {
    Self::iter_fields(slice)
  }

  /// 从条目迭代器批量编码为连续紧凑切片
  pub fn encode<'a, I>(entries: I) -> Result<Vec<u8>>
  where
    I: IntoIterator<Item = (&'a [u8], &'a [u8], Option<u64>)>,
  {
    let iter = entries.into_iter();
    let (lower, _) = iter.size_hint();
    let mut buf = Vec::with_capacity(COMPACT_HASH_COUNT_SIZE + lower * 32);
    buf.extend_from_slice(&0u16.to_be_bytes());

    for (field, value, expire_at_ms) in iter {
      Self::set_field(&mut buf, field, value, expire_at_ms)?;
    }

    Ok(buf)
  }
}

/// 紧凑连续内存哈希容器（拥有所有权）
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactHash {
  raw: Vec<u8>,
}

impl CompactHash {
  /// 创建新的空紧凑哈希容器
  #[inline]
  pub fn new() -> Self {
    Self {
      raw: vec![0u8; COMPACT_HASH_COUNT_SIZE],
    }
  }

  /// 创建指定预分配容量的紧凑哈希容器
  #[inline]
  pub fn with_capacity(cap: usize) -> Self {
    let mut raw = Vec::with_capacity(cap.max(COMPACT_HASH_COUNT_SIZE));
    raw.extend_from_slice(&0u16.to_be_bytes());
    Self { raw }
  }

  /// 从已有字节切片解析构建
  #[inline]
  pub fn from_vec(raw: Vec<u8>) -> Result<Self> {
    if raw.len() < COMPACT_HASH_COUNT_SIZE {
      return Err(Error::BufferTooShort {
        expected: COMPACT_HASH_COUNT_SIZE,
        actual: raw.len(),
      });
    }
    // 校验条目数
    let _ = CompactHashCodec::count(&raw)?;
    Ok(Self { raw })
  }

  /// 获取底层切片
  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    &self.raw
  }

  /// 解构获取底层字节 Vec
  #[inline(always)]
  pub fn into_vec(self) -> Vec<u8> {
    self.raw
  }

  /// 元素数量
  #[inline(always)]
  pub fn len(&self) -> usize {
    CompactHashCodec::count(&self.raw).unwrap_or(0)
  }

  /// 是否为空
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// 查找字段
  #[inline(always)]
  pub fn find(&self, field: &[u8]) -> Option<FieldValueRef<'_>> {
    CompactHashCodec::find(&self.raw, field)
  }

  /// 查找字段值切片
  #[inline(always)]
  pub fn find_field(&self, field: &[u8]) -> Option<&[u8]> {
    CompactHashCodec::find_field(&self.raw, field)
  }

  /// 设置字段
  #[inline(always)]
  pub fn set_field(
    &mut self,
    field: &[u8],
    value: &[u8],
    expire_at_ms: Option<u64>,
  ) -> Result<bool> {
    CompactHashCodec::set_field(&mut self.raw, field, value, expire_at_ms)
  }

  /// 删除字段
  #[inline(always)]
  pub fn delete_field(&mut self, field: &[u8]) -> Result<bool> {
    CompactHashCodec::delete_field(&mut self.raw, field)
  }

  /// 原地清理已过期字段并压缩物理内存
  #[inline(always)]
  pub fn purge_expired(&mut self, now: u64) -> Result<usize> {
    CompactHashCodec::purge_expired(&mut self.raw, now)
  }

  /// 原地单遍融合清理过期条目并删除指定字段
  #[inline(always)]
  pub fn purge_and_delete(&mut self, fields: &[&[u8]], now: u64) -> Result<(usize, usize)> {
    CompactHashCodec::purge_and_delete(&mut self.raw, fields, now)
  }

  /// 迭代字段
  #[inline(always)]
  pub fn iter_fields(&self) -> CompactHashIter<'_> {
    CompactHashCodec::iter_fields(&self.raw)
  }
}

impl Deref for CompactHash {
  type Target = [u8];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    &self.raw
  }
}

impl AsRef<[u8]> for CompactHash {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    &self.raw
  }
}
