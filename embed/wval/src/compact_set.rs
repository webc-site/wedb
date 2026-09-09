use core::{cmp::Ordering, iter::FusedIterator, ops::Deref, ptr, result::Result as StdResult};

use crate::error::{Error, Result};

/// 紧凑集合只读流式迭代器（零堆分配、零拷贝）
#[derive(Debug, Clone)]
pub struct CompactSetIter<'a> {
  slice: &'a [u8],
  offset: usize,
  remaining: usize,
}

impl<'a> Iterator for CompactSetIter<'a> {
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.remaining == 0 || self.offset >= self.slice.len() {
      return None;
    }

    let (entry_len, member) = CompactSetCodec::parse_entry(self.slice, self.offset).ok()?;
    self.offset += entry_len;
    self.remaining -= 1;
    Some(member)
  }

  #[inline(always)]
  fn size_hint(&self) -> (usize, Option<usize>) {
    (self.remaining, Some(self.remaining))
  }
}

impl<'a> ExactSizeIterator for CompactSetIter<'a> {
  #[inline(always)]
  fn len(&self) -> usize {
    self.remaining
  }
}

impl<'a> FusedIterator for CompactSetIter<'a> {}

/// 紧凑集合元素总数前缀字节数（2 字节大端整数）
pub const COMPACT_SET_COUNT_SIZE: usize = 2;
/// 紧凑集合成员长度前缀字节数（2 字节大端整数）
pub const COMPACT_SET_LEN_SIZE: usize = 2;

/// 紧凑无序集合（内存字典序排列）无状态编解码器
pub struct CompactSetCodec;

impl CompactSetCodec {
  /// 从只读切片解析元素总数（const fn，单次模式匹配零越界检查）
  #[inline]
  pub const fn count(slice: &[u8]) -> Result<usize> {
    match slice {
      [b0, b1, ..] => Ok(u16::from_be_bytes([*b0, *b1]) as usize),
      _ => Err(Error::BufferTooShort {
        expected: COMPACT_SET_COUNT_SIZE,
        actual: slice.len(),
      }),
    }
  }

  /// 解析指定偏移处的单个条目 `(entry_total_len, member_slice)`
  #[inline]
  pub fn parse_entry(slice: &[u8], offset: usize) -> Result<(usize, &[u8])> {
    if offset + COMPACT_SET_LEN_SIZE > slice.len() {
      return Err(Error::BufferTooShort {
        expected: offset + COMPACT_SET_LEN_SIZE,
        actual: slice.len(),
      });
    }
    let m_len = u16::from_be_bytes([slice[offset], slice[offset + 1]]) as usize;
    let m_start = offset + COMPACT_SET_LEN_SIZE;
    let m_end = m_start + m_len;

    if m_end > slice.len() {
      return Err(Error::BufferTooShort {
        expected: m_end,
        actual: slice.len(),
      });
    }

    Ok((COMPACT_SET_LEN_SIZE + m_len, &slice[m_start..m_end]))
  }

  /// 内部辅助：构建条目在连续切片中的起始偏移数组（栈优先，小集合零堆分配）
  #[inline]
  fn collect_offsets<F, R>(slice: &[u8], count: usize, f: F) -> Result<R>
  where
    F: FnOnce(&[usize]) -> R,
  {
    const STACK_CAP: usize = 512;
    if count <= STACK_CAP {
      let mut stack_offsets = [0usize; STACK_CAP];
      let mut offset = COMPACT_SET_COUNT_SIZE;
      for slot in stack_offsets.iter_mut().take(count) {
        *slot = offset;
        let (entry_len, _) = Self::parse_entry(slice, offset)?;
        offset += entry_len;
      }
      Ok(f(&stack_offsets[..count]))
    } else {
      let mut heap_offsets = Vec::with_capacity(count);
      let mut offset = COMPACT_SET_COUNT_SIZE;
      for _ in 0..count {
        heap_offsets.push(offset);
        let (entry_len, _) = Self::parse_entry(slice, offset)?;
        offset += entry_len;
      }
      Ok(f(&heap_offsets))
    }
  }

  /// 二分查找成员索引
  ///
  /// 返回：
  /// - `Ok(usize)`: 找到成员，返回其在有序集合中的索引
  /// - `Err(usize)`: 未找到成员，返回其应插入的有序索引位置
  ///
  /// 复杂度：O(N) 构建偏移数组 + O(log N) 二分比较（紧凑布局无常驻索引，偏移需现算）
  pub fn binary_search(slice: &[u8], member: &[u8]) -> Result<StdResult<usize, usize>> {
    if slice.len() < COMPACT_SET_COUNT_SIZE {
      return Ok(Err(0));
    }
    let count = u16::from_be_bytes([slice[0], slice[1]]) as usize;
    if count == 0 {
      return Ok(Err(0));
    }

    Self::collect_offsets(slice, count, |offsets| {
      let mut low = 0;
      let mut high = count;

      while low < high {
        let mid = (low + high) / 2;
        let entry_offset = offsets[mid];
        let m_len = u16::from_be_bytes([slice[entry_offset], slice[entry_offset + 1]]) as usize;
        let m =
          &slice[entry_offset + COMPACT_SET_LEN_SIZE..entry_offset + COMPACT_SET_LEN_SIZE + m_len];

        match m.cmp(member) {
          Ordering::Less => low = mid + 1,
          Ordering::Greater => high = mid,
          Ordering::Equal => return Ok(mid),
        }
      }
      Err(low)
    })
  }

  /// 零堆分配有序查找成员是否存在（单次遍历，字典序提前终止）
  #[inline]
  pub fn contains(slice: &[u8], member: &[u8]) -> bool {
    if slice.len() < COMPACT_SET_COUNT_SIZE {
      return false;
    }
    let count = u16::from_be_bytes([slice[0], slice[1]]) as usize;
    let mut offset = COMPACT_SET_COUNT_SIZE;
    for _ in 0..count {
      let Ok((entry_len, m)) = Self::parse_entry(slice, offset) else {
        return false;
      };
      match m.cmp(member) {
        Ordering::Equal => return true,
        Ordering::Greater => return false,
        Ordering::Less => {}
      }
      offset += entry_len;
    }
    false
  }

  /// 校验紧凑集合只读切片的完整性与字典序单调递增性（零堆分配）
  pub fn validate(slice: &[u8]) -> Result<usize> {
    let count = Self::count(slice)?;
    let mut offset = COMPACT_SET_COUNT_SIZE;
    let mut prev: Option<&[u8]> = None;

    for _ in 0..count {
      let (entry_len, member) = Self::parse_entry(slice, offset)?;
      if let Some(prev_m) = prev
        && prev_m >= member
      {
        return Err(Error::CorruptedCompactData(
          "紧凑集合成员未按严格字典序单调递增排列或存在重复项",
        ));
      }
      prev = Some(member);
      offset += entry_len;
    }

    if offset != slice.len() {
      return Err(Error::BufferTooShort {
        expected: offset,
        actual: slice.len(),
      });
    }

    Ok(count)
  }

  /// 有序插入成员并去重（单趟流式定位插入偏移，内存切片原位零多余堆分配）
  pub fn insert(buf: &mut Vec<u8>, member: &[u8]) -> Result<bool> {
    if member.len() > u16::MAX as usize {
      return Err(Error::KeyLengthOverflow(member.len()));
    }

    if buf.is_empty() {
      buf.extend_from_slice(&0u16.to_be_bytes());
    } else if buf.len() < COMPACT_SET_COUNT_SIZE {
      return Err(Error::BufferTooShort {
        expected: COMPACT_SET_COUNT_SIZE,
        actual: buf.len(),
      });
    }

    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if count == 0 {
      buf.truncate(COMPACT_SET_COUNT_SIZE);
      let new_entry_len = COMPACT_SET_LEN_SIZE + member.len();
      buf.reserve(new_entry_len);
      buf.extend_from_slice(&(member.len() as u16).to_be_bytes());
      buf.extend_from_slice(member);
      buf[0..COMPACT_SET_COUNT_SIZE].copy_from_slice(&1u16.to_be_bytes());
      return Ok(true);
    }

    let mut offset = COMPACT_SET_COUNT_SIZE;
    let mut insert_pos = None;

    for _ in 0..count {
      let (entry_len, m) = Self::parse_entry(buf, offset)?;
      match m.cmp(member) {
        Ordering::Equal => return Ok(false),
        Ordering::Greater => {
          insert_pos = Some(offset);
          break;
        }
        Ordering::Less => {
          offset += entry_len;
        }
      }
    }

    if count >= u16::MAX as usize {
      return Err(Error::CompactCountOverflow(count + 1));
    }

    let new_entry_len = COMPACT_SET_LEN_SIZE + member.len();
    let old_len = buf.len();
    buf.reserve(new_entry_len);

    let insert_pos = insert_pos.unwrap_or(offset);

    if insert_pos == old_len {
      buf.extend_from_slice(&(member.len() as u16).to_be_bytes());
      buf.extend_from_slice(member);
    } else {
      // 安全性保证：上方 reserve(new_entry_len) 已确保容量 >= old_len + new_entry_len，
      // [insert_pos, old_len) 为已初始化字节；memmove 腾位后原位写入新条目，全程不越界
      unsafe {
        let p = buf.as_mut_ptr();
        ptr::copy(
          p.add(insert_pos),
          p.add(insert_pos + new_entry_len),
          old_len - insert_pos,
        );
        ptr::copy_nonoverlapping(
          (member.len() as u16).to_be_bytes().as_ptr(),
          p.add(insert_pos),
          COMPACT_SET_LEN_SIZE,
        );
        ptr::copy_nonoverlapping(
          member.as_ptr(),
          p.add(insert_pos + COMPACT_SET_LEN_SIZE),
          member.len(),
        );
        buf.set_len(old_len + new_entry_len);
      }
    }

    let new_count = (count + 1) as u16;
    buf[0..COMPACT_SET_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
    Ok(true)
  }

  /// 定位并删除成员（就地内存连续收缩）
  pub fn remove(buf: &mut Vec<u8>, member: &[u8]) -> Result<bool> {
    if buf.len() < COMPACT_SET_COUNT_SIZE {
      return Ok(false);
    }
    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if count == 0 {
      return Ok(false);
    }

    let mut offset = COMPACT_SET_COUNT_SIZE;
    for _ in 0..count {
      let (entry_len, m) = Self::parse_entry(buf, offset)?;
      match m.cmp(member) {
        Ordering::Equal => {
          // 尾部整体前移覆盖被删条目，随后物理收缩（与紧凑哈希删除路径一致的安全 API 实现）
          buf.copy_within(offset + entry_len.., offset);
          buf.truncate(buf.len() - entry_len);
          let new_count = (count - 1) as u16;
          buf[0..COMPACT_SET_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
          if new_count == 0 && buf.capacity() > 64 {
            buf.shrink_to_fit();
          }
          return Ok(true);
        }
        Ordering::Greater => return Ok(false),
        Ordering::Less => {}
      }
      offset += entry_len;
    }
    Ok(false)
  }

  /// 获取全量成员流式切片迭代器（按字典序排列）
  #[inline]
  pub fn iter_members(slice: &[u8]) -> CompactSetIter<'_> {
    let count = if slice.len() >= COMPACT_SET_COUNT_SIZE {
      u16::from_be_bytes([slice[0], slice[1]]) as usize
    } else {
      0
    };
    CompactSetIter {
      slice,
      offset: COMPACT_SET_COUNT_SIZE,
      remaining: count,
    }
  }

  /// 批量编码成员集合为紧凑内存（排序去重，单次线性写入）
  pub fn encode<'a, I>(members: I) -> Result<Vec<u8>>
  where
    I: IntoIterator<Item = &'a [u8]>,
  {
    let iter = members.into_iter();
    let (lower, _) = iter.size_hint();
    let mut mems: Vec<&'a [u8]> = Vec::with_capacity(lower);
    for m in iter {
      if m.len() > u16::MAX as usize {
        return Err(Error::KeyLengthOverflow(m.len()));
      }
      mems.push(m);
    }
    mems.sort_unstable();
    mems.dedup();

    if mems.len() > u16::MAX as usize {
      return Err(Error::CompactCountOverflow(mems.len()));
    }

    let payload_len: usize = mems.iter().map(|m| COMPACT_SET_LEN_SIZE + m.len()).sum();
    let mut buf = Vec::with_capacity(COMPACT_SET_COUNT_SIZE + payload_len);
    buf.extend_from_slice(&(mems.len() as u16).to_be_bytes());
    for m in mems {
      buf.extend_from_slice(&(m.len() as u16).to_be_bytes());
      buf.extend_from_slice(m);
    }

    Ok(buf)
  }
}

/// 紧凑连续内存有序集合容器（拥有所有权）
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactSet {
  raw: Vec<u8>,
}

impl CompactSet {
  /// 创建新的空紧凑集合容器
  #[inline]
  pub fn new() -> Self {
    Self {
      raw: vec![0u8; COMPACT_SET_COUNT_SIZE],
    }
  }

  /// 创建指定预分配容量的紧凑集合容器
  #[inline]
  pub fn with_capacity(cap: usize) -> Self {
    let mut raw = Vec::with_capacity(cap.max(COMPACT_SET_COUNT_SIZE));
    raw.extend_from_slice(&0u16.to_be_bytes());
    Self { raw }
  }

  /// 从已有字节切片解析构建
  #[inline]
  pub fn from_vec(raw: Vec<u8>) -> Result<Self> {
    CompactSetCodec::validate(&raw)?;
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
    CompactSetCodec::count(&self.raw).unwrap_or(0)
  }

  /// 是否为空
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// 二分查找成员索引
  #[inline(always)]
  pub fn binary_search(&self, member: &[u8]) -> Result<StdResult<usize, usize>> {
    CompactSetCodec::binary_search(&self.raw, member)
  }

  /// 二分查找是否存在指定成员
  #[inline(always)]
  pub fn contains(&self, member: &[u8]) -> bool {
    CompactSetCodec::contains(&self.raw, member)
  }

  /// 有序插入成员并去重
  #[inline(always)]
  pub fn insert(&mut self, member: &[u8]) -> Result<bool> {
    CompactSetCodec::insert(&mut self.raw, member)
  }

  /// 二分查找并删除成员
  #[inline(always)]
  pub fn remove(&mut self, member: &[u8]) -> Result<bool> {
    CompactSetCodec::remove(&mut self.raw, member)
  }

  /// 迭代有序成员
  #[inline(always)]
  pub fn iter_members(&self) -> CompactSetIter<'_> {
    CompactSetCodec::iter_members(&self.raw)
  }

  /// 清空集合并重置为初始空状态（立即物理收敛底层内存）
  #[inline]
  pub fn clear(&mut self) {
    self.raw.clear();
    self.raw.extend_from_slice(&0u16.to_be_bytes());
    self.raw.shrink_to_fit();
  }
}

impl<'a> IntoIterator for &'a CompactSet {
  type Item = &'a [u8];
  type IntoIter = CompactSetIter<'a>;

  #[inline(always)]
  fn into_iter(self) -> Self::IntoIter {
    self.iter_members()
  }
}

impl Deref for CompactSet {
  type Target = [u8];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    &self.raw
  }
}

impl AsRef<[u8]> for CompactSet {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    &self.raw
  }
}
