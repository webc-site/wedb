use core::{cmp::Ordering, iter::FusedIterator, ops::Deref, ptr, result::Result as StdResult};

use wbase::simd::fast_key_eq;

use crate::{
  error::{Error, Result},
  zset::{decode_order_preserving_f64, encode_order_preserving_f64},
};

/// 紧凑有序集合条目零拷贝切片视图
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZSetEntryRef<'a> {
  /// 浮点分值
  pub score: f64,
  /// 8 字节大端保序编码分值
  pub order_score: [u8; 8],
  /// 成员二进制切片
  pub member: &'a [u8],
}

impl<'a> Deref for ZSetEntryRef<'a> {
  type Target = [u8];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.member
  }
}

impl<'a> AsRef<[u8]> for ZSetEntryRef<'a> {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    self.member
  }
}

/// 紧凑有序集合全量流式切片迭代器
#[derive(Debug, Clone)]
pub struct CompactZSetIter<'a> {
  slice: &'a [u8],
  offset: usize,
  remaining: usize,
}

impl<'a> Iterator for CompactZSetIter<'a> {
  type Item = ZSetEntryRef<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.remaining == 0 || self.offset >= self.slice.len() {
      return None;
    }

    let (entry_len, entry) = CompactZSetCodec::parse_entry(self.slice, self.offset).ok()?;
    self.offset += entry_len;
    self.remaining -= 1;
    Some(entry)
  }

  #[inline(always)]
  fn size_hint(&self) -> (usize, Option<usize>) {
    (self.remaining, Some(self.remaining))
  }
}

impl<'a> ExactSizeIterator for CompactZSetIter<'a> {
  #[inline(always)]
  fn len(&self) -> usize {
    self.remaining
  }
}

impl<'a> FusedIterator for CompactZSetIter<'a> {}

/// 紧凑有序集合元素总数前缀字节数（2 字节大端整数）
pub const COMPACT_ZSET_COUNT_SIZE: usize = 2;
/// 紧凑有序集合保序分值字节数（8 字节）
pub const COMPACT_ZSET_SCORE_SIZE: usize = 8;
/// 紧凑有序集合成员长度前缀字节数（2 字节大端整数）
pub const COMPACT_ZSET_LEN_SIZE: usize = 2;
/// 紧凑有序集合条目定长头部大小（8 字节保序分值 + 2 字节成员长度 = 10 字节）
pub const COMPACT_ZSET_ENTRY_HEADER_SIZE: usize = COMPACT_ZSET_SCORE_SIZE + COMPACT_ZSET_LEN_SIZE;

/// 紧凑有序集合无状态编解码器
pub struct CompactZSetCodec;

impl CompactZSetCodec {
  /// 从只读切片解析元素总数（const fn，单次模式匹配零越界检查）
  #[inline]
  pub const fn count(slice: &[u8]) -> Result<usize> {
    match slice {
      [b0, b1, ..] => Ok(u16::from_be_bytes([*b0, *b1]) as usize),
      _ => Err(Error::BufferTooShort {
        expected: COMPACT_ZSET_COUNT_SIZE,
        actual: slice.len(),
      }),
    }
  }

  /// 解析指定偏移处的单个条目 `(entry_total_len, ZSetEntryRef)`
  #[inline]
  pub fn parse_entry(slice: &[u8], offset: usize) -> Result<(usize, ZSetEntryRef<'_>)> {
    let (total_len, order_score, member) = Self::parse_entry_header(slice, offset)?;
    let score = decode_order_preserving_f64(order_score);
    Ok((
      total_len,
      ZSetEntryRef {
        score,
        order_score,
        member,
      },
    ))
  }

  /// 快速解析指定偏移处的条目头部与成员借用，不提前解码浮点数（极大加速过滤与查找）
  #[inline(always)]
  pub fn parse_entry_header(slice: &[u8], offset: usize) -> Result<(usize, [u8; 8], &[u8])> {
    let header_end = match offset.checked_add(COMPACT_ZSET_ENTRY_HEADER_SIZE) {
      Some(end) if end <= slice.len() => end,
      _ => {
        return Err(Error::BufferTooShort {
          expected: offset + COMPACT_ZSET_ENTRY_HEADER_SIZE,
          actual: slice.len(),
        });
      }
    };
    unsafe {
      let ptr = slice.as_ptr().add(offset);
      let order_score = (ptr as *const [u8; COMPACT_ZSET_SCORE_SIZE]).read_unaligned();
      let m_len = u16::from_be_bytes(
        (ptr.add(COMPACT_ZSET_SCORE_SIZE) as *const [u8; COMPACT_ZSET_LEN_SIZE]).read_unaligned(),
      ) as usize;
      let total_len = match COMPACT_ZSET_ENTRY_HEADER_SIZE.checked_add(m_len) {
        Some(l) => l,
        None => return Err(Error::RecordSizeOverflow),
      };
      let m_end = match header_end.checked_add(m_len) {
        Some(end) if end <= slice.len() => end,
        _ => {
          return Err(Error::BufferTooShort {
            expected: header_end.saturating_add(m_len),
            actual: slice.len(),
          });
        }
      };
      Ok((
        total_len,
        order_score,
        slice.get_unchecked(header_end..m_end),
      ))
    }
  }

  /// 校验紧凑有序集合切片合法性（大小端对齐、单调保序与长度匹配）
  pub fn validate(slice: &[u8]) -> Result<usize> {
    let count = Self::count(slice)?;
    let mut offset = COMPACT_ZSET_COUNT_SIZE;
    let mut prev: Option<([u8; 8], &[u8])> = None;

    for _ in 0..count {
      let (entry_len, order_score, member) = Self::parse_entry_header(slice, offset)?;
      if let Some((prev_order, prev_member)) = prev {
        let ord = prev_order
          .cmp(&order_score)
          .then_with(|| prev_member.cmp(member));
        if ord != Ordering::Less {
          return Err(Error::CorruptedCompactData(
            "紧凑有序集合成员未按严格保序递增排列或存在重复项",
          ));
        }
      }
      prev = Some((order_score, member));
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

  /// 内部辅助：构建条目起始偏移数组（栈优先，小集合零堆分配）
  #[inline]
  fn collect_offsets<F, R>(slice: &[u8], count: usize, f: F) -> Result<R>
  where
    F: FnOnce(&[usize]) -> R,
  {
    const STACK_CAP: usize = 256;
    if count <= STACK_CAP {
      let mut stack_offsets = [0usize; STACK_CAP];
      let mut offset = COMPACT_ZSET_COUNT_SIZE;
      for slot in stack_offsets.iter_mut().take(count) {
        *slot = offset;
        let (entry_len, ..) = Self::parse_entry_header(slice, offset)?;
        offset += entry_len;
      }
      Ok(f(&stack_offsets[..count]))
    } else {
      let mut heap_offsets = Vec::with_capacity(count);
      let mut offset = COMPACT_ZSET_COUNT_SIZE;
      for _ in 0..count {
        heap_offsets.push(offset);
        let (entry_len, ..) = Self::parse_entry_header(slice, offset)?;
        offset += entry_len;
      }
      Ok(f(&heap_offsets))
    }
  }

  /// 在已知偏移切片上执行二分查找
  ///
  /// # 参数约束
  /// `offsets` 必须由 [Self::collect_offsets] 在同一 `slice` 上构建（各元素均指向合法条目边界），
  /// 条目解析经 [Self::parse_entry_header] 全程越界校验，任意输入皆不会产生未定义行为。
  #[inline]
  pub fn binary_search_offsets(
    slice: &[u8],
    offsets: &[usize],
    order_score: [u8; 8],
    member: &[u8],
  ) -> Result<StdResult<usize, usize>> {
    let mut low = 0;
    let mut high = offsets.len();

    while low < high {
      let mid = (low + high) / 2;
      let (_, entry_order, m) = Self::parse_entry_header(slice, offsets[mid])?;

      let ord = entry_order.cmp(&order_score).then_with(|| m.cmp(member));

      match ord {
        Ordering::Less => low = mid + 1,
        Ordering::Greater => high = mid,
        Ordering::Equal => return Ok(Ok(mid)),
      }
    }
    Ok(Err(low))
  }

  /// 基于二分查找定位 `(order_score, member)` 索引（O(N) 偏移构建 + O(log N) 比较）
  ///
  /// - `Ok(idx)`: `(order_score, member)` 已存在，返回其索引
  /// - `Err(idx)`: 不存在，返回其应插入的有序索引
  pub fn binary_search_entry(
    slice: &[u8],
    order_score: [u8; 8],
    member: &[u8],
  ) -> Result<StdResult<usize, usize>> {
    if slice.len() < COMPACT_ZSET_COUNT_SIZE {
      return Ok(Err(0));
    }
    let count = Self::count(slice)?;
    if count == 0 {
      return Ok(Err(0));
    }

    Self::collect_offsets(slice, count, |offsets| {
      Self::binary_search_offsets(slice, offsets, order_score, member)
    })
    .and_then(|found| found)
  }

  /// 扫描定位成员排名（0-indexed），不存在返回 None
  pub fn rank_of(slice: &[u8], member: &[u8]) -> Option<usize> {
    if slice.len() < COMPACT_ZSET_COUNT_SIZE {
      return None;
    }
    let count = u16::from_be_bytes([slice[0], slice[1]]) as usize;
    let mut offset = COMPACT_ZSET_COUNT_SIZE;

    for rank in 0..count {
      let (entry_len, _, m) = Self::parse_entry_header(slice, offset).ok()?;
      if fast_key_eq(m, member) {
        return Some(rank);
      }
      offset += entry_len;
    }

    None
  }

  /// 获取指定排名的元素切片，越界返回 None
  pub fn key_at_rank(slice: &[u8], rank: usize) -> Option<&[u8]> {
    if slice.len() < COMPACT_ZSET_COUNT_SIZE {
      return None;
    }
    let count = u16::from_be_bytes([slice[0], slice[1]]) as usize;
    if rank >= count {
      return None;
    }

    let mut offset = COMPACT_ZSET_COUNT_SIZE;
    for cur_rank in 0..count {
      let (entry_len, _, m) = Self::parse_entry_header(slice, offset).ok()?;
      if cur_rank == rank {
        return Some(m);
      }
      offset += entry_len;
    }

    None
  }

  /// 获取指定排名的条目视图
  pub fn entry_at_rank<'a>(slice: &'a [u8], rank: usize) -> Option<ZSetEntryRef<'a>> {
    if slice.len() < COMPACT_ZSET_COUNT_SIZE {
      return None;
    }
    let count = u16::from_be_bytes([slice[0], slice[1]]) as usize;
    if rank >= count {
      return None;
    }

    let mut offset = COMPACT_ZSET_COUNT_SIZE;
    for cur_rank in 0..count {
      let (entry_len, entry) = Self::parse_entry(slice, offset).ok()?;
      if cur_rank == rank {
        return Some(entry);
      }
      offset += entry_len;
    }

    None
  }

  /// 获取成员的分数值
  pub fn score_of(slice: &[u8], member: &[u8]) -> Option<f64> {
    if slice.len() < COMPACT_ZSET_COUNT_SIZE {
      return None;
    }
    let count = u16::from_be_bytes([slice[0], slice[1]]) as usize;
    let mut offset = COMPACT_ZSET_COUNT_SIZE;

    for _ in 0..count {
      let (entry_len, order_score, m) = Self::parse_entry_header(slice, offset).ok()?;
      if fast_key_eq(m, member) {
        let score = decode_order_preserving_f64(order_score);
        return Some(score);
      }
      offset += entry_len;
    }

    None
  }

  /// 插入或更新成员分值（新插入返回 true，更新已存在元素分值返回 false）
  pub fn insert(buf: &mut Vec<u8>, score: f64, member: &[u8]) -> Result<bool> {
    if member.len() > u16::MAX as usize {
      return Err(Error::KeyLengthOverflow(member.len()));
    }

    if buf.is_empty() {
      buf.extend_from_slice(&0u16.to_be_bytes());
    } else if buf.len() < COMPACT_ZSET_COUNT_SIZE {
      return Err(Error::BufferTooShort {
        expected: COMPACT_ZSET_COUNT_SIZE,
        actual: buf.len(),
      });
    }

    let order_score = encode_order_preserving_f64(score);

    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if count == 0 {
      // 防御：收敛到纯计数前缀，杜绝残留脏字节破坏紧凑布局
      buf.truncate(COMPACT_ZSET_COUNT_SIZE);
    }

    const STACK_CAP: usize = 256;
    let mut stack_offsets = [0usize; STACK_CAP];
    let mut existing_found = None;
    let mut offset = COMPACT_ZSET_COUNT_SIZE;

    if count <= STACK_CAP {
      for slot in stack_offsets.iter_mut().take(count) {
        *slot = offset;
        let (entry_len, entry_order, m) = Self::parse_entry_header(buf, offset)?;
        if fast_key_eq(m, member) {
          existing_found = Some((offset, entry_len, entry_order));
          break;
        }
        offset += entry_len;
      }
    } else {
      for _ in 0..count {
        let (entry_len, entry_order, m) = Self::parse_entry_header(buf, offset)?;
        if fast_key_eq(m, member) {
          existing_found = Some((offset, entry_len, entry_order));
          break;
        }
        offset += entry_len;
      }
    }

    let is_new = if let Some((old_offset, old_len, old_order_score)) = existing_found {
      if old_order_score == order_score {
        return Ok(false);
      }
      let old_buf_len = buf.len();
      buf.copy_within(old_offset + old_len..old_buf_len, old_offset);
      buf.truncate(old_buf_len - old_len);
      let new_count = (count - 1) as u16;
      buf[0..COMPACT_ZSET_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
      false
    } else {
      true
    };

    let count_after_del = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if count_after_del >= u16::MAX as usize {
      return Err(Error::CompactCountOverflow(count_after_del + 1));
    }

    let insert_offset = if count_after_del == 0 {
      buf.len()
    } else if is_new && count_after_del <= STACK_CAP {
      let insert_idx = match Self::binary_search_offsets(
        buf,
        &stack_offsets[..count_after_del],
        order_score,
        member,
      )? {
        Ok(idx) | Err(idx) => idx,
      };
      if insert_idx == count_after_del {
        buf.len()
      } else {
        stack_offsets[insert_idx]
      }
    } else {
      Self::collect_offsets(buf, count_after_del, |offsets| {
        Self::binary_search_offsets(buf, offsets, order_score, member).map(|found| {
          let idx = match found {
            Ok(idx) | Err(idx) => idx,
          };
          if idx == count_after_del {
            buf.len()
          } else {
            offsets[idx]
          }
        })
      })??
    };

    let new_entry_len = COMPACT_ZSET_ENTRY_HEADER_SIZE + member.len();
    let old_len = buf.len();
    buf.reserve(new_entry_len);

    let [s0, s1, s2, s3, s4, s5, s6, s7] = order_score;
    let [l0, l1] = (member.len() as u16).to_be_bytes();
    let entry_header = [s0, s1, s2, s3, s4, s5, s6, s7, l0, l1];

    if insert_offset == old_len {
      buf.extend_from_slice(&entry_header);
      buf.extend_from_slice(member);
    } else {
      // 安全性保证：上方 reserve(new_entry_len) 已确保容量 >= old_len + new_entry_len，
      // [insert_offset, old_len) 为已初始化字节；memmove 腾位后原位写入新条目，全程不越界
      unsafe {
        let p = buf.as_mut_ptr();
        ptr::copy(
          p.add(insert_offset),
          p.add(insert_offset + new_entry_len),
          old_len - insert_offset,
        );
        ptr::copy_nonoverlapping(
          entry_header.as_ptr(),
          p.add(insert_offset),
          COMPACT_ZSET_ENTRY_HEADER_SIZE,
        );
        ptr::copy_nonoverlapping(
          member.as_ptr(),
          p.add(insert_offset + COMPACT_ZSET_ENTRY_HEADER_SIZE),
          member.len(),
        );
        buf.set_len(old_len + new_entry_len);
      }
    }

    let final_count = (count_after_del + 1) as u16;
    buf[0..COMPACT_ZSET_COUNT_SIZE].copy_from_slice(&final_count.to_be_bytes());

    Ok(is_new)
  }

  /// 删除指定成员（存在并删除返回 true，不存在返回 false）
  pub fn remove(buf: &mut Vec<u8>, member: &[u8]) -> Result<bool> {
    if buf.len() < COMPACT_ZSET_COUNT_SIZE {
      return Ok(false);
    }
    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if count == 0 {
      return Ok(false);
    }

    let mut offset = COMPACT_ZSET_COUNT_SIZE;
    for _ in 0..count {
      let (entry_len, _, m) = Self::parse_entry_header(buf, offset)?;
      if fast_key_eq(m, member) {
        // 尾部整体前移覆盖被删条目，随后物理收缩（与紧凑哈希删除路径一致的安全 API 实现）
        buf.copy_within(offset + entry_len.., offset);
        buf.truncate(buf.len() - entry_len);
        let new_count = (count - 1) as u16;
        buf[0..COMPACT_ZSET_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
        return Ok(true);
      }
      offset += entry_len;
    }

    Ok(false)
  }

  /// 读取指定偏移处的 8 字节保序分值（偏移由 [Self::collect_offsets] 产生，恒不越界）
  #[inline(always)]
  fn score_at(slice: &[u8], off: usize) -> [u8; COMPACT_ZSET_SCORE_SIZE] {
    unsafe { (slice.as_ptr().add(off) as *const [u8; COMPACT_ZSET_SCORE_SIZE]).read_unaligned() }
  }

  /// 单趟构建偏移并二分定位分值区间，返回 `(区间起始字节偏移, 区间内元素个数)`
  ///
  /// 复用给 [Self::count_score_range] 与 [Self::range_with_options]，消除重复的左右边界搜索；
  /// 时间复杂度 O(N) 偏移构建 + 两轮 O(log N) 边界二分，空间复杂度 O(1)（栈优先）。
  fn score_range_span(
    slice: &[u8],
    count: usize,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
  ) -> Result<(usize, usize)> {
    let min_order = encode_order_preserving_f64(min);
    let max_order = encode_order_preserving_f64(max);
    if min_order > max_order || (min_order == max_order && (!min_inclusive || !max_inclusive)) {
      return Ok((slice.len(), 0));
    }

    Self::collect_offsets(slice, count, |offsets| {
      // 1. 左边界：首个满足下界的条目索引
      let mut low = 0;
      let mut high = count;
      while low < high {
        let mid = (low + high) / 2;
        let entry_order = Self::score_at(slice, offsets[mid]);
        let satisfies = if min_inclusive {
          entry_order >= min_order
        } else {
          entry_order > min_order
        };
        if !satisfies {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      let start_idx = low;

      // 2. 从 start_idx 起寻找右边界：首个超出上界的条目索引
      let mut right_low = start_idx;
      let mut high = count;
      while right_low < high {
        let mid = (right_low + high) / 2;
        let entry_order = Self::score_at(slice, offsets[mid]);
        let exceeds = if max_inclusive {
          entry_order > max_order
        } else {
          entry_order >= max_order
        };
        if !exceeds {
          right_low = mid + 1;
        } else {
          high = mid;
        }
      }
      let end_idx = right_low;

      let start_offset = if start_idx < count {
        offsets[start_idx]
      } else {
        slice.len()
      };
      (start_offset, end_idx.saturating_sub(start_idx))
    })
  }

  /// 区间分值元素计数（闭区间 [min, max]）
  #[inline(always)]
  pub fn count_range(slice: &[u8], min: f64, max: f64) -> usize {
    Self::count_score_range(slice, min, true, max, true)
  }

  /// 区间分值元素计数（支持开闭区间控制；O(N) 偏移构建 + O(log N) 边界二分）
  pub fn count_score_range(
    slice: &[u8],
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
  ) -> usize {
    if slice.len() < COMPACT_ZSET_COUNT_SIZE {
      return 0;
    }
    let count = match Self::count(slice) {
      Ok(c) if c > 0 => c,
      _ => return 0,
    };

    Self::score_range_span(slice, count, min, min_inclusive, max, max_inclusive)
      .map_or(0, |(_, remaining)| remaining)
  }

  /// 获取指定分值范围的切片流式迭代器（闭区间 [min, max]）
  #[inline(always)]
  pub fn range(slice: &[u8], min: f64, max: f64) -> CompactZSetIter<'_> {
    Self::range_with_options(slice, min, true, max, true)
  }

  /// 获取指定分值范围的切片流式迭代器（支持开闭区间控制）
  pub fn range_with_options(
    slice: &[u8],
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
  ) -> CompactZSetIter<'_> {
    let empty_iter = CompactZSetIter {
      slice,
      offset: slice.len(),
      remaining: 0,
    };

    if slice.len() < COMPACT_ZSET_COUNT_SIZE {
      return empty_iter;
    }
    let count = match Self::count(slice) {
      Ok(c) if c > 0 => c,
      _ => return empty_iter,
    };

    match Self::score_range_span(slice, count, min, min_inclusive, max, max_inclusive) {
      Ok((start_offset, remaining)) => CompactZSetIter {
        slice,
        offset: start_offset,
        remaining,
      },
      Err(_) => empty_iter,
    }
  }

  /// 获取全量成员流式切片迭代器
  #[inline]
  pub fn iter_members(slice: &[u8]) -> CompactZSetIter<'_> {
    let count = if slice.len() >= COMPACT_ZSET_COUNT_SIZE {
      u16::from_be_bytes([slice[0], slice[1]]) as usize
    } else {
      0
    };
    CompactZSetIter {
      slice,
      offset: COMPACT_ZSET_COUNT_SIZE,
      remaining: count,
    }
  }

  /// 批量编码有序集合（同成员后写覆盖先写，单次排序 O(N log N) 批量构建）
  pub fn encode<'a, I>(entries: I) -> Result<Vec<u8>>
  where
    I: IntoIterator<Item = (f64, &'a [u8])>,
  {
    let mut items: Vec<([u8; COMPACT_ZSET_SCORE_SIZE], &[u8])> = entries
      .into_iter()
      .map(|(score, member)| {
        if member.len() > u16::MAX as usize {
          return Err(Error::KeyLengthOverflow(member.len()));
        }
        Ok((encode_order_preserving_f64(score), member))
      })
      .collect::<Result<_>>()?;

    // 按成员稳定排序后保留每组最后一次出现的分值（对齐逐条 insert 的覆盖语义）
    items.sort_by(|a, b| a.1.cmp(b.1));
    items.reverse();
    items.dedup_by(|a, b| a.1 == b.1);
    items.reverse();

    if items.len() > u16::MAX as usize {
      return Err(Error::CompactCountOverflow(items.len()));
    }

    // 最终排列：保序分值优先、成员字典序次之
    items.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));

    let payload_len: usize = items
      .iter()
      .map(|(_, m)| COMPACT_ZSET_ENTRY_HEADER_SIZE + m.len())
      .sum();
    let mut buf = Vec::with_capacity(COMPACT_ZSET_COUNT_SIZE + payload_len);
    buf.extend_from_slice(&(items.len() as u16).to_be_bytes());
    for (order_score, member) in items {
      let [s0, s1, s2, s3, s4, s5, s6, s7] = order_score;
      let [l0, l1] = (member.len() as u16).to_be_bytes();
      buf.extend_from_slice(&[s0, s1, s2, s3, s4, s5, s6, s7, l0, l1]);
      buf.extend_from_slice(member);
    }

    Ok(buf)
  }
}

/// 紧凑连续内存有序集合容器（拥有所有权）
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CompactZSet {
  raw: Vec<u8>,
}

impl CompactZSet {
  /// 创建新的空紧凑有序集合
  #[inline]
  pub fn new() -> Self {
    Self {
      raw: vec![0u8; COMPACT_ZSET_COUNT_SIZE],
    }
  }

  /// 创建指定预分配容量的紧凑有序集合
  #[inline]
  pub fn with_capacity(cap: usize) -> Self {
    let mut raw = Vec::with_capacity(cap.max(COMPACT_ZSET_COUNT_SIZE));
    raw.extend_from_slice(&0u16.to_be_bytes());
    Self { raw }
  }

  /// 从已有字节切片解析构建
  #[inline]
  pub fn from_vec(raw: Vec<u8>) -> Result<Self> {
    if raw.len() < COMPACT_ZSET_COUNT_SIZE {
      return Err(Error::BufferTooShort {
        expected: COMPACT_ZSET_COUNT_SIZE,
        actual: raw.len(),
      });
    }
    let _ = CompactZSetCodec::validate(&raw)?;
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
    CompactZSetCodec::count(&self.raw).unwrap_or(0)
  }

  /// 是否为空
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// 获取成员排名
  #[inline(always)]
  pub fn rank_of(&self, member: &[u8]) -> Option<usize> {
    CompactZSetCodec::rank_of(&self.raw, member)
  }

  /// 按排名获取成员切片
  #[inline(always)]
  pub fn key_at_rank(&self, rank: usize) -> Option<&[u8]> {
    CompactZSetCodec::key_at_rank(&self.raw, rank)
  }

  /// 获取成员分值
  #[inline(always)]
  pub fn score_of(&self, member: &[u8]) -> Option<f64> {
    CompactZSetCodec::score_of(&self.raw, member)
  }

  /// 插入或更新成员分值
  #[inline(always)]
  pub fn insert(&mut self, score: f64, member: &[u8]) -> Result<bool> {
    CompactZSetCodec::insert(&mut self.raw, score, member)
  }

  /// 删除成员
  #[inline(always)]
  pub fn remove(&mut self, member: &[u8]) -> Result<bool> {
    CompactZSetCodec::remove(&mut self.raw, member)
  }

  /// 分值范围计数
  #[inline(always)]
  pub fn count_range(&self, min: f64, max: f64) -> usize {
    CompactZSetCodec::count_range(&self.raw, min, max)
  }

  /// 分值区间计数（支持开闭区间控制）
  #[inline(always)]
  pub fn count_score_range(
    &self,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
  ) -> usize {
    CompactZSetCodec::count_score_range(&self.raw, min, min_inclusive, max, max_inclusive)
  }

  /// 分值范围流式切片迭代
  #[inline(always)]
  pub fn range(&self, min: f64, max: f64) -> CompactZSetIter<'_> {
    CompactZSetCodec::range(&self.raw, min, max)
  }

  /// 分值区间切片流式迭代（支持开闭区间控制）
  #[inline(always)]
  pub fn range_with_options(
    &self,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
  ) -> CompactZSetIter<'_> {
    CompactZSetCodec::range_with_options(&self.raw, min, min_inclusive, max, max_inclusive)
  }

  /// 全量迭代
  #[inline(always)]
  pub fn iter_members(&self) -> CompactZSetIter<'_> {
    CompactZSetCodec::iter_members(&self.raw)
  }

  /// 从字节切片创建
  #[inline]
  pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
    Self::from_vec(bytes.to_vec())
  }

  /// 获取底层切片别名
  #[inline(always)]
  pub fn as_bytes(&self) -> &[u8] {
    self.as_slice()
  }

  /// 元素数量别名 (ZCARD)
  #[inline(always)]
  pub fn zcard(&self) -> usize {
    self.len()
  }

  /// 成员排名 (ZRANK)
  #[inline(always)]
  pub fn zrank(&self, member: &[u8]) -> Option<usize> {
    self.rank_of(member)
  }

  /// 成员降序排名 (ZREVRANK)
  #[inline(always)]
  pub fn zrevrank(&self, member: &[u8]) -> Option<usize> {
    self.rank_of(member).map(|r| self.len() - 1 - r)
  }

  /// 成员分值 (ZSCORE)
  #[inline(always)]
  pub fn zscore(&self, member: &[u8]) -> Option<f64> {
    self.score_of(member)
  }

  /// 清空集合并重置为初始空状态
  #[inline]
  pub fn clear(&mut self) {
    self.raw.clear();
    self.raw.extend_from_slice(&0u16.to_be_bytes());
    self.raw.shrink_to_fit();
  }

  /// 弹出最小元素 (ZPOPMIN 1)
  pub fn pop_min(&mut self) -> Option<(Vec<u8>, f64)> {
    if self.is_empty() {
      return None;
    }
    let (entry_len, entry) =
      CompactZSetCodec::parse_entry(&self.raw, COMPACT_ZSET_COUNT_SIZE).ok()?;
    let member = entry.member.to_vec();
    let score = entry.score;
    self.raw.copy_within(
      COMPACT_ZSET_COUNT_SIZE + entry_len..,
      COMPACT_ZSET_COUNT_SIZE,
    );
    self.raw.truncate(self.raw.len() - entry_len);
    let new_count = (self.len() - 1) as u16;
    self.raw[0..COMPACT_ZSET_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
    Some((member, score))
  }

  /// 弹出最大元素 (ZPOPMAX 1)
  pub fn pop_max(&mut self) -> Option<(Vec<u8>, f64)> {
    let count = self.len();
    if count == 0 {
      return None;
    }
    let last_offset =
      CompactZSetCodec::collect_offsets(&self.raw, count, |offsets| offsets[count - 1]).ok()?;
    let (_, entry) = CompactZSetCodec::parse_entry(&self.raw, last_offset).ok()?;
    let member = entry.member.to_vec();
    let score = entry.score;
    self.raw.truncate(last_offset);
    let new_count = (count - 1) as u16;
    self.raw[0..COMPACT_ZSET_COUNT_SIZE].copy_from_slice(&new_count.to_be_bytes());
    Some((member, score))
  }

  /// 按排名范围获取元素 (支持 Redis 负数索引及反转)
  pub fn zrange(&self, start: isize, stop: isize, rev: bool) -> Vec<(Vec<u8>, f64)> {
    let len = self.len() as isize;
    if len == 0 {
      return Vec::new();
    }
    let actual_start = if start < 0 {
      len.saturating_add(start).max(0)
    } else {
      start
    };
    let actual_stop = if stop < 0 {
      len.saturating_add(stop)
    } else {
      stop
    };
    if actual_start >= len || actual_start > actual_stop {
      return Vec::new();
    }
    let actual_start = actual_start as usize;
    let actual_stop = (actual_stop.min(len - 1)) as usize;
    let total_len = len as usize;
    let mut items = Vec::with_capacity(actual_stop - actual_start + 1);
    let _ = CompactZSetCodec::collect_offsets(&self.raw, total_len, |offsets| {
      if rev {
        let rev_start = total_len - 1 - actual_start;
        let rev_stop = total_len - 1 - actual_stop;
        for &offset in offsets[rev_stop..=rev_start].iter().rev() {
          if let Ok((_, entry)) = CompactZSetCodec::parse_entry(&self.raw, offset) {
            items.push((entry.member.to_vec(), entry.score));
          }
        }
      } else {
        for &offset in &offsets[actual_start..=actual_stop] {
          if let Ok((_, entry)) = CompactZSetCodec::parse_entry(&self.raw, offset) {
            items.push((entry.member.to_vec(), entry.score));
          }
        }
      }
    });
    items
  }

  /// Bitcode 极速编码
  #[inline]
  pub fn to_bitcode(&self) -> Vec<u8> {
    bitcode::encode(&self.raw)
  }

  /// Bitcode 极速解码
  #[inline]
  pub fn from_bitcode(bytes: &[u8]) -> Result<Self> {
    let raw: Vec<u8> = bitcode::decode(bytes)?;
    Self::from_vec(raw)
  }
}

impl<'a> IntoIterator for &'a CompactZSet {
  type Item = ZSetEntryRef<'a>;
  type IntoIter = CompactZSetIter<'a>;

  #[inline(always)]
  fn into_iter(self) -> Self::IntoIter {
    self.iter_members()
  }
}

impl Deref for CompactZSet {
  type Target = [u8];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    &self.raw
  }
}

impl AsRef<[u8]> for CompactZSet {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    &self.raw
  }
}
