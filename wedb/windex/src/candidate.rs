use std::{iter::Chain, ops::Index, slice::Iter};

/// 栈上候选地址小列表，避免热点查询时的高频堆分配（内嵌 8 槽位 64B 数组）
#[derive(Debug)]
pub struct CandidateAddresses {
  buf: [u64; 8],
  len: u8,
  extra: Vec<u64>,
}

impl Default for CandidateAddresses {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl CandidateAddresses {
  /// 构造一个全空的候选地址小列表
  #[inline]
  pub const fn new() -> Self {
    Self {
      buf: [0; 8],
      len: 0,
      extra: Vec::new(),
    }
  }

  /// 追加一个候选逻辑地址
  #[inline]
  pub fn push(&mut self, addr: u64) {
    if (self.len as usize) < self.buf.len() {
      self.buf[self.len as usize] = addr;
      self.len += 1;
    } else {
      self.extra.push(addr);
    }
  }

  /// 检查列表是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// 获取候选地址总数
  #[inline]
  pub fn len(&self) -> usize {
    (self.len as usize) + self.extra.len()
  }

  /// 获取候选地址双端迭代器（栈数组优先，链上溢出堆切片）
  #[inline]
  pub fn iter(&self) -> impl DoubleEndedIterator<Item = &u64> {
    self.buf[..self.len as usize]
      .iter()
      .chain(self.extra.iter())
  }

  /// 判定是否包含指定逻辑地址
  #[inline]
  pub fn contains(&self, addr: u64) -> bool {
    self.buf[..self.len as usize].contains(&addr) || self.extra.contains(&addr)
  }

  /// 按地址降序快速排列（保证优先匹配最新写入的逻辑地址）
  #[inline]
  pub fn sort_descending(&mut self) {
    if self.extra.is_empty() {
      let len = self.len as usize;
      match len {
        0 | 1 => {}
        2 => {
          if self.buf[0] < self.buf[1] {
            self.buf.swap(0, 1);
          }
        }
        _ => {
          self.buf[..len].sort_unstable_by(|a, b| b.cmp(a));
        }
      }
    } else {
      let buf_len = self.len as usize;
      self.extra.reserve(buf_len);
      self.extra.extend_from_slice(&self.buf[..buf_len]);
      self.extra.sort_unstable_by(|a, b| b.cmp(a));
      let main_len = self.buf.len().min(self.extra.len());
      self.buf[..main_len].copy_from_slice(&self.extra[..main_len]);
      self.extra.drain(..main_len);
      self.len = main_len as u8;
    }
  }
}

impl Index<usize> for CandidateAddresses {
  type Output = u64;

  #[inline]
  fn index(&self, idx: usize) -> &Self::Output {
    let len = self.len as usize;
    if idx < len {
      &self.buf[idx]
    } else {
      &self.extra[idx - len]
    }
  }
}

impl PartialEq for CandidateAddresses {
  fn eq(&self, other: &Self) -> bool {
    self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
  }
}

impl Eq for CandidateAddresses {}

/// 候选地址拥有权迭代器
///
/// 由 [`CandidateAddresses::into_iter`] 产出；crate 根不导出（opaque 迭代器，
/// 调用方经 `IntoIterator` 使用，无需命名本类型）。
pub struct CandidateAddressesIntoIter {
  candidates: CandidateAddresses,
  start: usize,
  end: usize,
}

impl Iterator for CandidateAddressesIntoIter {
  type Item = u64;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.start >= self.end {
      return None;
    }
    let val = self.candidates[self.start];
    self.start += 1;
    Some(val)
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    let rem = self.end - self.start;
    (rem, Some(rem))
  }
}

impl ExactSizeIterator for CandidateAddressesIntoIter {}

impl IntoIterator for CandidateAddresses {
  type Item = u64;
  type IntoIter = CandidateAddressesIntoIter;

  #[inline]
  fn into_iter(self) -> Self::IntoIter {
    let end = self.len();
    CandidateAddressesIntoIter {
      candidates: self,
      start: 0,
      end,
    }
  }
}

impl<'a> IntoIterator for &'a CandidateAddresses {
  type Item = &'a u64;
  type IntoIter = Chain<Iter<'a, u64>, Iter<'a, u64>>;

  #[inline]
  fn into_iter(self) -> Self::IntoIter {
    self.buf[..self.len as usize]
      .iter()
      .chain(self.extra.iter())
  }
}
