//! AOF 地址向量：固定容量的子日志地址集合，支持序列化、比较与原子式更新
//! （对标 libs/server/AOF/AofAddress.cs:AofAddress）。
//!
//! C# 为 `fixed long addresses[4]` + 长度字节；Rust 以 `[i64; MAX_SUBLOG_COUNT]`
//! + `length` 承接，序列化布局逐字节一致（`[length u8][addresses i64 × length]`）。

use std::ops::{Index, IndexMut};

/// 单个地址字节数（8B LE）。
pub const AOF_ADDRESS_BYTES: usize = size_of::<i64>();

/// 支持的最大物理子日志数。
pub const MAX_SUBLOG_COUNT: usize = 4;

/// AOF 操作使用的固定尺寸地址集合。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofAddress {
  /// 有效地址数（1..=MAX_SUBLOG_COUNT）。
  length: u8,
  /// 地址槽位。
  addresses: [i64; MAX_SUBLOG_COUNT],
}

impl Default for AofAddress {
  fn default() -> Self {
    Self::create(1, 0)
  }
}

impl AofAddress {
  /// libs/server/AOF/AofAddress.cs:AofAddress(length)（构造）。
  pub fn new(length: i32) -> Self {
    let length = length.clamp(0, MAX_SUBLOG_COUNT as i32) as u8;
    Self {
      length,
      addresses: [0; MAX_SUBLOG_COUNT],
    }
  }

  /// 有效长度（C# `Length` 属性）。
  pub fn length(&self) -> i32 {
    i32::from(self.length)
  }

  /// 序列化字节长度：`1 + 8 * length`（C# `Span` 长度）。
  pub fn span_len(&self) -> usize {
    1 + AOF_ADDRESS_BYTES * self.length as usize
  }

  /// 下标读写（越界 panic 由调用契约排除；安全接口返回 Option）。
  pub fn get(&self, i: usize) -> Option<i64> {
    self.addresses.get(i).copied()
  }

  pub fn set(&mut self, i: usize, value: i64) {
    if i < MAX_SUBLOG_COUNT {
      self.addresses[i] = value;
    }
  }
}

impl Index<usize> for AofAddress {
  type Output = i64;

  #[inline]
  fn index(&self, index: usize) -> &Self::Output {
    &self.addresses[index]
  }
}

impl IndexMut<usize> for AofAddress {
  #[inline]
  fn index_mut(&mut self, index: usize) -> &mut Self::Output {
    &mut self.addresses[index]
  }
}

impl AofAddress {
  /// libs/server/AOF/AofAddress.cs:Equals
  ///
  /// 地址序列逐位相等（长度必须一致）。
  pub fn equals(&self, other: &AofAddress) -> bool {
    self.length == other.length
      && self.addresses[..self.length as usize] == other.addresses[..other.length as usize]
  }

  /// libs/server/AOF/AofAddress.cs:FromSpan
  ///
  /// 从字节切片按 i64 LE 序读取（长度 = 字节数 >> 3）。
  pub fn from_span(span: &[u8]) -> Self {
    let length = (span.len() >> 3).min(MAX_SUBLOG_COUNT);
    let mut result = AofAddress::new(length as i32);
    for (i, chunk) in span
      .as_chunks::<AOF_ADDRESS_BYTES>()
      .0
      .iter()
      .take(length)
      .enumerate()
    {
      result.addresses[i] = i64::from_le_bytes(*chunk);
    }
    result
  }

  /// 序列化为字节向量：`[length u8][i64 LE × length]`（C# Serialize）。
  pub fn serialize(&self) -> Vec<u8> {
    let mut out = Vec::with_capacity(self.span_len() + 1);
    out.push(self.length);
    for &addr in &self.addresses[..self.length as usize] {
      out.extend_from_slice(&addr.to_le_bytes());
    }
    out
  }

  /// [`AofAddress::serialize`] 的逆操作（C# Deserialize；`data` 不含长度字节时
  /// 按既有字节解析到可用范围）。
  pub fn deserialize(data: &[u8]) -> Self {
    let Some(&length) = data.first() else {
      return Self::default();
    };
    let mut result = AofAddress::new(i32::from(length.min(MAX_SUBLOG_COUNT as u8)));
    for (i, chunk) in data[1..]
      .as_chunks::<AOF_ADDRESS_BYTES>()
      .0
      .iter()
      .take(length as usize)
      .enumerate()
    {
      result.addresses[i] = i64::from_le_bytes(*chunk);
    }
    result
  }

  /// 逗号分隔的有效地址串（C# ToString）。
  pub fn to_aof_string(&self) -> String {
    let len = self.length as usize;
    if len == 0 {
      return String::new();
    }
    let mut sb = String::new();
    let mut itoa_buf = itoa::Buffer::new();
    sb.push_str(itoa_buf.format(self.addresses[0]));
    for &addr in &self.addresses[1..len] {
      sb.push(',');
      sb.push_str(itoa_buf.format(addr));
    }
    sb
  }

  /// libs/server/AOF/AofAddress.cs:FromString
  ///
  /// 解析逗号分隔的地址串；非法字符返回 None（C# 抛 FormatException）。
  pub fn from_string(input: &str) -> Option<Self> {
    if input.is_empty() {
      return Some(Self {
        length: 0,
        addresses: [0; MAX_SUBLOG_COUNT],
      });
    }
    // 逗号数决定槽位数。
    let count = 1 + input.bytes().filter(|&b| b == b',').count();
    if count > MAX_SUBLOG_COUNT {
      return None;
    }
    let mut result = AofAddress::new(count as i32);
    let mut idx = 0usize;
    let mut value = 0i64;
    let mut negative = false;
    for c in input.bytes() {
      match c {
        b',' => {
          result.set(idx, if negative { -value } else { value });
          idx += 1;
          value = 0;
          negative = false;
        }
        b'0'..=b'9' => value = value.wrapping_mul(10).wrapping_add(i64::from(c - b'0')),
        b'-' => negative = true,
        _ => return None,
      }
    }
    result.set(idx, if negative { -value } else { value });
    Some(result)
  }

  /// libs/server/AOF/AofAddress.cs:SetValueIf
  ///
  /// 全部等于 `comparand` 的槽位替换为 `value`。
  pub fn set_value_if(&mut self, value: i64, comparand: i64) {
    for addr in &mut self.addresses[..self.length as usize] {
      if *addr == comparand {
        *addr = value;
      }
    }
  }

  /// libs/server/AOF/AofAddress.cs:SetValue
  ///
  /// 全部槽位替换为 `value`。
  pub fn set_value(&mut self, value: i64) {
    for addr in &mut self.addresses[..self.length as usize] {
      *addr = value;
    }
  }

  /// libs/server/AOF/AofAddress.cs:Create
  ///
  /// 分配指定长度并填 `value`。
  pub fn create(length: i32, value: i64) -> Self {
    let mut result = AofAddress::new(length);
    result.set_value(value);
    result
  }

  /// libs/server/AOF/AofAddress.cs:Min
  ///
  /// 逐槽位取两输入的较小值。
  pub fn min(a: &AofAddress, b: &AofAddress) -> AofAddress {
    let mut result = AofAddress::new(a.length());
    let len = (a.length as usize).min(b.length as usize);
    for ((r, &x), &y) in result.addresses[..len]
      .iter_mut()
      .zip(&a.addresses[..len])
      .zip(&b.addresses[..len])
    {
      *r = x.min(y);
    }
    result
  }

  /// libs/server/AOF/AofAddress.cs:MinExchange
  ///
  /// 逐槽位收敛到与 `address` 的较小值。
  pub fn min_exchange(&mut self, address: &AofAddress) {
    let len = (self.length as usize).min(address.length as usize);
    for (a, &b) in self.addresses[..len]
      .iter_mut()
      .zip(&address.addresses[..len])
    {
      *a = (*a).min(b);
    }
  }

  /// libs/server/AOF/AofAddress.cs:MaxExchange
  ///
  /// 逐槽位推进到与 `address` 的较大值。
  pub fn max_exchange(&mut self, address: i64) {
    for addr in &mut self.addresses[..self.length as usize] {
      *addr = (*addr).max(address);
    }
  }

  /// libs/server/AOF/AofAddress.cs:AnyLesser
  ///
  /// 任一槽位严格小于对应槽位。
  pub fn any_lesser(&self, address: &AofAddress) -> bool {
    let len = (self.length as usize).min(address.length as usize);
    self.addresses[..len]
      .iter()
      .zip(&address.addresses[..len])
      .any(|(&a, &b)| a < b)
  }

  /// libs/server/AOF/AofAddress.cs:AnyGreater
  ///
  /// 任一槽位严格大于对应槽位。
  pub fn any_greater(&self, address: &AofAddress) -> bool {
    let len = (self.length as usize).min(address.length as usize);
    self.addresses[..len]
      .iter()
      .zip(&address.addresses[..len])
      .any(|(&a, &b)| a > b)
  }

  /// libs/server/AOF/AofAddress.cs:Diff
  ///
  /// 逐槽位差值向量。
  pub fn diff(&self, other: &AofAddress) -> AofAddress {
    let mut result = AofAddress::new(other.length());
    let len = (self.length as usize).min(other.length as usize);
    for ((r, &s), &o) in result.addresses[..len]
      .iter_mut()
      .zip(&self.addresses[..len])
      .zip(&other.addresses[..len])
    {
      *r = s - o;
    }
    result
  }

  /// libs/server/AOF/AofAddress.cs:AggregateDiff
  ///
  /// 逐槽位差值求和。
  pub fn aggregate_diff(&self, aof_address: &AofAddress) -> i64 {
    let len = (self.length as usize).min(aof_address.length as usize);
    self.addresses[..len]
      .iter()
      .zip(&aof_address.addresses[..len])
      .map(|(&a, &b)| a - b)
      .sum()
  }

  /// libs/server/AOF/AofAddress.cs:EqualsAll
  ///
  /// 全部槽位逐一相等。
  pub fn equals_all(&self, input: &AofAddress) -> bool {
    let len = self.length as usize;
    if len != input.length as usize {
      return false;
    }
    self.addresses[..len] == input.addresses[..len]
  }

  /// libs/server/AOF/AofAddress.cs:IsOutOfRange
  ///
  /// 任一槽位越出 [begin, end] 区间。
  pub fn is_out_of_range(&self, begin: &AofAddress, end: &AofAddress) -> bool {
    let len = self.length as usize;
    self.addresses[..len]
      .iter()
      .zip(&begin.addresses[..len])
      .zip(&end.addresses[..len])
      .any(|((&a, &b), &e)| a < b || a > e)
  }

  /// libs/server/AOF/AofAddress.cs:Max
  ///
  /// 最大槽位值（下界 0）。
  pub fn max(&self) -> i64 {
    self.addresses[..self.length as usize]
      .iter()
      .copied()
      .fold(0i64, i64::max)
  }

  /// 最小槽位值（上界 0）。
  pub fn min_value(&self) -> i64 {
    self.addresses[..self.length as usize]
      .iter()
      .copied()
      .fold(0i64, i64::min)
  }
}

#[cfg(test)]
mod tests {
  use super::AofAddress;

  #[test]
  fn serialize_roundtrip() {
    let mut a = AofAddress::create(3, 100);
    a.set(1, -5);
    a.set(2, 123456789);
    let bytes = a.serialize();
    assert_eq!(bytes[0], 3);
    assert_eq!(AofAddress::deserialize(&bytes), a);
    // span 形态（无长度字节）。
    assert_eq!(AofAddress::from_span(&bytes[1..]), a);
  }

  #[test]
  fn string_roundtrip_and_rejects() {
    let a = AofAddress::from_string("1,-2,300").unwrap();
    assert_eq!(a.length(), 3);
    assert_eq!(a.get(1), Some(-2));
    assert_eq!(a.to_aof_string(), "1,-2,300");
    assert!(AofAddress::from_string("1,x,3").is_none());
  }

  #[test]
  fn compare_and_range_ops() {
    let a = AofAddress::create(2, 100);
    let b = AofAddress::create(2, 150);
    assert!(a.any_lesser(&b));
    assert!(b.any_greater(&a));
    assert_eq!(a.diff(&b).get(0), Some(-50));
    assert_eq!(a.aggregate_diff(&b), -100);
    assert_eq!(b.max(), 150);
    assert_eq!(b.min_value(), 0);

    let mut c = a;
    c.set_value_if(999, 100);
    assert_eq!(c.get(0), Some(999));
    c.set_value(7);
    assert!(c.equals_all(&AofAddress::create(2, 7)));

    let mut d = AofAddress::create(2, 200);
    d.min_exchange(&AofAddress::create(2, 120));
    assert_eq!(d.get(0), Some(120));
    assert_eq!(d.get(1), Some(120));

    let begin = AofAddress::create(2, 50);
    let end = AofAddress::create(2, 150);
    assert!(!d.is_out_of_range(&begin, &end));
    let out = AofAddress::create(2, 200);
    assert!(out.is_out_of_range(&begin, &end));
  }
}
