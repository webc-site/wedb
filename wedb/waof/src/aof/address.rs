//! AOF 地址向量：固定容量的子日志地址集合，支持比较与原子式更新
//! （对标 libs/server/AOF/AofAddress.cs:AofAddress）。
//!
//! C# 为 `fixed long addresses[4]` 加长度字节；Rust 以
//! `[i64; MAX_SUBLOG_COUNT]` 加 `length` 承接。
//!
//! 编码面：bitcode 结构体编码（复制历史 / 检查点条目等结构体内嵌持久化）、
//! 裸 8B LE 字节切片（from_span，对标 C# FromSpan，复制命令线格式）与
//! 逗号分隔文本（from_string / to_aof_string，对标 C# FromString /
//! ToString，RESP 参数面）。独立的带长度字节 byte[] 形态
//! （C# Serialize/Deserialize）无消费面，不落地。

use std::ops::{Index, IndexMut};

use itoa::Buffer;

/// 单个地址字节数（8B LE）。
pub const AOF_ADDRESS_BYTES: usize = size_of::<i64>();

/// 支持的最大物理子日志数。
pub const MAX_SUBLOG_COUNT: usize = 4;

/// AOF 操作使用的固定尺寸地址集合。
#[derive(Debug, Clone, Copy, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
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

  /// libs/server/AOF/AofAddress.cs:ToString
  ///
  /// 逗号分隔的有效地址串。
  pub fn to_aof_string(&self) -> String {
    let len = self.length as usize;
    if len == 0 {
      return String::new();
    }
    let mut sb = String::new();
    let mut itoa_buf = Buffer::new();
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

  /// libs/server/AOF/AofAddress.cs:MaxExchange
  ///
  /// 逐槽位推进到与 `address` 的较大值。
  pub fn max_exchange(&mut self, address: i64) {
    for addr in &mut self.addresses[..self.length as usize] {
      *addr = (*addr).max(address);
    }
  }

  /// libs/server/AOF/AofAddress.cs:MonotonicUpdate
  ///
  /// 逐槽位单调推进（取大才写）：位点向量的防回退单点，判据方向即「新值严格
  /// 大于当前值才落写」，调用点不得再内联复刻。长度口径取本向量 `length`，
  /// 越出 `update.length` 的槽位不写——与调用侧既有「取不到槽位即跳过」等价
  /// （位点向量恒非负且只升不降，缺槽按 0 参与比较必然不成立）
  pub fn monotonic_update(&mut self, update: &AofAddress) {
    let shared = (self.length as usize).min(update.length as usize);
    for (me, &next) in self
      .addresses
      .iter_mut()
      .take(shared)
      .zip(update.addresses.iter())
    {
      if next > *me {
        *me = next;
      }
    }
  }

  /// 单槽位单调推进（对位 C# `MonotonicUpdate(long update, int physicalSublogIdx)`
  /// 重载，与 [`AofAddress::monotonic_update`] 同名同判据，故不重复挂锚）：
  /// `index` 越出有效槽位数即不写，边界口径同 [`AofAddress::set`]
  pub fn monotonic_update_slot(&mut self, value: i64, index: usize) {
    if index < self.length as usize && value > self.addresses[index] {
      self.addresses[index] = value;
    }
  }

  /// libs/server/AOF/AofAddress.cs:MinExchange
  ///
  /// 逐槽位取小推进（安全水位只降不升方向的单点判据）：越出 `address.length`
  /// 的槽位保持原值，与 [`AofAddress::monotonic_update`] 同一长度口径
  pub fn min_exchange(&mut self, address: &AofAddress) {
    let shared = (self.length as usize).min(address.length as usize);
    for (me, &other) in self
      .addresses
      .iter_mut()
      .take(shared)
      .zip(address.addresses.iter())
    {
      *me = (*me).min(other);
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
  /// 任一槽位越出 `[begin, end]` 可服务区间。
  pub fn is_out_of_range(&self, begin: &AofAddress, end: &AofAddress) -> bool {
    let len = (self.length as usize)
      .min(begin.length as usize)
      .min(end.length as usize);
    self.addresses[..len]
      .iter()
      .zip(begin.addresses[..len].iter().zip(&end.addresses[..len]))
      .any(|(&addr, (&low, &high))| addr < low || addr > high)
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
  use super::{AOF_ADDRESS_BYTES, AofAddress, MAX_SUBLOG_COUNT};

  #[test]
  fn span_roundtrip() {
    let mut a = AofAddress::create(3, 100);
    a.set(1, -5);
    a.set(2, 123456789);
    // 裸 8B LE 字节切片（生产复制线格式，无长度字节）。
    let mut bytes = Vec::with_capacity(AOF_ADDRESS_BYTES * 3);
    for i in 0..3 {
      bytes.extend_from_slice(&a[i].to_le_bytes());
    }
    assert_eq!(AofAddress::from_span(&bytes), a);
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
    assert_eq!(a.diff(&b).get(0), Some(-50));
    assert_eq!(a.aggregate_diff(&b), -100);
    assert_eq!(b.max(), 150);
    assert_eq!(b.min_value(), 0);

    let mut c = a;
    c.set_value(7);
    assert!(c.equals_all(&AofAddress::create(2, 7)));
  }

  /// 位点向量单调推进与取小推进：判据方向单点在 address.rs，回退写必须被拒
  #[test]
  fn monotonic_update_and_min_exchange() {
    let mut cur = AofAddress::create(3, 100);
    cur.set(1, 200);
    let mut next = AofAddress::create(2, 150);
    next.set(1, 180);
    cur.monotonic_update(&next);
    // 槽 0 取大写入、槽 1 回退值被拒、越出 update.length 的槽 2 原值不动
    assert_eq!((cur[0], cur[1], cur[2]), (150, 200, 100));

    cur.monotonic_update_slot(160, 0);
    cur.monotonic_update_slot(155, 0);
    cur.monotonic_update_slot(999, MAX_SUBLOG_COUNT);
    assert_eq!((cur[0], cur[1]), (160, 200));

    let mut limit = AofAddress::create(3, i64::MAX);
    limit.min_exchange(&cur);
    assert_eq!((limit[0], limit[1], limit[2]), (160, 200, 100));
  }
}
