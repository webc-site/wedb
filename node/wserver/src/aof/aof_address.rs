//! AOF 地址向量：固定容量的子日志地址集合，支持序列化、比较与原子式更新
//! （对标 libs/server/AOF/AofAddress.cs:AofAddress）。
//!
//! C# 为 `fixed long addresses[4]` + 长度字节；Rust 以 `[i64; MAX_SUBLOG_COUNT]`
//! + `length` 承接，序列化布局逐字节一致（`[length u8][addresses i64 × length]`）。

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
    1 + 8 * self.length as usize
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

  /// libs/server/AOF/AofAddress.cs:Equals
  ///
  /// 地址序列逐位相等（长度必须一致）。
  pub fn equals(&self, other: &AofAddress) -> bool {
    self.length == other.length && self.addresses[..self.length as usize] == other.addresses[..other.length as usize]
  }

  /// libs/server/AOF/AofAddress.cs:FromSpan
  ///
  /// 从字节切片按 i64 LE 序读取（长度 = 字节数 >> 3）。
  pub fn from_span(span: &[u8]) -> Self {
    let length = (span.len() >> 3).min(MAX_SUBLOG_COUNT);
    let mut result = AofAddress::new(length as i32);
    for (i, chunk) in span.chunks_exact(8).take(length).enumerate() {
      result.addresses[i] = i64::from_le_bytes(chunk.try_into().expect("chunks_exact(8) 长度恰为 8"));
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
    for (i, chunk) in data[1..].chunks_exact(8).take(length as usize).enumerate() {
      result.addresses[i] = i64::from_le_bytes(chunk.try_into().expect("chunks_exact(8) 长度恰为 8"));
    }
    result
  }

  /// 逗号分隔的有效地址串（C# ToString）。
  pub fn to_aof_string(&self) -> String {
    let mut sb = String::new();
    sb.push_str(&self.addresses[0].to_string());
    for &addr in &self.addresses[1..self.length as usize] {
      sb.push(',');
      sb.push_str(&addr.to_string());
    }
    sb
  }

  /// libs/server/AOF/AofAddress.cs:FromString
  ///
  /// 解析逗号分隔的地址串；非法字符返回 None（C# 抛 FormatException）。
  pub fn from_string(input: &str) -> Option<Self> {
    // 逗号数决定槽位数。
    let count = 1 + input.bytes().filter(|&b| b == b',').count();
    let mut result = AofAddress::new(count as i32);
    let mut idx = 0usize;
    let mut value = 0i64;
    let mut negative = false;
    for c in input.bytes() {
      match c {
        b',' => {
          result.set(idx, value * if negative { -1 } else { 1 });
          idx += 1;
          value = 0;
          negative = false;
        }
        b'0'..=b'9' => value = value.wrapping_mul(10).wrapping_add(i64::from(c - b'0')),
        b'-' => negative = true,
        _ => return None,
      }
    }
    result.set(idx, value * if negative { -1 } else { 1 });
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
    for i in 0..a.length as usize {
      result.addresses[i] = a.addresses[i].min(b.addresses[i]);
    }
    result
  }

  /// libs/server/AOF/AofAddress.cs:MinExchange
  ///
  /// 逐槽位收敛到与 `address` 的较小值。
  pub fn min_exchange(&mut self, address: &AofAddress) {
    for i in 0..self.length as usize {
      self.addresses[i] = self.addresses[i].min(address.addresses[i]);
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
    (0..self.length as usize).any(|i| self.addresses[i] < address.addresses[i])
  }

  /// libs/server/AOF/AofAddress.cs:AnyGreater
  ///
  /// 任一槽位严格大于对应槽位。
  pub fn any_greater(&self, address: &AofAddress) -> bool {
    (0..self.length as usize).any(|i| self.addresses[i] > address.addresses[i])
  }

  /// 任意槽位大于等于 `value`（C# AnyGreater(long) 的语义：全部 <= value 为 true）。
  pub fn any_greater_than(&self, value: i64) -> bool {
    !(0..self.length as usize).all(|i| self.addresses[i] <= value)
  }

  /// libs/server/AOF/AofAddress.cs:Diff
  ///
  /// 逐槽位差值向量。
  pub fn diff(&self, other: &AofAddress) -> AofAddress {
    let mut result = AofAddress::new(other.length());
    for i in 0..other.length as usize {
      result.addresses[i] = self.addresses[i] - other.addresses[i];
    }
    result
  }

  /// libs/server/AOF/AofAddress.cs:AggregateDiff
  ///
  /// 逐槽位差值求和。
  pub fn aggregate_diff(&self, aof_address: &AofAddress) -> i64 {
    (0..self.length as usize).map(|i| self.addresses[i] - aof_address.addresses[i]).sum()
  }

  /// 对单一数值的聚合差（C# AggregateDiff(long)）。
  pub fn aggregate_diff_value(&self, value: i64) -> i64 {
    (0..self.length as usize).map(|i| self.addresses[i] - value).sum()
  }

  /// libs/server/AOF/AofAddress.cs:EqualsAll
  ///
  /// 全部槽位逐一相等。
  pub fn equals_all(&self, input: &AofAddress) -> bool {
    (0..self.length as usize).all(|i| self.addresses[i] == input.addresses[i])
  }

  /// libs/server/AOF/AofAddress.cs:IsOutOfRange
  ///
  /// 任一槽位越出 [begin, end] 区间。
  pub fn is_out_of_range(&self, begin: &AofAddress, end: &AofAddress) -> bool {
    (0..self.length as usize).any(|i| self.addresses[i] < begin.addresses[i] || self.addresses[i] > end.addresses[i])
  }

  /// libs/server/AOF/AofAddress.cs:Max
  ///
  /// 最大槽位值（下界 0）。
  pub fn max(&self) -> i64 {
    self.addresses[..self.length as usize].iter().copied().fold(0i64, i64::max)
  }

  /// libs/server/AOF/AofAddress.cs:Min
  ///
  /// 最小槽位值（上界 0）。
  pub fn min_value(&self) -> i64 {
    self.addresses[..self.length as usize].iter().copied().fold(0i64, i64::min)
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
    assert!(b.any_greater_than(120));
    assert!(!a.any_greater_than(100));
    assert_eq!(a.diff(&b).get(0), Some(-50));
    assert_eq!(a.aggregate_diff(&b), -100);
    assert_eq!(b.max(), 150);
    assert_eq!(b.min_value(), 0);

    let mut c = a;
    c.set_value_if(999, 100);
    assert_eq!(c.get(0), Some(999));
    c.set_value(7);
    assert!(c.equals_all(&AofAddress::create(2, 7)));
  }
}
