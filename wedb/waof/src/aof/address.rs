//! AOF 地址向量：固定容量的子日志地址集合，支持比较与原子式更新
//! （对标 libs/server/AOF/AofAddress.cs:AofAddress）。
//!
//! C# 为 `fixed long addresses[4]` 加长度字节；Rust 以
//! `[i64; MAX_SUBLOG_COUNT]` 加 `length` 承接。
//!
//! 编码面四形，全部对标 C#：bitcode 结构体编码（复制历史 / 检查点条目等
//! 结构体内嵌持久化）、带 1 字节长度前缀的二进制（to_aof_binary /
//! from_aof_binary，对标 C# ToByteArray/Serialize 与 FromByteArray/
//! Deserialize，FAILREPLICATIONOFFSET 请求线格式——C# 发端
//! GarnetClientExtensions.cs:61 / 收端 RespClusterFailoverCommands.cs:151
//! 双端同形）、裸 8B LE 字节切片（from_span，对标 C# FromSpan，复制流
//! 命令线格式）与逗号分隔文本（from_string / to_aof_string，对标 C#
//! FromString / ToString，RESP 应答参数面）。
//!
//! 自研依据: AOF 地址算术（对标 C# TsavoriteLog 地址域 test.hlog/TsavoriteLogAddressRangeTests.cs）

use std::ops::{Index, IndexMut};

use itoa::Buffer;

/// 单个地址字节数（8B LE）。
pub const AOF_ADDRESS_BYTES: usize = size_of::<i64>();

/// 支持的最大物理子日志数。
pub const MAX_SUBLOG_COUNT: usize = 4;

/// AOF 操作使用的固定尺寸地址集合。
///
/// libs/server/AOF/AofAddress.cs:Equals（C# 逐槽位相等由派生 `PartialEq` 承接）
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
  #[inline]
  pub const fn new(length: i32) -> Self {
    let length = if length < 0 {
      0
    } else if length > MAX_SUBLOG_COUNT as i32 {
      MAX_SUBLOG_COUNT as u8
    } else {
      length as u8
    };
    Self {
      length,
      addresses: [0; MAX_SUBLOG_COUNT],
    }
  }

  /// 有效长度（C# `Length` 属性）。
  #[inline]
  pub const fn length(&self) -> i32 {
    self.length as i32
  }

  /// 下标读写（越界 panic 由调用契约排除；安全接口返回 Option）。
  #[inline]
  pub const fn get(&self, i: usize) -> Option<i64> {
    if i < self.length as usize {
      Some(self.addresses[i])
    } else {
      None
    }
  }

  #[inline]
  pub fn set(&mut self, i: usize, value: i64) {
    if i < MAX_SUBLOG_COUNT {
      self.addresses[i] = value;
    }
  }

  /// 有效地址切片借用（零拷贝）
  #[inline]
  pub fn as_slice(&self) -> &[i64] {
    &self.addresses[..self.length as usize]
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
    let mut sb = String::with_capacity(len * 21);
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
  /// 解析逗号分隔的地址串。刻意差异三处，前两处为上游缺陷修复
  /// （登记根 doc/zh/deviations.md §26「AofAddress 逗号串解析」条）：
  /// - 负号逐段生效并逐段复位："1,-2,300" → [1, -2, 300]。C# 逗号分支只写
  ///   value、不施加也不复位 negative，负号只对末段生效
  ///   （"1,-2,300" → [1, 2, -300]）
  /// - 段数超 MAX_SUBLOG_COUNT 返回 None。C# 构造仅 Debug.Assert 门禁，
  ///   release 固定数组越界写
  /// - 非法字符返回 None（C# 抛 FormatException）
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
          result.set(
            idx,
            if negative {
              value.wrapping_neg()
            } else {
              value
            },
          );
          idx += 1;
          value = 0;
          negative = false;
        }
        b'0'..=b'9' => value = value.wrapping_mul(10).wrapping_add(i64::from(c - b'0')),
        b'-' => negative = true,
        _ => return None,
      }
    }
    result.set(
      idx,
      if negative {
        value.wrapping_neg()
      } else {
        value
      },
    );
    Some(result)
  }

  /// libs/server/AOF/AofAddress.cs:ToByteArray（Serialize :194-199）
  ///
  /// 带 1 字节长度前缀的二进制形：`[length][逐槽 8B 小端]`（C#
  /// BinaryWriter.Write(long) 即小端）。CLUSTER FAILREPLICATIONOFFSET
  /// 请求线格式，对标 C# 发端 GarnetClientExtensions.cs:61
  pub fn to_aof_binary(&self) -> Vec<u8> {
    let len = self.length as usize;
    let mut buf = Vec::with_capacity(1 + len * AOF_ADDRESS_BYTES);
    buf.push(self.length);
    for &addr in &self.addresses[..len] {
      buf.extend_from_slice(&addr.to_le_bytes());
    }
    buf
  }

  /// libs/server/AOF/AofAddress.cs:FromByteArray（Deserialize :206-213）
  ///
  /// [`AofAddress::to_aof_binary`] 同形读回；前缀越界或与实长不符返回
  /// None（C# BinaryReader 读越实长抛 EndOfStreamException，release 前缀
  /// 超上限固定数组越界写，均属缺陷不复刻）。收端消费位点见
  /// RespClusterFailoverCommands.cs:151
  pub fn from_aof_binary(data: &[u8]) -> Option<Self> {
    let (&length, body) = data.split_first()?;
    let length = usize::from(length);
    if length > MAX_SUBLOG_COUNT || body.len() != length * AOF_ADDRESS_BYTES {
      return None;
    }
    let mut result = AofAddress::new(length as i32);
    for (i, chunk) in body.as_chunks::<AOF_ADDRESS_BYTES>().0.iter().enumerate() {
      result.addresses[i] = i64::from_le_bytes(*chunk);
    }
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
  #[inline]
  pub const fn create(length: i32, value: i64) -> Self {
    let length = if length < 0 {
      0
    } else if length > MAX_SUBLOG_COUNT as i32 {
      MAX_SUBLOG_COUNT as u8
    } else {
      length as u8
    };
    let mut addresses = [0; MAX_SUBLOG_COUNT];
    let mut i = 0;
    while i < length as usize {
      addresses[i] = value;
      i += 1;
    }
    Self { length, addresses }
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
