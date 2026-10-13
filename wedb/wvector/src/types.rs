//! 向量集合共享枚举（对标 libs/server/Storage/Session/MainStore/VectorStoreOps.cs 中的枚举定义）
//!
//! C# 侧这些枚举位于 VectorStoreOps.cs（StorageSession partial）；
//! Rust 侧存储会话桥接层不承载类型，故在本域内承接同判别值定义。

/// 向量数据量化方式（控制向量元素到实际存储字节的映射）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VectorQuantType {
  Invalid = 0,
  /// 原样存储，无量化（Redis 兼容量化器均期望 F32 输入）。
  NoQuant = 1,
  /// 二值量化（1 bit）。
  Bin = 2,
  /// 8 bit 标量量化。
  Q8 = 3,
  /// 8 bit 无符号原样存储（扩展量化；XPREQ8 别名于此）。
  XnoQuantU8 = 4,
  /// 8 bit 有符号原样存储（扩展量化）。
  XnoQuantI8 = 5,
  /// 8 bit 有符号二值量化（扩展量化）。
  XbinI8 = 6,
  /// 8 bit 无符号二值量化（扩展量化）。
  XbinU8 = 7,
}

/// 向量值数据格式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VectorValueType {
  Invalid = 0,
  /// 单精度浮点（FP32）。
  FP32 = 1,
  /// 无符号 8 bit（扩展格式；XB8 别名于此）。
  XU8 = 2,
  /// 有符号 8 bit（扩展格式）。
  XI8 = 3,
}

/// DiskANN 检索结果的 id 输出格式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum VectorIdFormat {
  #[default]
  Invalid = 0,
  /// 数据前有 4 字节无符号长度前缀。
  I32LengthPrefixed,
}

/// 向量相似度距离度量（对齐 DiskANN Metric）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VectorDistanceMetricType {
  /// 余弦相似度。
  Cosine = 0,
  /// 内积。
  InnerProduct = 1,
  /// 平方欧氏距离（L2-Squared）。
  L2 = 2,
  /// 归一化余弦相似度（XCosine_Normalized）。
  XCosineNormalized = 3,
}

impl VectorDistanceMetricType {
  /// C# 枚举 ToString 名（错误文案插值逐字节对齐，如 `XCosine_Normalized`）。
  pub const fn csharp_name(self) -> &'static str {
    match self {
      Self::Cosine => "Cosine",
      Self::InnerProduct => "InnerProduct",
      Self::L2 => "L2",
      Self::XCosineNormalized => "XCosine_Normalized",
    }
  }
}

/// 向量集合索引键的标志位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorSetFlags(u8);

impl VectorSetFlags {
  /// 默认，无标志。
  pub const NONE: Self = Self(0);
  /// 该键的删除不应调度关联数据与上下文的清理（重命名进行中）。
  pub const SUPPRESS_CLEANUP: Self = Self(1 << 0);

  /// 原始字节。
  #[inline]
  pub const fn bits(self) -> u8 {
    self.0
  }

  /// 由原始字节构造。
  #[inline]
  pub const fn from_bits(bits: u8) -> Self {
    Self(bits)
  }

  /// 是否包含给定标志。
  #[inline]
  pub const fn contains(self, other: Self) -> bool {
    self.0 & other.0 == other.0
  }

  /// 并入给定标志。
  #[inline]
  pub const fn union(self, other: Self) -> Self {
    Self(self.0 | other.0)
  }

  /// 剥除给定标志。
  #[inline]
  pub const fn difference(self, other: Self) -> Self {
    Self(self.0 & !other.0)
  }
}
