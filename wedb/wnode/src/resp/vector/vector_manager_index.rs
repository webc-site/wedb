//! 索引记录编解码（对标 libs/server/Resp/Vector/VectorManager.Index.cs）
//!
//! `Index` 是存储在 Vector Set 索引键 VALUE 下的 56 字节元数据，
//! 是一切向量集合操作的公共入口。C# 以显式布局结构 + `Unsafe.As` 直读；
//! Rust 侧以小端字节序的编解码函数承接同一磁盘格式。

use wvector::{IndexConfig, VectorDistanceMetricType, VectorQuantType, VectorSetFlags};

/// 索引记录字节数（对齐 C# Index.Size = 56）。
pub const INDEX_SIZE: usize = 56;

/// Index.Flags 字段的字节偏移（对齐 C# 显式布局 `[FieldOffset(40)] VectorSetFlags Flags`）。
/// 在 garnet 中的相对路径: libs/server/Resp/Vector/VectorManager.Index.cs:Index
const INDEX_FLAGS_OFFSET: usize = 40;

/// 向量集合索引记录（56 字节磁盘格式）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Index {
  /// 向量集合上下文（决定命名空间范围；必须是 ContextStep 的整数倍）。
  pub context: u64,
  /// 原生索引指针（C# 为 nint；Rust 侧作为存在性标记：0 = 未初始化）。
  pub index_ptr: u64,
  /// 向量维度。
  pub dimensions: u32,
  /// 降维后维度（0 = 不降维）。
  pub reduce_dims: u32,
  /// 每层链接数（M）。
  pub num_links: u32,
  /// 构建期探索因子。
  pub build_exploration_factor: u32,
  /// 量化类型。
  pub quant_type: VectorQuantType,
  /// 距离度量。
  pub distance_metric: VectorDistanceMetricType,
  /// 标志位。
  pub flags: VectorSetFlags,
}

impl Default for Index {
  fn default() -> Self {
    Self {
      context: 0,
      index_ptr: 0,
      dimensions: 0,
      reduce_dims: 0,
      num_links: 0,
      build_exploration_factor: 0,
      quant_type: VectorQuantType::Invalid,
      distance_metric: VectorDistanceMetricType::Cosine,
      flags: VectorSetFlags::NONE,
    }
  }
}

impl Index {
  /// libs/server/Resp/Vector/VectorManager.Index.cs:ReadIndex
  ///
  /// 从磁盘字节解码（要求恰好 56 字节）。
  pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
    if bytes.len() != INDEX_SIZE {
      return None;
    }
    let rd_u64 = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
    let rd_u32 = |off: usize| u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
    let rd_i32 = |off: usize| i32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
    Some(Self {
      context: rd_u64(0),
      index_ptr: rd_u64(8),
      dimensions: rd_u32(16),
      reduce_dims: rd_u32(20),
      num_links: rd_u32(24),
      build_exploration_factor: rd_u32(28),
      quant_type: quant_from_i32(rd_i32(32)),
      distance_metric: metric_from_i32(rd_i32(36)),
      flags: VectorSetFlags::from_bits(bytes[INDEX_FLAGS_OFFSET]),
    })
  }

  /// 编码为磁盘字节（56 字节）。
  pub fn to_bytes(&self) -> [u8; INDEX_SIZE] {
    let mut out = [0u8; INDEX_SIZE];
    out[0..8].copy_from_slice(&self.context.to_le_bytes());
    out[8..16].copy_from_slice(&self.index_ptr.to_le_bytes());
    out[16..20].copy_from_slice(&self.dimensions.to_le_bytes());
    out[20..24].copy_from_slice(&self.reduce_dims.to_le_bytes());
    out[24..28].copy_from_slice(&self.num_links.to_le_bytes());
    out[28..32].copy_from_slice(&self.build_exploration_factor.to_le_bytes());
    out[32..36].copy_from_slice(&(self.quant_type as i32).to_le_bytes());
    out[36..40].copy_from_slice(&(self.distance_metric as i32).to_le_bytes());
    out[INDEX_FLAGS_OFFSET] = self.flags.bits();
    // [44..56] 原为 GUID 预留，保持全零
    out
  }

  /// 转换为索引几何配置。
  pub fn index_config(&self) -> IndexConfig {
    IndexConfig {
      dims: self.dimensions,
      reduce_dims: self.reduce_dims,
      quant_type: self.quant_type,
      distance_metric: self.distance_metric,
      build_exploration_factor: self.build_exploration_factor,
      num_links: self.num_links,
    }
  }
}

/// i32 判别值 → 量化类型（未知值收敛为 Invalid）。
pub fn quant_from_i32(v: i32) -> VectorQuantType {
  match v {
    1 => VectorQuantType::NoQuant,
    2 => VectorQuantType::Bin,
    3 => VectorQuantType::Q8,
    4 => VectorQuantType::XnoQuantU8,
    5 => VectorQuantType::XnoQuantI8,
    6 => VectorQuantType::XbinI8,
    7 => VectorQuantType::XbinU8,
    _ => VectorQuantType::Invalid,
  }
}

/// i32 → 距离度量（未知值收敛为 Cosine 的 Invalid 语义：-1）。
pub fn metric_from_i32(v: i32) -> VectorDistanceMetricType {
  match v {
    0 => VectorDistanceMetricType::Cosine,
    1 => VectorDistanceMetricType::InnerProduct,
    2 => VectorDistanceMetricType::L2,
    3 => VectorDistanceMetricType::XCosineNormalized,
    _ => VectorDistanceMetricType::Cosine,
  }
}
