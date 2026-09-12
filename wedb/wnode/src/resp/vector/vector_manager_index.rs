//! 索引记录编解码（对标 libs/server/Resp/Vector/VectorManager.Index.cs）
//!
//! `Index` 是存储在 Vector Set 索引键 VALUE 下的 56 字节元数据，
//! 是一切向量集合操作的公共入口。C# 以显式布局结构 + `Unsafe.As` 直读；
//! Rust 侧以小端字节序的编解码函数承接同一磁盘格式。

use super::{
  hnsw::HnswConfig,
  vector_types::{VectorDistanceMetricType, VectorQuantType, VectorSetFlags},
};

/// 索引记录字节数（对齐 C# Index.Size = 56）。
pub const INDEX_SIZE: usize = 56;

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
      flags: VectorSetFlags::from_bits(bytes[40]),
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
    out[40] = self.flags.bits();
    // [44..56] 原为 GUID 预留，保持全零
    out
  }

  /// 转换为 HNSW 索引几何配置。
  pub fn hnsw_config(&self) -> HnswConfig {
    HnswConfig {
      dims: self.dimensions,
      reduce_dims: self.reduce_dims,
      quant: self.quant_type,
      metric: self.distance_metric,
      build_exploration_factor: self.build_exploration_factor,
      num_links: self.num_links,
    }
  }
}

use super::vector_manager::VectorManager;

impl VectorManager {
  /// libs/server/Resp/Vector/VectorManager.Index.cs:CreateIndex
  ///
  /// 构造新索引记录并写入登记表：分配上下文、落几何参数，
  /// 原生索引由 [`super::disk_ann_service::DiskANNService`] 同步创建。
  pub fn create_index_in(
    &self,
    key: &[u8],
    params: &super::vector_manager_locking::CreateIndexParams,
  ) -> Result<Index, super::vector_manager::VectorManagerResult> {
    let context = self
      .next_vector_set_context(0)
      .ok_or(super::vector_manager::VectorManagerResult::Invalid)?;
    let index = Index {
      context,
      index_ptr: 1,
      dimensions: params.dims,
      reduce_dims: params.reduce_dims,
      num_links: params.num_links,
      build_exploration_factor: params.build_exploration_factor,
      quant_type: params.quant,
      distance_metric: params.distance_metric,
      flags: super::vector_types::VectorSetFlags::NONE,
    };
    self.service.create_index(context, index.hnsw_config());
    self.write_stored_index(key, &index.to_bytes());
    Ok(index)
  }

  /// libs/server/Resp/Vector/VectorManager.Index.cs:DropIndex
  ///
  /// 丢弃先前构造的索引（指针为空则无事可做）。
  pub fn drop_index_record(&self, index_value: &[u8]) {
    self.drop_in_memory_index(index_value);
  }

  /// libs/server/Resp/Vector/VectorManager.Index.cs:SetContextForMigration
  ///
  /// 改写记录中的上下文为目标迁移上下文，并砸掉索引指针
  /// （目标节点不得误认为索引已在本机重建）。
  pub fn set_context_for_migration(index_value: &mut [u8], new_context: u64) {
    debug_assert_ne!(new_context, 0, "0 为特殊上下文，不可指派");
    debug_assert_eq!(index_value.len(), INDEX_SIZE, "索引记录尺寸不符");
    index_value[0..8].copy_from_slice(&new_context.to_le_bytes());
    index_value[8..16].fill(0);
  }

  /// libs/server/Resp/Vector/VectorManager.Index.cs:MarkSuppressCleanup
  ///
  /// 对登记表中键记录置位 SuppressCleanup（重命名期间抑制清理）。
  pub fn mark_suppress_cleanup(&self, key: &[u8]) {
    let mut flags = super::vector_types::VectorSetFlags::NONE;
    if let Some(index) = self
      .read_stored_index(key)
      .and_then(|bytes| Index::from_bytes(&bytes))
    {
      flags = index.flags;
    }
    flags = flags.union(super::vector_types::VectorSetFlags::SUPPRESS_CLEANUP);
    self.set_flags(key, flags);
  }

  /// libs/server/Resp/Vector/VectorManager.Index.cs:ClearSuppressCleanup
  ///
  /// 清除 SuppressCleanup（单标志下等价于置 None）。
  pub fn clear_suppress_cleanup(&self, key: &[u8]) {
    self.set_flags(key, super::vector_types::VectorSetFlags::NONE);
  }

  /// libs/server/Resp/Vector/VectorManager.Index.cs:SetFlags
  ///
  /// 写入标志位（经登记表 RMW 承接，假定并发修改已被锁阻止）。
  pub fn set_flags(&self, key: &[u8], flags: super::vector_types::VectorSetFlags) {
    let mut bytes = match self.read_stored_index(key) {
      Some(bytes) => bytes,
      None => return,
    };
    bytes[40] = flags.bits();
    self.write_stored_index(key, &bytes);
  }

  /// libs/server/Resp/Vector/VectorManager.Index.cs:SetIndexFlags
  ///
  /// 原地更新记录中的 Flags 字段。
  pub fn set_index_flags(index_value: &mut [u8], flags: super::vector_types::VectorSetFlags) {
    debug_assert_eq!(index_value.len(), INDEX_SIZE, "索引记录尺寸不符");
    index_value[40] = flags.bits();
  }
}

/// i32 → 量化类型（未知值收敛为 Invalid）。
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

#[cfg(test)]
mod tests {
  use super::*;

  fn sample() -> Index {
    Index {
      context: 8,
      index_ptr: 0xDEAD_BEEF,
      dimensions: 32,
      reduce_dims: 0,
      num_links: 8,
      build_exploration_factor: 200,
      quant_type: VectorQuantType::Q8,
      distance_metric: VectorDistanceMetricType::InnerProduct,
      flags: VectorSetFlags::SUPPRESS_CLEANUP,
    }
  }

  #[test]
  fn index_layout_roundtrip() {
    let idx = sample();
    let bytes = idx.to_bytes();
    assert_eq!(bytes.len(), INDEX_SIZE);
    // C# 布局：Context[0..8] IndexPtr[8..16] Dimensions[16..20] ReduceDims[20..24]
    assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 8);
    assert_eq!(u32::from_le_bytes(bytes[16..20].try_into().unwrap()), 32);
    assert_eq!(bytes[40], 1);

    let back = Index::from_bytes(&bytes).unwrap();
    assert_eq!(back, idx);
  }

  #[test]
  fn read_index_rejects_bad_size() {
    assert!(Index::from_bytes(&[0u8; 55]).is_none());
    assert!(Index::from_bytes(&[0u8; 57]).is_none());
  }

  #[test]
  fn enum_decode_tables() {
    assert_eq!(quant_from_i32(3), VectorQuantType::Q8);
    assert_eq!(quant_from_i32(0), VectorQuantType::Invalid);
    assert_eq!(quant_from_i32(99), VectorQuantType::Invalid);
    assert_eq!(metric_from_i32(1), VectorDistanceMetricType::InnerProduct);
    assert_eq!(metric_from_i32(-1), VectorDistanceMetricType::Cosine);
  }
}
