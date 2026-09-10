//! 向量集合存储操作（对标 libs/server/Storage/Session/MainStore/VectorStoreOps.cs，C# 为 StorageSession partial）
//!
//! 缺口总述：C# 侧全部 VectorSet* 经 VectorManager（HNSW 图索引 + 量化器，
//! libs/server/Vector/*）操作向量集合对象；Rust 侧向量引擎域
//!（resp/vector、objects/vector）为并行转写域且 wkv/wobject 均无向量索引
//! 原语，本域无法落地真实的近邻检索/嵌入计算。全部入口按 C# 语义收敛为
//! "键不存在 → GarnetStatus::NotFound"，并以本注释为统一缺口说明。

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::api::garnet_status::GarnetStatus;

/// 向量集合操作通用降级返回（依赖链：libs/server/Vector/VectorManager.cs:VectorManager
/// → HNSW 图索引；缺 wkv/wobject 向量索引入口，见模块级缺口总述）
fn vector_unavailable() -> GarnetStatus {
  GarnetStatus::NotFound
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// VADD：向向量集合登记元素向量（依赖 VectorManager/HNSW，见模块级缺口）
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetAdd
  pub async fn vector_set_add(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VREM：移除元素
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetRemove
  pub async fn vector_set_remove(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VSETATTR：设置元素属性 JSON
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetSetAttribute
  pub async fn vector_set_set_attribute(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VSIM：向量相似度批量检索
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetValueSimilarity
  pub async fn vector_set_value_similarity(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VSIM（按已存在元素检索）
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetElementSimilarity
  pub async fn vector_set_element_similarity(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VEMB：取元素嵌入向量
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetEmbedding
  pub async fn vector_set_embedding(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VEMB（原始量化值 + 量化类型/范数/范围出参）
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetRawEmbedding
  pub async fn vector_set_raw_embedding(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VDIMS：取集合向量维度
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetDimensions
  pub async fn vector_set_dimensions(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VINFO：集合元信息
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetInfo
  pub async fn vector_set_info(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VCARD：集合基数
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetCardinality
  pub async fn vector_set_cardinality(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VISMEMBER：元素存在性
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetIsMember
  pub async fn vector_set_is_member(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VLINKS：元素近邻链接
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetLinks
  pub async fn vector_set_links(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VRANDMEMBER：随机取样元素
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetRandomMembers
  pub async fn vector_set_random_members(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// VGETATTR：读取元素属性
  ///
  /// libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetGetAttribute
  pub async fn vector_set_get_attribute(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.assert_vector_set_absent(key).await?;
    Ok(vector_unavailable())
  }

  /// 键不存在时返回 NotFound（C# 各 VectorSet* 对缺失键的统一返回）
  async fn assert_vector_set_absent(&self, key: &[u8]) -> wkv::Result<()> {
    if self.read_string(key).await?.is_none() {
      self.note_notfound();
    }
    Ok(())
  }
}
