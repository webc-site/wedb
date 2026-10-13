//! 元素数据访问族（对标 libs/server/Resp/Vector/VectorManager.cs 的
//! 属性/嵌入读取段与 VectorManager.ElementData.cs 的元素数据面）：
//! 属性流读取（i32 长度前缀串接）、嵌入读取（f32 展开 / 原始量化记录）、
//! 成员判定与属性流视图 [`AttributeView`]，自 vector_manager.rs 核心拆出。

use wvector::{LengthPrefixedIter, VectorQuantType, store::StoreCallbacks, unpack_length_prefixed};

use super::{
  types::{ERR_VECTOR_SERVICE_RESPONSE, VectorManagerResult, VectorOpError, err},
  vector_manager::VectorManager,
  vector_manager_index::Index,
};

impl<S: StoreCallbacks> VectorManager<S> {
  // ======================== 属性读取 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:FetchSingleVectorElementAttributes
  ///
  /// 读取单个元素的属性（须持有防止集合被丢弃的锁）。
  pub async fn fetch_single_vector_element_attributes(
    &self,
    index_value: &[u8],
    element: &[u8],
  ) -> Option<Vec<u8>> {
    self.assert_have_storage_session();
    let index = Index::from_bytes(index_value)?;
    // 命令面缺席语义保持对标 C#：读失败/属性缺失/元素缺席统一 nil 应答
    self
      .service
      .get_attribute(index.context, element)
      .await
      .ok()?
  }

  /// libs/server/Resp/Vector/VectorManager.cs:FetchVectorElementAttributes
  ///
  /// 读取一批元素的属性，产出 i32 长度前缀串接的属性流。
  pub async fn fetch_vector_element_attributes(&self, context: u64, ids: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for element in unpack_length_prefixed(ids) {
      let attr = self
        .service
        .get_attribute(context, element)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
      out.extend_from_slice(&(attr.len() as i32).to_le_bytes());
      out.extend_from_slice(&attr);
    }
    out
  }

  // ======================== 嵌入读取 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:TryGetEmbedding
  ///
  /// 读取元素嵌入向量（按量化类型展开为 f32）。`Ok(None)`=元素缺席；
  /// `Err`=id 映射/占用位读失败上抛，禁折 nil 假阴性（store.rs 回调契约）。
  pub async fn try_get_embedding(
    &self,
    index_value: &[u8],
    element: &[u8],
  ) -> Result<Option<Vec<f32>>, VectorOpError> {
    self.assert_have_storage_session();
    let Some(index) = Index::from_bytes(index_value) else {
      return Ok(None);
    };
    let Some(embedding) = self.service.embedding_of(index.context, element).await else {
      return Ok(None);
    };

    // 元素可能已被删除 —— 校验内部 id 仍有效；判定链读失败上抛错误帧
    let internal = match self.service.internal_id_of(index.context, element).await {
      Ok(internal) => internal,
      Err(e) => {
        log::error!("VEMB 内部 id 解析读失败: {e}");
        return err(VectorManagerResult::Invalid, ERR_VECTOR_SERVICE_RESPONSE);
      }
    };
    let valid = match internal {
      Some(internal) => {
        match self
          .service
          .check_internal_id_valid(index.context, internal)
          .await
        {
          Ok(valid) => valid,
          Err(e) => {
            log::error!("VEMB 内部 id 有效性判定读失败: {e}");
            return err(VectorManagerResult::Invalid, ERR_VECTOR_SERVICE_RESPONSE);
          }
        }
      }
      None => false,
    };
    if !valid {
      return Ok(None);
    }
    Ok(Some(embedding))
  }

  /// libs/server/Resp/Vector/VectorManager.cs:TryGetRawEmbedding
  ///
  /// 读取元素原始量化数据 + 量化类型/范数/范围。
  pub async fn try_get_raw_embedding(
    &self,
    index_value: &[u8],
    element: &[u8],
  ) -> Option<(Vec<u8>, VectorQuantType, f64, Option<f64>)> {
    self.assert_have_storage_session();
    let index = Index::from_bytes(index_value)?;
    let quant = self.service.quant_of(index.context)?;

    // 对齐 C# TryGetRawEmbedding 读序：NoQuant 系无量化向量的稳态，直读
    // 完整向量；量化系读量化记录（QuantizedVector），记录缺失（回填未完成）
    // 回退完整向量——RAW 载荷宽度即量化记录规范宽（Q8 dim+20、Bin dim/8+6
    // 量级），恒回全精度会使按 quantType 解析的客户端错读、迁移帧膨胀
    let bytes = if matches!(
      quant,
      VectorQuantType::NoQuant | VectorQuantType::XnoQuantU8 | VectorQuantType::XnoQuantI8
    ) {
      self
        .service
        .get_full_vector(index.context, element)
        .await
        .ok()?
    } else {
      match self.service.get_quant_vector(index.context, element).await {
        Some(bytes) => bytes,
        None => self
          .service
          .get_full_vector(index.context, element)
          .await
          .ok()?,
      }
    };

    // 对齐 C#：占位值（DiskANN 无直接等价物）
    let norm = 1.0;
    let range = (quant == VectorQuantType::Q8).then_some(1.0);

    Some((bytes, quant, norm, range))
  }

  /// libs/server/Resp/Vector/VectorManager.cs:IsMember
  ///
  /// 元素是否属于该向量集合。`Err`=存储读失败上抛（VISMEMBER 故障窗回
  /// ERR 错误帧），禁折 `false` 假阴性（store.rs 回调契约）。
  pub async fn is_member(&self, index_value: &[u8], element: &[u8]) -> Result<bool, VectorOpError> {
    let Some(index) = Index::from_bytes(index_value) else {
      return Ok(false);
    };
    match self
      .service
      .check_external_id_valid(index.context, element)
      .await
    {
      Ok(member) => Ok(member),
      Err(e) => {
        log::error!("VISMEMBER 存在性判定读失败: {e}");
        err(VectorManagerResult::Invalid, ERR_VECTOR_SERVICE_RESPONSE)
      }
    }
  }
}

/// 属性流视图（供后置过滤逐项读取）。
pub struct AttributeView<'a> {
  /// i32 长度前缀串接的属性字节。
  pub raw: &'a [u8],
}

impl<'a> AttributeView<'a> {
  /// 零分配迭代各属性字节段。
  #[inline]
  pub fn iter(&self) -> LengthPrefixedIter<'a> {
    LengthPrefixedIter::new(self.raw)
  }
}
