//! 向量索引服务（对标 libs/server/Resp/Vector/DiskANNService.cs）
//!
//! C# 侧 DiskANNService 是对原生 diskann_garnet 库的 P/Invoke 薄封装，
//! 并以 (context | 项类型) 组合的命名空间把向量/邻接表/属性/映射持久化到存储；
//! Rust 侧以本域自建的 [`super::hnsw::HnswIndex`] 承接同一接口语义：
//! context → 索引实例的外部 id 映射 / 属性表 / 量化状态由服务持有。
//! 进入本服务的向量字节均已由 `vector_manager_element_data::prepare_vector_data`
//! 规约为量化器的原生格式。

use std::sync::Arc;

use parking_lot::Mutex;
use whasher::{GxPapayaMap as ConcurrentMap, new_papaya_map};

use super::{
  hnsw::{HnswConfig, HnswIndex, decode_native, decode_values, distance},
  vector_types::VectorQuantType,
};

/// 内部项类型命名空间位（对标 DiskANNService.FullVector 等常量）。
pub mod term {
  /// 完整向量。
  pub const FULL_VECTOR: u64 = 0;
  /// 邻接表。
  pub const NEIGHBOR_LIST: u64 = 1;
  /// 量化后向量。
  pub const QUANTIZED_VECTOR: u64 = 2;
  /// 属性。
  pub const ATTRIBUTES: u64 = 3;
  /// 元数据。
  pub const METADATA: u64 = 4;
  /// 内部 id 映射（外部 id → 内部 id）。
  pub const INTERNAL_ID_MAP: u64 = 5;
  /// 外部 id 映射（内部 id → 外部 id）。
  pub const EXTERNAL_ID_MAP: u64 = 6;
}

/// 插入结果（对标 NativeDiskANNMethods.DiskANNInsertResult）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskAnnInsertResult {
  /// 插入失败（重复元素 / 维度不匹配 / 索引不存在）。
  False = 0,
  /// 插入成功。
  True = 1,
  /// 插入成功且需要量化（建表 + 回填）。
  QuantizationRequested = 2,
}

/// 检索命中项：外部 id + 距离。
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
  /// 外部 id（元素键字节）。
  pub external_id: Vec<u8>,
  /// 距离（越小越相似）。
  pub distance: f32,
}

/// 单个 context 对应的索引实例（全部字段并发安全，可经 Arc 共享）。
struct DiskAnnIndex {
  /// HNSW 图索引。
  hnsw: Mutex<HnswIndex>,
  /// 外部 id → 内部 id。
  external_to_internal: ConcurrentMap<Vec<u8>, u32>,
  /// 内部 id → 外部 id。
  internal_to_external: ConcurrentMap<u32, Vec<u8>>,
  /// 外部 id → 属性字节。
  attributes: ConcurrentMap<Vec<u8>, Vec<u8>>,
}

/// 向量索引服务：context → 索引实例的并发注册表。
#[derive(Default)]
pub struct DiskANNService {
  indexes: ConcurrentMap<u64, Arc<DiskAnnIndex>>,
}

impl DiskANNService {
  /// libs/server/Resp/Vector/DiskANNService.cs:CreateIndex
  ///
  /// 为 context 建立索引；context 已存在时按 RecreateIndex 语义重建图。
  pub fn create_index(&self, context: u64, mut config: HnswConfig) -> bool {
    config.build_exploration_factor = config.build_exploration_factor.max(1);
    config.num_links = config.num_links.max(1);
    match self.indexes.pin().get(&context).cloned() {
      // 重建：清空图并套用新几何
      Some(existing) => {
        existing.hnsw.lock().clear_for_recreate(config);
        true
      }
      None => {
        let index = DiskAnnIndex {
          hnsw: Mutex::new(HnswIndex::new(config)),
          external_to_internal: new_papaya_map(),
          internal_to_external: new_papaya_map(),
          attributes: new_papaya_map(),
        };
        self.indexes.pin().insert(context, Arc::new(index));
        true
      }
    }
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:DropIndex
  pub fn drop_index(&self, context: u64) {
    self.indexes.pin().remove(&context);
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:Insert
  ///
  /// `vector` 为已规约的原生格式字节；元素已存在或维度不匹配时返回 False。
  pub fn insert(
    &self,
    context: u64,
    external_id: &[u8],
    vector: &[u8],
    attributes: &[u8],
  ) -> DiskAnnInsertResult {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return DiskAnnInsertResult::False;
    };
    let mut hnsw = index.hnsw.lock();
    let dims = hnsw.config().dims as usize;

    // 外部 id 查重 + 维度校验
    if index.external_to_internal.pin().contains_key(external_id)
      || decode_native_len(vector, hnsw.config().quant) != dims
    {
      return DiskAnnInsertResult::False;
    }

    let needs_quantization = !hnsw.quant_table_ready();
    let internal_id = {
      let mut rng = fastrand::Rng::new();
      hnsw.insert(vector, &mut rng)
    };

    index
      .external_to_internal
      .pin()
      .insert(external_id.to_vec(), internal_id);
    index
      .internal_to_external
      .pin()
      .insert(internal_id, external_id.to_vec());
    if !attributes.is_empty() {
      index
        .attributes
        .pin()
        .insert(external_id.to_vec(), attributes.to_vec());
    }

    if needs_quantization {
      DiskAnnInsertResult::QuantizationRequested
    } else {
      DiskAnnInsertResult::True
    }
  }

  /// 删除元素（C# Service.Remove）。
  pub fn remove(&self, context: u64, external_id: &[u8]) -> bool {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return false;
    };
    let internal_id = match index.external_to_internal.pin().remove(external_id) {
      Some(&id) => id,
      None => return false,
    };
    index.internal_to_external.pin().remove(&internal_id);
    index.attributes.pin().remove(external_id);
    index.hnsw.lock().remove(internal_id)
  }

  /// 索引创建/重建后是否需要立即调度量化建表（对应 CreateIndex 的 out quantizationRequested 承接）。
  pub fn needs_quantization(&self, context: u64) -> bool {
    self
      .indexes
      .pin()
      .get(&context)
      .is_some_and(|i| !i.hnsw.lock().quant_table_ready())
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:BuildQuantizationTable
  pub fn build_quantization_table(&self, context: u64) -> bool {
    self
      .indexes
      .pin()
      .get(&context)
      .is_some_and(|i| i.hnsw.lock().build_quant_table())
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:BackfillQuantizedVectors
  ///
  /// 按 (task_index, task_count) 分片回填量化向量；回填本身幂等，
  /// 以首分片执行整表回填承载分片语义。
  pub fn backfill_quantized_vectors(&self, context: u64, task_index: usize, task_count: usize) {
    if task_count == 0 || !task_index.is_multiple_of(task_count) {
      return;
    }
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return;
    };
    index.hnsw.lock().build_quant_table();
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:backfill_quant_vectors
  ///
  /// 原生导入名（小写）的内部分派别名。
  pub fn backfill_quant_vectors(&self, context: u64, task_index: usize, task_count: usize) {
    self.backfill_quantized_vectors(context, task_index, task_count)
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:SearchVector
  ///
  /// 以原生格式查询向量检索；`predicate` 为外部 id 谓词（内联过滤通道）。
  /// 索引不存在或维度不匹配时返回 Err。
  pub fn search_vector(
    &self,
    context: u64,
    vector: &[u8],
    count: usize,
    search_exploration_factor: usize,
    predicate: &mut impl FnMut(&[u8]) -> bool,
  ) -> Result<Vec<SearchHit>, i32> {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return Err(-1);
    };
    let query = {
      let hnsw = index.hnsw.lock();
      if decode_native_len(vector, hnsw.config().quant) != hnsw.config().dims as usize {
        return Err(-1);
      }
      // 查询字节恒为真实值空间 f32（prepare_vector_data 已按量化器原生格式规约）
      decode_native(vector, hnsw.config().quant)
    };
    Self::search_values(&index, &query, count, search_exploration_factor, predicate)
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:SearchElement
  ///
  /// 以既有元素为查询中心检索（其存储向量解码至真实值空间后检索；
  /// Q8 建表后存储为量化字节，不可按 f32 直读）。
  pub fn search_element(
    &self,
    context: u64,
    external_id: &[u8],
    count: usize,
    search_exploration_factor: usize,
    predicate: &mut impl FnMut(&[u8]) -> bool,
  ) -> Result<Vec<SearchHit>, i32> {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return Err(-1);
    };
    let internal = match index.external_to_internal.pin().get(external_id).copied() {
      Some(id) => id,
      None => return Err(-1),
    };
    let query = {
      let hnsw = index.hnsw.lock();
      let Some(bytes) = hnsw.vector_of(internal) else {
        return Err(-1);
      };
      decode_values(bytes, hnsw.config().quant, hnsw.quant_table())
    };
    Self::search_values(&index, &query, count, search_exploration_factor, predicate)
  }

  /// 以真实值空间查询向量检索（SearchVector / SearchElement 共用路径）。
  fn search_values(
    index: &Arc<DiskAnnIndex>,
    query: &[f32],
    count: usize,
    search_exploration_factor: usize,
    predicate: &mut impl FnMut(&[u8]) -> bool,
  ) -> Result<Vec<SearchHit>, i32> {
    let internal = index.internal_to_external.pin();
    let mut internal_predicate = |id: u32| internal.get(&id).is_some_and(|eid| predicate(eid));
    let hits = index.hnsw.lock().search(
      query,
      count,
      search_exploration_factor,
      &mut internal_predicate,
    );
    drop(internal);

    let ext = index.internal_to_external.pin();
    Ok(
      hits
        .into_iter()
        .filter_map(|(id, dist)| {
          ext.get(&id).map(|eid| SearchHit {
            external_id: eid.clone(),
            distance: dist,
          })
        })
        .collect(),
    )
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:ContinueSearch
  ///
  /// C# 侧未实现（NotImplementedException，分页续检占位）；
  /// Rust 侧对齐为不支持错误。
  pub fn continue_search(&self, _context: u64, _continuation: u64) -> Result<Vec<SearchHit>, i32> {
    Err(-1)
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:CheckInternalIdValid
  pub fn check_internal_id_valid(&self, context: u64, internal_id: u32) -> bool {
    self
      .indexes
      .pin()
      .get(&context)
      .is_some_and(|i| i.hnsw.lock().is_internal_id_valid(internal_id))
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:CheckExternalIdValid
  pub fn check_external_id_valid(&self, context: u64, external_id: &[u8]) -> bool {
    self
      .indexes
      .pin()
      .get(&context)
      .is_some_and(|i| i.external_to_internal.pin().contains_key(external_id))
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:SetAttribute
  pub fn set_attribute(&self, context: u64, external_id: &[u8], attribute: &[u8]) -> bool {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return false;
    };
    if !index.external_to_internal.pin().contains_key(external_id) {
      return false;
    }
    index
      .attributes
      .pin()
      .insert(external_id.to_vec(), attribute.to_vec());
    true
  }

  /// 属性项读取通道（Attributes 项）。
  pub fn get_attribute(&self, context: u64, external_id: &[u8]) -> Option<Vec<u8>> {
    let index = self.indexes.pin().get(&context).cloned()?;
    index.attributes.pin().get(external_id).cloned()
  }

  /// 完整向量读取通道（FullVector 项）。
  pub fn get_full_vector(&self, context: u64, external_id: &[u8]) -> Option<Vec<u8>> {
    let index = self.indexes.pin().get(&context).cloned()?;
    let internal = *index.external_to_internal.pin().get(external_id)?;
    let hnsw = index.hnsw.lock();
    hnsw.vector_of(internal).map(|v| v.to_vec())
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:card
  pub fn card(&self, context: u64) -> u64 {
    self
      .indexes
      .pin()
      .get(&context)
      .map_or(0, |i| i.hnsw.lock().len() as u64)
  }

  /// 层 0 邻接读取通道（VLINKS）。
  pub fn links_of(&self, context: u64, external_id: &[u8]) -> Option<Vec<Vec<u8>>> {
    let index = self.indexes.pin().get(&context).cloned()?;
    let internal = *index.external_to_internal.pin().get(external_id)?;
    let hnsw = index.hnsw.lock();
    let links = hnsw.links_of(internal)?;
    let ext = index.internal_to_external.pin();
    Some(links.iter().filter_map(|id| ext.get(id).cloned()).collect())
  }

  /// 随机取样元素外部 id（VRANDMEMBER 通道）。
  pub fn sample(&self, context: u64, count: usize) -> Vec<Vec<u8>> {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return Vec::new();
    };
    let ids = index.hnsw.lock().sample(count);
    let ext = index.internal_to_external.pin();
    ids.iter().filter_map(|id| ext.get(id).cloned()).collect()
  }

  /// 内部 id 解析通道（InternalIdMap 项读取）。
  pub fn internal_id_of(&self, context: u64, external_id: &[u8]) -> Option<u32> {
    let index = self.indexes.pin().get(&context).cloned()?;
    index.external_to_internal.pin().get(external_id).copied()
  }

  /// 两元素距离直通（ElementSimilarity 的精确补充通道）。
  pub fn distance_between(&self, context: u64, a: &[u8], b: &[u8]) -> Option<f32> {
    let index = self.indexes.pin().get(&context).cloned()?;
    // 双方解码至真实值空间（Q8 建表后为量化字节，不可按 f32 直读）
    let va = self.embedding_of(context, a)?;
    let vb = self.embedding_of(context, b)?;
    let metric = index.hnsw.lock().config().metric;
    Some(distance(&va, &vb, metric))
  }

  /// 索引维度查询。
  pub fn dims_of(&self, context: u64) -> Option<u32> {
    self
      .indexes
      .pin()
      .get(&context)
      .map(|i| i.hnsw.lock().config().dims)
  }

  /// 量化类型查询。
  pub fn quant_of(&self, context: u64) -> Option<VectorQuantType> {
    self
      .indexes
      .pin()
      .get(&context)
      .map(|i| i.hnsw.lock().config().quant)
  }

  /// 元素嵌入向量展开为真实值空间 f32（VEMB 通道；
  /// Q8 建表后存储为量化字节，按量化表反量化而非按 f32 直读）。
  pub fn embedding_of(&self, context: u64, external_id: &[u8]) -> Option<Vec<f32>> {
    let index = self.indexes.pin().get(&context).cloned()?;
    let internal = *index.external_to_internal.pin().get(external_id)?;
    let hnsw = index.hnsw.lock();
    let bytes = hnsw.vector_of(internal)?;
    Some(decode_values(
      bytes,
      hnsw.config().quant,
      hnsw.quant_table(),
    ))
  }
}

/// 按量化类型解码后的元素个数。
fn decode_native_len(bytes: &[u8], quant: VectorQuantType) -> usize {
  match quant {
    VectorQuantType::XnoQuantU8
    | VectorQuantType::XbinU8
    | VectorQuantType::XnoQuantI8
    | VectorQuantType::XbinI8 => bytes.len(),
    _ => bytes.len() / 4,
  }
}

#[cfg(test)]
mod tests {
  use super::{super::vector_types::VectorDistanceMetricType, *};

  fn svc() -> DiskANNService {
    DiskANNService::default()
  }

  fn f32_bytes(vals: &[f32]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
  }

  #[test]
  fn create_insert_search_roundtrip() {
    let service = svc();
    assert!(service.create_index(
      8,
      HnswConfig::new(
        2,
        0,
        VectorQuantType::NoQuant,
        VectorDistanceMetricType::L2,
        64,
        8
      ),
    ));

    let res = service.insert(8, b"a", &f32_bytes(&[0.0, 0.0]), b"{\"k\":1}");
    assert_eq!(res, DiskAnnInsertResult::True);
    let res = service.insert(8, b"b", &f32_bytes(&[1.0, 1.0]), b"");
    assert_eq!(res, DiskAnnInsertResult::True);
    // 重复插入 → False
    let res = service.insert(8, b"a", &f32_bytes(&[5.0, 5.0]), b"");
    assert_eq!(res, DiskAnnInsertResult::False);
    // 维度不匹配 → False
    let res = service.insert(8, b"c", &f32_bytes(&[1.0]), b"");
    assert_eq!(res, DiskAnnInsertResult::False);

    assert_eq!(service.card(8), 2);
    assert!(service.check_external_id_valid(8, b"a"));
    assert!(!service.check_external_id_valid(8, b"zz"));

    let hits = service
      .search_vector(8, &f32_bytes(&[0.1, 0.1]), 2, 32, &mut |_| true)
      .unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].external_id, b"a".to_vec());

    // 按元素检索
    let hits = service
      .search_element(8, b"b", 1, 32, &mut |_| true)
      .unwrap();
    assert_eq!(hits[0].external_id, b"b".to_vec());

    // 谓词过滤
    let hits = service
      .search_vector(8, &f32_bytes(&[0.0, 0.0]), 2, 32, &mut |id| id == b"b")
      .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].external_id, b"b".to_vec());

    // 维度不匹配检索
    assert_eq!(
      service.search_vector(8, &f32_bytes(&[1.0]), 1, 8, &mut |_| true),
      Err(-1)
    );
  }

  #[test]
  fn attributes_and_embeddings() {
    let service = svc();
    service.create_index(
      3,
      HnswConfig::new(
        2,
        0,
        VectorQuantType::NoQuant,
        VectorDistanceMetricType::Cosine,
        32,
        4,
      ),
    );
    service.insert(3, b"x", &f32_bytes(&[1.0, 0.0]), b"{\"t\":\"a\"}");
    service.insert(3, b"y", &f32_bytes(&[0.0, 1.0]), b"{\"t\":\"b\"}");

    assert_eq!(
      service.get_attribute(3, b"x").unwrap(),
      b"{\"t\":\"a\"}".to_vec()
    );
    assert!(service.set_attribute(3, b"x", b"{\"t\":\"c\"}"));
    assert_eq!(
      service.get_attribute(3, b"x").unwrap(),
      b"{\"t\":\"c\"}".to_vec()
    );
    assert!(!service.set_attribute(3, b"nope", b"{}"));

    let emb = service.embedding_of(3, b"y").unwrap();
    assert_eq!(emb, vec![0.0, 1.0]);

    let links = service.links_of(3, b"x").unwrap();
    assert!(!links.is_empty());
    assert_eq!(service.sample(3, 5).len(), 2);

    // 正交向量余弦距离 1.0
    let d = service.distance_between(3, b"x", b"y").unwrap();
    assert!((d - 1.0).abs() < 1e-5);

    assert_eq!(service.dims_of(3), Some(2));
    assert_eq!(service.quant_of(3), Some(VectorQuantType::NoQuant));
  }

  #[test]
  fn quantization_request_lifecycle() {
    let service = svc();
    service.create_index(
      5,
      HnswConfig::new(
        2,
        0,
        VectorQuantType::Q8,
        VectorDistanceMetricType::L2,
        32,
        4,
      ),
    );
    // Q8 表未建 → 首批插入要求量化
    assert!(service.needs_quantization(5));
    let res = service.insert(5, b"p", &f32_bytes(&[0.0, 100.0]), b"");
    assert_eq!(res, DiskAnnInsertResult::QuantizationRequested);
    let res = service.insert(5, b"q", &f32_bytes(&[100.0, 0.0]), b"");
    assert_eq!(res, DiskAnnInsertResult::QuantizationRequested);
    // 建表后插入 → 正常成功
    assert!(service.build_quantization_table(5));
    assert!(!service.needs_quantization(5));
    let res = service.insert(5, b"r", &f32_bytes(&[50.0, 50.0]), b"");
    assert_eq!(res, DiskAnnInsertResult::True);
    // 建表后插入就地量化：RAW 通道读出量化字节（每维 1 字节）
    assert_eq!(service.get_full_vector(5, b"r").unwrap().len(), 2);

    // 分片回填（幂等）
    service.backfill_quantized_vectors(5, 0, 4);
    service.backfill_quantized_vectors(5, 1, 4);
    // 建表幂等
    assert!(service.build_quantization_table(5));

    // 建表回填后检索仍能区分象限（Q8 量化空间）
    let hits = service
      .search_vector(5, &f32_bytes(&[0.0, 100.0]), 1, 32, &mut |_| true)
      .unwrap();
    assert_eq!(hits[0].external_id, b"p".to_vec());

    // Q8 建表后：按元素检索 / 嵌入展开 / 两元素距离均走真实值空间
    let hits = service
      .search_element(5, b"q", 1, 32, &mut |_| true)
      .unwrap();
    assert_eq!(hits[0].external_id, b"q".to_vec());
    let emb = service.embedding_of(5, b"p").unwrap();
    assert_eq!(emb.len(), 2);
    // p=[0,100] 与 q=[100,0] 在 L2 下彼此远离，与自身距离为 0
    let d_self = service.distance_between(5, b"p", b"p").unwrap();
    assert!(d_self.abs() < 1.0);
    let d_cross = service.distance_between(5, b"p", b"q").unwrap();
    assert!(d_cross > d_self);
  }

  #[test]
  fn remove_and_drop() {
    let service = svc();
    service.create_index(
      9,
      HnswConfig::new(
        1,
        0,
        VectorQuantType::XnoQuantU8,
        VectorDistanceMetricType::L2,
        32,
        4,
      ),
    );
    assert_eq!(
      service.insert(9, b"u", &[7u8], b""),
      DiskAnnInsertResult::True
    );
    assert_eq!(
      service.insert(9, b"v", &[9u8], b""),
      DiskAnnInsertResult::True
    );
    assert_eq!(service.card(9), 2);

    assert!(service.remove(9, b"u"));
    assert!(!service.remove(9, b"u"));
    assert_eq!(service.card(9), 1);
    assert!(service.get_full_vector(9, b"u").is_none());

    service.drop_index(9);
    assert_eq!(service.card(9), 0);
    assert_eq!(service.search_vector(9, &[1], 1, 8, &mut |_| true), Err(-1));
    assert!(!service.check_internal_id_valid(9, 0));
  }

  #[test]
  fn continue_search_unsupported_and_id_channel() {
    let service = svc();
    // 分页续检在 C# 侧即为 NotImplemented，Rust 对齐为 Err
    assert_eq!(service.continue_search(1, 7), Err(-1));

    service.create_index(
      2,
      HnswConfig::new(
        1,
        0,
        VectorQuantType::NoQuant,
        VectorDistanceMetricType::L2,
        16,
        2,
      ),
    );
    service.insert(2, b"s", &f32_bytes(&[1.0]), b"");
    assert_eq!(service.internal_id_of(2, b"s"), Some(0));
    assert_eq!(service.internal_id_of(2, b"none"), None);
    assert!(service.check_internal_id_valid(2, 0));
    assert!(!service.check_internal_id_valid(2, 42));
  }
}
