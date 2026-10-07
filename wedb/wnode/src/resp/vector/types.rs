//! 向量域共享常量与类型（错误文案 / 尺寸上限 / AOF 哨兵 / 结果码 /
//! 参数面），自 vector_manager.rs 核心拆出（对标 garnet VectorManager.cs
//! 头部常量区与 ElementData 参数面；路径经 vector_manager 再导出维持）。

use std::thread::available_parallelism;

use wvector::{VectorDistanceMetricType, VectorIdFormat, VectorQuantType, VectorValueType};

use super::vector_manager_index::INDEX_SIZE;

/// 向量集索引头非法文案（本域多处复用；resp_server_session_vectors 亦引用）。
pub(crate) const ERR_VECTOR_SET_INDEX: &[u8] = b"ERR Invalid vector set index";
/// 向量集过滤器编译失败文案（本域多处复用）。
pub(crate) const ERR_COMPILING_FILTER: &[u8] = b"ERR Compiling filter failed";
/// 向量服务内部错误回复文案（本域多处复用）。
pub const ERR_VECTOR_SERVICE_RESPONSE: &[u8] = b"ERR Error indicating response from vector service";
/// 量化方式与既有集合不一致文案（vector session 域同用）。
pub const ERR_QUANTIZATION_MISMATCH: &[u8] =
  b"ERR asked quantization mismatch with existing vector set";
/// 维度不匹配文案（format! 需字面量模板，收敛为本函数一处定义）。
pub(super) fn dimension_mismatch(got: usize, set: u32) -> String {
  format!("ERR Vector dimension mismatch - got {got} but set has {set}")
}

/// 上下文步长（必须为 2 的幂；对齐 C# ContextStep）。
pub const CONTEXT_STEP: u64 = 8;

/// 索引记录字节数（对齐 Index.Size = 56）。
pub const INDEX_SIZE_BYTES: usize = INDEX_SIZE;

/// VADD/VREM/VSETATTR 经 StringInput.arg1 携带的特殊 RMW 操作哨兵
/// （对齐 C# 各 AppendLogArg；rust 侧重放端按 cmd 判别，哨兵随条目携带
/// 供审计与 C# 语义对照）。
/// 用户 VADD 元素插入，副本重放。AOF: YES。InitialUpdater: NO。
pub const VADD_APPEND_LOG_ARG: i64 = i64::MIN;
/// 用户 VREM 元素删除，副本重放。AOF: YES。InitialUpdater: NO。
pub const VREM_APPEND_LOG_ARG: i64 = VADD_APPEND_LOG_ARG + 1;
/// 用户 VSETATTR 更新，副本重放。AOF: YES。InitialUpdater: NO。
pub const VSETATTR_APPEND_LOG_ARG: i64 = VREM_APPEND_LOG_ARG + 1;
/// 迁移索引条目（import_migrated_index 合成写；复用 Vadd 命令通道，arg1
/// 哨兵区分于用户 VADD）。重放面按参数补建登记表与内存索引、零元素插入，
/// 空向量集重启后可重建。AOF: YES。
pub const VSETINDEX_APPEND_LOG_ARG: i64 = VSETATTR_APPEND_LOG_ARG + 1;

/// 存储于日志记录、用于把 INDEX 键识别为 Vector Set 的字节。
/// （元素键在独立命名空间中跟踪，不携带特殊 RecordType）
///
/// rust 侧职责范围：仅作 RENAME 合成写条目的 arg1 哨兵（AOF 重放/副本端
/// 按 cmd=Rename + arg1=RECORD_TYPE 分派向量集登记表迁移）。恢复链路的
/// 登记旁路记录判别由
/// [`VectorRegistrySubTag`](wval::VectorRegistrySubTag)
/// 强类型子标签承接，与本常量无关。
pub const RECORD_TYPE: u8 = 1;

/// 向量维度上限（对齐 Redis VSET_MAX_VECTOR_DIM = 65,536）。
pub const MAX_VECTOR_DIMENSIONS: u32 = 1 << 16;

/// 单次 VSIM 可请求的结果数上限（防溢出与单命令过量分配）。
pub const MAX_RETRIEVE_COUNT: usize = 100_000_000;

/// 自适应 L 内联过滤的最大放大系数。
pub const MAX_FILTERING_SCALE_FACTOR: usize = 256;

/// 构建与搜索的最大探索因子（EF）上限（对齐 Redis 硬限制 1,000,000）。
pub const MAX_EXPLORATION_FACTOR: usize = 1_000_000;

/// 单个上下文元数据承载的上下文数（三张位图各 64 位，一位一上下文）。
pub const CONTEXTS_PER_METADATA: u64 = 64;

/// 上下文元数据记录字节数（4×u64 位图 + 64×u16 槽位）。
pub const CONTEXT_METADATA_SIZE: usize = 4 * 8 + CONTEXTS_PER_METADATA as usize * 2;

/// VectorManager 操作结果。
#[derive(Debug, PartialEq, Clone)]
pub enum VectorManagerResult {
  Invalid = 0,
  OK,
  BadParams,
  Duplicate,
  MissingElement,
  /// 上下文分配耗尽（C# GarnetException "Maximum Vector Set allocations
  /// exceeded, cannot issue new context" 的状态化对位；区别于存储失败的
  /// Invalid，VADD 慢臂据此出各自文案）
  MaxAllocationsExceeded,
}

impl VectorManagerResult {
  /// 结果码 → 默认错误文案（对齐 C# 各 "ERR ..." 常量）。
  pub fn error_msg(self) -> &'static [u8] {
    match self {
      Self::Invalid => b"ERR Invalid vector set operation",
      Self::OK => b"",
      Self::BadParams => b"ERR Invalid parameters for vector set operation",
      Self::Duplicate => b"ERR Vector set element already exists",
      Self::MissingElement => b"ERR Vector set element does not exist",
      Self::MaxAllocationsExceeded => {
        b"ERR Maximum Vector Set allocations exceeded, cannot issue new context"
      }
    }
  }
}

/// 操作失败附带错误文案（对齐 C# out errorMsg）。
#[derive(Debug, PartialEq)]
pub struct VectorOpError {
  /// 结果码。
  pub result: VectorManagerResult,
  /// 错误文案（RESP 原样前缀）。
  pub message: Vec<u8>,
}

impl VectorOpError {
  /// 带文案的失败值（`message` 落堆为拥有态，供 RESP 原样前缀）。
  #[inline]
  pub(super) fn new(result: VectorManagerResult, message: &[u8]) -> Self {
    Self {
      result,
      message: message.to_vec(),
    }
  }
}

/// `Err` 侧便捷构造（原 `try_add`/`value_similarity` 两处同名 `err` 闭包的单点承接）。
#[inline]
pub(super) fn err<T>(result: VectorManagerResult, message: &[u8]) -> Result<T, VectorOpError> {
  Err(VectorOpError::new(result, message))
}

/// 相似度检索的查询载荷（[`VectorManager::similarity_search`] 两路入参）：
/// 值路传规约后的查询向量字节，元素路传既有元素外部 id。
pub(super) enum SimilarityQuery<'a> {
  /// 查询向量（FP32 规约字节）
  Vector(&'a [u8]),
  /// 既有元素外部 id
  Element(&'a [u8]),
}

/// 相似度检索输出（对齐 C# 的多个 SpanByteAndMemory 出参）。
#[derive(Debug, Default)]
pub struct SimilarityOutput {
  /// 命中元素 id（i32 长度前缀串接）。
  pub output_ids: Vec<u8>,
  /// 命中距离（f32 × found）。
  pub output_distances: Vec<f32>,
  /// 命中属性（i32 长度前缀串接，缺失元素为长度 0）。
  pub output_attributes: Vec<u8>,
  /// 过滤位图（bit i = 结果 i 通过过滤）。
  pub filter_bitmap: Vec<u8>,
  /// 结果 id 格式。
  pub id_format: VectorIdFormat,
  /// 命中数（有后置过滤时保持全量命中数、不回写为通过数——C# 以 `_ =`
  /// 弃 [`super::vector_manager_filter::apply_post_filter`] 返回值，通过数
  /// 由 [`Self::filter_bitmap`] 表达，应答上限由序列化端 popcount 收敛）。
  pub found: usize,
}

/// 构造选项（对齐 C# 构造器入参子集）。
#[derive(Default)]
pub struct VectorManagerOptions {
  /// Vector Set 预览是否启用。
  pub is_enabled: bool,
  /// 量化任务数（0 = 默认并发度）。
  pub quantization_task_count: usize,
}

/// 量化任务数原始值 → 生效分片数单点归一（对标 C# VectorManager.cs:227-228
/// `VectorSetQuantizationTaskCount == 0 ? Environment.ProcessorCount : _`：
/// 0 折物理核数、非 0 钳 [1,1024] 上界；构造期与装配尾段注入共用此唯一归一式，
/// 杜绝两处判定分叉）
pub(super) fn normalize_quantization_task_count(raw: usize) -> usize {
  match raw {
    0 => available_parallelism().map(|n| n.get()).unwrap_or(4),
    n => n.min(1024),
  }
}

/// 向量集合登记元素参数（对齐 C# TryAdd 参数面）。
pub struct VectorAddArgs<'a> {
  pub element: &'a [u8],
  pub value_type: VectorValueType,
  pub values: &'a [u8],
  pub attributes: &'a [u8],
  pub reduce_dims: u32,
  pub quant_type: VectorQuantType,
  pub num_links: u32,
  pub distance_metric: VectorDistanceMetricType,
}

impl<'a> VectorAddArgs<'a> {
  /// 构造具有默认几何约束的向量写入参数。
  pub fn new(
    element: &'a [u8],
    value_type: VectorValueType,
    values: &'a [u8],
    attributes: &'a [u8],
  ) -> Self {
    Self {
      element,
      value_type,
      values,
      attributes,
      reduce_dims: 0,
      quant_type: VectorQuantType::NoQuant,
      num_links: 8,
      distance_metric: VectorDistanceMetricType::L2,
    }
  }
}

/// 向量相似度检索参数（对齐 C# ValueSimilarity/ElementSimilarity 参数面）。
#[derive(Copy, Clone)]
pub struct VectorSearchOptions<'a> {
  pub count: usize,
  pub search_exploration_factor: usize,
  pub filter: &'a [u8],
  pub max_filtering_effort: usize,
  pub delta: f32,
  pub include_attributes: bool,
}

impl<'a> Default for VectorSearchOptions<'a> {
  fn default() -> Self {
    Self {
      count: 10,
      search_exploration_factor: 32,
      filter: b"",
      max_filtering_effort: 0,
      delta: f32::INFINITY,
      include_attributes: false,
    }
  }
}
