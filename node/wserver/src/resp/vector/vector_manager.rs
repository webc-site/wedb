//! 向量集合管理器（对标 libs/server/Resp/Vector/VectorManager.cs）
//!
//! 各 C# partial（ContextMetadata/Index/Filter/Locking/ElementData/Cleanup/
//! Migration/Replication/Quantization/Callbacks）按 Rust 惯例拆分到本目录的
//! `vector_manager__*.rs` 模块，以多个 `impl VectorManager` 块承接。
//!
//! C# 侧经 Tsavorite 存储会话 + 原生 DiskANN 操作向量集合；Rust 侧以本域
//! 自建的 [`DiskANNService`]（HNSW）承接索引语义，存储读写以域内键值
//! 登记表过渡（wkv 集成后替换为真实存储会话）。

use std::{
  collections::BTreeSet,
  sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use gxhash::HashMap;
use parking_lot::Mutex;

use super::{
  cleanup::vector_set_cleanup_work_channel::VectorSetCleanupWorkChannel,
  disk_ann_service::{DiskANNService, DiskAnnInsertResult},
  vector_manager__cleanup::{CleanupGate, CleanupRuntime},
  vector_manager__context_metadata::ContextMetadata,
  vector_manager__index::{INDEX_SIZE, Index},
  vector_manager__locking::VectorSetLocks,
  vector_manager__quantization::{QuantizationChannel, QuantizationState, QuantizationStep},
  vector_manager__replication::ReplicationRuntime,
  vector_manager_element_data::prepare_vector_data,
  vector_types::{
    VectorDistanceMetricType, VectorIdFormat, VectorQuantType, VectorSetFlags, VectorValueType,
  },
};

/// 上下文步长（必须为 2 的幂；对齐 C# ContextStep）。
pub const CONTEXT_STEP: u64 = 8;

/// 首块保留的存储级元数据命名空间（0 为非法上下文，不可使用）。
pub const METADATA_NAMESPACE: u8 = 1;

/// 索引记录字节数（对齐 Index.Size = 56）。
pub const INDEX_SIZE_BYTES: usize = INDEX_SIZE;

/// VADD/VREM 经 StringInput.arg1 携带的特殊 RMW 操作哨兵。
/// - AOF: YES（副本可重放）/ NO（绝不入日志）。
/// - InitialUpdater: YES/NO —— 决定该 arg 能否在记录缺失时新建存储记录，
///   新增 arg 时须防止并发 DEL 下桩记录复活。
pub const VADD_APPEND_LOG_ARG: i64 = i64::MIN; // 用户 VADD 元素插入，副本重放。AOF: YES。InitialUpdater: NO。
/// 本地桩的原生索引重建。AOF: NO。InitialUpdater: NO。
pub const RECREATE_INDEX_ARG: i64 = VADD_APPEND_LOG_ARG + 2;
/// 用户 VREM 元素删除，副本重放。AOF: YES。InitialUpdater: NO。
pub const VREM_APPEND_LOG_ARG: i64 = RECREATE_INDEX_ARG + 1;
/// 迁移元素键。AOF: YES。InitialUpdater: YES（空哑键）。
pub const MIGRATE_ELEMENT_KEY_LOG_ARG: i64 = VREM_APPEND_LOG_ARG + 1;
/// 迁移索引键。AOF: YES。InitialUpdater: YES（空哑键）。
pub const MIGRATE_INDEX_KEY_LOG_ARG: i64 = MIGRATE_ELEMENT_KEY_LOG_ARG + 1;
/// VADD 置位标志。AOF: YES。InitialUpdater: NO（记录必须存在）。
pub const VADD_SET_FLAGS_ARG: i64 = MIGRATE_INDEX_KEY_LOG_ARG + 1;
/// 新桩记录创建。AOF: NO。InitialUpdater: YES。
pub const CREATE_INDEX_ARG: i64 = VADD_SET_FLAGS_ARG + 1;
/// 用户 VSETATTR 更新，副本重放。AOF: YES。InitialUpdater: NO。
pub const VSETATTR_APPEND_LOG_ARG: i64 = CREATE_INDEX_ARG + 1;

/// 存储于日志记录、用于把 INDEX 键识别为 Vector Set 的字节。
/// （元素键在独立命名空间中跟踪，不携带特殊 RecordType）
pub const RECORD_TYPE: u8 = 1;

/// 每个 id 的最小空间（4 字节长度前缀 + 至少 8 字节 id）。
pub const MINIMUM_SPACE_PER_ID: usize = 4 + 8;

/// 向量维度上限（对齐 Redis VSET_MAX_VECTOR_DIM = 65,536）。
pub const MAX_VECTOR_DIMENSIONS: u32 = 1 << 16;

/// 单次 VSIM 可请求的结果数上限（防溢出与单命令过量分配）。
pub const MAX_RETRIEVE_COUNT: usize = 100_000_000;

/// 自适应 L 内联过滤的最大放大系数。
pub const MAX_FILTERING_SCALE_FACTOR: usize = 256;

/// 构建与搜索的最大探索因子（EF）上限（对齐 Redis 硬限制 1,000,000）。
pub const MAX_EXPLORATION_FACTOR: usize = 1_000_000;

/// 上下文元数据记录字节数（4×u64 位图 + 64×u16 槽位）。
pub const CONTEXT_METADATA_SIZE: usize = 4 * 8 + 64 * 2;

/// VectorManager 操作结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorManagerResult {
  #[default]
  Invalid = 0,
  OK,
  BadParams,
  Duplicate,
  MissingElement,
}

/// 操作失败附带错误文案（对齐 C# out errorMsg）。
#[derive(Debug, Clone, PartialEq)]
pub struct VectorOpError {
  /// 结果码。
  pub result: VectorManagerResult,
  /// 错误文案（RESP 原样前缀）。
  pub message: Vec<u8>,
}

/// 相似度检索输出（对齐 C# 的多个 SpanByteAndMemory 出参）。
#[derive(Debug, Clone, Default, PartialEq)]
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
  /// 命中数。
  pub found: usize,
}

/// 构造选项（对齐 C# 构造器入参子集）。
#[derive(Debug, Clone, Default)]
pub struct VectorManagerOptions {
  /// Vector Set 预览是否启用。
  pub is_enabled: bool,
  /// 量化任务数（0 = 默认并发度）。
  pub quantization_task_count: usize,
}

/// 向量集合管理器。
pub struct VectorManager {
  /// Vector Set 预览是否启用。
  pub is_enabled: bool,
  /// 所属逻辑数据库编号。
  pub db_id: i32,
  /// 原生索引服务承接（DiskANN → HNSW）。
  pub service: DiskANNService,
  /// 上下文元数据数组（首个为存储级元数据块）。
  pub context_metadatas: Mutex<Vec<ContextMetadata>>,
  /// 待落盘的元数据下标集合。
  pub dirty_context_metadatas: Mutex<BTreeSet<usize>>,
  /// 元数据持久化承接层（wkv 集成前为域内记录表；键 = 元数据下标）。
  pub metadata_store: Mutex<HashMap<i32, [u8; CONTEXT_METADATA_SIZE]>>,
  /// 恢复期发现的索引上下文集合。
  pub recovered_indexes: Mutex<HashMap<u64, u8>>,
  /// 恢复期发现的元数据记录。
  pub recovered_metadata: Mutex<HashMap<i32, ContextMetadata>>,
  /// 已请求丢弃的内存索引（键 → context）。
  pub requested_drops: Mutex<HashMap<Vec<u8>, u64>>,
  /// 可能已删除的键（清理完成性检查）。
  pub potentially_deleted: Mutex<HashMap<Vec<u8>, u64>>,
  /// VADD/VREM 键 → 索引记录登记表（存储会话桥接承接）。
  pub(crate) key_index_registry: Mutex<HashMap<Vec<u8>, [u8; INDEX_SIZE_BYTES]>>,
  /// 向量集合键锁注册表。
  pub vector_set_locks: VectorSetLocks,
  /// 清理任务通道（context 载荷）。
  pub cleanup_task_channel: VectorSetCleanupWorkChannel<u64>,
  /// 请求清理通道（context 载荷）。
  pub request_cleanup_task_channel: VectorSetCleanupWorkChannel<u64>,
  /// 请求丢弃通道（无载荷信号）。
  pub request_drop_task_channel: VectorSetCleanupWorkChannel<()>,
  /// 量化工作通道。
  pub quantization_channel: QuantizationChannel,
  /// 量化分片数。
  pub quantization_task_count: usize,
  /// 建表请求数（测试观测）。
  pub quantization_requests_processed: AtomicUsize,
  /// 回填请求数（测试观测）。
  pub quantization_backfills_processed: AtomicUsize,
  /// 清理暂停闸门。
  pub cleanup_gate: CleanupGate,
  /// 清理运行时（任务生命周期/静默等待）。
  pub cleanup_runtime: CleanupRuntime,
  /// 副本运行时（重放通道 + 阻塞事件）。
  pub replication: ReplicationRuntime,
  /// 存储会话承接标记（对齐 C# Debug 断言的 ActiveThreadSession）。
  storage_session_attached: AtomicBool,
}

impl VectorManager {
  /// 构造管理器（对齐 C# VectorManager 构造器的参数校验与通道装配）。
  pub fn new(options: VectorManagerOptions) -> Self {
    // C# 校验：VectorSetQuantizationTaskCount ∈ [0, ProcessorCount]
    let quantization_task_count = match options.quantization_task_count {
      0 => 4, // 默认并发度（C# 取 Environment.ProcessorCount）
      n => n.min(1024),
    };

    Self {
      is_enabled: options.is_enabled,
      db_id: 0,
      service: DiskANNService::default(),
      context_metadatas: Mutex::new(vec![Default::default()]),
      dirty_context_metadatas: Mutex::new(BTreeSet::new()),
      metadata_store: Mutex::new(HashMap::default()),
      recovered_indexes: Mutex::new(HashMap::default()),
      recovered_metadata: Mutex::new(HashMap::default()),
      requested_drops: Mutex::new(HashMap::default()),
      potentially_deleted: Mutex::new(HashMap::default()),
      key_index_registry: Mutex::new(HashMap::default()),
      vector_set_locks: VectorSetLocks::default(),
      cleanup_task_channel: VectorSetCleanupWorkChannel::new(),
      request_cleanup_task_channel: VectorSetCleanupWorkChannel::new(),
      request_drop_task_channel: VectorSetCleanupWorkChannel::new(),
      quantization_channel: QuantizationChannel::new(),
      quantization_task_count,
      quantization_requests_processed: AtomicUsize::new(0),
      quantization_backfills_processed: AtomicUsize::new(0),
      cleanup_gate: CleanupGate::new(),
      cleanup_runtime: CleanupRuntime::new(),
      replication: ReplicationRuntime::new(),
      storage_session_attached: AtomicBool::new(true),
    }
  }

  /// 关联逻辑数据库编号（C# 构造器注入 dbId）。
  pub fn with_db_id(mut self, db_id: i32) -> Self {
    self.db_id = db_id;
    self
  }

  /// libs/server/Resp/Vector/VectorManager.cs:AssertHaveStorageSession
  ///
  /// C# 为 DEBUG 断言（ActiveThreadSession 非空）；Rust 侧校验会话承接标记。
  pub fn assert_have_storage_session(&self) {
    debug_assert!(
      self.storage_session_attached.load(Ordering::Relaxed),
      "进入此路径前必须已挂载存储会话"
    );
  }

  /// libs/server/Resp/Vector/VectorManager.cs:errorMsg
  ///
  /// 结果码 → 默认错误文案（对齐 C# 各 "ERR ..." 常量）。
  pub fn error_msg(result: VectorManagerResult) -> &'static [u8] {
    match result {
      VectorManagerResult::Invalid => b"ERR Invalid vector set operation",
      VectorManagerResult::OK => b"",
      VectorManagerResult::BadParams => b"ERR Invalid parameters for vector set operation",
      VectorManagerResult::Duplicate => b"ERR Vector set element already exists",
      VectorManagerResult::MissingElement => b"ERR Vector set element does not exist",
    }
  }

  // ======================== 缓冲区尺寸保障 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:EnsureDistanceBufferSize
  ///
  /// 保障距离输出缓冲至少 `retrieve_count * sizeof(f32)` 字节。
  pub fn ensure_distance_buffer_size(buffer: &mut Vec<u8>, retrieve_count: usize) {
    let size_bytes = retrieve_count * 4;
    debug_assert!(retrieve_count <= MAX_RETRIEVE_COUNT, "结果数超出上限");
    if buffer.len() < size_bytes {
      buffer.resize(size_bytes, 0);
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:EnsureIdBufferSize
  ///
  /// 保障 id 输出缓冲至少 `retrieve_count * MINIMUM_SPACE_PER_ID` 字节。
  pub fn ensure_id_buffer_size(buffer: &mut Vec<u8>, retrieve_count: usize) {
    let size_bytes = retrieve_count * MINIMUM_SPACE_PER_ID;
    debug_assert!(retrieve_count <= MAX_RETRIEVE_COUNT, "结果数超出上限");
    if buffer.len() < size_bytes {
      buffer.resize(size_bytes, 0);
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:EnsureFilterBitmapSize
  ///
  /// 保障过滤位图缓冲至少 `ceil(resultCount / 8)` 字节。
  pub fn ensure_filter_bitmap_size(buffer: &mut Vec<u8>, result_count: usize) {
    let size_bytes = (result_count + 7) >> 3;
    if buffer.len() < size_bytes {
      buffer.resize(size_bytes, 0);
    }
  }

  // ======================== 元素增删改查 ========================

  /// 向量集合登记元素向量（对齐 C# TryAdd；假定索引已锁定）。
  #[allow(clippy::too_many_arguments)]
  pub fn try_add(
    &self,
    key: &[u8],
    index_value: &[u8],
    element: &[u8],
    value_type: VectorValueType,
    values: &[u8],
    attributes: &[u8],
    provided_reduce_dims: u32,
    provided_quant_type: VectorQuantType,
    provided_num_links: u32,
    provided_distance_metric: VectorDistanceMetricType,
  ) -> Result<VectorManagerResult, VectorOpError> {
    self.assert_have_storage_session();

    let err = |result: VectorManagerResult, message: &[u8]| {
      Err(VectorOpError {
        result,
        message: message.to_vec(),
      })
    };

    let Some(index) = Index::from_bytes(index_value) else {
      return err(
        VectorManagerResult::BadParams,
        b"ERR Invalid vector set index",
      );
    };

    // 与既有集合定义逐项比对（对齐 C# 的 REDUCE/量化/M/度量校验）
    if provided_reduce_dims != 0 && provided_reduce_dims != index.reduce_dims {
      return err(
        VectorManagerResult::BadParams,
        b"ERR Provided REDUCE does not match Vector Set definition",
      );
    }
    if provided_quant_type != VectorQuantType::Invalid && provided_quant_type != index.quant_type {
      return err(
        VectorManagerResult::BadParams,
        b"ERR asked quantization mismatch with existing vector set",
      );
    }
    if provided_distance_metric != index.distance_metric {
      return err(
        VectorManagerResult::BadParams,
        format!(
          "ERR Distance metric mismatch - got {} but set has {}",
          provided_distance_metric.csharp_name(),
          index.distance_metric.csharp_name()
        )
        .as_bytes(),
      );
    }
    if provided_num_links != index.num_links {
      // 对齐 Redis 行为
      return err(
        VectorManagerResult::BadParams,
        b"ERR asked M value mismatch with existing vector set",
      );
    }

    // 数据规约（量化器原生格式）
    let prepared = match prepare_vector_data(index.quant_type, value_type, values) {
      Ok(p) => p,
      Err(e) => return err(VectorManagerResult::BadParams, e.message()),
    };

    if prepared.element_count != index.dimensions as usize {
      return err(
        VectorManagerResult::BadParams,
        format!(
          "ERR Vector dimension mismatch - got {} but set has {}",
          prepared.element_count, index.dimensions
        )
        .as_bytes(),
      );
    }
    if provided_reduce_dims == 0 && index.reduce_dims != 0 {
      // 对齐 Redis 的（略显怪异的）行为
      return err(
        VectorManagerResult::BadParams,
        format!(
          "ERR Vector dimension mismatch - got {} but set has {}",
          prepared.element_count, index.reduce_dims
        )
        .as_bytes(),
      );
    }

    match self
      .service
      .insert(index.context, element, &prepared.bytes, attributes)
    {
      DiskAnnInsertResult::True => Ok(VectorManagerResult::OK),
      DiskAnnInsertResult::QuantizationRequested => {
        // 建表请求以 Vector Set 键入量化通道（回填分片在表就绪后调度）
        let _ = self
          .quantization_channel
          .try_publish(QuantizationState::new(
            key.to_vec(),
            QuantizationStep::BuildQuantizationTable,
            0,
          ));
        Ok(VectorManagerResult::OK)
      }
      DiskAnnInsertResult::False => Ok(VectorManagerResult::Duplicate),
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:TryRemove
  ///
  /// 按元素键删除向量及其属性。
  pub fn try_remove(&self, index_value: &[u8], element: &[u8]) -> VectorManagerResult {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return VectorManagerResult::Invalid;
    };

    if self.service.remove(index.context, element) {
      VectorManagerResult::OK
    } else {
      VectorManagerResult::MissingElement
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:TrySetAttribute
  pub fn try_set_attribute(&self, index_value: &[u8], element: &[u8], attribute: &[u8]) -> bool {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return false;
    };
    self
      .service
      .set_attribute(index.context, element, attribute)
  }

  // ======================== 删除与丢弃 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:RequestDeletion
  ///
  /// 按索引键 VALUE 请求删除 Vector Set：登记清理并让索引服务自清。
  pub fn request_deletion(&self, value: &[u8]) {
    if value.len() != INDEX_SIZE_BYTES {
      log::warn!("Ignored Vector Set deletion due to size mismatch");
      return;
    }

    let Some(index) = Index::from_bytes(value) else {
      return;
    };

    // 携带 SuppressCleanup 标志的删除被忽略 —— 通常表明重命名正在进行
    if index.flags.contains(VectorSetFlags::SUPPRESS_CLEANUP) {
      return;
    }

    if !self.request_cleanup_task_channel.try_publish(index.context) {
      log::error!("Could not submit request for Vector Set cleanup, aborting delete");
      return;
    }

    // 让索引服务自行清理
    self.drop_index(value);
  }

  /// libs/server/Resp/Vector/VectorManager.cs:RequestDropInMemoryIndex
  ///
  /// 记录被逐出至磁盘时请求丢弃内存索引；需防止重建与丢弃竞态。
  pub fn request_drop_in_memory_index(&self, key: &[u8], value: &[u8]) {
    if value.len() != INDEX_SIZE_BYTES {
      log::warn!("Ignored Vector Set drop index due to size mismatch");
      return;
    }

    let Some(index) = Index::from_bytes(value) else {
      return;
    };

    // SuppressCleanup 通常表明重命名进行中，存在另一索引指针持有者
    if index.flags.contains(VectorSetFlags::SUPPRESS_CLEANUP) {
      return;
    }

    // 索引可能已从磁盘恢复但从未初始化 —— 无需丢弃
    if index.index_ptr != 0 {
      if self
        .requested_drops
        .lock()
        .insert(key.to_vec(), index.context)
        .is_some()
      {
        log::error!("Drop triggered multiple times for same index");
        return;
      }
      let _ = self.request_drop_task_channel.try_publish(());
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:DropInMemoryIndex
  ///
  /// 请求索引服务丢弃其索引。
  pub fn drop_in_memory_index(&self, value: &[u8]) {
    if value.len() != INDEX_SIZE_BYTES {
      log::warn!("Ignored Vector Set drop index due to size mismatch");
      return;
    }
    self.drop_index(value);
  }

  /// 索引丢弃（对齐 C# DropIndex：指针非零时才丢弃）。
  fn drop_index(&self, value: &[u8]) {
    let Some(index) = Index::from_bytes(value) else {
      return;
    };
    if index.index_ptr == 0 {
      // 索引从未拉起，无可丢弃
      return;
    }
    self.service.drop_index(index.context);
  }

  /// libs/server/Resp/Vector/VectorManager.cs:ClearIndexPointer
  ///
  /// 清空记录中的索引指针；下次触碰记录时将重建索引。
  pub fn clear_index_pointer(value: &mut [u8]) {
    if value.len() != INDEX_SIZE_BYTES {
      return;
    }
    value[8..16].fill(0);
  }

  // ======================== 相似度检索 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:ValueSimilarity
  ///
  /// 以查询向量做相似度检索；`filter` 非空时执行内联过滤 + 结果位图。
  #[allow(clippy::too_many_arguments)]
  pub fn value_similarity(
    &self,
    index_value: &[u8],
    value_type: VectorValueType,
    values: &[u8],
    count: usize,
    search_exploration_factor: usize,
    filter: &[u8],
    include_attributes: bool,
  ) -> Result<SimilarityOutput, VectorOpError> {
    self.assert_have_storage_session();

    let err = |result: VectorManagerResult, message: &[u8]| {
      Err(VectorOpError {
        result,
        message: message.to_vec(),
      })
    };

    let Some(index) = Index::from_bytes(index_value) else {
      return err(
        VectorManagerResult::BadParams,
        b"ERR Invalid vector set index",
      );
    };
    let effective_ef = search_exploration_factor.max(count);

    // 查询向量规约
    let prepared = match prepare_vector_data(index.quant_type, value_type, values) {
      Ok(p) => p,
      Err(e) => return err(VectorManagerResult::BadParams, e.message()),
    };
    if prepared.element_count != index.dimensions as usize {
      return err(
        VectorManagerResult::BadParams,
        b"ERR Dimensions provided do not match Vector Set dimensions",
      );
    }

    // 内联过滤谓词（Compile → 逐候选 EvaluateCandidateFilter）
    let mut compiled = None;
    if !filter.is_empty() {
      match super::expr_compiler::try_compile(filter) {
        Ok(program) => compiled = Some(program),
        Err(_) => {
          return err(
            VectorManagerResult::BadParams,
            b"ERR Compiling filter failed",
          );
        }
      }
    }

    // 编译一次、逐候选复用（C# 经 ThreadStatic 状态复用，无重编译）
    let mut predicate = |external_id: &[u8]| -> bool {
      match &mut compiled {
        None => true,
        Some(program) => {
          self.evaluate_candidate_filter(index.context, external_id, program, filter)
        }
      }
    };

    let hits = self
      .service
      .search_vector(
        index.context,
        &prepared.bytes,
        count,
        effective_ef,
        &mut predicate,
      )
      .map_err(|_| VectorOpError {
        result: VectorManagerResult::BadParams,
        message: b"ERR Error indicating response from vector service".to_vec(),
      })?;

    self.build_similarity_output(index.context, hits, filter, include_attributes)
  }

  /// libs/server/Resp/Vector/VectorManager.cs:ElementSimilarity
  ///
  /// 以既有元素为查询中心做相似度检索。
  pub fn element_similarity(
    &self,
    index_value: &[u8],
    element: &[u8],
    count: usize,
    search_exploration_factor: usize,
    filter: &[u8],
    include_attributes: bool,
  ) -> Result<SimilarityOutput, VectorOpError> {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return Err(VectorOpError {
        result: VectorManagerResult::BadParams,
        message: b"ERR Invalid vector set index".to_vec(),
      });
    };
    let effective_ef = search_exploration_factor.max(count);

    // 元素不存在：对齐 C# VectorSetElementSimilarity 的 MissingElement 出参
    //（会话层据此写 "Element not in Vector Set"）
    if !self.service.check_external_id_valid(index.context, element) {
      return Err(VectorOpError {
        result: VectorManagerResult::MissingElement,
        message: super::resp_server_session_vectors::ERR_ELEMENT_NOT_IN_SET.to_vec(),
      });
    }

    let mut compiled = None;
    if !filter.is_empty() {
      match super::expr_compiler::try_compile(filter) {
        Ok(program) => compiled = Some(program),
        Err(_) => {
          return Err(VectorOpError {
            result: VectorManagerResult::BadParams,
            message: b"ERR Compiling filter failed".to_vec(),
          });
        }
      }
    }

    // 编译一次、逐候选复用（C# 经 ThreadStatic 状态复用，无重编译）
    let mut predicate = |external_id: &[u8]| -> bool {
      match &mut compiled {
        None => true,
        Some(program) => {
          self.evaluate_candidate_filter(index.context, external_id, program, filter)
        }
      }
    };

    let hits = self
      .service
      .search_element(index.context, element, count, effective_ef, &mut predicate)
      .map_err(|_| VectorOpError {
        result: VectorManagerResult::BadParams,
        message: b"ERR Error indicating response from vector service".to_vec(),
      })?;

    self.build_similarity_output(index.context, hits, filter, include_attributes)
  }

  /// 组装检索输出：id/距离/属性/位图缓冲（对齐 C# 出参布局）。
  fn build_similarity_output(
    &self,
    context: u64,
    hits: Vec<super::disk_ann_service::SearchHit>,
    filter: &[u8],
    include_attributes: bool,
  ) -> Result<SimilarityOutput, VectorOpError> {
    let found = hits.len();
    let mut output = SimilarityOutput {
      found,
      id_format: VectorIdFormat::I32LengthPrefixed,
      ..Default::default()
    };

    // id（长度前缀）+ 距离
    for hit in &hits {
      output
        .output_ids
        .extend_from_slice(&(hit.external_id.len() as i32).to_le_bytes());
      output.output_ids.extend_from_slice(&hit.external_id);
      output.output_distances.push(hit.distance);
    }

    // 属性（长度前缀；缺失元素长度 0）
    if include_attributes || !filter.is_empty() {
      for hit in &hits {
        let attr = self
          .service
          .get_attribute(context, &hit.external_id)
          .unwrap_or_default();
        output
          .output_attributes
          .extend_from_slice(&(attr.len() as i32).to_le_bytes());
        output.output_attributes.extend_from_slice(&attr);
      }
    }

    // 过滤位图（后置过滤）
    if !filter.is_empty() {
      Self::ensure_filter_bitmap_size(&mut output.filter_bitmap, found);
      let view = AttributeView {
        raw: &output.output_attributes,
      };
      let _ = super::vector_manager__filter::apply_post_filter(
        filter,
        found,
        &view,
        &mut output.filter_bitmap,
      );
    }

    Ok(output)
  }

  // ======================== 属性读取 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:FetchSingleVectorElementAttributes
  ///
  /// 读取单个元素的属性（须持有防止集合被丢弃的锁）。
  pub fn fetch_single_vector_element_attributes(
    &self,
    index_value: &[u8],
    element: &[u8],
  ) -> VectorManagerResult {
    self.assert_have_storage_session();
    let Some(index) = Index::from_bytes(index_value) else {
      return VectorManagerResult::Invalid;
    };
    if self.service.get_attribute(index.context, element).is_some() {
      VectorManagerResult::OK
    } else {
      VectorManagerResult::MissingElement
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:FetchVectorElementAttributes
  ///
  /// 读取一批元素的属性，产出 i32 长度前缀串接的属性流。
  pub fn fetch_vector_element_attributes(&self, context: u64, ids: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for element in unpack_length_prefixed(ids) {
      let attr = self
        .service
        .get_attribute(context, element)
        .unwrap_or_default();
      out.extend_from_slice(&(attr.len() as i32).to_le_bytes());
      out.extend_from_slice(&attr);
    }
    out
  }

  // ======================== 嵌入读取 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:TryGetEmbedding
  ///
  /// 读取元素嵌入向量（按量化类型展开为 f32）。
  pub fn try_get_embedding(&self, index_value: &[u8], element: &[u8]) -> Option<Vec<f32>> {
    self.assert_have_storage_session();
    let index = Index::from_bytes(index_value)?;
    let embedding = self.service.embedding_of(index.context, element)?;

    // 元素可能已被删除 —— 校验内部 id 仍有效
    let internal = self.service.internal_id_of(index.context, element)?;
    if !self
      .service
      .check_internal_id_valid(index.context, internal)
    {
      return None;
    }
    Some(embedding)
  }

  /// libs/server/Resp/Vector/VectorManager.cs:TryGetRawEmbedding
  ///
  /// 读取元素原始量化数据 + 量化类型/范数/范围。
  pub fn try_get_raw_embedding(
    &self,
    index_value: &[u8],
    element: &[u8],
  ) -> Option<(Vec<u8>, VectorQuantType, f64, Option<f64>)> {
    self.assert_have_storage_session();
    let index = Index::from_bytes(index_value)?;
    let quant = self.service.quant_of(index.context)?;

    // NoQuant 系无量化向量的稳态，直接读完整向量；
    // 其余先读量化向量，量化未完成时回退完整向量
    let bytes = self.service.get_full_vector(index.context, element)?;

    // 对齐 C#：占位值（DiskANN 无直接等价物）
    let norm = 1.0;
    let range = (quant == VectorQuantType::Q8).then_some(1.0);

    Some((bytes, quant, norm, range))
  }

  /// libs/server/Resp/Vector/VectorManager.cs:IsMember
  ///
  /// 元素是否属于该向量集合。
  pub fn is_member(&self, index_value: &[u8], element: &[u8]) -> bool {
    let Some(index) = Index::from_bytes(index_value) else {
      return false;
    };
    self.service.check_external_id_valid(index.context, element)
  }

  // ======================== 恢复 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:ReconcileRecoveredState
  ///
  /// 汇总恢复期积累的簿记：还原元数据、放弃失败的迁移、
  /// 清理未恢复的在用上下文。返回 false 表示保留非空校验失败。
  pub fn reconcile_recovered_state(&self, require_no_reserved_contexts: bool) -> bool {
    if !self.is_enabled {
      return true;
    }

    let mut needs_updated = false;
    let mut metas = self.context_metadatas.lock();

    if require_no_reserved_contexts {
      for (i, meta) in metas.iter().enumerate() {
        if !meta.is_empty() {
          log::error!(
            "Vector Set context reservation was not empty at index {i} when rebuilding after a full store replacement; expected the preceding flush to have cleared it"
          );
          return false;
        }
      }
    }

    // 还原恢复期发现的元数据
    {
      let recovered = self.recovered_metadata.lock();
      if !recovered.is_empty() {
        let max_context = recovered.keys().copied().max().unwrap_or(0);
        *metas = vec![Default::default(); (max_context + 1) as usize];
        for i in 0..metas.len() {
          if let Some(meta) = recovered.get(&(i as i32)) {
            metas[i] = *meta;
          }
        }
      }
    }

    // 恢复期标记迁移中的上下文即迁移失败 —— 尽快收回
    for i in 0..metas.len() {
      if let Some(abandoned) = metas[i].get_migrating() {
        for ctx in abandoned {
          metas[i].mark_migration_complete(i != 0, ctx, u16::MAX);
          metas[i].mark_cleaning_up(i != 0, ctx);
        }
        self.dirty_context_metadatas.lock().insert(i);
        needs_updated = true;
      }
    }

    // 清理中的上下文若有恢复记录 → 撤销清理标记
    {
      let recovered = self.recovered_indexes.lock();
      for &context in recovered.keys() {
        let (context_index, context_value) = Self::decompose_context(context);
        if let Some(meta) = metas.get_mut(context_index) {
          let allow_zero = context_index != 0;
          if meta.is_cleaning_up(allow_zero, context_value) {
            meta.clear_is_cleaning_up(allow_zero, context_value);
            self.dirty_context_metadatas.lock().insert(context_index);
            needs_updated = true;
          }
        }
      }
    }

    // 在用但未恢复的上下文 → 标记清理
    for i in 0..metas.len() {
      let offset = Self::offset_for_context_metadata(i);
      for j in 0..64u64 {
        let context = offset + j * CONTEXT_STEP;
        // 上下文 0 非法
        if context == 0 {
          continue;
        }
        // 已恢复，无需处理
        if self.recovered_indexes.lock().contains_key(&context) {
          continue;
        }
        let (_, context_value) = Self::decompose_context(context);
        let allow_zero = i != 0;
        if metas[i].is_in_use(allow_zero, context_value)
          && !metas[i].is_cleaning_up(allow_zero, context_value)
        {
          metas[i].mark_cleaning_up(allow_zero, context_value);
          self.dirty_context_metadatas.lock().insert(i);
        }
      }
    }

    self.recovered_indexes.lock().clear();

    if needs_updated {
      self.update_context_metadata();
    }

    // 恢复未完成的清理
    let _ = self.cleanup_task_channel.try_publish(0);

    true
  }

  /// libs/server/Resp/Vector/VectorManager.cs:RecoveredVectorSetIndexKey
  ///
  /// 恢复期为每个 Vector Set 索引键调用。
  pub fn recovered_vector_set_index_key(&self, value: &[u8]) {
    if value.len() != INDEX_SIZE_BYTES {
      return;
    }
    let Some(index) = Index::from_bytes(value) else {
      return;
    };
    self.recovered_indexes.lock().insert(index.context, 0);
  }

  /// libs/server/Resp/Vector/VectorManager.cs:RecoveredContextMetadata
  ///
  /// 恢复期为每条 ContextMetadata 记录调用（空记录剪枝，重复记录报错）。
  pub fn recovered_context_metadata(&self, key: &[u8], value: &[u8]) -> bool {
    if value.len() != CONTEXT_METADATA_SIZE || key.len() != 4 {
      return true;
    }

    let record_index = i32::from_le_bytes(key.try_into().unwrap_or([0; 4]));
    let Some(metadata) = ContextMetadata::from_bytes(value) else {
      return true;
    };

    // 恢复期可剪除空元数据（间隙由 ReconcileRecoveredState 补齐）
    if metadata.is_empty() {
      return true;
    }

    let mut recovered = self.recovered_metadata.lock();
    if recovered.insert(record_index, metadata).is_some() {
      log::error!("Recovered multiple instances of the same ContextMetadata: {record_index}");
      return false;
    }
    true
  }

  /// libs/server/Resp/Vector/VectorManager.cs:SanitizeAndTrackIngestedRecordIfApplicable
  ///
  /// 校验并路由以原始字节进入存储的记录（检查点快照 / 磁盘级全量同步）；
  /// 随后应调用 [`Self::reconcile_recovered_state`] 重建上下文保留。
  pub fn sanitize_and_track_ingested_record_if_applicable(
    &self,
    tombstone: bool,
    namespace_bytes: Option<&[u8]>,
    record_type: u8,
    key: &[u8],
    value: &mut [u8],
  ) -> bool {
    if tombstone {
      return true;
    }

    // 元数据记录按命名空间识别，索引记录按记录类型识别
    if let Some(ns) = namespace_bytes {
      if self.is_enabled && ns.len() == 1 && ns[0] == METADATA_NAMESPACE {
        // 上下文元数据载入需暂存（键 = 元数据数组下标 i32 LE）
        return self.recovered_context_metadata(key, value);
      }
      return true;
    }

    if record_type == RECORD_TYPE {
      // 句柄属于写入进程 —— 即使 Vector Set 未启用也必须清指针，
      // 否则稍后启用时会沿指针访问已死亡的地址空间
      Self::clear_index_pointer(value);

      if self.is_enabled {
        self.recovered_vector_set_index_key(value);
      }
    }

    true
  }
}

/// i32 长度前缀流的切片迭代。
pub fn unpack_length_prefixed(bytes: &[u8]) -> Vec<&[u8]> {
  let mut out = Vec::new();
  let mut rest = bytes;
  while rest.len() >= 4 {
    let len = i32::from_le_bytes(rest[..4].try_into().unwrap_or([0; 4])).max(0) as usize;
    let total = 4 + len;
    if rest.len() < total {
      break;
    }
    out.push(&rest[4..total]);
    rest = &rest[total..];
  }
  out
}

/// 属性流视图（供后置过滤逐项读取）。
pub struct AttributeView<'a> {
  /// i32 长度前缀串接的属性字节。
  pub raw: &'a [u8],
}

impl AttributeView<'_> {
  /// 迭代各属性字节段。
  pub fn segments(&self) -> Vec<&[u8]> {
    unpack_length_prefixed(self.raw)
  }
}

#[cfg(test)]
mod tests {
  use super::{
    super::{
      vector_manager__index::Index,
      vector_types::{VectorDistanceMetricType, VectorQuantType},
    },
    *,
  };

  fn manager() -> VectorManager {
    VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    })
  }

  fn fresh_index(context: u64, dims: u32) -> Index {
    Index {
      context,
      index_ptr: 1,
      dimensions: dims,
      reduce_dims: 0,
      num_links: 8,
      build_exploration_factor: 64,
      quant_type: VectorQuantType::NoQuant,
      distance_metric: VectorDistanceMetricType::L2,
      flags: VectorSetFlags::NONE,
    }
  }

  fn f32_bytes(vals: &[f32]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
  }

  #[test]
  fn buffer_size_helpers() {
    let mut buf = Vec::new();
    VectorManager::ensure_distance_buffer_size(&mut buf, 10);
    assert_eq!(buf.len(), 40);
    // 不缩容
    VectorManager::ensure_distance_buffer_size(&mut buf, 5);
    assert_eq!(buf.len(), 40);

    let mut ids = Vec::new();
    VectorManager::ensure_id_buffer_size(&mut ids, 4);
    assert_eq!(ids.len(), 4 * MINIMUM_SPACE_PER_ID);

    let mut bits = Vec::new();
    VectorManager::ensure_filter_bitmap_size(&mut bits, 65);
    assert_eq!(bits.len(), 9);
    VectorManager::ensure_filter_bitmap_size(&mut bits, 8);
    assert_eq!(bits.len(), 9);
  }

  #[test]
  fn log_args_are_contiguous() {
    assert_eq!(VADD_APPEND_LOG_ARG, i64::MIN);
    // DeleteAfterDropArg 曾位于 VADD 与 RECREATE 之间（+1 空档保留）
    assert_eq!(RECREATE_INDEX_ARG, VADD_APPEND_LOG_ARG + 2);
    assert_eq!(VREM_APPEND_LOG_ARG, RECREATE_INDEX_ARG + 1);
    assert_eq!(MIGRATE_ELEMENT_KEY_LOG_ARG, VREM_APPEND_LOG_ARG + 1);
    assert_eq!(MIGRATE_INDEX_KEY_LOG_ARG, MIGRATE_ELEMENT_KEY_LOG_ARG + 1);
    assert_eq!(VADD_SET_FLAGS_ARG, MIGRATE_INDEX_KEY_LOG_ARG + 1);
    assert_eq!(CREATE_INDEX_ARG, VADD_SET_FLAGS_ARG + 1);
    assert_eq!(VSETATTR_APPEND_LOG_ARG, CREATE_INDEX_ARG + 1);
    assert_eq!(RECORD_TYPE, 1);
    assert_eq!(MAX_VECTOR_DIMENSIONS, 65_536);
    assert_eq!(MAX_FILTERING_SCALE_FACTOR, 256);
    assert_eq!(MAX_EXPLORATION_FACTOR, 1_000_000);
  }

  #[test]
  fn error_msg_table() {
    assert_eq!(VectorManager::error_msg(VectorManagerResult::OK), b"");
    assert!(VectorManager::error_msg(VectorManagerResult::MissingElement).starts_with(b"ERR"));
    assert_eq!(VectorManagerResult::default(), VectorManagerResult::Invalid);
  }

  #[test]
  fn clear_index_pointer_only_for_index_sized() {
    let mut value = fresh_index(8, 4).to_bytes();
    value[8..16].copy_from_slice(&123u64.to_le_bytes());
    VectorManager::clear_index_pointer(&mut value);
    assert_eq!(u64::from_le_bytes(value[8..16].try_into().unwrap()), 0);

    // 非 56 字节不动
    let mut junk = vec![1u8; 10];
    VectorManager::clear_index_pointer(&mut junk);
    assert!(junk.iter().all(|b| *b == 1));
  }

  #[test]
  fn add_remove_and_query_lifecycle() {
    let manager = manager();
    // C# TryAdd 假定索引已由 ReadOrCreateVectorIndex 建好；测试侧先行建原生索引
    manager.service.create_index(
      16,
      2,
      0,
      VectorQuantType::NoQuant,
      64,
      8,
      VectorDistanceMetricType::L2,
    );
    let index = fresh_index(16, 2).to_bytes();

    // VADD
    let res = manager.try_add(
      b"set",
      &index,
      b"elem-a",
      VectorValueType::FP32,
      &f32_bytes(&[1.0, 1.0]),
      b"{\"k\":1}",
      0,
      VectorQuantType::Invalid,
      8,
      VectorDistanceMetricType::L2,
    );
    assert_eq!(res.unwrap(), VectorManagerResult::OK);

    // 重复 VADD → Duplicate
    let res = manager.try_add(
      b"set",
      &index,
      b"elem-a",
      VectorValueType::FP32,
      &f32_bytes(&[2.0, 2.0]),
      b"",
      0,
      VectorQuantType::Invalid,
      8,
      VectorDistanceMetricType::L2,
    );
    assert_eq!(res.unwrap(), VectorManagerResult::Duplicate);

    // 维度不匹配
    let res = manager.try_add(
      b"set",
      &index,
      b"elem-b",
      VectorValueType::FP32,
      &f32_bytes(&[1.0]),
      b"",
      0,
      VectorQuantType::Invalid,
      8,
      VectorDistanceMetricType::L2,
    );
    assert_eq!(res.unwrap_err().result, VectorManagerResult::BadParams);

    // M 不匹配
    let res = manager.try_add(
      b"set",
      &index,
      b"elem-b",
      VectorValueType::FP32,
      &f32_bytes(&[1.0, 2.0]),
      b"",
      0,
      VectorQuantType::Invalid,
      4,
      VectorDistanceMetricType::L2,
    );
    assert_eq!(
      res.unwrap_err().message,
      b"ERR asked M value mismatch with existing vector set".to_vec()
    );

    // 成员判定 / 属性 / 嵌入
    assert!(manager.is_member(&index, b"elem-a"));
    assert!(!manager.is_member(&index, b"elem-z"));
    assert_eq!(
      manager.fetch_single_vector_element_attributes(&index, b"elem-a"),
      VectorManagerResult::OK
    );

    let emb = manager.try_get_embedding(&index, b"elem-a").unwrap();
    assert_eq!(emb, vec![1.0, 1.0]);

    let (raw, quant, norm, range) = manager.try_get_raw_embedding(&index, b"elem-a").unwrap();
    assert_eq!(quant, VectorQuantType::NoQuant);
    assert!((norm - 1.0).abs() < f64::EPSILON);
    assert!(range.is_none());
    assert_eq!(raw, f32_bytes(&[1.0, 1.0]));

    // VREM
    assert_eq!(
      manager.try_remove(&index, b"elem-a"),
      VectorManagerResult::OK
    );
    assert_eq!(
      manager.try_remove(&index, b"elem-a"),
      VectorManagerResult::MissingElement
    );
    assert!(!manager.is_member(&index, b"elem-a"));
    assert_eq!(manager.try_get_embedding(&index, b"elem-a"), None);
  }

  #[test]
  fn value_similarity_with_post_filter() {
    let manager = manager();
    manager.service.create_index(
      32,
      2,
      0,
      VectorQuantType::NoQuant,
      64,
      8,
      VectorDistanceMetricType::L2,
    );
    let index = fresh_index(32, 2).to_bytes();

    for (name, v, attr) in [
      ("near", vec![1.0, 0.0], "{\"n\": 1}"),
      ("far", vec![5.0, 5.0], "{\"n\": 100}"),
      ("mid", vec![2.0, 0.0], "{\"n\": 5}"),
    ] {
      manager
        .try_add(
          b"set",
          &index,
          name.as_bytes(),
          VectorValueType::FP32,
          &f32_bytes(&v),
          attr.as_bytes(),
          0,
          VectorQuantType::Invalid,
          8,
          VectorDistanceMetricType::L2,
        )
        .unwrap();
    }

    // 无过滤：返回按距离排序的全部
    let out = manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        3,
        32,
        b"",
        true,
      )
      .unwrap();
    assert_eq!(out.found, 3);
    assert_eq!(out.id_format, VectorIdFormat::I32LengthPrefixed);
    assert!(out.output_distances[0] < out.output_distances[2]);

    // 过滤 .n > 1：near 被排除
    let out = manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        3,
        32,
        b".n > 1",
        true,
      )
      .unwrap();
    assert_eq!(out.found, 2);
    let ids: Vec<&[u8]> = unpack_length_prefixed(&out.output_ids);
    assert!(ids.contains(&b"far".as_slice()) && ids.contains(&b"mid".as_slice()));
    // 位图：过滤项置位
    let passed = out
      .filter_bitmap
      .iter()
      .map(|b| b.count_ones())
      .sum::<u32>();
    assert_eq!(passed, 2);

    // 过滤编译失败
    let out = manager.value_similarity(
      &index,
      VectorValueType::FP32,
      &f32_bytes(&[1.0, 0.0]),
      3,
      32,
      b".n > >",
      false,
    );
    assert_eq!(
      out.unwrap_err().message,
      b"ERR Compiling filter failed".to_vec()
    );
  }

  #[test]
  fn element_similarity_and_attributes_batch() {
    let manager = manager();
    manager.service.create_index(
      48,
      1,
      0,
      VectorQuantType::NoQuant,
      64,
      8,
      VectorDistanceMetricType::L2,
    );
    let index = fresh_index(48, 1).to_bytes();

    manager
      .try_add(
        b"set",
        &index,
        b"x",
        VectorValueType::FP32,
        &f32_bytes(&[0.0]),
        b"{\"a\":1}",
        0,
        VectorQuantType::Invalid,
        8,
        VectorDistanceMetricType::L2,
      )
      .unwrap();
    manager
      .try_add(
        b"set",
        &index,
        b"y",
        VectorValueType::FP32,
        &f32_bytes(&[10.0]),
        b"",
        0,
        VectorQuantType::Invalid,
        8,
        VectorDistanceMetricType::L2,
      )
      .unwrap();

    let out = manager
      .element_similarity(&index, b"x", 2, 32, b"", false)
      .unwrap();
    assert_eq!(out.found, 2);

    // 缺失元素 → MissingElement + "Element not in Vector Set"（对齐 C# 出参文案）
    let err = manager
      .element_similarity(&index, b"ghost", 2, 32, b"", false)
      .unwrap_err();
    assert_eq!(err.result, VectorManagerResult::MissingElement);
    assert_eq!(err.message, b"Element not in Vector Set".to_vec());

    // 批量属性
    let mut ids = Vec::new();
    ids.extend_from_slice(&1i32.to_le_bytes());
    ids.extend_from_slice(b"x");
    ids.extend_from_slice(&1i32.to_le_bytes());
    ids.extend_from_slice(b"y");
    let attrs = manager.fetch_vector_element_attributes(48, &ids);
    let parsed = unpack_length_prefixed(&attrs);
    assert_eq!(parsed[0], b"{\"a\":1}".as_slice());
    assert_eq!(parsed[1], b"".as_slice());

    // VSETATTR
    assert!(manager.try_set_attribute(&index, b"y", b"{\"b\":2}"));
    assert!(!manager.try_set_attribute(&index, b"none", b"{}"));
  }

  #[test]
  fn request_deletion_respects_suppress_cleanup() {
    let manager = manager();
    let mut index = fresh_index(64, 2);
    manager.service.create_index(
      64,
      2,
      0,
      VectorQuantType::NoQuant,
      16,
      4,
      VectorDistanceMetricType::L2,
    );
    // card 统计的是元素数：各插入一个元素以便区分丢弃前后
    assert_eq!(
      manager
        .service
        .insert(64, b"e", &f32_bytes(&[0.0, 0.0]), b""),
      DiskAnnInsertResult::True
    );

    // 正常删除：发布清理请求 + 丢弃索引
    manager.request_deletion(&index.to_bytes());
    assert!(manager.request_cleanup_task_channel.has_pending());
    // 模拟 requestCleanup worker 领取请求（SuppressCleanup 判定应不受影响）
    assert_eq!(manager.request_cleanup_task_channel.try_read(), Some(64));
    assert_eq!(manager.service.card(64), 0);

    // SuppressCleanup：删除被忽略
    manager.service.create_index(
      66,
      2,
      0,
      VectorQuantType::NoQuant,
      16,
      4,
      VectorDistanceMetricType::L2,
    );
    assert_eq!(
      manager
        .service
        .insert(66, b"e", &f32_bytes(&[0.0, 0.0]), b""),
      DiskAnnInsertResult::True
    );
    index.context = 66;
    index.flags = VectorSetFlags::SUPPRESS_CLEANUP;
    manager.request_deletion(&index.to_bytes());
    assert!(!manager.request_cleanup_task_channel.has_pending());
    assert_eq!(manager.service.card(66), 1);

    // 尺寸不符：忽略
    manager.request_deletion(&[0u8; 10]);
  }

  #[test]
  fn drop_in_memory_index_flow() {
    let manager = manager();
    manager.service.create_index(
      80,
      2,
      0,
      VectorQuantType::NoQuant,
      16,
      4,
      VectorDistanceMetricType::L2,
    );
    assert_eq!(
      manager
        .service
        .insert(80, b"e", &f32_bytes(&[0.0, 0.0]), b""),
      DiskAnnInsertResult::True
    );

    let mut index = fresh_index(80, 2);
    let key = b"dropkey".to_vec();

    // 请求丢弃：登记 + 信号
    manager.request_drop_in_memory_index(&key, &index.to_bytes());
    assert!(manager.requested_drops.lock().contains_key(&key));
    assert!(manager.request_drop_task_channel.has_pending());

    // 重复请求被拒
    manager.request_drop_in_memory_index(&key, &index.to_bytes());
    assert_eq!(manager.requested_drops.lock().len(), 1);

    // SuppressCleanup 忽略
    index.flags = VectorSetFlags::SUPPRESS_CLEANUP;
    manager.request_drop_in_memory_index(b"other", &index.to_bytes());
    assert!(
      !manager
        .requested_drops
        .lock()
        .contains_key(b"other".as_slice())
    );

    // 直接丢弃
    assert_eq!(manager.service.card(80), 1);
    manager.drop_in_memory_index(&index.to_bytes());
    assert_eq!(manager.service.card(80), 0);
  }

  #[test]
  fn recovery_reconciliation_flow() {
    let manager = manager();

    // 预置一条在用上下文
    let ctx = manager.next_vector_set_context(1).unwrap();

    // 恢复：登记索引键（上下文已恢复）
    let record = fresh_index(ctx, 4).to_bytes();
    manager.recovered_vector_set_index_key(&record);

    // 恢复元数据记录
    let meta_bytes = manager.context_metadatas.lock()[0].to_bytes();
    assert!(manager.recovered_context_metadata(&0i32.to_le_bytes(), &meta_bytes));
    // 重复恢复同一块 → 失败
    assert!(!manager.recovered_context_metadata(&0i32.to_le_bytes(), &meta_bytes));
    // 尺寸不符 → 忽略
    assert!(manager.recovered_context_metadata(&0i32.to_le_bytes(), &[0u8; 8]));
    assert!(manager.recovered_context_metadata(&[0u8; 8], &meta_bytes));

    // 汇总：无失败迁移
    assert!(manager.reconcile_recovered_state(false));

    // 非法尺寸的删除/恢复记录被忽略
    manager.request_deletion(&[0u8; 3]);
    manager.recovered_vector_set_index_key(&[0u8; 3]);

    // require_no_reserved_contexts=true 且存在保留 → 失败
    let manager2 = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });
    let _ = manager2.next_vector_set_context(0);
    assert!(!manager2.reconcile_recovered_state(true));
  }

  #[test]
  fn sanitize_ingested_records() {
    let manager = manager();

    // 元数据命名空间记录：路由至 recovered_context_metadata（空记录剪枝 → true）
    let mut meta = [0u8; CONTEXT_METADATA_SIZE];
    assert!(manager.sanitize_and_track_ingested_record_if_applicable(
      false,
      Some(&[METADATA_NAMESPACE]),
      0,
      &0i32.to_le_bytes(),
      &mut meta,
    ));

    // 索引记录：清指针 + 登记
    let mut value = fresh_index(96, 2).to_bytes();
    assert!(manager.sanitize_and_track_ingested_record_if_applicable(
      false,
      None,
      RECORD_TYPE,
      b"k",
      &mut value
    ));
    assert!(manager.recovered_indexes.lock().contains_key(&96));
    assert_eq!(u64::from_le_bytes(value[8..16].try_into().unwrap()), 0);

    // 墓碑记录：忽略
    let mut value2 = fresh_index(98, 2).to_bytes();
    assert!(manager.sanitize_and_track_ingested_record_if_applicable(
      true,
      None,
      RECORD_TYPE,
      b"k",
      &mut value2
    ));
    assert!(!manager.recovered_indexes.lock().contains_key(&98));
  }

  #[test]
  fn storage_session_assertion() {
    let manager = manager();
    manager.assert_have_storage_session();
    assert!(VectorManager::index_has_suppress_cleanup(&Index {
      flags: VectorSetFlags::SUPPRESS_CLEANUP,
      ..Index::default()
    }));
  }
}
