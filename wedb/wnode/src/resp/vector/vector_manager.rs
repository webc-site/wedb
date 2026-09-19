//! 向量集合管理器（对标 libs/server/Resp/Vector/VectorManager.cs）
//!
//! 各 C# partial（ContextMetadata/Index/Filter/Locking/ElementData/Cleanup/
//! Replication/Quantization）按 Rust 惯例拆分到本目录的
//! `vector_manager_*.rs` 模块，以多个 `impl VectorManager` 块承接。
//!
//! C# 侧经 Tsavorite 存储会话 + 原生 DiskANN 操作向量集合；Rust 侧以本域
//! 自建的 [`DiskANNService`]（HNSW）承接索引语义，元素数据经
//! [`WedbVectorStoreCallbacks`] 直写 wkv，索引记录驻留域内登记表，副本/
//! 恢复经 AOF 条目重放重建（见 vector_manager_replication.rs）。

use std::{
  collections::BTreeSet,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
};

use parking_lot::{Mutex, RwLock};
use wbase::{
  map::{ConcurrentMap, new_concurrent_map},
  pool::{EventWorkQueue, EventWorkSet},
};
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, LengthPrefixedIter, SearchHit, SearchParams,
  VectorDistanceMetricType, VectorIdFormat, VectorQuantType, VectorSetFlags, VectorValueType,
  prepare_vector_data, try_compile, unpack_length_prefixed,
};

use super::{
  vector_manager_cleanup::{CleanupGate, CleanupRuntime},
  vector_manager_context_metadata::ContextMetadata,
  vector_manager_index::{INDEX_SIZE, Index},
  vector_manager_locking::{
    RegistryDomain, VectorSetKeyLocks, VectorSetLocks, registry_key, split_registry_key,
  },
  vector_manager_quantization::{QuantizationChannel, QuantizationState, QuantizationStep},
  vector_manager_replication::VectorAofSink,
};

/// 向量集索引头非法文案（本域多处复用；resp_server_session_vectors 亦引用）。
pub const ERR_VECTOR_SET_INDEX: &[u8] = b"ERR Invalid vector set index";
/// 向量集过滤器编译失败文案（本域多处复用）。
pub const ERR_COMPILING_FILTER: &[u8] = b"ERR Compiling filter failed";
/// 向量服务内部错误回复文案（本域多处复用）。
pub const ERR_VECTOR_SERVICE_RESPONSE: &[u8] = b"ERR Error indicating response from vector service";
/// 量化方式与既有集合不一致文案（vector session 域同用）。
pub const ERR_QUANTIZATION_MISMATCH: &[u8] =
  b"ERR asked quantization mismatch with existing vector set";
/// 维度不匹配文案（format! 需字面量模板，收敛为本函数一处定义）。
fn dimension_mismatch(got: usize, set: u32) -> String {
  format!("ERR Vector dimension mismatch - got {got} but set has {set}")
}

/// 登记表域回收范围（FLUSH 族三臂，域值与广播条目载荷同源）。
///
/// C# 每库独立 Tsavorite 日志，FLUSH 物理截断即索引记录随库整体消亡
/// （GarnetServer.cs:426-427 每库 store/AOF）；rust 共享单日志换号隔离，
/// 登记表回收必须显式联动，此为同等清库语义在 rust 拓扑下的唯一实现。
#[derive(Debug, Clone, Copy)]
pub enum RegistryReclaim {
  /// 单库：物理域 (vns, 换号前旧 vdb)——FLUSHDB 载荷域。
  Database { vns: u64, vdb: u64 },
  /// 整命名空间：换号前旧 vns——FLUSHNS 载荷域。
  Namespace { vns: u64 },
  /// 全域——FLUSHALL / reset。
  All,
}

impl RegistryReclaim {
  /// 域命中判定单点。
  #[inline]
  fn matches(self, domain: RegistryDomain) -> bool {
    match self {
      Self::Database { vns, vdb } => domain.vns == vns && domain.vdb == vdb,
      Self::Namespace { vns } => domain.vns == vns,
      Self::All => true,
    }
  }
}

/// 上下文步长（必须为 2 的幂；对齐 C# ContextStep）。
pub const CONTEXT_STEP: u64 = 8;

/// 首块保留的存储级元数据命名空间（0 为非法上下文，不可使用）。
pub const METADATA_NAMESPACE: u8 = 1;

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
pub const RECORD_TYPE: u8 = 1;

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

impl VectorManagerResult {
  /// 结果码 → 默认错误文案（对齐 C# 各 "ERR ..." 常量）。
  pub fn error_msg(self) -> &'static [u8] {
    match self {
      Self::Invalid => b"ERR Invalid vector set operation",
      Self::OK => b"",
      Self::BadParams => b"ERR Invalid parameters for vector set operation",
      Self::Duplicate => b"ERR Vector set element already exists",
      Self::MissingElement => b"ERR Vector set element does not exist",
    }
  }
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

use wvector::store::StoreCallbacks;

use crate::resp::vector::vector_store_callbacks::WedbVectorStoreCallbacks;

/// 向量集合管理器。
pub struct VectorManager<S: StoreCallbacks = WedbVectorStoreCallbacks<wdev::SegmentedDevice>> {
  /// Vector Set 预览是否启用（构造初值取 [`VectorManagerOptions::is_enabled`]，
  /// 生产装配期经宿主注入配置终值一次——对位 C# 构造器
  /// `IsEnabled = serverOptions.EnableVectorSetPreview`；运行期只读）。
  pub is_enabled: AtomicBool,
  /// 原生索引服务承接（DiskANN → HNSW）。
  pub service: DiskANNService<S>,
  /// 上下文元数据数组（首个为存储级元数据块）。
  pub context_metadatas: Mutex<Vec<ContextMetadata>>,
  /// 待落盘的元数据下标集合。
  pub dirty_context_metadatas: Mutex<BTreeSet<usize>>,
  /// 元数据持久化承接层（wkv 集成前为域内记录表；键 = 元数据下标）。
  pub metadata_store: ConcurrentMap<i32, [u8; CONTEXT_METADATA_SIZE]>,
  /// 恢复期发现的索引上下文集合。
  pub recovered_indexes: ConcurrentMap<u64, u8>,
  /// 恢复期发现的元数据记录。
  pub recovered_metadata: ConcurrentMap<i32, ContextMetadata>,
  /// 已请求丢弃的内存索引（键 → context）。
  pub requested_drops: EventWorkSet<Vec<u8>, u64>,
  /// VADD/VREM 键 → 索引记录登记表（存储会话桥接承接）。
  pub(crate) key_index_registry: ConcurrentMap<Vec<u8>, [u8; INDEX_SIZE_BYTES]>,
  /// 向量集合键锁注册表。
  pub vector_set_locks: VectorSetLocks,
  /// 清理任务通道（context 载荷）。
  pub cleanup_task_channel: EventWorkQueue<u64>,
  /// 请求清理通道（context 载荷）。
  pub request_cleanup_task_channel: EventWorkQueue<u64>,
  /// 请求丢弃通道（无载荷信号）。
  pub request_drop_task_channel: EventWorkQueue<()>,
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
  /// AOF 直推注入端口（装配期注入；None = 无 AOF 域，合成写静默跳过）。
  pub(crate) aof_sink: RwLock<Option<Arc<VectorAofSink>>>,
  /// 存储会话承接标记（对齐 C# Debug 断言的 ActiveThreadSession）。
  storage_session_attached: AtomicBool,
  /// 存储回调通道（对标 C# ActiveThreadSession 存储上下文）。
  pub callbacks: Callbacks<S>,
}

impl<S: StoreCallbacks> VectorManager<S> {
  /// Vector Set 预览是否启用（装配期定值，运行期只读）。
  ///
  /// libs/server/Resp/Vector/VectorManager.cs:IsEnabled
  #[inline]
  pub fn is_enabled(&self) -> bool {
    self.is_enabled.load(Ordering::Relaxed)
  }

  /// 构造管理器（对齐 C# VectorManager 构造器的参数校验与通道装配）。
  ///
  /// `callbacks` 为存储回调注入面（C# ActiveThreadSession 经 Tsavorite 会话
  /// 回调落盘的等价承接），由宿主在装配期绑定 wkv 存储会话后注入；
  /// 索引创建经 [`Self::create_index`] 统一携带。
  pub fn new(options: VectorManagerOptions, callbacks: Callbacks<S>) -> Self {
    let quantization_task_count = match options.quantization_task_count {
      0 => 4,
      n => n.min(1024),
    };

    Self {
      is_enabled: AtomicBool::new(options.is_enabled),
      service: DiskANNService::default(),
      context_metadatas: Mutex::new(vec![Default::default()]),
      dirty_context_metadatas: Mutex::new(BTreeSet::new()),
      metadata_store: new_concurrent_map(),
      recovered_indexes: new_concurrent_map(),
      recovered_metadata: new_concurrent_map(),
      requested_drops: EventWorkSet::new(),
      key_index_registry: new_concurrent_map(),
      vector_set_locks: VectorSetLocks::default(),
      cleanup_task_channel: EventWorkQueue::new(),
      request_cleanup_task_channel: EventWorkQueue::new(),
      request_drop_task_channel: EventWorkQueue::new(),
      quantization_channel: QuantizationChannel::new(),
      quantization_task_count,
      quantization_requests_processed: AtomicUsize::new(0),
      quantization_backfills_processed: AtomicUsize::new(0),
      cleanup_gate: CleanupGate::new(),
      cleanup_runtime: CleanupRuntime::new(),
      aof_sink: RwLock::new(None),
      storage_session_attached: AtomicBool::new(true),
      callbacks,
    }
  }

  /// 装配 AOF 直推注入端口（服务器级 AOF 点亮时由宿主注入；C# 侧合成写
  /// 经主日志 RMW 自动入日志的等价装配位）。
  pub fn set_aof_sink(&self, sink: Arc<VectorAofSink>) {
    *self.aof_sink.write() = Some(sink);
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

  #[inline]
  pub fn error_msg(result: VectorManagerResult) -> &'static [u8] {
    result.error_msg()
  }

  // ======================== 缓冲区尺寸保障 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:EnsureFilterBitmapSize
  ///
  /// 保障过滤位图缓冲至少 `ceil(resultCount / 8)` 字节。
  pub fn ensure_filter_bitmap_size(buffer: &mut Vec<u8>, result_count: usize) {
    let size_bytes = (result_count + 7) >> 3;
    if buffer.len() < size_bytes {
      buffer.resize(size_bytes, 0);
    }
  }
}

/// 向量集合登记元素参数（对齐 C# TryAdd 参数面）。
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, Copy)]
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

// ======================== 元素增删改查 ========================

impl<S: StoreCallbacks> VectorManager<S> {
  /// 向量集合登记元素向量（对齐 C# TryAdd；假定索引已锁定）。
  pub fn try_add(
    &self,
    prefix: &[u8],
    key: &[u8],
    index_value: &[u8],
    args: &VectorAddArgs<'_>,
  ) -> Result<VectorManagerResult, VectorOpError> {
    self.assert_have_storage_session();

    let err = |result: VectorManagerResult, message: &[u8]| {
      Err(VectorOpError {
        result,
        message: message.to_vec(),
      })
    };

    let Some(index) = Index::from_bytes(index_value) else {
      return err(VectorManagerResult::BadParams, ERR_VECTOR_SET_INDEX);
    };

    // 与既有集合定义逐项比对（对齐 C# 的 REDUCE/量化/M/度量校验）
    if args.reduce_dims != 0 && args.reduce_dims != index.reduce_dims {
      return err(
        VectorManagerResult::BadParams,
        b"ERR Provided REDUCE does not match Vector Set definition",
      );
    }
    if args.quant_type != VectorQuantType::Invalid && args.quant_type != index.quant_type {
      return err(VectorManagerResult::BadParams, ERR_QUANTIZATION_MISMATCH);
    }
    if args.distance_metric != index.distance_metric {
      return err(
        VectorManagerResult::BadParams,
        format!(
          "ERR Distance metric mismatch - got {} but set has {}",
          args.distance_metric.csharp_name(),
          index.distance_metric.csharp_name()
        )
        .as_bytes(),
      );
    }
    if args.num_links != index.num_links {
      // 对齐 Redis 行为
      return err(
        VectorManagerResult::BadParams,
        b"ERR asked M value mismatch with existing vector set",
      );
    }

    // 数据规约（量化器原生格式）
    let prepared = match prepare_vector_data(index.quant_type, args.value_type, args.values) {
      Ok(p) => p,
      Err(e) => return err(VectorManagerResult::BadParams, e.message()),
    };

    if prepared.element_count != index.dimensions as usize {
      return err(
        VectorManagerResult::BadParams,
        dimension_mismatch(prepared.element_count, index.dimensions).as_bytes(),
      );
    }
    if args.reduce_dims == 0 && index.reduce_dims != 0 {
      // 对齐 Redis 的（略显怪异的）行为
      return err(
        VectorManagerResult::BadParams,
        dimension_mismatch(prepared.element_count, index.reduce_dims).as_bytes(),
      );
    }

    match self.service.insert(
      index.context,
      args.element,
      &prepared.bytes,
      args.attributes,
    ) {
      DiskAnnInsertResult::True => Ok(VectorManagerResult::OK),
      DiskAnnInsertResult::QuantizationRequested => {
        // 建表请求以登记表复合键入量化通道（回填分片在表就绪后调度）
        let rk = registry_key(prefix, key);
        if !self.quantization_channel.push(QuantizationState::new(
          rk.as_slice().to_vec(),
          QuantizationStep::BuildQuantizationTable,
          0,
        )) {
          log::warn!("建表请求发布至量化通道失败");
        }
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

    if !self.request_cleanup_task_channel.push(index.context) {
      log::error!("Could not submit request for Vector Set cleanup, aborting delete");
      return;
    }

    // 让索引服务自行清理
    self.drop_index(value);
  }

  /// 用户键删除单点的登记表缺席收口承接（经
  /// [`crate::storage::session::storage_session::vector_registry_delete_hook`]
  /// 由 wkv 双域删除判未命中时回调，对标 C# MainStore RemoveKey 回调 →
  /// RequestDeletion，GarnetRecordTriggers.cs:OnDispose Deleted 臂）；亦供 SET
  /// 覆写守卫与重命名/迁移直接摘除（rust 索引记录驻留登记表，删除 = 登记清理
  /// + 摘除登记表项，键随之消失）。返回是否确有删除。
  pub fn delete_vector_set(&self, prefix: &[u8], key: &[u8]) -> bool {
    let rk = registry_key(prefix, key);
    self.delete_vector_set_of(rk.as_slice())
  }

  /// 复合登记键直删单点（回收/迁移等已持复合键的内部轴键面）。
  pub(crate) fn delete_vector_set_of(&self, rk: &[u8]) -> bool {
    let Some(index_value) = self.stored_index_of(rk) else {
      return false;
    };
    self.request_deletion(&index_value);
    self.remove_stored_index(rk);
    true
  }

  // ======================== FLUSH 域回收 ========================

  /// 登记表域回收单点漏斗（FLUSHDB/FLUSHNS/FLUSHALL/reset 主端执行段与
  /// AOF Flush 族重放臂共用）。
  ///
  /// 逐条目走 [`split_registry_key`] 比对域值，命中项复用既有
  /// [`Self::request_deletion`] + 摘表通道，不另造清理编排；换号语义下
  /// 旧域条目在新域不可达，回收即清库（重启经 AOF 带域条目按域重建）。
  pub fn reclaim_registry_domain(&self, reclaim: RegistryReclaim) {
    let victims: Vec<Vec<u8>> = self
      .key_index_registry
      .pin()
      .iter()
      .filter(|(rk, _)| {
        split_registry_key(rk.as_slice()).is_some_and(|(domain, _)| reclaim.matches(domain))
      })
      .map(|(rk, _)| rk.as_slice().to_vec())
      .collect();
    for rk in &victims {
      self.delete_vector_set_of(rk);
    }
  }

  /// 登记表域内用户键枚举单点（DBSIZE/KEYS/SCAN 慢路径投影）。
  ///
  /// `f` 收剥域后的用户键切片；域判定按复合键字节前缀比对——OPPV 变长
  /// 首字节查表定长，不同域的 [NsVarint][DbVarint] 段互不为字节前缀，
  /// starts_with 即精确域命中（与 [`registry_key`] 布局同源，零二次解码）。
  pub fn for_each_domain_user_key(&self, prefix: &[u8], mut f: impl FnMut(&[u8])) {
    self.key_index_registry.pin().iter().for_each(|(rk, _)| {
      if rk.starts_with(prefix)
        && let Some((_, user_key)) = split_registry_key(rk.as_slice())
      {
        f(user_key);
      }
    });
  }

  /// 登记表域内计数单点（DBSIZE 向量增量；字节前缀域比对同上）。
  pub fn registry_domain_count(&self, prefix: &[u8]) -> usize {
    self
      .key_index_registry
      .pin()
      .iter()
      .filter(|(rk, _)| rk.starts_with(prefix))
      .count()
  }

  // ======================== 重命名迁移 ========================

  /// libs/server/Resp/Vector/VectorManager.Index.cs:MarkSuppressCleanup
  /// libs/server/Resp/Vector/VectorManager.Index.cs:SetFlags
  ///
  /// 重命名窗口开启：置位登记表记录的 SUPPRESS_CLEANUP，使窗口内旧名删除
  ///（request_deletion 的忽略分支）不触发共享上下文清理。C# Mark→Set 两层
  /// RMW 假写链在此合并为登记表直写单入口，无独立 SetFlags 形态。
  pub fn mark_suppress_cleanup(&self, prefix: &[u8], key: &[u8]) {
    let rk = registry_key(prefix, key);
    let Some(bytes) = self.stored_index_of(rk.as_slice()) else {
      return;
    };
    let Some(mut index) = Index::from_bytes(&bytes) else {
      return;
    };
    index.flags = index.flags.union(VectorSetFlags::SUPPRESS_CLEANUP);
    self.put_stored_index(rk.as_slice(), &index.to_bytes());
  }

  /// RENAME 向量集登记表迁移（C# UnifiedStoreOps.cs 的 RENAME 向量分支的
  /// 登记表对偶：MarkSuppressCleanup(old) → SET(new)=标记前快照 →
  /// UpdateHashSlot(old→new) → DELETE(old) 窗口内清理被抑制）。
  ///
  /// libs/server/Resp/Vector/VectorManager.Index.cs:ClearSuppressCleanup
  ///（C# SET(new) 失败时回滚开窗的恢复路径；rust 登记表直写无失败回滚
  /// 形态，且快照在锁后读取、窗口标志不随拷入迁移，清除面结构性消失）。
  ///
  /// 次序逐位对齐 C#：先取标记前快照（拷入新名的记录不带窗口标志，
  /// 新名后续删除照常触发清理）→ 开窗（旧名删除被抑制）→ 新名注册
  ///（同上下文同几何，索引服务零迁移，复用 delete/import 基建）→
  /// 槽位同步 → 摘除旧名（request_deletion 因 SUPPRESS_CLEANUP 被抑制）。
  /// 返回是否确有迁移（旧名未登记即无操作）。
  ///
  /// 上下游锁界面：旧/新名条带独占锁全程持有（C# txnManager 双键排他锁的
  /// [`VectorSetLocks`] 对偶，按条带序号定序获取防交叉死锁）；锁轴与登记
  /// 表同为复合键域，跨库同名键互不串扰。
  pub fn rename_vector_set(&self, prefix: &[u8], old_key: &[u8], new_key: &[u8]) -> bool {
    let rk_old = registry_key(prefix, old_key);
    let rk_new = registry_key(prefix, new_key);
    self.rename_vector_set_of(rk_old.as_slice(), rk_new.as_slice())
  }

  /// 复合登记键直迁单点（AOF 重放臂等已持复合键的内部轴键面）。
  pub(crate) fn rename_vector_set_of(&self, rk_old: &[u8], rk_new: &[u8]) -> bool {
    // 双键条带锁：同条带单次获取（parking_lot 非重入），异条带按序号定序。
    // 快照读取必须在取锁之后——锁前读到的登记快照可能已被并发写推翻
    let _locks = VectorSetKeyLocks::acquire(&self.vector_set_locks, rk_old, rk_new);

    let Some(index_value) = self.stored_index_of(rk_old) else {
      return false;
    };

    // 开窗：旧名记录置 SUPPRESS_CLEANUP（mark_suppress_cleanup 的轴键形态），
    // 窗口内旧名删除不触发共享上下文清理
    if let Some(mut index) = Index::from_bytes(&index_value) {
      index.flags = index.flags.union(VectorSetFlags::SUPPRESS_CLEANUP);
      self.put_stored_index(rk_old, &index.to_bytes());
    }
    // 新名注册（标记前快照，不带窗口标志）→ 旧名摘除（清理被抑制，仅摘表项）
    self.put_stored_index(rk_new, &index_value);
    self.delete_vector_set_of(rk_old);
    true
  }

  /// libs/server/Resp/Vector/VectorManager.cs:RequestDropInMemoryIndex
  ///
  /// 记录被逐出至磁盘时请求丢弃内存索引；需防止重建与丢弃竞态。
  /// 丢弃通道载荷为登记表复合键（与锁协议同域）。
  pub fn request_drop_in_memory_index(&self, prefix: &[u8], key: &[u8], value: &[u8]) {
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
      let rk = registry_key(prefix, key);
      if !self
        .requested_drops
        .try_add(rk.as_slice().to_vec(), index.context)
      {
        log::error!("Drop triggered multiple times for same index");
        return;
      }
      let _ = self.request_drop_task_channel.push(());
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
  /// 以查询向量做相似度检索；`filter` 非空时执行内联过滤 + 结果位图，
  /// 候选队列按 `max_filtering_effort` 放大；`delta` 为最大距离截断（EPSILON）。
  pub fn value_similarity(
    &self,
    index_value: &[u8],
    value_type: VectorValueType,
    values: &[u8],
    opts: &VectorSearchOptions<'_>,
  ) -> Result<SimilarityOutput, VectorOpError> {
    self.assert_have_storage_session();

    let err = |result: VectorManagerResult, message: &[u8]| {
      Err(VectorOpError {
        result,
        message: message.to_vec(),
      })
    };

    let Some(index) = Index::from_bytes(index_value) else {
      return err(VectorManagerResult::BadParams, ERR_VECTOR_SET_INDEX);
    };
    let effective_ef = effective_search_ef(
      opts.search_exploration_factor,
      opts.count,
      opts.filter,
      opts.max_filtering_effort,
    );

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

    if !opts.filter.is_empty() && try_compile(opts.filter).is_err() {
      return err(VectorManagerResult::BadParams, ERR_COMPILING_FILTER);
    }

    let search_params = SearchParams {
      count: opts.count,
      search_exploration_factor: effective_ef,
      filter_len: opts.filter.len(),
      max_filtering_effort: opts.max_filtering_effort,
    };

    let output = self
      .service
      .search_vector(index.context, &prepared.bytes, search_params)
      .map_err(|_| VectorOpError {
        result: VectorManagerResult::BadParams,
        message: ERR_VECTOR_SERVICE_RESPONSE.to_vec(),
      })?;
    let mut hits = output.hits();
    apply_delta_cutoff(&mut hits, opts.delta);

    self.build_similarity_output(index.context, hits, opts.filter, opts.include_attributes)
  }

  /// libs/server/Resp/Vector/VectorManager.cs:ElementSimilarity
  ///
  /// 以既有元素为查询中心做相似度检索（过滤/截断语义同 [`Self::value_similarity`]）。
  pub fn element_similarity(
    &self,
    index_value: &[u8],
    element: &[u8],
    opts: &VectorSearchOptions<'_>,
  ) -> Result<SimilarityOutput, VectorOpError> {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return Err(VectorOpError {
        result: VectorManagerResult::BadParams,
        message: ERR_VECTOR_SET_INDEX.to_vec(),
      });
    };
    let effective_ef = effective_search_ef(
      opts.search_exploration_factor,
      opts.count,
      opts.filter,
      opts.max_filtering_effort,
    );

    // 元素不存在：对齐 C# VectorSetElementSimilarity 的 MissingElement 出参
    //（会话层据此写 "Element not in Vector Set"）
    if !self.service.check_external_id_valid(index.context, element) {
      return Err(VectorOpError {
        result: VectorManagerResult::MissingElement,
        message: super::resp_server_session_vectors::ERR_ELEMENT_NOT_IN_SET.to_vec(),
      });
    }

    if !opts.filter.is_empty() && try_compile(opts.filter).is_err() {
      return Err(VectorOpError {
        result: VectorManagerResult::BadParams,
        message: ERR_COMPILING_FILTER.to_vec(),
      });
    }

    let search_params = SearchParams {
      count: opts.count,
      search_exploration_factor: effective_ef,
      filter_len: opts.filter.len(),
      max_filtering_effort: opts.max_filtering_effort,
    };

    let output = self
      .service
      .search_element(index.context, element, search_params)
      .map_err(|_| VectorOpError {
        result: VectorManagerResult::BadParams,
        message: ERR_VECTOR_SERVICE_RESPONSE.to_vec(),
      })?;
    let mut hits = output.hits();
    apply_delta_cutoff(&mut hits, opts.delta);

    self.build_similarity_output(index.context, hits, opts.filter, opts.include_attributes)
  }

  /// 组装检索输出：id/距离/属性/位图缓冲（对齐 C# 出参布局）。
  fn build_similarity_output(
    &self,
    context: u64,
    hits: Vec<SearchHit>,
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
      let passed = super::vector_manager_filter::apply_post_filter(
        filter,
        found,
        &view,
        &mut output.filter_bitmap,
      );

      let mut new_output_ids = Vec::new();
      let mut new_output_distances = Vec::new();
      let mut new_output_attributes = Vec::new();
      let mut new_filter_bitmap = vec![0u8; passed.div_ceil(8)];

      let mut new_idx = 0;
      for (i, hit) in hits.iter().enumerate() {
        if (output.filter_bitmap[i >> 3] & (1 << (i & 7))) != 0 {
          new_output_ids.extend_from_slice(&(hit.external_id.len() as i32).to_le_bytes());
          new_output_ids.extend_from_slice(&hit.external_id);
          new_output_distances.push(hit.distance);

          if include_attributes {
            let attr = self
              .service
              .get_attribute(context, &hit.external_id)
              .unwrap_or_default();
            new_output_attributes.extend_from_slice(&(attr.len() as i32).to_le_bytes());
            new_output_attributes.extend_from_slice(&attr);
          }

          new_filter_bitmap[new_idx >> 3] |= 1 << (new_idx & 7);
          new_idx += 1;
        }
      }

      output.found = passed;
      output.output_ids = new_output_ids;
      output.output_distances = new_output_distances;
      output.output_attributes = new_output_attributes;
      output.filter_bitmap = new_filter_bitmap;
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
    if !self.is_enabled() {
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
      let recovered = self.recovered_metadata.pin();
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
      let recovered = self.recovered_indexes.pin();
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
        if self.recovered_indexes.pin().contains_key(&context) {
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

    self.recovered_indexes.pin().clear();

    if needs_updated {
      self.update_context_metadata();
    }

    // 恢复未完成的清理
    let _ = self.cleanup_task_channel.push(0);

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
    self.recovered_indexes.pin().insert(index.context, 0);
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

    let recovered = self.recovered_metadata.pin();
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
      if self.is_enabled() && ns.len() == 1 && ns[0] == METADATA_NAMESPACE {
        // 上下文元数据载入需暂存（键 = 元数据数组下标 i32 LE）
        return self.recovered_context_metadata(key, value);
      }
      return true;
    }

    if record_type == RECORD_TYPE {
      // 句柄属于写入进程 —— 即使 Vector Set 未启用也必须清指针，
      // 否则稍后启用时会沿指针访问已死亡的地址空间
      Self::clear_index_pointer(value);

      if self.is_enabled() {
        self.recovered_vector_set_index_key(value);
      }
    }

    true
  }
}

/// 有效检索探索因子：`max(EF, count)`；带过滤时按 FILTER-EF effort 放大候选队列
/// （对齐 C# maxFilteringEffort 入参的过滤过取语义）。
fn effective_search_ef(
  ef: usize,
  count: usize,
  filter: &[u8],
  max_filtering_effort: usize,
) -> usize {
  let base = ef.max(count);
  if filter.is_empty() {
    base
  } else {
    base.max(count.saturating_mul(max_filtering_effort.max(1)))
  }
}

/// EPSILON 最大距离截断（有限 delta 时剔除超距命中）。
fn apply_delta_cutoff(hits: &mut Vec<SearchHit>, delta: f32) {
  if delta.is_finite() {
    hits.retain(|h| h.distance <= delta);
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

  /// 迭代各属性字节段。
  pub fn segments(&self) -> Vec<&'a [u8]> {
    self.iter().collect()
  }
}

impl<'a> IntoIterator for &'a AttributeView<'a> {
  type Item = &'a [u8];
  type IntoIter = LengthPrefixedIter<'a>;

  #[inline]
  fn into_iter(self) -> Self::IntoIter {
    self.iter()
  }
}
