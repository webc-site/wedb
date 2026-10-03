//! 向量集合管理器（对标 libs/server/Resp/Vector/VectorManager.cs）
//!
//! 各 C# partial（ContextMetadata/Index/Filter/Locking/ElementData/Cleanup/
//! Replication/Quantization）按 Rust 惯例拆分到本目录的
//! `vector_manager_*.rs` 模块，以多个 `impl VectorManager` 块承接；
//! 另将共享常量/类型拆至 [`super::types`]，相似度检索族拆至
//! `vector_manager_similarity`，FLUSH 域回收族拆至 `vector_manager_reclaim`
//! （上述拆出项经本模块 `pub use` 再导出，路径不变）。
//!
//! C# 侧经 Tsavorite 存储会话 + 原生 DiskANN 操作向量集合；Rust 侧以本域
//! 自建的 [`DiskANNService`]（HNSW）承接索引语义，元素数据经
//! [`WedbVectorStoreCallbacks`] 直写 wkv，索引记录驻留域内登记表，副本/
//! 恢复经 AOF 条目重放重建（见 vector_manager_replication.rs）。

use std::{
  collections::BTreeSet,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  },
};

use parking_lot::{Mutex, RwLock};
use wbase::{
  map::{ConcurrentMap, new_concurrent_map},
  pool::EventWorkQueue,
};
use wkv::WatchHook;
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, VectorQuantType, VectorSetFlags,
  prepare_vector_data, store::StoreCallbacks,
};

// 拆出项再导出（常量/类型 → types，元素数据族 → element_data，域回收族 →
// reclaim）：外部与族内既有 `vector_manager::X` 调用路径全部保持不变
pub use super::{
  types::{
    CONTEXT_METADATA_SIZE, CONTEXT_STEP, CONTEXTS_PER_METADATA, ERR_QUANTIZATION_MISMATCH,
    ERR_VECTOR_SERVICE_RESPONSE, INDEX_SIZE_BYTES, MAX_EXPLORATION_FACTOR,
    MAX_FILTERING_SCALE_FACTOR, MAX_RETRIEVE_COUNT, MAX_VECTOR_DIMENSIONS, RECORD_TYPE,
    SimilarityOutput, VADD_APPEND_LOG_ARG, VREM_APPEND_LOG_ARG, VSETATTR_APPEND_LOG_ARG,
    VSETINDEX_APPEND_LOG_ARG, VectorAddArgs, VectorManagerOptions, VectorManagerResult,
    VectorOpError, VectorSearchOptions,
  },
  vector_manager_element_data::AttributeView,
  vector_manager_reclaim::RegistryReclaim,
};
use super::{
  types::{ERR_VECTOR_SET_INDEX, dimension_mismatch, err, normalize_quantization_task_count},
  vector_manager_cleanup::{CleanupGate, CleanupRuntime},
  vector_manager_context_metadata::ContextMetadata,
  vector_manager_index::Index,
  vector_manager_locking::{
    VectorSetKeyLocks, VectorSetLocks, domain_prefix, registry_key, split_registry_key,
  },
  vector_manager_quantization::{QuantizationChannel, QuantizationState, QuantizationStep},
  vector_manager_replication::VectorAofSink,
  vector_registry_recovery::{
    RegistryPersistence, index_registry_physical_key, metadata_registry_physical_key,
  },
};
use crate::resp::vector::vector_store_callbacks::{
  DedicatedVectorSessionFactory, WedbVectorStoreCallbacks, active_vector_session_bound,
};

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
  /// VADD/VREM 键 → 索引记录登记表（存储会话桥接承接）。
  pub(crate) key_index_registry: ConcurrentMap<Vec<u8>, [u8; INDEX_SIZE_BYTES]>,
  /// 向量集合键锁注册表。
  pub vector_set_locks: VectorSetLocks,
  /// 清理任务通道（context 载荷）。
  pub cleanup_task_channel: EventWorkQueue<u64>,
  /// 请求清理通道（context 载荷）。
  pub request_cleanup_task_channel: EventWorkQueue<u64>,
  /// 量化工作通道。
  pub quantization_channel: QuantizationChannel,
  /// 量化分片数（原子量：C# 构造期由 serverOptions.VectorSetQuantizationTaskCount
  /// 定值，本仓冷启动装配三件套漏斗 `open_node_with_config` 拿不到 NodeArgs，
  /// 与 `is_enabled` 同形改内部可变——装配尾段 args 链单点注入生效值，消费面
  /// worker 惰性拉起 / 回填分片读点均在 accept 之后，时序天然安全）
  pub quantization_task_count: AtomicUsize,
  /// 建表请求数（INFO bg_task_health 登记计数 + 测试观测；`Arc` 供构造期
  /// 一次性登记进 wbase::supervise 快照，量化吞吐冻结不再与空闲不可区分）
  pub quantization_requests_processed: Arc<AtomicU64>,
  /// 回填请求数（INFO bg_task_health 登记计数 + 测试观测；`Arc` 同上）
  pub quantization_backfills_processed: Arc<AtomicU64>,
  /// 登记摘除失败数（INFO bg_task_health 登记计数 + 测试观测；remove 路径
  /// 全部失败模式——登记表 miss / 无会话 / 写透失败——单点归入同一计数：
  /// 摘除失败即内存已摘而盘上墓碑缺，重启回建触发幽灵复活，纯日志易被
  /// 淹没，计数面常驻可巡检；`Arc` 供构造期一次性登记进 wbase::supervise
  /// 快照，形态同上两条量化计数）
  pub vector_registry_remove_failures: Arc<AtomicU64>,
  /// 清理暂停闸门。
  pub cleanup_gate: CleanupGate,
  /// 清理运行时（任务生命周期/静默等待）。
  pub cleanup_runtime: CleanupRuntime,
  /// AOF 直推注入端口（装配期注入；None = 无 AOF 域，合成写静默跳过）。
  pub(crate) aof_sink: RwLock<Option<Arc<VectorAofSink>>>,
  /// 登记旁路记录持久化承接（装配期一次性注入；未注入时写透零开销旁路：
  /// 纯内存登记表形态，测试夹具与嵌入式裸装配沿用。经
  /// [`RegistryPersistence`] 写透 wkv 存储域 [`KeyTag::VectorRegistry`]
  /// 旁路记录，登记表与上下文元数据随检查点持久，重启恢复回建——对标
  /// C# 索引/元数据记录驻主存随检查点持久的同等语义。dyn 擦除仅此字段
  /// 一处（故障替身注入边界，RPITIT trait 非 dyn 兼容有案），下游写透链
  /// （persist_registry_index 族）async fn 直 await 装箱 future，无二次
  /// 装箱）。
  pub(crate) registry_store: OnceLock<Arc<dyn RegistryPersistence>>,
  /// 向量写面 WATCH 版本推进钩子（装配期一次性注入；未注入时零开销旁路，
  /// 对标 [`wkv::WatchHook`] 的 WedbStore::set_watch_hook 装配形态）。
  watch_bump: OnceLock<WatchHook>,
  /// 专用向量会话工厂（后台清理/量化与恢复回建臂每次处理项经
  /// [`Self::bind_dedicated_session`] 自持一份会话再落盘）。
  ///
  /// 注入槽具 last-wins 覆写语义（装配尾段覆写，非先设先得）：嵌入式三件套
  /// 形态在 `open_node_with_config` 钉自身 store 的默认工厂，生产 `Node` 装配
  /// 尾段 `from_parts` 再以现取现用置换槽工厂覆写为终态（工单
  /// zcode-r137c-snaplock2 宗二不回退）——两形态共用本单点，禁第二工厂源。
  ///
  /// 对标 C# 清理任务的专用 `dropSession`
  ///（libs/server/Resp/Vector/VectorManager.Cleanup.cs:149）与
  /// `ActiveThreadSession` 的「谁干活谁自备会话」口径：本管理器不再持有
  /// 任何跨连接共享的存储会话，故随无状态回调一同保持 `Send + Sync`。
  dedicated_session_factory: Mutex<Option<DedicatedVectorSessionFactory>>,
  /// 存储回调通道（对标 C# ActiveThreadSession 存储上下文）。
  pub callbacks: Callbacks<S>,
}

/// 专用会话守卫的 `Send` 类型通行证（与
/// [`super::vector_registry_recovery::RegistrySessionRef`] 同一论证：compio
/// thread-per-core 下任务不迁线程，future 及其栈上值（含守卫 drop）同在
/// 其所属任务线程发生，`Send` 纯为满足 wkv DeleteMissHook 装箱契约的
/// 静态要求）。
///
/// 守卫须持有至写透 future 完成——兜底专用会话的属主即本守卫（所有权）
/// 与写透 future（引用）两侧，先于 `.await` 让位即引用悬垂
/// （use-after-free，engine_swap_hook_bundle SIGSEGV 实证）；连接会话形态
/// 下引用随 exec 段存活，守卫本就无须持有（`None` 直通）。
///
/// 负载假设：同线程任一时刻至多一个未绑定兜底臂在飞（后台臂单任务串行、
/// 连接臂 exec 段整段绑定），兜底会话不被同核他臂短路共享跨 await。
pub(crate) struct DomainGuardSend {
  /// 仅承接所有权：drop 即还原线程槽并销毁专用会话
  pub(crate) _guard: Option<super::vector_store_callbacks::ActiveDedicatedVectorSession>,
}

// SAFETY: 见类型注释（任务不迁线程，future 只在其所属任务线程上 poll）。
unsafe impl Send for DomainGuardSend {}

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
  /// 索引创建经 `create_index_locked` 统一携带。
  pub fn new(options: VectorManagerOptions, callbacks: Callbacks<S>) -> Self {
    let quantization_task_count =
      normalize_quantization_task_count(options.quantization_task_count);

    Self {
      is_enabled: AtomicBool::new(options.is_enabled),
      service: DiskANNService::default(),
      context_metadatas: Mutex::new(vec![Default::default()]),
      dirty_context_metadatas: Mutex::new(BTreeSet::new()),
      metadata_store: new_concurrent_map(),
      recovered_indexes: new_concurrent_map(),
      recovered_metadata: new_concurrent_map(),
      key_index_registry: new_concurrent_map(),
      vector_set_locks: VectorSetLocks::default(),
      cleanup_task_channel: EventWorkQueue::new(),
      request_cleanup_task_channel: EventWorkQueue::new(),
      quantization_channel: QuantizationChannel::new(),
      quantization_task_count: AtomicUsize::new(quantization_task_count),
      quantization_requests_processed: Arc::new(AtomicU64::new(0)),
      quantization_backfills_processed: Arc::new(AtomicU64::new(0)),
      vector_registry_remove_failures: Arc::new(AtomicU64::new(0)),
      cleanup_gate: CleanupGate::new(),
      cleanup_runtime: CleanupRuntime::new(),
      aof_sink: RwLock::new(None),
      registry_store: OnceLock::new(),
      watch_bump: OnceLock::new(),
      dedicated_session_factory: Mutex::new(None),
      callbacks,
    }
  }

  /// 装配 AOF 直推注入端口（服务器级 AOF 点亮时由宿主注入；C# 侧合成写
  /// 经主日志 RMW 自动入日志的等价装配位）。
  pub fn set_aof_sink(&self, sink: Arc<VectorAofSink>) {
    *self.aof_sink.write() = Some(sink);
  }

  /// 装配尾段注入量化任务数生效值（对标 C# VectorManager 构造器读
  /// `serverOptions.VectorSetQuantizationTaskCount` 的注入位；本仓冷启动
  /// 三件套漏斗 `open_node_with_config` 无 NodeArgs，构造期恒 0 落默认核数，
  /// 真实 CLI 值经 `open_from_args_with_config` 装配链尾段单点覆写，与
  /// [`Self::is_enabled`] 的 `with_vector_set_preview` 同形。入参为 `max(0)`
  /// 折叠后的原始值，归一逻辑复用 [`normalize_quantization_task_count`]）
  pub fn set_quantization_task_count(&self, raw: usize) {
    self
      .quantization_task_count
      .store(normalize_quantization_task_count(raw), Ordering::Relaxed);
  }

  /// 注入向量写面 WATCH 版本推进钩子（装配期一次性调用；重复注入返回假）。
  ///
  /// 对标 C# 向量写经 Unified RMW 成功钩子无条件 IncrementVersion
  ///（libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:54/:119/:166）
  /// 与 wkv 引擎级 `WedbStore::set_watch_hook` 的同形装配：向量化登记表变更
  /// （try_add / try_remove / try_set_attribute / rename_vector_set 新旧键 /
  /// rename_vector_set_of 旧键）不经 wkv 用户键写入口，版本推进由本钩子在
  /// 写漏斗内部收口
  pub fn set_watch_bump(&self, hook: WatchHook) -> bool {
    self.watch_bump.set(hook).is_ok()
  }

  /// 向量登记表变更的 WATCH 版本推进单点（写漏斗内部消费；未装配
  /// 零开销旁路）。键口径为 (会话物理前缀, 用户键裸字节)，与 wkv 引擎级
  /// 写面钩子同进 [`version_map_watch_hook`](crate::storage::session::storage_session::version_map_watch_hook)
  /// 的 scoped 哈希单点，跨租户/跨库同名向量集互不串扰。
  /// 族内可见（pub(crate)）：AOF 重放臂（vector_manager_replication）迁移
  /// 成功后同样经此推进，一处定义禁第二推进形态
  #[inline]
  pub(crate) fn bump_watch(&self, prefix: &[u8], user_key: &[u8]) {
    if let Some(hook) = self.watch_bump.get() {
      hook.call(prefix, user_key);
    }
  }

  /// 注入登记旁路记录持久化承接（服务装配期一次；未注入即纯内存登记表
  /// 形态，登记不随检查点持久）。
  pub fn attach_registry_store(&self, store: Arc<dyn RegistryPersistence>) {
    let _ = self.registry_store.set(store);
  }

  /// 注入专用向量会话工厂（未注入即无后台落盘能力，纯内存形态的向量域
  /// 测试夹具沿用）。
  ///
  /// last-wins 覆写（装配尾段覆写语义，见字段注记）：默认工厂可被置换槽
  /// 工厂或测试侧显式工厂覆写，覆写后旧工厂即刻失效。
  ///
  /// 工厂由宿主以存储句柄构造，每次调用产出一份**新会话并已绑定至本执行域**
  /// 的句柄（对标 C# 清理任务 `new(dropSession)` 的一次性自备会话）。
  pub fn attach_dedicated_session_factory(&self, factory: DedicatedVectorSessionFactory) {
    *self.dedicated_session_factory.lock() = Some(factory);
  }

  /// 为当前执行域取一份专用向量会话（后台清理/量化/恢复臂的处理项粒度调用）。
  ///
  /// 返回值必须持有至本次处理项的同步段结束（`#[must_use]`）；跨 `.await`
  /// 持有会破坏同线程多任务的绑定栈纪律（见
  /// [`vector_store_callbacks`](super::vector_store_callbacks) 模块头）。
  /// 未注入工厂或会话资源耗尽即返回 `None`，调用方按失败口径处理，
  /// 严禁降级为无会话写。
  pub fn bind_dedicated_session(
    &self,
  ) -> Option<super::vector_store_callbacks::ActiveDedicatedVectorSession> {
    // 短临界区仅取工厂句柄引用，会话构造在锁外完成（工厂内开存储会话，
    // 严禁持槽锁调用）
    let factory = self.dedicated_session_factory.lock().clone()?;
    let session = factory();
    if session.is_none() {
      log::error!("向量后台臂取专用会话失败：存储会话槽位不可用");
    }
    session
  }

  /// 本执行域未绑会话时兜底自持一份专用会话（任意线程触达的收口兜底，
  /// 如 wkv 缺席删除钩子、AOF Flush 族重放臂）；已绑定（命令分派段、
  /// 后台处理项段）即空操作零开销。
  ///
  /// 返回值同样须持有至本次处理项的同步段结束（`#[must_use]`）。
  pub(crate) fn ensure_dedicated_session(
    &self,
  ) -> Option<super::vector_store_callbacks::ActiveDedicatedVectorSession> {
    if active_vector_session_bound() {
      return None;
    }
    self.bind_dedicated_session()
  }

  /// 索引登记写透（`put_stored_index` 单点消费；未注入零开销旁路）。
  ///
  /// 会话引用在**调用点同段的首 poll** 取定（登记写透承接体内），引用存活期
  /// 由会话属主承担：连接会话随 exec 段存活，兜底专用会话由守卫持有至写透
  /// 完成（见 [`Self::delete_vector_set_of`] 与 `DomainGuardSend`），禁止在
  /// await 前让位守卫（use-after-free，engine_swap_hook_bundle SIGSEGV 实证）。
  /// 调用链全程同步段直 `.await`，取定时点与旧装箱形态等价。
  #[inline]
  pub(crate) async fn persist_registry_index(
    &self,
    rk: &[u8],
    bytes: &[u8; INDEX_SIZE_BYTES],
  ) -> bool {
    match self.registry_store.get() {
      Some(store) => {
        store
          .put(index_registry_physical_key(rk).as_slice(), bytes)
          .await
      }
      None => true,
    }
  }

  /// 索引登记摘除写透（`remove_stored_index` 单点消费；未注入零开销旁路
  /// （纯内存形态，返回成功）。取会话时点论证同 `persist_registry_index`）。
  #[inline]
  pub(crate) async fn evict_registry_index(&self, rk: &[u8]) -> bool {
    match self.registry_store.get() {
      Some(store) => {
        store
          .remove(index_registry_physical_key(rk).as_slice())
          .await
      }
      None => true,
    }
  }

  /// 上下文元数据落盘写透（`flush_dirty_context_metadata` 单点消费；未注入
  /// 零开销旁路。取会话时点论证同 `persist_registry_index`）。
  #[inline]
  pub(crate) async fn persist_registry_metadata(
    &self,
    index: i32,
    bytes: &[u8; CONTEXT_METADATA_SIZE],
  ) -> bool {
    match self.registry_store.get() {
      Some(store) => {
        store
          .put(metadata_registry_physical_key(index).as_slice(), bytes)
          .await
      }
      None => true,
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:AssertHaveStorageSession
  ///
  /// C# 为 DEBUG 断言（`[ThreadStatic] ActiveThreadSession` 非空）；Rust 侧
  /// 同形校验本执行域的线程槽绑定（见
  /// [`active_vector_session_bound`](super::vector_store_callbacks::active_vector_session_bound)）。
  pub fn assert_have_storage_session(&self) {
    debug_assert!(
      active_vector_session_bound(),
      "进入此路径前必须已在当前执行域绑定向量存储会话"
    );
  }

  #[inline]
  pub fn error_msg(result: VectorManagerResult) -> &'static [u8] {
    result.error_msg()
  }
}

// ======================== 元素增删改查 ========================

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.cs:TryAdd
  ///
  /// 向量集合登记元素向量（对齐 C# TryAdd；假定索引已锁定）。
  pub async fn try_add(
    &self,
    prefix: &[u8],
    key: &[u8],
    index_value: &[u8],
    args: &VectorAddArgs<'_>,
  ) -> Result<VectorManagerResult, VectorOpError> {
    self.assert_have_storage_session();

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

    match self
      .service
      .insert(
        index.context,
        args.element,
        &prepared.bytes,
        args.attributes,
      )
      .await
    {
      DiskAnnInsertResult::True => {
        // 登记面变更即 WATCH 版本推进（对标 C# Unified RMW 成功钩子
        // IncrementVersion；重复添加 Duplicate 零变更不推）
        self.bump_watch(prefix, key);
        Ok(VectorManagerResult::OK)
      }
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
        self.bump_watch(prefix, key);
        Ok(VectorManagerResult::OK)
      }
      DiskAnnInsertResult::False => Ok(VectorManagerResult::Duplicate),
      // 存储写失败（起点装载 / 属性写失败已回滚摘除）透明映射进既有存储
      // 错误通道：会话层 Err 臂回 ERR 错误帧、不写 AOF，杜绝误报 Duplicate
      // 造成应答与存储分叉。全仓该错误态仅此一处映射（service.rs 一处产生）
      DiskAnnInsertResult::StoreError => Err(VectorOpError {
        result: VectorManagerResult::Invalid,
        message: ERR_VECTOR_SERVICE_RESPONSE.to_vec(),
      }),
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:TryRemove
  ///
  /// 按元素键删除向量及其属性；删除成功即 WATCH 版本推进（同 [`Self::try_add`]
  /// 口径，带会话物理前缀，`prefix` 与登记表 `registry_key` 寻址域同源）。
  /// 锁契约（对标 C# VectorStoreOps 的 VectorSetRemove 件锁点 :225 using 锁
  /// 罩住 TryRemove 写体）：本口不自取条带锁，调用方必须已持该键共享读
  /// 守卫（防删排挡，见 [`Self::delete_vector_set`] 排空集自述）
  ///
  /// 去 bool 化：存储读失败走 [`VectorOpError`] 通道（同款先例即 [`Self::try_add`]
  /// 存储写失败透明映射臂）——C# `del ? OK : MissingElement` 两臂在 rust
  /// 拆成三态：缺元素→`Ok(MissingElement)`（应答 0 不变），存储故障→Err
  /// （会话层 ERR 错误帧、不写 AOF），杜绝故障窗假成功应答与存储分叉。
  pub async fn try_remove(
    &self,
    prefix: &[u8],
    key: &[u8],
    index_value: &[u8],
    element: &[u8],
  ) -> Result<VectorManagerResult, VectorOpError> {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return Ok(VectorManagerResult::Invalid);
    };

    match self.service.remove(index.context, element).await {
      Ok(true) => {
        self.bump_watch(prefix, key);
        Ok(VectorManagerResult::OK)
      }
      Ok(false) => Ok(VectorManagerResult::MissingElement),
      Err(e) => {
        log::error!("VREM 存储读失败 key {key:?} element {element:?}: {e}");
        Err(VectorOpError {
          result: VectorManagerResult::Invalid,
          message: ERR_VECTOR_SERVICE_RESPONSE.to_vec(),
        })
      }
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:TrySetAttribute
  ///
  /// 属性写入成功即 WATCH 版本推进（同 [`Self::try_add`] 口径）。
  /// 锁契约（对标 C# VectorStoreOps 的 VectorSetSetAttribute 件锁点 :262
  /// using 锁罩住 TrySetAttribute 写体）：本口不自取条带锁，调用方必须已
  /// 持该键共享读守卫（防删排挡，同 [`Self::try_remove`] 口径）。
  ///
  /// `Err`=存储读/写失败上抛（同 [`Self::try_remove`] 三态口径），禁折
  /// `false` 假阴性；元素/集合缺席仍 `Ok(false)`。
  pub async fn try_set_attribute(
    &self,
    prefix: &[u8],
    key: &[u8],
    index_value: &[u8],
    element: &[u8],
    attribute: &[u8],
  ) -> Result<bool, VectorOpError> {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return Ok(false);
    };
    match self
      .service
      .set_attribute(index.context, element, attribute)
      .await
    {
      Ok(true) => {
        self.bump_watch(prefix, key);
        Ok(true)
      }
      Ok(false) => Ok(false),
      Err(e) => {
        log::error!("VSETATTR 存储读写失败 key {key:?} element {element:?}: {e}");
        Err(VectorOpError {
          result: VectorManagerResult::Invalid,
          message: ERR_VECTOR_SERVICE_RESPONSE.to_vec(),
        })
      }
    }
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
  /// RequestDeletion（记录触发器 GarnetRecordTriggers 的 OnDispose Deleted 臂）；亦供 SET
  /// 覆写守卫与重命名/迁移直接摘除（rust 索引记录驻留登记表，删除 = 登记清理
  /// + 摘除登记表项，键随之消失）。返回是否确有删除。
  ///
  /// 锁协议对标 libs/server/Resp/Vector/VectorManager.Locking.cs:
  /// ReadForDeleteVectorIndex——锁前登记快速命中复核（DEL/UNLINK 常态
  /// 非向量键，未命中零锁争用即返回），命中后取条带独占写锁排空全部在途
  /// 共享读与写，持锁完成索引丢弃与登记摘除，杜绝删除穿透读锁屏障销毁在
  /// 用索引。排空集 = 锁定读面全集：十二命令臂 VSIM/VEMB/VCARD/VDIM/
  /// VGETATTR/VINFO/VISMEMBER/VLINKS/VRANDMEMBER/VREM/VSETATTR 持共享锁
  /// 跨命令体（read_vector_index）、VADD 持读或建锁（read_or_create_vector
  /// _index），全部落在屏障内。
  pub async fn delete_vector_set(&self, prefix: &[u8], key: &[u8]) -> bool {
    let rk = registry_key(prefix, key);
    // 锁前快速命中复核：非向量键常态删除不引入条带锁争用
    if self.stored_index_of(rk.as_slice()).is_none() {
      return false;
    }
    // 条带独占写锁：排空在途读者/写者后摘登记表（delete_vector_set_of 锁内
    // 复核登记，锁前快照可能已被并发写推翻；异步锁等待方在 await 点让出
    // 线程，摘除写透跨 await 持守卫不阻塞同核任务队列）
    let _lock = self.vector_set_locks.acquire_exclusive(rk.as_slice()).await;
    self.delete_vector_set_of(rk.as_slice()).await
  }

  /// 复合登记键直删单点（回收/迁移等已持复合键的内部轴键面）。
  ///
  /// 内部无锁原子执行体，调用方必须已持有 `rk` 条带独占写锁
  /// （同条带非重入）：[`Self::delete_vector_set`] /
  /// [`Self::reclaim_registry_domain`] / 迁移删除臂持单键锁进入；
  /// [`Self::rename_vector_set_of`] 沿用外层 [`VectorSetKeyLocks`]
  /// 双键独占锁，重复加锁即自死锁。
  ///
  /// 执行域兜底：本口经 wkv 缺席删除钩子可被**任意持会话线程**调入
  /// （连接 exec 段已绑定；对象回收/TTL 清扫/AOF 重放等后台臂未绑定），
  /// 未绑定时自持一份专用会话兜底（对标 C# 钩子臂在调用线程自备会话，
  /// 登记表写透与摘除须有当前执行域会话可用）。条带锁与会话域正交：
  /// 锁排空在途读写，专用会话仅供登记表访问，两者须同时成立。
  pub(crate) async fn delete_vector_set_of(&self, rk: &[u8]) -> bool {
    let Some(index_value) = self.stored_index_of(rk) else {
      return false;
    };
    // 兜底守卫跨写透 `.await` 持有（Send 通行证见 [`DomainGuardSend`]）：
    // 登记摘除 future 在**同步调用点**（RegistryPersistence::remove 体内）
    // 取定会话引用并随 future 跨冷区 `.await` 自持，兜底专用会话的唯二属主
    // 是本守卫，守卫先于 await 让位即引用悬垂；连接会话形态下引用随 exec
    // 段存活，守卫无须持有（`None` 直通），两种形态同由本持有口径覆盖
    let _domain = DomainGuardSend {
      _guard: self.ensure_dedicated_session(),
    };
    self.request_deletion(&index_value);
    let write = self.remove_stored_index(rk).await;
    if !write {
      log::error!("delete_vector_set_of: 登记摘除失败: {rk:?}");
    }
    true
  }

  // ======================== 重命名迁移 ========================

  /// RENAME 向量集登记表迁移（C# UnifiedStoreOps.cs 的 RENAME 向量分支的
  /// 登记表对偶：MarkSuppressCleanup(old) → SET(new)=标记前快照 →
  /// UpdateHashSlot(old→new) → DELETE(old) 窗口内清理被抑制）。
  ///
  /// libs/server/Resp/Vector/VectorManager.Index.cs:ClearSuppressCleanup
  ///（C# SET(new) 失败时回滚开窗的恢复路径；rust 以标记前快照原形重写旧键
  /// 承接，快照在锁后读取，锁内无并发写，原形重写即清除窗口标志）。
  ///
  /// 次序逐位对齐 C#：先取标记前快照（拷入新名的记录不带窗口标志，
  /// 新名后续删除照常触发清理）→ 开窗（旧名删除被抑制）→ 新名注册
  ///（同上下文同几何，索引服务零迁移，复用 delete/import 基建）→
  /// 槽位同步 → 摘除旧名（request_deletion 因 SUPPRESS_CLEANUP 被抑制）。
  /// 开窗写透失败即中止：复原形单点恢复旧键写前原值、返回 false（窗口是
  /// 摘旧名臂的安全前提，标记未落续行会以无标志记录触发 request_deletion
  /// 误清新旧名共享的上下文）。新名注册写透失败即回滚：快照原形重写旧键、
  /// 返回 false、不摘旧集；回滚重写走复原形单点，再败时恢复写前原值
  ///（开窗标记记录），旧键登记保全不撤。返回是否确有迁移（旧名未登记即
  /// 无操作；开窗中止与新名写透失败回滚亦 false，调用方须拒本命令，对位
  /// C# 失败臂 NOTFOUND）。
  ///
  /// WATCH 推进：迁移成功即新键 SET(newKey) 语义位恰一推（见体内文注），
  /// 旧键 DELETE(old) 一推在 [`Self::rename_vector_set_of`] 迁移内——
  /// 一命令双键各一推，与 C# RENAME 全 4 case 对齐；迁移失败（新键写透
  /// 失败回滚）两键均不推，对位 C# 失败臂无版本推进。
  ///
  /// 上下游锁界面：旧/新名条带独占锁全程持有（C# txnManager 双键排他锁的
  /// [`VectorSetLocks`] 对偶，按条带序号定序获取防交叉死锁）；锁轴与登记
  /// 表同为复合键域，跨库同名键互不串扰。
  pub async fn rename_vector_set(&self, prefix: &[u8], old_key: &[u8], new_key: &[u8]) -> bool {
    let rk_old = registry_key(prefix, old_key);
    let rk_new = registry_key(prefix, new_key);
    let migrated = self
      .rename_vector_set_of(rk_old.as_slice(), rk_new.as_slice())
      .await;
    if migrated {
      // 新键 SET(newKey) 语义位 WATCH 推进恰一次：C# RENAME 主流程 SET(newKey)
      // 全 4 case 恒执行恒推（libs/server/Storage/Session/UnifiedStore/
      // UnifiedStoreOps.cs:363-364 + UpsertMethods.cs PostInitialWriter 无条件
      // IncrementVersion）。dst 缺席臂 = delete_string 未命中一推（wkv 缺席墓碑
      // 同向计入，MainStore DeleteMethods.cs:16）+ 此处一推 = 两推；dst 存活臂
      // = 清退一推 + 此处一推 = 两推——与 C# DELETE(newKey) + SET(newKey) 双推
      // 对齐，不合并
      self.bump_watch(prefix, new_key);
    }
    migrated
  }

  /// 复合登记键直迁单点（AOF 重放臂等已持复合键的内部轴键面）。
  pub(crate) async fn rename_vector_set_of(&self, rk_old: &[u8], rk_new: &[u8]) -> bool {
    // 双键条带锁：同条带单次获取，异条带按序号定序（async 获取在 await 点
    // 让出，定序协议杜绝交叉互等）。
    // 快照读取必须在取锁之后——锁前读到的登记快照可能已被并发写推翻
    let _locks = VectorSetKeyLocks::acquire(&self.vector_set_locks, rk_old, rk_new).await;

    let Some(index_value) = self.stored_index_of(rk_old) else {
      return false;
    };

    // 开窗：旧名记录置 SUPPRESS_CLEANUP，窗口内旧名删除不触发共享上下文清理
    // libs/server/Resp/Vector/VectorManager.Index.cs:MarkSuppressCleanup
    // libs/server/Resp/Vector/VectorManager.Index.cs:SetFlags
    //（C# Mark→Set 两层 RMW 假写链在此合并为登记表直写单步，无独立 SetFlags 形态）
    // 写透失败即中止迁移：复原形单点恢复写前原值，旧键登记保全可重试。
    // 窗口是摘旧名臂的安全前提——标记未落时内存已复原为无标志原值，续行
    // 会在摘旧名臂以无标志记录触发 request_deletion，误清新旧名共享的
    // 原生索引上下文；对位 C# 复原形 ClearSuppressCleanup(oldKey) →
    // NOTFOUND（C# 标记内存 RMW 不可失败，rust 写透失败臂承其恢复语义）
    if let Some(mut index) = Index::from_bytes(&index_value) {
      index.flags = index.flags.union(VectorSetFlags::SUPPRESS_CLEANUP);
      if !self
        .put_stored_index_restoring(rk_old, &index.to_bytes())
        .await
      {
        log::error!(
          "rename_vector_set_of: 标记旧键 SUPPRESS_CLEANUP 写透失败，迁移中止: {rk_old:?}"
        );
        return false;
      }
    }
    // 新名注册（快照剥除窗口标志，不带窗口标志——对位 C# 标记前快照拷入
    // 新名：正常首迁快照本就无标志，剥除幂等零开销；上一轮失败留存登记的
    // 重试快照携带 SUPPRESS_CLEANUP 时防其迁入新名，否则新名后续删除误
    // 抑制清理致上下文泄漏）；写透失败即回滚保旧集：以标记前快照原形重写
    // 旧键旁路记录（清 SUPPRESS_CLEANUP，对位 C# SET(newKey) 非
    // OK → ClearSuppressCleanup(oldKey) → NOTFOUND 的失败臂，libs/server/
    // Storage/Session/UnifiedStore/UnifiedStoreOps.cs RENAME 向量分支），不摘
    // 旧集、不推 WATCH，旧键登记与原生索引保全可重试。回滚重写走复原形
    // 单点：重写再败时失败臂恢复写前原值（开窗标记记录，内存与盘面一致），
    // 不再整条撤除旧键登记把旧集从用户视图抹掉
    let new_value = match Index::from_bytes(&index_value) {
      Some(mut index) => {
        index.flags = index.flags.difference(VectorSetFlags::SUPPRESS_CLEANUP);
        index.to_bytes()
      }
      None => index_value,
    };
    if !self.put_stored_index(rk_new, &new_value).await {
      log::error!("rename_vector_set_of: 新键登记写透失败: {rk_new:?}");
      if !self.put_stored_index_restoring(rk_old, &index_value).await {
        log::error!("rename_vector_set_of: 回滚旧键登记写透失败: {rk_old:?}");
      }
      return false;
    }
    // 旧名摘除（清理被抑制，仅摘表项）
    self.delete_vector_set_of(rk_old).await;
    // WATCH 版本推进两点分工（一命令一推进纪律）：旧键 DELETE(old) 推进在
    // 登记表迁移内（此处，C# RENAME 向量臂 DELETE(old) 经同族 DeleteMethods
    // 推进）；新键 SET(newKey) 推进在用户键迁移入口 [`Self::rename_vector_set`]。
    // AOF 重放臂 replay_vector_set_rename 不经此两语义位，迁移成功后在臂内
    // 对新旧键各恰一推（r7-data 条 2，与主端口径及 C# 重放恒 IncrementVersion
    // 对齐）
    // 复合登记键自带 `[NsVarint][DbVarint]` 归属前缀（registry_key 单点拼装），
    // 剥域后前缀与用户键一并交 bump_watch 走 scoped 哈希，与主端口径同源
    let (domain, old_user_key) = split_registry_key(rk_old);
    self.bump_watch(domain_prefix(domain).as_slice(), old_user_key);
    true
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

  /// libs/server/Resp/Vector/VectorManager.Index.cs:DropIndex
  ///
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

  // ======================== 恢复 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:ReconcileRecoveredState
  ///
  /// 汇总恢复期积累的簿记：还原元数据、放弃失败的迁移、
  /// 清理未恢复的在用上下文。返回 false 表示保留非空校验失败。
  pub async fn reconcile_recovered_state(&self, require_no_reserved_contexts: bool) -> bool {
    if !self.is_enabled() {
      return true;
    }

    let mut needs_updated = false;
    // 待后台清理的上下文（标记清理处精确收集；C# 消费端对哨兵 0 做
    // 全量扫描，rust 清理协程为按 context 精确清理，故此处逐一投递）
    let mut contexts_to_cleanup = Vec::new();
    // 元数据守卫作用域化：不跨 await（clippy::await_holding_lock），且避免
    // update_context_metadata 内部再取同一锁的潜在死锁
    {
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
          for (i, meta) in metas.iter_mut().enumerate() {
            if let Some(recovered_meta) = recovered.get(&(i as i32)) {
              *meta = *recovered_meta;
            }
          }
        }
      }

      // 恢复期标记迁移中的上下文即迁移失败 —— 尽快收回
      for (i, meta) in metas.iter_mut().enumerate() {
        if let Some(abandoned) = meta.get_migrating() {
          for ctx in abandoned {
            meta.mark_migration_complete(i != 0, ctx, u16::MAX);
            meta.mark_cleaning_up(i != 0, ctx);
            contexts_to_cleanup.push(Self::offset_for_context_metadata(i) + u64::from(ctx));
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
    }

    // 在用但未恢复的上下文 → 标记清理（已恢复者免处理；sweep 内部自锁
    // 元数据，须待上块守卫释放后调用——先读恢复集后清空的次序保持原实现）
    needs_updated |= self.sweep_cleanable_contexts(
      |context, _| !self.recovered_indexes.pin().contains_key(&context),
      &mut contexts_to_cleanup,
    );
    self.recovered_indexes.pin().clear();

    if needs_updated {
      self.update_context_metadata().await;
    }

    // 恢复未完成的清理：逐上下文投递（C# 尾部 push 哨兵 0 由消费端
    // 全量扫描承接；rust 清理协程按 context 精确清理，对位语义相同）
    for context in contexts_to_cleanup {
      let _ = self.cleanup_task_channel.push(context);
    }

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
}
