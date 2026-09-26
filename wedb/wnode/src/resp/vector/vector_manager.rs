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
  future::{Future, ready},
  pin::Pin,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  },
  thread::available_parallelism,
};

use parking_lot::{Mutex, RwLock};
use wbase::{
  map::{ConcurrentMap, HashSet, new_concurrent_map},
  pool::EventWorkQueue,
};
use wkv::WatchHook;
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, LengthPrefixedIter, SearchHit, SearchParams,
  VectorDistanceMetricType, VectorIdFormat, VectorQuantType, VectorSetFlags, VectorValueType,
  prepare_vector_data, try_compile, unpack_length_prefixed,
};

use super::{
  vector_manager_cleanup::{CleanupGate, CleanupRuntime},
  vector_manager_context_metadata::ContextMetadata,
  vector_manager_filter::InlineFilterGuard,
  vector_manager_index::{INDEX_SIZE, Index},
  vector_manager_locking::{
    RegistryDomain, VectorSetKeyLocks, VectorSetLocks, domain_prefix, registry_key,
    registry_user_key, split_registry_key,
  },
  vector_manager_quantization::{QuantizationChannel, QuantizationState, QuantizationStep},
  vector_manager_replication::VectorAofSink,
  vector_registry_recovery::{
    RegistryPersistence, index_registry_physical_key, metadata_registry_physical_key,
  },
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
  /// 单库：物理域 (vns, 换号前旧 vdb) 与可选逻辑槽位盖章——FLUSHDB 载荷域。
  Database {
    vns: u64,
    vdb: u64,
    slot: Option<u16>,
  },
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
      Self::Database { vns, vdb, .. } => domain.vns == vns && domain.vdb == vdb,
      Self::Namespace { vns } => domain.vns == vns,
      Self::All => true,
    }
  }
}

/// 上下文步长（必须为 2 的幂；对齐 C# ContextStep）。
pub const CONTEXT_STEP: u64 = 8;

/// 域回收扫尾轮数（快照-逐键锁回收的竞速窗口补扫轮数；双轮即"存量 +
/// 第一轮锁间隙漏收"两段，残差见 [`VectorManager::reclaim_registry_domain`] 头注）。
const RECLAIM_SWEEP_ROUNDS: usize = 2;

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

impl VectorOpError {
  /// 带文案的失败值（`message` 落堆为拥有态，供 RESP 原样前缀）。
  #[inline]
  fn new(result: VectorManagerResult, message: &[u8]) -> Self {
    Self {
      result,
      message: message.to_vec(),
    }
  }
}

/// `Err` 侧便捷构造（原 `try_add`/`value_similarity` 两处同名 `err` 闭包的单点承接）。
#[inline]
fn err<T>(result: VectorManagerResult, message: &[u8]) -> Result<T, VectorOpError> {
  Err(VectorOpError::new(result, message))
}

/// 相似度检索的查询载荷（[`VectorManager::similarity_search`] 两路入参）：
/// 值路传规约后的查询向量字节，元素路传既有元素外部 id。
enum SimilarityQuery<'a> {
  /// 查询向量（FP32 规约字节）
  Vector(&'a [u8]),
  /// 既有元素外部 id
  Element(&'a [u8]),
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
  /// 命中数（有后置过滤时保持全量命中数、不回写为通过数——C# 以 `_ =`
  /// 弃 [`super::vector_manager_filter::apply_post_filter`] 返回值，通过数
  /// 由 [`Self::filter_bitmap`] 表达，应答上限由序列化端 popcount 收敛）。
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

/// 量化任务数原始值 → 生效分片数单点归一（对标 C# VectorManager.cs:227-228
/// `VectorSetQuantizationTaskCount == 0 ? Environment.ProcessorCount : _`：
/// 0 折物理核数、非 0 钳 [1,1024] 上界；构造期与装配尾段注入共用此唯一归一式，
/// 杜绝两处判定分叉）
fn normalize_quantization_task_count(raw: usize) -> usize {
  match raw {
    0 => available_parallelism().map(|n| n.get()).unwrap_or(4),
    n => n.min(1024),
  }
}

use wvector::store::StoreCallbacks;

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
  /// C# 索引/元数据记录驻主存随检查点持久的同等语义）。
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
struct DomainGuardSend {
  /// 仅承接所有权：drop 即还原线程槽并销毁专用会话
  _guard: Option<super::vector_store_callbacks::ActiveDedicatedVectorSession>,
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
  /// 索引创建经 [`Self::create_index`] 统一携带。
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
  /// 返回装箱 future 而非 async fn：`RegistryPersistence::put` 在**同步
  /// 调用点**即取定会话引用，future 自持引用跨 await——引用存活期归会话
  /// 属主承担：连接会话随 exec 段存活，兜底专用会话由守卫持有至写透完成
  /// （见 [`Self::delete_vector_set_of`] 与 `DomainGuardSend`），禁止在
  /// await 前让位守卫（use-after-free，engine_swap_hook_bundle SIGSEGV 实证）。
  #[inline]
  pub(crate) fn persist_registry_index(
    &self,
    rk: &[u8],
    bytes: &[u8; INDEX_SIZE_BYTES],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    match self.registry_store.get() {
      Some(store) => store.put(index_registry_physical_key(rk).as_slice(), bytes),
      None => Box::pin(ready(true)),
    }
  }

  /// 索引登记摘除写透（`remove_stored_index` 单点消费；未注入零开销旁路
  /// （纯内存形态，返回成功）。返回装箱 future 的取会话时点论证同
  /// `persist_registry_index`）。
  #[inline]
  pub(crate) fn evict_registry_index(
    &self,
    rk: &[u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    match self.registry_store.get() {
      Some(store) => store.remove(index_registry_physical_key(rk).as_slice()),
      None => Box::pin(ready(true)),
    }
  }

  /// 上下文元数据落盘写透（`flush_dirty_context_metadata` 单点消费；未注入
  /// 零开销旁路。返回装箱 future 的取会话时点论证同 `persist_registry_index`）。
  #[inline]
  pub(crate) fn persist_registry_metadata(
    &self,
    index: i32,
    bytes: &[u8; CONTEXT_METADATA_SIZE],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    match self.registry_store.get() {
      Some(store) => store.put(metadata_registry_physical_key(index).as_slice(), bytes),
      None => Box::pin(ready(true)),
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
  /// 锁契约（对标 C# VectorStoreOps.cs:VectorSetRemove 锁点 :225 using 锁
  /// 罩住 TryRemove 写体）：本口不自取条带锁，调用方必须已持该键共享读
  /// 守卫（防删排挡，见 [`Self::delete_vector_set`] 排空集自述）
  pub async fn try_remove(
    &self,
    prefix: &[u8],
    key: &[u8],
    index_value: &[u8],
    element: &[u8],
  ) -> VectorManagerResult {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return VectorManagerResult::Invalid;
    };

    if self.service.remove(index.context, element).await {
      self.bump_watch(prefix, key);
      VectorManagerResult::OK
    } else {
      VectorManagerResult::MissingElement
    }
  }

  /// libs/server/Resp/Vector/VectorManager.cs:TrySetAttribute
  ///
  /// 属性写入成功即 WATCH 版本推进（同 [`Self::try_add`] 口径）。
  /// 锁契约（对标 C# VectorStoreOps.cs:VectorSetSetAttribute 锁点 :262
  /// using 锁罩住 TrySetAttribute 写体）：本口不自取条带锁，调用方必须已
  /// 持该键共享读守卫（防删排挡，同 [`Self::try_remove`] 口径）
  pub async fn try_set_attribute(
    &self,
    prefix: &[u8],
    key: &[u8],
    index_value: &[u8],
    element: &[u8],
    attribute: &[u8],
  ) -> bool {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return false;
    };
    let ok = self
      .service
      .set_attribute(index.context, element, attribute)
      .await;
    if ok {
      self.bump_watch(prefix, key);
    }
    ok
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

  // ======================== FLUSH 域回收 ========================

  /// FLUSH 域回收与恢复对账两臂共用的全表清扫骨架（唯一实现，禁第二逐位
  /// 扫描形态；内部自锁 context_metadatas，调用方不得持其守卫）：遍历各
  /// 元数据块内在用（context ≠ 0 非法哨兵）且未标清理的上下文，对
  /// `accept(context, 槽位盖章)` 放行者盖清理标记、收进 `out` 并标脏所在
  /// 块；返回本轮是否有标记。纯同步位图读改，不跨 await。
  fn sweep_cleanable_contexts(
    &self,
    mut accept: impl FnMut(u64, u16) -> bool,
    out: &mut Vec<u64>,
  ) -> bool {
    let mut marked_any = false;
    let mut metas = self.context_metadatas.lock();
    for (i, meta) in metas.iter_mut().enumerate() {
      let (allow_zero, offset) = (i != 0, Self::offset_for_context_metadata(i));
      let mut marked = false;
      for j in 0..CONTEXTS_PER_METADATA {
        let context = offset + j * CONTEXT_STEP;
        let (_, context_value) = Self::decompose_context(context);
        if context != 0
          && meta.is_in_use(allow_zero, context_value)
          && !meta.is_cleaning_up(allow_zero, context_value)
          && accept(context, meta.slots[j as usize])
        {
          meta.mark_cleaning_up(allow_zero, context_value);
          out.push(context);
          marked = true;
        }
      }
      if marked {
        self.dirty_context_metadatas.lock().insert(i);
        marked_any = true;
      }
    }
    marked_any
  }

  /// 登记表域回收单点漏斗（FLUSHDB/FLUSHNS/FLUSHALL/reset 主端执行段与
  /// AOF Flush 族重放臂共用）。
  ///
  /// 逐条目走 [`split_registry_key`] 比对域值，命中项复用既有
  /// [`Self::request_deletion`] + 摘表通道，不另造清理编排；换号语义下
  /// 旧域条目在新域不可达，回收即清库（重启经 AOF 带域条目按域重建）。
  ///
  /// 双轮扫尾：回收走「快照-逐键锁」，与 worker 线程在途 VADD/RENAME 竞速
  /// 时快照点之后完成落表的条目不在 victims（死域登记条目 + 原生索引 +
  /// context in_use 位运行期滞留）。单轮快照窗口以第二轮补扫收窄——第一轮
  /// 逐键锁间隙中完成落表的漏收条目由第二轮快照捕获；残差仅剩第二轮快照点
  /// 之后的在途落表（概率性运行期泄漏，AOF 全序保证重启重放收敛），不引入
  /// 全域登记锁（违背数据面零锁纪律）。
  pub async fn reclaim_registry_domain(&self, reclaim: RegistryReclaim) {
    let mut target_slots: HashSet<u16> = HashSet::default();
    if let RegistryReclaim::Database { slot: Some(s), .. } = reclaim {
      target_slots.insert(s);
    }

    for _ in 0..RECLAIM_SWEEP_ROUNDS {
      let victims: Vec<Vec<u8>> = self
        .key_index_registry
        .pin()
        .iter()
        .filter(|(rk, _)| reclaim.matches(split_registry_key(rk.as_slice()).0))
        .map(|(rk, _)| rk.as_slice().to_vec())
        .collect();
      if victims.is_empty() {
        break;
      }
      for rk in &victims {
        // 若外部未显式提供 slot，从被清退键的既有元数据中提取 slot 盖章
        if let Some(index_bytes) = self.stored_index_of(rk)
          && let Some(index) = Index::from_bytes(&index_bytes)
        {
          let (c_idx, c_val) = Self::decompose_context(index.context);
          if let Some(meta) = self.context_metadatas.lock().get(c_idx) {
            target_slots.insert(meta.slots[(c_val / (CONTEXT_STEP as u16)) as usize]);
          }
        }
        // 逐 victim 条带独占锁（对齐 C# RunRequestDropTaskAsync 逐键
        // AcquireExclusiveLock → DropIndex → Release 协议）；域级清库语义下
        // 键间锁间隙无碍——换号后旧域条目在新域不可达
        let _lock = self.vector_set_locks.acquire_exclusive(rk).await;
        self.delete_vector_set_of(rk).await;
      }
    }

    // 漏收兜底：按 context_metadatas 反查归属已清除 hash_slot 的残留上下文，补投清理通道，
    // 消除在途 VADD/RENAME 与 FLUSHDB 竞速导致的 context 位与原生索引内存滞留
    let live_contexts: HashSet<u64> = self
      .key_index_registry
      .pin()
      .iter()
      .filter_map(|(rk, bytes)| {
        let (domain, _) = split_registry_key(rk.as_slice());
        if !reclaim.matches(domain) {
          Index::from_bytes(bytes.as_slice()).map(|idx| idx.context)
        } else {
          None
        }
      })
      .collect();

    let all_domains = matches!(reclaim, RegistryReclaim::All);
    let mut orphan_contexts = Vec::new();
    let needs_meta_update = self.sweep_cleanable_contexts(
      |context, slot| {
        (all_domains || target_slots.contains(&slot)) && !live_contexts.contains(&context)
      },
      &mut orphan_contexts,
    );

    if needs_meta_update {
      self.update_context_metadata().await;
    }
    for context in orphan_contexts {
      self.service.drop_index(context);
      if !self.cleanup_task_channel.push(context) {
        log::warn!("Could not enqueue orphan Vector Set cleanup: {context}");
      }
    }
  }

  /// 登记表域内用户键枚举单点（DBSIZE/KEYS/SCAN 慢路径投影）。
  ///
  /// `f` 收剥域后的用户键切片（[`registry_user_key`] 单点）；域判定按复合键字节
  /// 前缀比对——OPPV 变长首字节查表定长，不同域的 [NsVarint][DbVarint] 段互不为
  /// 字节前缀，starts_with 即精确域命中（与 [`registry_key`] 布局同源，零二次解码）。
  pub fn for_each_domain_user_key(&self, prefix: &[u8], mut f: impl FnMut(&[u8])) {
    self.key_index_registry.pin().iter().for_each(|(rk, _)| {
      if rk.starts_with(prefix) {
        f(registry_user_key(rk.as_slice()));
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
  /// WATCH 推进：迁移成功即新键 SET(newKey) 语义位恰一推（见体内文注），
  /// 旧键 DELETE(old) 一推在 [`Self::rename_vector_set_of`] 迁移内——
  /// 一命令双键各一推，与 C# RENAME 全 4 case 对齐。
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
    if let Some(mut index) = Index::from_bytes(&index_value) {
      index.flags = index.flags.union(VectorSetFlags::SUPPRESS_CLEANUP);
      if !self.put_stored_index(rk_old, &index.to_bytes()).await {
        log::error!("rename_vector_set_of: 标记旧键 SUPPRESS_CLEANUP 写透失败: {rk_old:?}");
      }
    }
    // 新名注册（标记前快照，不带窗口标志）→ 旧名摘除（清理被抑制，仅摘表项）
    if !self.put_stored_index(rk_new, &index_value).await {
      log::error!("rename_vector_set_of: 新键登记写透失败: {rk_new:?}");
    }
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

  // ======================== 相似度检索 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:ValueSimilarity
  ///
  /// 以查询向量做相似度检索；`filter` 非空时执行内联过滤 + 结果位图，
  /// 候选队列按 `max_filtering_effort` 放大；`delta` 为最大距离截断（EPSILON）。
  pub async fn value_similarity(
    &self,
    index_value: &[u8],
    value_type: VectorValueType,
    values: &[u8],
    opts: &VectorSearchOptions<'_>,
  ) -> Result<SimilarityOutput, VectorOpError> {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return err(VectorManagerResult::BadParams, ERR_VECTOR_SET_INDEX);
    };

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

    self
      .similarity_search(&index, opts, SimilarityQuery::Vector(&prepared.bytes))
      .await
  }

  /// libs/server/Resp/Vector/VectorManager.cs:ElementSimilarity
  ///
  /// 以既有元素为查询中心做相似度检索（过滤/截断语义同 [`Self::value_similarity`]）。
  /// 刻意偏差：元素存在性前置检查激活 C# 会话层死分支文案，见 `doc/zh/deviations.md` §79。
  pub async fn element_similarity(
    &self,
    index_value: &[u8],
    element: &[u8],
    opts: &VectorSearchOptions<'_>,
  ) -> Result<SimilarityOutput, VectorOpError> {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return err(VectorManagerResult::BadParams, ERR_VECTOR_SET_INDEX);
    };

    // 刻意偏差（见 doc/zh/deviations.md §79）：前置元素存在性检查激活 C# 会话层死分支
    // 文案 "Element not in Vector Set"（C# 实际回落量化 mismatch 误导文案；严禁回改）
    if !self
      .service
      .check_external_id_valid(index.context, element)
      .await
    {
      return err(
        VectorManagerResult::MissingElement,
        super::resp_server_session_vectors::ERR_ELEMENT_NOT_IN_SET,
      );
    }

    self
      .similarity_search(&index, opts, SimilarityQuery::Element(element))
      .await
  }

  /// 相似度检索的共享检索段（[`Self::value_similarity`] 与
  /// [`Self::element_similarity`] 两路唯一实现）：两路差异仅查询载荷与前置
  /// 校验（留在各自入口），本段承接有效 EF 折算 → 过滤程序单次编译 → 守卫下
  /// 服务调用 → EPSILON 截断 → 输出组装，错误文案与调用次序逐字保持原双路实现。
  async fn similarity_search(
    &self,
    index: &Index,
    opts: &VectorSearchOptions<'_>,
    query: SimilarityQuery<'_>,
  ) -> Result<SimilarityOutput, VectorOpError> {
    // 编译一次并持有程序（内联过滤装配与此校验同源，禁二次编译）；口径
    // 不变：非法 FILTER → ERR Compiling filter failed
    let program = if opts.filter.is_empty() {
      None
    } else {
      match try_compile(opts.filter) {
        Ok(program) => Some(program),
        Err(_) => return err(VectorManagerResult::BadParams, ERR_COMPILING_FILTER),
      }
    };

    let search_params = SearchParams {
      count: opts.count,
      search_exploration_factor: effective_search_ef(opts),
      filter_len: opts.filter.len(),
      max_filtering_effort: opts.max_filtering_effort,
    };

    // 内联过滤装配（对标 C# ValueSimilarity 的 filterState 构造 →
    // InlineFilterStatePtr = &filterState → finally 置 null，
    // libs/server/Resp/Vector/VectorManager.cs:862-905 与 ElementSimilarity
    // 的同构装配 :1034-1075）：守卫覆盖 service.search_* 全程（async 检索下
    // 守卫跨 `.await` 存活——thread-local 槽 save/restore 按同线程 LIFO 嵌套，
    // 逐任务在自己作用域内 bind/drop 即互不串扰；图检索逐候选内联求值标量
    // 过滤，离开作用域自动还原。无过滤时无守卫，回调回落放行）。
    let output = {
      let _inline_filter = program.map(|program| InlineFilterGuard::bind(opts.filter, program));
      let hits = match query {
        SimilarityQuery::Vector(vector) => {
          self
            .service
            .search_vector(index.context, vector, search_params)
            .await
        }
        SimilarityQuery::Element(element) => {
          self
            .service
            .search_element(index.context, element, search_params)
            .await
        }
      };
      hits.map_err(|_| {
        VectorOpError::new(VectorManagerResult::BadParams, ERR_VECTOR_SERVICE_RESPONSE)
      })?
    };
    let mut hits = output.hits();
    apply_delta_cutoff(&mut hits, opts.delta);

    self
      .build_similarity_output(index.context, hits, opts.filter, opts.include_attributes)
      .await
  }

  /// 组装检索输出：id/距离/属性/位图缓冲（对齐 C# 出参布局）。
  async fn build_similarity_output(
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

    // 属性（长度前缀；缺失元素长度 0）——单次读取，过滤求值与应答序列化
    // 共用同一份（禁二次存储读取）
    if include_attributes || !filter.is_empty() {
      output.output_attributes = self
        .fetch_vector_element_attributes(context, &output.output_ids)
        .await;
    }

    // 过滤位图（后置过滤）：零压实契约——位图按原结果下标置位，序列化端
    // write_resp2/write_resp3 按位跳过未过项（C# ApplyPostFilter 同款
    // "No in-place compaction"，保留命中槽位与位图索引的 1:1 对应）；
    // output.found 保持全量命中数，应答上限由序列化端 popcount 收敛
    if !filter.is_empty() {
      Self::ensure_filter_bitmap_size(&mut output.filter_bitmap, found);
      let view = AttributeView {
        raw: &output.output_attributes,
      };
      super::vector_manager_filter::apply_post_filter(
        filter,
        found,
        &view,
        &mut output.filter_bitmap,
      );
      // 属性仅为过滤求值而读（调用方未选属性）——求值完即释放，不随
      // 输出滞留
      if !include_attributes {
        output.output_attributes = Vec::new();
      }
    }

    Ok(output)
  }

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
    self.service.get_attribute(index.context, element).await
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
  pub async fn try_get_embedding(&self, index_value: &[u8], element: &[u8]) -> Option<Vec<f32>> {
    self.assert_have_storage_session();
    let index = Index::from_bytes(index_value)?;
    let embedding = self.service.embedding_of(index.context, element).await?;

    // 元素可能已被删除 —— 校验内部 id 仍有效
    let internal = self.service.internal_id_of(index.context, element).await?;
    if !self
      .service
      .check_internal_id_valid(index.context, internal)
      .await
    {
      return None;
    }
    Some(embedding)
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
      self.service.get_full_vector(index.context, element).await?
    } else {
      match self.service.get_quant_vector(index.context, element).await {
        Some(bytes) => bytes,
        None => self.service.get_full_vector(index.context, element).await?,
      }
    };

    // 对齐 C#：占位值（DiskANN 无直接等价物）
    let norm = 1.0;
    let range = (quant == VectorQuantType::Q8).then_some(1.0);

    Some((bytes, quant, norm, range))
  }

  /// libs/server/Resp/Vector/VectorManager.cs:IsMember
  ///
  /// 元素是否属于该向量集合。
  pub async fn is_member(&self, index_value: &[u8], element: &[u8]) -> bool {
    let Some(index) = Index::from_bytes(index_value) else {
      return false;
    };
    self
      .service
      .check_external_id_valid(index.context, element)
      .await
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

/// 有效检索探索因子：`max(EF, count)`；带过滤时按 FILTER-EF effort 放大候选队列
/// （对齐 C# maxFilteringEffort 入参的过滤过取语义）。
fn effective_search_ef(opts: &VectorSearchOptions<'_>) -> usize {
  let base = opts.search_exploration_factor.max(opts.count);
  if opts.filter.is_empty() {
    base
  } else {
    base.max(opts.count.saturating_mul(opts.max_filtering_effort.max(1)))
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
}
