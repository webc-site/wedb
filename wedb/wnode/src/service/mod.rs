//! 节点服务编排：存储引擎（wkv）与 AOF 日志层（GarnetLog + waof 磁盘子日志）
//! 的统一收口
//!
//! 对标 Garnet 的单一 AOF 机制：写入端统一经 [`crate::aof::garnet_log::GarnetLog::enqueue`]
//! （唯一条目编码定义），重放端统一经 [`AofProcessor`]（唯一重放分发），
//! 磁盘承载为 [`crate::aof::waof_sublog::WaofSublog`]（waof `WalLog` 的 GarnetLog 后端适配），
//! 域装配唯一入口 [`single_log_aof`]（库管理面与数据写入面共享同一物理
//! 日志实例）。协议载荷为 [`crate::aof::replay_input::ReplayInput`]（C# StringInput 的
//! 序列化形态），apply → log 顺序由 wkv 写监听端口固化，条目 store_version
//! 取写入时存储版本（checkpoint token，重放端跳过低版本条目）。

use std::{
  future::Future,
  io,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use parking_lot::RwLock;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use waof::{AofAddress, AofEntryType, WalLog};
use wbase::{
  align::{DEFAULT_SECTOR_SIZE, prev_power_of2},
  supervise::{self, supervise_resumable},
};

/// 生产主日志默认段大小（1GB，对标 C# ServerOptions.SegmentSize = "1g" 默认值与 GarnetServerOptions.cs:801 恒分段）
pub(crate) const DEFAULT_MAIN_LOG_SEGMENT_SIZE: u64 = 1 << 30;

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const AOF_SIZE_LIMIT_TASK: &str = "aof_size_limit";
/// 同上（索引自动扩容任务）
const INDEX_AUTO_GROW_TASK: &str = "index_auto_grow";
use wbftree::{DEFAULT_MIGRATION_CHUNK_SIZE, RANGE_INDEX_STUB_SIZE, RangeIndexStub};
use wcol::{
  RespInputFlags,
  itembroker::{collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker},
};
use wconf::{
  ConfigReconcile, DEFAULT_READ_CACHE_MEMORY_SIZE, HlogProjection, NodeArgs, RuntimeServerConfig,
  RuntimeServerOptions, ServerConfigType,
  node_options::DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS,
};
use wdev::{Device, SegmentedDevice};
use wkv::{
  Error, StoreConfig, StoreEvent, StoreEventSink, StoreSession, WedbStore,
  store::resize::IndexResizeState,
};
use wlua::{LuaTimeoutManager, StoreScriptCache};
use wmetric::{SessionMetricsHandle, SlowLogContainer};
use wpubsub::subscribe_broker::SubscribeBroker;
use wresp::command::RespCommand;
#[cfg(feature = "tls")]
use wtls::ServerTlsConfig;
use wtxn::{TxnBarrier, TxnBarrierTicket, TxnLockTable, WatchVersionMap};
use wval::{KeyTag, NO_ETAG, NamespaceDbCodec, TaggedKeyBuf};
use wvector::Callbacks;

#[cfg(feature = "tls")]
use crate::server::tls_config_from_node;
use crate::{
  aof::{
    AofProcessor, AofSettings, AofWriteContext,
    aof_processor::ReplayTarget,
    garnet_append_only_file::GarnetAppendOnlyFile,
    readconsistency::replica_read_session_context::ReadSessionState,
    recover::aof_recover::AofRecover,
    replay_input::{EMPTY_REPLAY_INPUT_BYTES, ReplayInputSlice},
    waof_sublog::single_log_aof,
  },
  config_owner::apply_config_reconcile,
  database::{GarnetDatabase, SingleDatabaseManager},
  primary_tasks::PrimaryTasks,
  range_index::range_index_manager_replication::{
    RangeIndexManagerReplication, RangeIndexStreamArgs,
  },
  resp::{
    RespSessionConsumer, SessionDependencies,
    garnet_api::{CheckpointCtx, CollectionNotify, StoreGarnetApi},
    info_provider::init_startup_ticks,
    metrics_commands::new_slow_log_container,
    objects::collection_item_source::CollectionItemSource,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_replication::VectorAofSink,
      vector_registry_recovery::WedbRegistryPersistence,
      vector_store_callbacks::{
        ActiveDedicatedVectorSession, ActiveVectorSessionGuard, DedicatedVectorSessionFactory,
        OwnedActiveVectorSession, WedbVectorStoreCallbacks,
      },
    },
  },
  servers::consumer_registry::ConsumerRegistry,
  storage::session::storage_session::{
    StorageSession, vector_registry_delete_hook, vector_version_watch_hook, version_map_watch_hook,
  },
  traits::{SessionProviderFace, WireFormat},
};

mod aof_sink;
mod checkpoint;
mod open;
mod provider;
mod swap;
mod wal;

use self::swap::build_txn_lock_table;

/// 节点共享引擎句柄类型别名（收敛 `Arc<WedbStore<D>>` 复合泛型签名）
pub type SharedStore<D> = Arc<WedbStore<D>>;

/// 引擎在线置换钩子束类型（入参 = 换入引擎；用于 WATCH 版本推进与 AOF 镜像重挂）
pub(crate) type EngineSwapHook = Arc<dyn Fn(&SharedStore<SegmentedDevice>) + Send + Sync>;

/// 单机网络服务：存储引擎会话 + AOF 日志层的编排门面
///
/// 内部持有一个 [`StoreSession`]（epoch 参与者随会话注册）。多连接场景下
/// 每连接经 [`Self::store`] 自行 `new_session`，本门面仅承担服务级编排
pub struct NodeService<D: Device> {
  session: StoreSession<D>,
  aof: Arc<GarnetAppendOnlyFile>,
  /// 范围索引 AOF 复制面单例（C# `StoreWrapper.rangeIndexManager` 一体三面
  /// ——命令面/升阶分块/流重组——在 rust 拆为引擎 + 复制面两件，引擎归
  /// [`SharedStore::range_index`]，本字段是在线复制面唯一实例；与
  /// `AofSinkContext.ri` 共持同一 Arc，宿主停机链经提供者转交此句柄收口）
  ri: Arc<RangeIndexManagerReplication>,
}

/// AOF 写事件监听器上下文（静态分发环境）
struct AofSinkContext {
  aof: Arc<GarnetAppendOnlyFile>,
  /// RI AOF 复制面：集合升阶树数据经既有 RangeIndexStreamChunk 通道灌入
  /// （与回放面 replay_into_session 各持一实例，同一引擎句柄派生）
  ri: Arc<RangeIndexManagerReplication>,
}

/// AOF 体积超限自动检查点后台任务（C# `libs/server/TaskManager/TaskType.cs:
/// AofSizeLimitTask` 执行体的对位；注册表托管面判定不移植，承接关系见
/// [`crate::primary_tasks`] 模块头）
///
/// libs/server/StoreWrapper.cs:AutoCheckpointBasedOnAofSizeLimitAsync 的宿主驱动：
/// 每轮 sleep 前现读 `runtime_config` 的 aof-size-limit-enforce-frequency 槽位
///（C# `Task.Delay(TimeSpan.FromSeconds(runtimeConfig.GetInt(...)))` 每轮现取
/// 对译，CONFIG SET 即时生效）。0 / 负值按最小 1s 收敛：C# 0 = Task.Delay(0)
/// 忙转、负值抛异常杀任务，rust 统一压到最小周期防忙转。无配置句柄形态
///（纯测试直调）回落 DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS 兜底。
/// 门控（尺寸预判 → 取暂停闸门 → 副本角色门 → 打点 → 还闸）唯一入口收敛
/// 在臂内，宿主不再绕过 trait 面内联第二套门控。刻意差异：单次失败仅记录日志
/// 不退出循环（C# catch 在 while 外，单次异常即退出杀任务且 AofSizeLimitTask 无重拉通路）。
/// 弱引用捕获，宿主释放后自然退出。任务体经 wbase
/// [`supervise_resumable`] 顶层监督（单点一次成型）：panic 臂落 log::error +
/// 监督快照计数后复位 `started`，由 CONFIG SET / 角色恢复重拉通路自然复活。
pub fn spawn_aof_size_limit_task(
  database_manager: Arc<SingleDatabaseManager<SegmentedDevice>>,
  aof_size_limit: u64,
  runtime_config: Option<Arc<RuntimeServerConfig>>,
  started: Arc<AtomicBool>,
) {
  let freq_secs = |rc: Option<&RuntimeServerConfig>| {
    rc.map(|rc| {
      rc.get_int(ServerConfigType::AofSizeLimitEnforceFrequency)
        .max(1) as u64
    })
    .unwrap_or(DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS)
  };
  let weak_dm = Arc::downgrade(&database_manager);
  drop(database_manager);
  spawn(supervise_resumable(
    AOF_SIZE_LIMIT_TASK,
    Arc::clone(&started),
    async move {
      loop {
        let secs = freq_secs(runtime_config.as_deref());
        sleep(Duration::from_secs(secs)).await;
        let Some(dm) = weak_dm.upgrade() else { break };
        if let Err(e) = dm
          .task_checkpoint_based_on_aof_size_limit_async(aof_size_limit)
          .await
        {
          log::error!("AofSizeLimitTask 自动检查点失败: {e}");
        }
      }
    },
  ))
  .detach();
}

/// 索引周期自动扩容后台任务（C# `libs/server/TaskManager/TaskType.cs:
/// IndexAutoGrowTask` 执行体的对位；注册表托管面判定不移植，承接关系见
/// [`crate::primary_tasks`] 模块头）
///
/// libs/server/StoreWrapper.cs:IndexAutoGrowTaskAsync 的宿主驱动：按
/// frequency_secs（最小 1s）周期驱动
/// [`crate::database::DatabaseManagerBase::grow_index_if_needed`]（对标
/// databaseManager.GrowIndexesIfNeededAsync）——溢出桶占比超阈值即自动翻倍
/// 扩容（溢出计数复用 windex 溢出池水位，扩容动作复用 wkv grow_index，
/// 勿另设第二套判定），索引达上限后任务退出（C# allIndexesMaxedOut 语义）。
/// 刻意差异：单次失败仅记录日志不退出循环（C# catch 在 while 外，单次异常即退出杀任务）。弱引用捕获，宿主释放后自然退出。
/// 任务体经 wbase [`supervise_resumable`] 顶层监督（单点一次成型）：panic 臂落
/// log::error + 监督快照计数后复位 `started`，由重拉通路自然复活。
pub fn spawn_index_auto_grow_task(
  database_manager: Arc<SingleDatabaseManager<SegmentedDevice>>,
  index_max_size: usize,
  resize_threshold: i64,
  frequency_secs: u64,
  started: Arc<AtomicBool>,
) {
  let interval = Duration::from_secs(frequency_secs.max(1));
  let weak_dm = Arc::downgrade(&database_manager);
  drop(database_manager);
  spawn(supervise_resumable(
    INDEX_AUTO_GROW_TASK,
    started,
    async move {
      loop {
        sleep(interval).await;
        let Some(dm) = weak_dm.upgrade() else { break };
        match dm
          .grow_indexes_if_needed(index_max_size, resize_threshold)
          .await
        {
          // 索引已达上限（C# allIndexesMaxedOut = true）：任务收敛退出
          Ok(true) => break,
          Ok(false) => {}
          Err(e) => log::error!("IndexAutoGrowTask 自动扩容失败: {e}"),
        }
      }
    },
  ))
  .detach();
}

/// Lua 超时装配（C# StoreWrapper 构造段 EnableLua 分支 + GarnetServer.cs:Start
/// 的 luaTimeoutManager.Start()）
///
/// `enable_lua` 且 `timeout_ms > 0`（C# Timeout != InfiniteTimeSpan）时建
/// 管理器并 [`LuaTimeoutManager::start`] 拉起专属看门狗线程（C# timerThread
/// 的等价：内核抢占式调度的独立 OS 线程，绝不经 compio reactor——协作式
/// tick 任务会被死循环脚本饿死，且本口在 `RespServerSessionOptions::from`
/// 装配链上，非 compio 线程调用曾是 panic 面）；否则返回 None（无超时形态，
/// VM 零轮询开销）。任意线程可调用。
pub fn assemble_lua_timeout(enable_lua: bool, timeout_ms: i64) -> Option<Arc<LuaTimeoutManager>> {
  (enable_lua && timeout_ms > 0).then(|| {
    let manager = Arc::new(LuaTimeoutManager::new(timeout_ms));
    manager.start();
    manager
  })
}

/// 单机/集群统一节点装配句柄：存储引擎 + 集合项经纪 + 向量集合管理器
pub(crate) type DefaultNodeHandles = (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  Arc<VectorManager>,
);

/// AOF 装配同构段产物（两装配臂共用）：WAL 物理日志 + AOF 门面 + 范围索引
/// 复制面单例（写监听节点仅装配期存活，句柄随段收尾析构）
struct AssembledAofStack {
  wal: Arc<WalLog<SegmentedDevice>>,
  aof: Arc<GarnetAppendOnlyFile>,
  ri: Arc<RangeIndexManagerReplication>,
}

/// 生产装配的存储引擎配置（自适应容量；内置 GC 默认禁用，对标 C#
/// GarnetServerOptions.ExpiredKeyDeletionScanFrequencySecs = -1：装配不越权
/// 开后台任务，启停一律走槽位——启动期 `--expired-key-deletion-scan-freq`
/// 经 `NodeArgs → RuntimeServerOptions` 播种槽位（对标 C# Options.cs:1033 →
/// RuntimeServerConfig.cs:264），运行期 CONFIG SET 同槽经
/// [`apply_config_reconcile`] 调停启停；本函数的 `config.gc` 仅作引擎初值，
/// 首轮会话即被槽位投影覆盖，不构成第二套启停真值源）
pub fn store_config() -> StoreConfig {
  StoreConfig::auto()
}

/// hlog 环形缓冲最小页数（对齐 wkv 内存预算规划器的页数下限；仅本模块
/// [`apply_hlog_overrides`] 消费，不外导）
const HLOG_MIN_NUM_PAGES: usize = 16;

/// 将 hlog 配置段投影到引擎配置（对标 C# GarnetServerOptions.GetSettings 的
/// KVSettings 装配段：PageSize / LogMemorySize 的 pageCount 推导 / MutableFraction
/// / EnableReadCache / ReadCacheMemorySize 页数推导）
///
/// - `page_size`：直接覆盖单页容量（wconf `validated` 已在 `size::validated_page_size_bits`
///   校验核上核过 2 的幂、扇区对齐与 `MIN_PAGE_SIZE_BYTES` 页容量下限；ReadCache 页
///   容量绑定本值，与 C# 主存页/read cache 页共用同一校验入口同形）；
/// - `memory_size`：环形缓冲页数按预算商推导 `prev_power_of2(预算/页容量)`——
///   单点实现见 `wbase::align::prev_power_of2`（对位 C# `Utility.PreviousPowerOf2`）；
///   C# bufferSize = NextPowerOf2(pageCount) 且页按需提交；whlog 为常驻整页分配
///   模型，向下取 2 的幂严守预算上界。预算商不足 [`HLOG_MIN_NUM_PAGES`] 页时
///   实配内存将静默超出声明预算，装配期显式拒绝（报错给出预算与页大小）；
/// - `mutable_fraction`：直接覆盖（wconf `validated` 已校验 10..=95 百分比区间）；
/// - `read_cache` + `read_cache_memory_size`：对标 C# GetSettings 的 ReadCache
///   装配段——开启时 `read_cache_num_pages = prev_power_of2(预算 / 主日志页容量)`
///   并经 wkv `StoreConfig::with_read_cache_pages` 唯一收敛口落值（≥2 页硬下界
///   钳位单点在该 setter，对标 C# AllocatorBase.cs:650-651；本处不另写钳位；
///   wedb ReadCache 页容量绑定主存页，见 wkv `StoreConfig::enable_read_cache`；
///   预算未显式配置取 `wconf::DEFAULT_READ_CACHE_MEMORY_SIZE`，16MB 主存页下
///   推导 64 页，与引擎默认页数一致）。
pub fn apply_hlog_overrides(
  config: &mut StoreConfig,
  overrides: HlogProjection,
) -> crate::Result<()> {
  if let Some(p) = overrides.page_size {
    config.page_size = p;
  }
  if let Some(m) = overrides.memory_size {
    let pages = m / config.page_size;
    if pages < HLOG_MIN_NUM_PAGES {
      return Err(crate::Error::InvalidArgument(format!(
        "hlog memory_size（{m} 字节）不足以按页容量 {} 字节配出最小 {HLOG_MIN_NUM_PAGES} 页环形缓冲（至少 {} 字节），请调大 memory_size 或调小 page_size",
        config.page_size,
        HLOG_MIN_NUM_PAGES * config.page_size,
      )));
    }
    config.num_pages = prev_power_of2(pages as u64) as usize;
  }
  if let Some(f) = overrides.mutable_fraction {
    config.mutable_fraction = f;
  }
  if overrides.read_cache {
    config.enable_read_cache = true;
    let budget = overrides
      .read_cache_memory_size
      .unwrap_or(DEFAULT_READ_CACHE_MEMORY_SIZE);
    let pages = prev_power_of2((budget / config.page_size) as u64) as usize;
    // 页数落值经 wkv 唯一收敛口 with_read_cache_pages（≥2 页硬下界钳位单点在
    // 该 setter，此处禁另写钳位成第二口径）
    *config = config.clone().with_read_cache_pages(pages)?;
  }
  // 升阶树页缓存总闸旋钮投影：None 保持装配基线（wkv DEFAULT_TREE_CACHE_BUDGET_BYTES
  // 256MiB，现网行为零漂移），Some 直取覆盖（0 = 不设限）；预算为装配期单点注入
  // RangeIndexManager，启动期一次性生效不做热更
  if let Some(budget) = overrides.tree_cache_budget {
    *config = config.clone().with_tree_cache_budget(budget);
  }
  // reviv 三旋钮投影（对标 C# GarnetServerOptions.cs:899-900/:912/:924 把
  // CopyReadsToTail / RevivifiableFraction 投影进 kvSettings 的 store 级形态，
  // 本函数即 wconf → StoreConfig 的唯一投影单点）：
  // - reviv（Options.cs:564-567）：直取覆盖，默认装配 false 与引擎基线同值；
  // - reviv_fraction（Options.cs:559）：None 不覆盖引擎默认；区间合法性单点在
  //   末尾 `StoreConfig::validate`（(0, mutable_fraction]），此处不复校；
  // - copy_reads_to_tail（Options.cs:128）：store 级真源入 StoreConfig，
  //   会话装配期从该真源取初值（wkv `StoreSession::new`），不构成第二套真源。
  config.enable_revivification = overrides.reviv;
  if let Some(f) = overrides.reviv_fraction {
    config.revivifiable_fraction = f;
  }
  config.copy_reads_to_tail = overrides.copy_reads_to_tail;
  config.validate()?;
  Ok(())
}

/// 生产装配 + 节点参数合并的存储引擎配置
///
/// [`NodeArgs`] hlog 配置段显式项覆盖自适应基线，None 项保持内存预算规划器
/// 推导值（大机预算 ≥ 1GB 推导 16MB 页，对标 C# PageSize = "16m" 默认）
pub fn store_config_from_node(node: &NodeArgs) -> crate::Result<StoreConfig> {
  let mut config = store_config();
  apply_hlog_overrides(&mut config, node.hlog.validated()?)?;
  Ok(config)
}

/// 集合项经纪 + 向量集合管理器装配对（冷启动与恢复装配共用产物）
type NodeComponents = (
  Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  Arc<VectorManager>,
);

/// 集合项经纪 + 向量集合管理器（随存储句柄派生的装配对；冷启动与恢复
/// 装配共用，经纪/向量各持独立存储会话）
///
/// `vector_preview` 为 Vector Set 预览开关的构造期定值（对标 C#
/// VectorManager 构造器 `IsEnabled = serverOptions.EnableVectorSetPreview`：
/// 构造期一次定值，恢复回建与命令面共用同一真值；运行期
/// `with_vector_set_preview` 仅服务冷启动装配链的兼容面）
fn node_components(
  store: &SharedStore<SegmentedDevice>,
  vector_preview: bool,
  quantization_task_count: usize,
) -> crate::Result<NodeComponents> {
  let broker_session = store.new_session()?;
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(broker_session),
  ))));

  // 向量存储回调无状态装配（对标 C# VectorManager 不持会话、回调读
  // `[ThreadStatic] ActiveThreadSession`）：命令面绑连接自己的会话，后台臂经
  // 下方工厂自持专用会话，本管理器不再有跨连接共享的向量存储会话
  let vector_manager = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: vector_preview,
      quantization_task_count,
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
  ));
  // 量化双计数登记进 wbase::supervise 快照（r30-bgthread 发现五：INFO
  // bg_task_health 行尾可见，量化吞吐冻结不再与空闲不可区分）
  supervise::register_counter(
    "qnt_requests_processed",
    Arc::clone(&vector_manager.quantization_requests_processed),
  );
  supervise::register_counter(
    "qnt_backfills_processed",
    Arc::clone(&vector_manager.quantization_backfills_processed),
  );
  // 登记摘除失败计数（remove 写透失败即幽灵复活危害面：内存已摘而盘上
  // 墓碑缺，重启回建复活旧索引；单点经既有监督快照暴露，杜绝纯日志淹没）
  supervise::register_counter(
    "vector_registry_remove_failures",
    Arc::clone(&vector_manager.vector_registry_remove_failures),
  );
  // 登记旁路记录写透钩子（同为无状态承接；登记表键自带记录域前缀、元数据键
  // 恒根前缀，落位与绑定会话的域无关——见 vector_registry_recovery 模块头）
  vector_manager.attach_registry_store(Arc::new(WedbRegistryPersistence::new()));
  // 后台专用会话工厂装配两处分形共用可覆写注入槽（last-wins）：嵌入式三件套
  // 经 open_node_with_config 钉自身 store 的默认工厂；Node 形态在 from_parts
  // 尾段以现取现用置换槽工厂覆写为终态——本函数拿不到槽，不在此装配
  Ok((broker, vector_manager))
}

/// 专用会话工厂的产出形态单点：从存储句柄取新会话并绑定当前执行域后装箱
/// （对标 C# HandleMigratedIndexKey「Spin up a new Storage Session if we don't
/// have one」的一次性自备会话，VectorManager.Migration.cs:164-177）
fn bound_dedicated_session(
  store: &SharedStore<SegmentedDevice>,
) -> Option<ActiveDedicatedVectorSession> {
  store
    .new_session()
    .ok()
    .map(OwnedActiveVectorSession::new)
    .map(ActiveDedicatedVectorSession::from_bound)
}

/// 钉死指定存储句柄的默认专用会话工厂（嵌入式三件套形态无引擎置换槽，
/// 钉自身 store 即正确；宗二「换引擎落户弃置实例」的顾虑不适用本形态，
/// Node 装配尾段经工厂槽覆写为置换槽工厂）
fn pinned_dedicated_session_factory(
  store: &SharedStore<SegmentedDevice>,
) -> DedicatedVectorSessionFactory {
  let store = Arc::clone(store);
  Arc::new(move || bound_dedicated_session(&store))
}

/// 打开分段存储设备，装配存储引擎、共享经纪与向量集合管理器
///
/// 单机/集群功能一致的节点装配三件套唯一入口（存储域初始化 + 经纪 +
/// 向量管理器，对标 C# GarnetServer InitializeServer）：`config` 由调用方
/// 投影——生产经 `store_config_from_node`（[`NodeArgs`] hlog 段覆盖自适应
/// 基线），测试与嵌入式显式注入小预算 [`StoreConfig`]
pub fn open_node_with_config(
  config: StoreConfig,
  data_path: impl AsRef<Path>,
) -> crate::Result<DefaultNodeHandles> {
  let device = SegmentedDevice::new(
    data_path.as_ref(),
    DEFAULT_MAIN_LOG_SEGMENT_SIZE,
    DEFAULT_SECTOR_SIZE,
  )?;
  let store = WedbStore::open_shared(config, Arc::new(device))?;
  // 冷启动嵌入式形态：无恢复回建窗口，预览关构造期定值（宿主经
  // with_vector_set_preview 点亮，服务 accept 前完成即等价构造期定值）
  let (broker, vector_manager) = node_components(&store, false, 0)?;
  // 三件套形态无引擎置换槽：补挂钉死自身 store 的默认专用会话工厂，
  // CLUSTER RESERVE 慢臂等兜底自持会话前置由此成立（工单
  // wnode-vector-registry-put-slow-arm-session-unbound）；生产 Node 链随后
  // 经 from_parts 尾段置换槽工厂覆写（last-wins），终态恒为置换槽形态
  vector_manager.attach_dedicated_session_factory(pinned_dedicated_session_factory(&store));
  Ok((store, broker, vector_manager))
}

/// 存储执行域会话提供者基座（单机/集群共用装配流程，模板方法模式）
///
/// 固化公共流程：new_session → StoreGarnetApi（挂向量集合管理器）→
/// 差异钩子 → inject_dependencies（统一注入会话依赖）；单机/集群功能一致，
/// 在线引擎置换槽（共享状态句柄，对标 C# storeWrapper 引擎原位重构——
/// 持当前在线引擎一份状态，宿主与集群反查同一槽，无第二份拷贝）
pub struct StoreSwapSlot<D: Device = SegmentedDevice> {
  inner: Arc<RwLock<Option<SharedStore<D>>>>,
}

/// 形态差异仅装配入口（[`Self::open_from_args`]）注入的 `decorate` 钩子——
/// 单机构造 `RespSessionConsumer::new`（无集群切面），集群构造 `with_cluster`
///（挂 ClusterSession 切面，会话选项基线与单机同口径，全模式自由切库）
pub struct StorageSessionProvider<F> {
  /// 装配期引擎初值（非取口：本结构的引擎取口唯一为 [`Self::store`]，
  /// 对标 C# libs/server/StoreWrapper.cs:41 `store => databaseManager.Store`
  /// 单计算属性——每次取值转发当前在线引擎，故不存在第二取口；直取本字段
  /// 会绕过在线置换，仅限本模块内部（装配期与未置换回落）使用。每连接
  /// new_session 派生独立纪元参与者；集群 CLUSTER RESET 的 HasKeysInSlots
  /// 扫描与 HARD 清库经 cluster.set_store 下达同一实例）
  store: SharedStore<SegmentedDevice>,
  /// 在线引擎置换槽（副本检查点导入闭环，C# storeWrapper.RecoverCheckpointAsync
  /// 引擎原位重构的 rust 等价——新引擎接管后续新会话装配，存量会话随批
  /// 纪元自然收敛；None = 未置换，读面统一走 [`Self::store`]）
  store_swap: StoreSwapSlot,
  /// 集合项经纪（服务器级共享；阻塞命令挂起/唤醒仲裁，对标 C#
  /// storeWrapper.itemBroker，经纪持独立取件会话）
  pub broker: Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  /// 向量集合管理器（服务器级共享，统一物理键存储回调落盘；向量命令经会话
  /// 级 can_serve_slot 槽位门——VADD/VREM/VSETATTR 带 wait_for_stable_slot
  /// 特判——通过后才入专用会话直写存储，与 C# CanServeSlot 一致，
  /// C# VectorManager 亦持独立 getTempSession）
  pub vector_manager: Arc<VectorManager>,
  /// WATCH 版本表（libs/server/GarnetDatabase.cs:55 VersionMap）
  pub watch_version_map: Arc<WatchVersionMap>,
  /// 引擎实例锁表（对标 C# `TsavoriteKV.LockTable`——
  /// core/Index/Tsavorite/Tsavorite.cs:105 `internal readonly
  /// OverflowBucketLockTable<...> LockTable` 随 :228 store 构造，归本实例
  /// 所有；会话侧经 core/ClientSession/SessionFunctionsWrapper.cs:30
  /// `_clientSession.store.LockTable` 取同一句柄。句柄为 Arc 薄克隆，
  /// 全仓无进程级静态锁表：两个 provider 实例即两把互不相干的锁表。
  /// 锁源为「当前引擎索引装载闭包」：键锁落 windex 哈希桶内嵌闩，每笔事务现取
  /// `store_swap` 指向的当前引擎 HashIndex 版本——粒度随 split 扩容细化、随引擎在线
  /// 置换（副本检查点导入）换面，与命令面 `Self::store` 同源，杜绝跨引擎错锁。对外无取口：
  /// 会话侧经 [`Self::session_dependencies`] 注入，全仓零直接消费）
  lock_table: TxnLockTable,
  /// 全局脚本缓存（C# StoreWrapper.cs:120/251 `storeScriptCache`：实例级
  /// ConcurrentDictionary 随存储基座单点构造，全连接会话共享——SCRIPT LOAD
  /// 加载的摘要任意连接 EVALSHA 即时可用，SCRIPT FLUSH 一刷全服生效；会话侧
  /// 经 [`Self::session_dependencies`] 注入，跨连接脚本共享契约的唯一实例）
  pub store_script_cache: Arc<StoreScriptCache>,
  /// 服务器级运行时配置（CONFIG GET/SET 与 OBJECT_SCAN_COUNT_LIMIT 热更源）
  pub runtime_config: Arc<RuntimeServerConfig>,
  /// 慢日志容器（服务器级共享，对标 C# StoreWrapper.cs:164/243
  /// slowLogContainer；容量 = SlowLogMaxEntries）
  pub slow_log_container: Arc<SlowLogContainer>,
  /// 检查点目录（SAVE/BGSAVE 落点，C# GetStoreCheckpointDirectory 口径：
  /// 数据目录下 Store/checkpoints）
  pub checkpoint_dir: PathBuf,
  /// 逻辑数据库管理器（对标 C# StoreWrapper.databaseManager；常驻单例，管理快照与 AOF）
  pub database_manager: Arc<SingleDatabaseManager<SegmentedDevice>>,
  /// 活跃消费者注册表（C# GarnetServerBase.activeHandlers 域；CLIENT
  /// LIST/KILL 与监视器枚举面）
  pub registry: Arc<ConsumerRegistry>,
  /// 发布订阅中枢（服务器级共享单例，C# GarnetServer.cs:277
  /// `!opts.DisablePubSub → new SubscribeBroker(...)`；None = --disable-pubsub
  /// 关闭形态，命令面按同款禁用文案回错）
  pub pubsub: Option<Arc<SubscribeBroker>>,
  /// Lua 超时管理器停机收口臂（对标 C# StoreWrapper.luaTimeoutManager 的
  /// Dispose 可达位）：与装配链会话选项持同一 Arc 实例（创建单点在
  /// [`assemble_lua_timeout`]，本字段仅停机句柄克隆，不设第二创建点）；
  /// None = 超时未启用
  lua_timeout: Option<Arc<LuaTimeoutManager>>,
  /// 范围索引 AOF 复制面单例（对标 C# `StoreWrapper.rangeIndexManager`：
  /// 随 NodeService AOF 装配单点创建，与事件汇共持同一实例；None = AOF 未
  /// 点亮——C# 该管理器不依赖 EnableAOF，引擎树释放由 [`Self::dispose_range_index`]
  /// 的引擎臂无条件承接）。宿主停机链取用面，仅 [`Self::dispose_range_index`]
  /// 消费，故不设独立取口
  ri: Option<Arc<RangeIndexManagerReplication>>,
  /// 过期键删除任务启动期注册已执行标志（C# StartPrimaryTasks 的
  /// ExpiredKeyDeletionTask 注册段：随首个会话按槽位经调停入口执行一次，
  /// 幂等；此后启停全归 CONFIG SET 调停）
  gc_scan_started: AtomicBool,
  /// Primary 类后台任务生命周期域（副本角色位 + 周期提交/对象收集任务
  /// 幂等启动位 + 收集互斥单写位；对标 C# storeWrapper 的 taskLifecycleLock
  /// 任务域，随首个会话建立惰性启动，集群层经 set_primary_tasks 共享）
  primary_tasks: Arc<PrimaryTasks>,
  /// AOF 体积限额字节（None = 未启用。C# StartPrimaryTasks 注册条件
  /// AofSizeLimit.Length > 0，builder 注入；检查周期真值在 wconf 槽
  /// aof-size-limit-enforce-frequency，任务循环每轮现取）
  aof_size_limit: Option<u64>,
  /// AOF 体积限额任务已拉起标志（随首个会话建立惰性启动，幂等；Arc 供
  /// spawn 任务体 panic 监督臂复位）
  aof_size_limit_started: Arc<AtomicBool>,
  /// 索引自动扩容任务参数（上限桶数, 阈值百分比, 检查周期秒；None = 未启用。
  /// C# StartGenericNodeTasks 注册条件 AdjustedIndexMaxCacheLines > 0）
  index_auto_grow: Option<(usize, i64, u64)>,
  /// 索引自动扩容任务已拉起标志（随首个会话建立惰性启动，幂等；Arc 供
  /// spawn 任务体 panic 监督臂复位）
  index_auto_grow_started: Arc<AtomicBool>,
  /// 量化消费者协程已拉起数量（上限 quantization_task_count，跨 worker runtime 分摊）
  quantization_started: AtomicUsize,
  /// AOF 门面（C# storeWrapper.appendOnlyFile；EnableAOF 门控——
  /// [`Self::open_with_config`] 默认关闭恒 None，
  /// [`Self::open_with_config_and_aof`] / [`Self::open_recovered_with_config_and_aof`] 点亮）
  aof: Option<Arc<GarnetAppendOnlyFile>>,
  /// 物理日志句柄（[`Self::open_with_config_and_aof`] /
  /// [`Self::open_recovered_with_config_and_aof`] 点亮；宿主据此装配集群复制
  /// 数据面——副本落盘目标与主端推流数据源）
  wal: Option<Arc<WalLog<SegmentedDevice>>>,
  /// --recover 重放后的 AOF 尾地址（仅
  /// [`Self::open_recovered_with_config_and_aof`] 形态点亮；对标 C#
  /// RecoverCheckpointAndAOFAsync 的 `replayedUntil`，宿主装配尾段据此回填
  /// 复制域位点 replicationOffset.SetValue）
  recovered_aof_tail: Option<AofAddress>,
  /// 访问控制列表（requirepass 认证源，对标 C# storeWrapper.serverOptions.AuthSettings）
  pub acl: Option<Arc<AccessControlList>>,
  /// 指标采样频率秒数（> 0 时逐连接装配会话指标共享句柄，对标 C#
  /// storeWrapper.trackStats = MetricsSamplingFrequency > 0 门控
  /// GarnetServer.cs:249 传入 StoreWrapper → sessionMetrics 创建；
  /// 0 = 默认关闭，从 NodeArgs 经装配链注入）
  metrics_sampling_frequency_secs: u64,
  /// TLS 证书热加载共享句柄（对标 C# storeWrapper.serverOptions.TlsOptions
  /// 单实例共享：CONFIG SET cert-file-name 经会话依赖注入触达同一实例在线
  /// 重载；None = 未装配 TLS。启动链单点装配见
  /// [`Self::open_from_args_with_config`]，共享句柄取口为
  /// [`SessionProviderFace::tls_config`]，builder 注入口为
  /// [`Self::with_tls_config`]）
  #[cfg(feature = "tls")]
  tls_config: Option<Arc<ServerTlsConfig>>,
  /// 差异钩子：按发送端装配会话消费者（None = 拒绝建连）
  decorate: F,
}

impl<F> StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>,
{
  /// 基础装配尾段（注册表进程级安装 + 双件与向量管理器诞生点配对 +
  /// 运行时配置/慢日志/PubSub 缺省；冷启动与恢复装配共用，AOF 字段由
  /// `open_with_config_and_aof` / `open_recovered_with_config_and_aof`
  /// 装配口点亮）
  fn from_parts(
    store: SharedStore<SegmentedDevice>,
    broker: Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
    vector_manager: Arc<VectorManager>,
    checkpoint_dir: PathBuf,
    decorate: F,
  ) -> crate::Result<Self> {
    let registry = Arc::new(ConsumerRegistry::new());
    // 覆盖式安装（槽文档声明单实例进程契约）：同进程第二实例覆盖后，先装
    // 实例的会话枚举到的是他实例连接、自身连接不可见不可 KILL，必须留痕
    // 禁静默
    if !registry.install_global() {
      log::warn!(
        "同进程第二实例覆盖进程级消费者注册表（单实例进程契约）：先装实例的 CLIENT 治理面被后装实例接管"
      );
    }
    let watch_version_map = Arc::new(WatchVersionMap::default());
    // 引擎在线置换槽须先于锁表构造：供锁源闭包读取「当前引擎」（副本检查点导入换持新引擎）
    let store_swap = StoreSwapSlot::new();
    // 引擎实例锁表（C# LockTable 随 store 构造：Tsavorite.cs:228
    // `LockTable = new OverflowBucketLockTable<TStoreFunctions, TAllocator>(this)`，
    // 一处构造、随本实例的会话句柄共享，无进程级静态表；锁源与全事务屏障装配
    // 单点收口于 [`build_txn_lock_table`]）
    let lock_table = build_txn_lock_table(Arc::clone(&store), &store_swap);
    // WATCH 写面收口接线（对齐 C# functionsState.watchVersionMap 与引擎同实例
    // 共享）：EXEC 校验表与本表合一，wkv 用户键写入口统一按键推进
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&watch_version_map)));
    // 向量写面 WATCH 推进接线（VADD/VREM/VSETATTR 与向量 RENAME 旧键的登记表
    // 变更不经 wkv 用户键写入口，写漏斗内部收口；对标 C# 向量写经 Unified
    // RMW 成功钩子无条件 IncrementVersion、RENAME 向量臂 DELETE(old) 同推）。
    // 向量侧以物理前缀寻址登记表，本接线经 vector_version_watch_hook 单点把
    // 物理域换算为当前引擎逻辑域入版本轨（经置换槽随引擎在线置换，换号后
    // 向量改写对在途 WATCH 同必 abort）
    vector_manager.set_watch_bump(vector_version_watch_hook(
      Arc::clone(&store),
      store_swap.clone(),
      Arc::clone(&watch_version_map),
    ));
    // 后台清理/量化与恢复回建臂的专用会话工厂（对标 C# RunCleanupTaskAsync 的
    // 一次性专用 dropSession：谁干活谁自备会话，产出即已绑定当前执行域）。
    // 现取现用置换槽与向量 WATCH 推进臂同源（工单 zcode-r137c-snaplock2 宗二
    // 换面第四件的引擎绑定肢）：钉死装配期实例会令副本 disk-based 换引擎后的
    // 登记回建/残影清退写透落户弃置引擎（共享设备半写即毁导入日志）
    let factory_store = Arc::clone(&store);
    let factory_swap = store_swap.clone();
    vector_manager.attach_dedicated_session_factory(Arc::new(move || {
      bound_dedicated_session(
        &factory_swap
          .get()
          .unwrap_or_else(|| Arc::clone(&factory_store)),
      )
    }));
    // 集合项经纪取件源挂接在线置换槽（第五绑定面）：置换后 try_get_result
    // 动态感知新引擎原地换代会话，解除对装配期旧引擎的 Arc 钉死
    broker
      .item_source()
      .attach_store_swap_slot(store_swap.clone());
    // 服务器级运行时配置（对标 C# RuntimeServerConfig 持 owner 的静态枚举
    // 分发替代：CONFIG SET 经 try_set 产出 ConfigReconcile 调停消息，由
    // CONFIG 命令域按 [`config_owner::apply_config_reconcile`] 就地送达存储
    // 引擎后台任务域（expired-key-deletion-scan-freq → 内置 GC 扫描循环
    // 启停/调频，libs/server/StoreWrapper.cs:ReconcilePrimaryTask））
    let runtime_config = Arc::new(RuntimeServerConfig::new(RuntimeServerOptions::default()));
    // 全局脚本缓存（C# StoreWrapper.cs:251 `this.storeScriptCache = [];`
    // 构造段：随存储基座单点构造一次，全部连接会话经 session_dependencies
    // 共享同一实例，跨连接脚本共享与 SCRIPT FLUSH 刷新契约由本单点承载）
    let store_script_cache = Arc::new(StoreScriptCache::default());
    // 慢日志容器（对标 C# StoreWrapper.cs:243 无条件构造，容量 =
    // SlowLogMaxEntries 默认 128；main 层经 with_runtime_server_options 覆盖）
    let slow_log_container =
      new_slow_log_container(RuntimeServerOptions::default().slow_log_max_entries);
    // 发布订阅中枢默认装配（C# DisablePubSub = false 即默认启用），main 层按
    // NodeArgs 经 with_pubsub_config 覆盖
    let pubsub = Some(Arc::new(SubscribeBroker::new()));
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      checkpoint_dir.clone(),
      None,
    ));
    let database_manager = Arc::new(SingleDatabaseManager::new(checkpoint_dir.clone(), db));
    // Primary 任务域与角色门同源装配：AOF 体积超限臂内读的角色位即此
    // Arc（集群层经 primary_tasks() 共享 suspend/resume 翻转）
    let primary_tasks = Arc::new(PrimaryTasks::default());
    database_manager.attach_primary_tasks(Arc::clone(&primary_tasks));
    // FLUSH 族登记表域回收联动注入（database_manager 诞生点即配对点：冷启
    // 默认臂与嵌入式裸装配单点收口，杜绝四臂唯默认臂漏致 FLUSH 族回收静默
    // 旁路；对标 C# VectorManager 随 StoreWrapper 单装配路径无条件配对——
    // garnet/libs/host/GarnetServer.cs:420 构造 / :468 CreateStore 形参）
    database_manager.attach_vector_manager(Arc::clone(&vector_manager));
    Ok(Self {
      store,
      store_swap,
      broker,
      vector_manager,
      watch_version_map,
      lock_table,
      store_script_cache,
      runtime_config,
      slow_log_container,
      checkpoint_dir,
      database_manager,
      registry,
      pubsub,
      lua_timeout: None,
      ri: None,
      gc_scan_started: AtomicBool::new(false),
      primary_tasks,
      aof_size_limit: None,
      aof_size_limit_started: Arc::new(AtomicBool::new(false)),
      index_auto_grow: None,
      index_auto_grow_started: Arc::new(AtomicBool::new(false)),
      quantization_started: AtomicUsize::new(0),
      aof: None,
      wal: None,
      recovered_aof_tail: None,
      acl: None,
      #[cfg(feature = "tls")]
      tls_config: None,
      metrics_sampling_frequency_secs: 0,
      decorate,
    })
  }
}
