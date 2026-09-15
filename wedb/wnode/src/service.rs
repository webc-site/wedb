//! 节点服务编排：存储引擎（wkv）与 AOF 日志层（GarnetLog + waof 磁盘子日志）
//! 的统一收口
//!
//! 对标 Garnet 的单一 AOF 机制：写入端统一经 [`crate::aof::garnet_log::GarnetLog::enqueue`]
//! （唯一条目编码定义），重放端统一经 [`AofProcessor`]（唯一重放分发），
//! 磁盘承载为 [`WaofSublog`]（waof `WalLog` 的 GarnetLog 后端适配），
//! 域装配唯一入口 [`single_log_aof`]（库管理面与数据写入面共享同一物理
//! 日志实例）。协议载荷为 [`ReplayInput`]（C# StringInput/ObjectInput 的
//! 序列化形态），apply → log 顺序由 wkv 写监听端口固化，条目 store_version
//! 取写入时存储版本（checkpoint token，重放端跳过低版本条目）。

use std::{
  fs::{OpenOptions, create_dir_all},
  io,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use parking_lot::{Mutex, RwLock};
use wacl::{AccessControlList, GarnetAclAuthenticator};
use waof::{AofAddress, WalConfig, WalLog};
use wbase::{convert::unix_time_in_milliseconds_from_ticks, entry_type::AofEntryType};
use wbftree::{RangeIndexStub, StorageBackendType, TreeTuning};
use wcol::{
  RespInputFlags,
  itembroker::{collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker},
};
use wconf::{NodeArgs, RuntimeServerConfig, RuntimeServerOptions};
use wcustom::{CustomCommandManager, SharedCustomCommandManager};
use wdatabase::{DEFAULT_VERSION_MAP_SIZE, GarnetDatabase, SingleDatabaseManager};
use wdev::{Device, SegmentedDevice};
use wkv::{
  DEFAULT_GC_COMPACTION_INTERVAL_MS, DEFAULT_GC_SCAN_INTERVAL_MS, StoreConfig, StoreEvent,
  StoreEventSink, StoreSession, WedbStore,
};
use wlua::LuaTimeoutManager;
use wmetric::SlowLogContainer;
use wpubsub::SubscribeBroker;
use wresp::RespCommand;
use wtxn::WatchVersionMap;
use wval::{KeyTag, NO_ETAG, NamespaceDbCodec, TaggedKeyBuf};
use wvector::Callbacks;

use crate::{
  aof::{
    AofProcessor, ReplayInput, ReplayInputSlice, aof_processor::ReplayTarget,
    garnet_append_only_file::GarnetAppendOnlyFile, garnet_log::RecordShape,
    recover::aof_recover::AofRecover, waof_sublog::single_log_aof,
  },
  resp::{
    RespSessionConsumer,
    garnet_api::{CheckpointCtx, StoreGarnetApi},
    metrics_commands::new_slow_log_container,
    objects::collection_item_source::CollectionItemSource,
    rangeindex::range_index_manager_replication::RangeIndexManagerReplication,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_replication::VectorAofSink,
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
  servers::consumer_registry::ConsumerRegistry,
  storage::session::storage_session::StorageSession,
  traits::{SessionProviderFace, WireFormat},
};

/// 节点共享引擎句柄类型别名（收敛 `Arc<WedbStore<D>>` 复合泛型签名）
pub type SharedStore<D> = Arc<WedbStore<D>>;

/// 单机网络服务：存储引擎会话 + AOF 日志层的编排门面
///
/// 内部持有一个 [`StoreSession`]（epoch 参与者随会话注册）。多连接场景下
/// 每连接经 [`Self::store`] 自行 `new_session`，本门面仅承担服务级编排
pub struct NodeService<D: Device> {
  session: StoreSession<D>,
  aof: Arc<GarnetAppendOnlyFile>,
}

/// 条目入队公共体（C# 各 WriteLog* 的 RecordShape 形态）
/// 空命令输入序列化预存（ReplayInput 默认形态无参数，36 字节全零固定序列）
const EMPTY_REPLAY_INPUT_BYTES: [u8; 36] = [0u8; 36];

/// 原始底层条目入队
#[inline]
fn enqueue_raw(
  aof: &GarnetAppendOnlyFile,
  op_type: AofEntryType,
  version: i64,
  key: &[u8],
  value: &[u8],
  input: &[u8],
) {
  aof.log().enqueue(&RecordShape {
    op_type,
    version,
    session_id: 0,
    key,
    value,
    input,
    database_id: 0,
  });
}

/// 切片零分配/低分配入队公共体
#[inline]
fn enqueue_slices<T: AsRef<[u8]>>(
  aof: &GarnetAppendOnlyFile,
  op_type: AofEntryType,
  version: i64,
  key: &[u8],
  value: &[u8],
  input: &ReplayInputSlice<'_, T>,
) {
  ReplayInput::with_encoded_slices(input, |serialized| {
    enqueue_raw(aof, op_type, version, key, value, serialized);
  });
}

/// 物理键编码（栈上分配避免堆分配，TaggedKeyBuf 最多内联 62 字节）
#[inline]
fn physical_key(ns: u64, db: u64, user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::String, user_key)
}

impl<D: Device> NodeService<D> {
  /// 组装节点服务（AOF 域由调用方装配后注入——单物理日志域唯一实例，
  /// 经 [`single_log_aof`] 工厂构造，与库管理面共享同一物理日志）
  ///
  /// 按运行态 GC 配置拉起内置后台循环（幂等，对标 Garnet 服务启动时注册
  /// ExpiredKeyDeletionTask）：调用方已经 `open_shared`/`start_gc` 启动过则此处
  /// 为 no-op；未启动且 `gc.enabled` 时在此补启——服务端形态 TTL 主动过期
  /// 与紧缩调度由此保证，不依赖调用方记得手动启动
  pub fn new(store: SharedStore<D>, aof: Arc<GarnetAppendOnlyFile>) -> crate::Result<Self>
  where
    D: 'static,
  {
    Self::assemble(store, aof)
  }

  /// 公共装配体：注册全部 AOF 写监听端口并拉起会话。
  fn assemble(store: SharedStore<D>, aof: Arc<GarnetAppendOnlyFile>) -> crate::Result<Self>
  where
    D: 'static,
  {
    // 各端口版本源快照（原子指针捕获消除循环引用与多余开销；对标 C# storeWrapper.store.CurrentVersion）
    let sink = Arc::clone(&aof);
    let ver_atomic = Arc::clone(store.current_version_atomic());
    let event_sink: StoreEventSink = Arc::new(move |event| {
      let ver = ver_atomic.load(Ordering::Acquire);
      match event {
        StoreEvent::Write {
          key,
          val,
          tombstone,
        } => {
          let tag = NamespaceDbCodec::decode_tag(key);
          if tag == Some(KeyTag::ObjectEnvelope) {
            // 信封墓碑照常入队；非墓碑整值写由 envelope upsert 承接
            if !tombstone {
              return;
            }
            enqueue_raw(
              &sink,
              AofEntryType::StoreDelete,
              ver,
              key,
              val,
              &EMPTY_REPLAY_INPUT_BYTES,
            );
            return;
          }
          if tag != Some(KeyTag::String) {
            return;
          }
          let op = if tombstone {
            AofEntryType::StoreDelete
          } else {
            AofEntryType::StoreUpsert
          };
          enqueue_raw(&sink, op, ver, key, val, &EMPTY_REPLAY_INPUT_BYTES);
        }
        StoreEvent::TtlWrite {
          ns,
          db,
          key,
          expire_at,
        } => {
          let (cmd, arg1) = match expire_at {
            Some(ticks) => (
              RespCommand::Pexpireat,
              unix_time_in_milliseconds_from_ticks(ticks),
            ),
            None => (RespCommand::Persist, 0),
          };
          let empty_args: [&[u8]; 0] = [];
          let input = ReplayInputSlice::new(cmd, &empty_args)
            .with_flags(RespInputFlags::DETERMINISTIC.bits())
            .with_args_num(arg1, 0, 0);
          enqueue_slices(
            &sink,
            AofEntryType::StoreRMW,
            ver,
            &physical_key(ns, db, key),
            &[],
            &input,
          );
        }
        StoreEvent::EtagWrite { ns, db, key, etag } => {
          let empty_args: [&[u8]; 0] = [];
          let input = ReplayInputSlice::new(RespCommand::Setwithetag, &empty_args)
            .with_flags(RespInputFlags::DETERMINISTIC.bits())
            .with_args_num(etag.unwrap_or(NO_ETAG), 0, 0);
          enqueue_slices(
            &sink,
            AofEntryType::StoreRMW,
            ver,
            &physical_key(ns, db, key),
            &[],
            &input,
          );
        }
        StoreEvent::ObjectRmw(notif) => {
          let input = ReplayInputSlice {
            cmd: RespCommand::None,
            flags: RespInputFlags::DETERMINISTIC.bits(),
            sub_id: notif.op_code,
            obj_type: notif.obj_type,
            arg1: notif.arg1 as i64,
            arg2: notif.arg2 as i64,
            arg3: 0,
            args: notif.args,
          };
          enqueue_slices(
            &sink,
            AofEntryType::ObjectStoreRMW,
            ver,
            notif.key,
            &[],
            &input,
          );
        }
        StoreEvent::EnvelopeUpsert { key, val } => {
          enqueue_raw(
            &sink,
            AofEntryType::ObjectStoreUpsert,
            ver,
            key,
            val,
            &EMPTY_REPLAY_INPUT_BYTES,
          );
        }
        StoreEvent::RangeIndexWrite {
          key,
          field,
          val,
          delete,
        } => {
          let (cmd, args): (RespCommand, &[&[u8]]) = if delete {
            (RespCommand::Ridel, &[field])
          } else {
            (RespCommand::Riset, &[field, val])
          };
          let input =
            ReplayInputSlice::new(cmd, args).with_flags(RespInputFlags::DETERMINISTIC.bits());
          enqueue_slices(
            &sink,
            AofEntryType::StoreRMW,
            ver,
            &physical_key(0, 0, key),
            &[],
            &input,
          );
        }
        StoreEvent::RangeIndexCreate {
          key,
          backend,
          tuning,
        } => {
          let stub = RangeIndexStub::from_tuning(0, &tuning, *backend);
          let mut stub_bytes = [0u8; wbftree::RANGE_INDEX_STUB_SIZE];
          if let Err(e) = stub.encode_into(&mut stub_bytes) {
            log::error!("RI.CREATE AOF 存根编码失败: {e}");
            return;
          }
          let stub_args = [&stub_bytes[..]];
          let input = ReplayInputSlice::new(RespCommand::Ricreate, &stub_args)
            .with_flags(RespInputFlags::DETERMINISTIC.bits());
          enqueue_slices(
            &sink,
            AofEntryType::StoreRMW,
            ver,
            &physical_key(0, 0, key),
            &[],
            &input,
          );
        }
        StoreEvent::RangeIndexDrop { key } => {
          enqueue_raw(
            &sink,
            AofEntryType::StoreDelete,
            ver,
            &physical_key(0, 0, key),
            &[],
            &EMPTY_REPLAY_INPUT_BYTES,
          );
        }
        StoreEvent::TtlPurge {
          ns,
          db,
          key,
          expire_at,
        } => {
          let empty_args: [&[u8]; 0] = [];
          let input = ReplayInputSlice::new(RespCommand::Delifexpim, &empty_args)
            .with_flags((RespInputFlags::DETERMINISTIC | RespInputFlags::EXPIRED).bits())
            .with_args_num(expire_at, 0, 0);
          enqueue_slices(
            &sink,
            AofEntryType::StoreRMW,
            ver,
            &physical_key(ns, db, key),
            &[],
            &input,
          );
        }
      }
    });
    if !store.set_event_sink(event_sink) {
      log::warn!("存储事件处理器重复注册");
    }
    store.start_gc();
    let session = store.new_session()?;
    Ok(Self { session, aof })
  }

  /// 存储引擎会话
  #[inline]
  pub fn session(&self) -> &StoreSession<D> {
    &self.session
  }

  /// 存储引擎句柄
  #[inline]
  pub fn store(&self) -> &SharedStore<D> {
    &self.session.store
  }

  /// AOF 句柄（GarnetLog 拓扑 + WaofSublog 磁盘承载；与库管理面共享同一
  /// 物理日志域）
  #[inline]
  pub fn aof(&self) -> &Arc<GarnetAppendOnlyFile> {
    &self.aof
  }

  /// 创建范围索引并预写 WAL（apply 成功后由 range_create_listener 同栈入队 WAL）
  pub async fn ri_create(
    &self,
    key: &[u8],
    storage_backend: StorageBackendType,
    tuning: TreeTuning,
  ) -> crate::Result<()> {
    self
      .session
      .range_index_create(key, storage_backend, tuning)
      .await?;
    Ok(())
  }

  /// 设置范围索引字段并预写 WAL
  pub async fn ri_set(&self, key: &[u8], field: &[u8], value: &[u8]) -> crate::Result<()> {
    self.session.range_index_set(key, field, value).await?;
    Ok(())
  }

  /// 删除范围索引字段并预写 WAL
  ///
  /// 与 Garnet `RangeIndexDel`"字段不存在则不写 AOF"的刻意差异：
  /// bf-tree 墓碑删除不区分字段是否存在（`BfTreeDeleteResult` 无
  /// NotFound 语义），故删除恒落日志，回放端按幂等删除处理
  pub async fn ri_del(&self, key: &[u8], field: &[u8]) -> crate::Result<bool> {
    let deleted = self.session.range_index_del(key, field).await?;
    Ok(deleted)
  }

  /// 将已提交 AOF 流按序重放到指定目标引擎会话（支持异构/跨设备会话）
  ///
  /// 统一走 [`AofProcessor`]（唯一重放分发）：构造目标 [`ReplayTarget`]
  /// 与范围索引重放面后交 [`AofRecover::single_log_recover`] 从提交位点扫描
  /// 至尾部（`scan_single_async` 跨环形窗口与历史磁盘段）。重放期间目标端
  /// AOF 监听端口暂停（对齐 C# 重放会话 `recordToAof: false`），重放写入
  /// 不镜像回写目标端 AOF。
  ///
  /// 版本基线：`store_version` 取目标存储当前版本（C# 恢复时
  /// `storeWrapper.store.CurrentVersion`——由 checkpoint 恢复流程设置；
  /// 重放中 `header.storeVersion < store_version` 的记录按
  /// `ShouldSkipRecord` 跳过，未从 checkpoint 恢复时为 0 = 全量重放）。
  pub async fn replay_into_session<D2: Device>(
    &self,
    target_session: &StoreSession<D2>,
  ) -> crate::Result<u64> {
    let _pause = target_session.store.pause_aof_listeners();
    let batch = target_session.enter_batch();
    let storage = StorageSession::new(
      batch,
      Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE)),
    );
    let mut processor = AofProcessor::new(Arc::clone(&self.aof));
    processor.set_range_index_manager(Arc::new(RangeIndexManagerReplication::new(Arc::clone(
      &target_session.store.range_index,
    ))));
    let target = ReplayTarget {
      session: &storage,
      store: Arc::clone(&target_session.store),
      store_version: target_session.store.current_version(),
    };
    let replayed = AofRecover::single_log_recover(&processor, &self.aof, 0, 0, -1, &target).await?;
    Ok(replayed)
  }
}

impl NodeService<SegmentedDevice> {
  /// 兼容入口：以 waof 日志直接装配单物理日志域（内部经 [`single_log_aof`]
  /// 工厂构造权威 AOF，供测试及轻量级单物理日志场景快速接入）。
  pub fn with_wal(
    store: SharedStore<SegmentedDevice>,
    wal: Arc<WalLog<SegmentedDevice>>,
  ) -> crate::Result<Self> {
    Self::with_node_args(&NodeArgs::default(), store, wal)
  }

  /// 基于通用 NodeArgs 节点参数与存储引擎组装单机服务
  pub fn with_node_args(
    args: &NodeArgs,
    store: SharedStore<SegmentedDevice>,
    wal: Arc<WalLog<SegmentedDevice>>,
  ) -> crate::Result<Self> {
    // 投影运行时选项（Options.cs:GetServerOptions 装配段：aof_commit_ms →
    // CommitFrequencyMs），与 provider 侧 with_runtime_server_options 同源
    let aof = single_log_aof(wal, &args.runtime_server_options());
    Self::assemble(store, aof)
  }
}

/// 发布订阅后台消费任务
///
/// libs/server/PubSub/SubscribeBroker.cs:StartAsync（ConsumeAllAsync 循环）
/// 的宿主驱动：等待待发队列新元素（事件驱动，零空转轮询）→ 批量分发
/// 广播到各订阅邮箱；中枢 dispose 关闭队列后自然退出
fn spawn_pubsub_consume_task(broker: Arc<SubscribeBroker>) {
  spawn(async move {
    while broker.wait_pending().await {
      broker.consume_pending();
    }
  })
  .detach();
}

/// Lua 脚本超时后台 tick 任务
///
/// libs/server/Lua/LuaTimeoutManager.cs:Start（专属定时线程循环）的
/// compio 宿主等价物：按 tick_millis（超时 / 10，最小 1ms）节拍驱动
/// [`LuaTimeoutManager::tick`]，到期 run 经共享截止槽 CAS 激活，VM safepoint
/// 中断回调随即抛出超时错误。进程级单任务（detached，随运行时退出）。
pub fn spawn_lua_timeout_tick(manager: Arc<LuaTimeoutManager>) {
  let tick_millis = u64::try_from(manager.tick_millis()).unwrap_or(1).max(1);
  spawn(async move {
    loop {
      sleep(Duration::from_millis(tick_millis)).await;
      manager.tick();
    }
  })
  .detach();
}

/// Lua 超时装配（C# StoreWrapper 构造段 EnableLua 分支 + GarnetServer.cs:Start
/// 的 luaTimeoutManager.Start()）
///
/// `enable_lua` 且 `timeout_ms > 0`（C# Timeout != InfiniteTimeSpan）时建
/// 管理器并启动 tick 任务；否则返回 None（无超时形态，VM 零轮询开销）。
/// 须在 compio 运行时内调用（tick 任务 spawn 面）。
pub fn assemble_lua_timeout(enable_lua: bool, timeout_ms: i64) -> Option<Arc<LuaTimeoutManager>> {
  let manager =
    (enable_lua && timeout_ms > 0).then(|| Arc::new(LuaTimeoutManager::new(timeout_ms)));
  if let Some(manager) = &manager {
    spawn_lua_timeout_tick(Arc::clone(manager));
  }
  manager
}

pub const DEFAULT_DATA_FILE: &str = "wedb.data";

/// 单机/集群统一节点装配句柄：存储引擎 + 集合项经纪 + 向量集合管理器
pub type DefaultNodeHandles = (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  Arc<VectorManager>,
);

/// 生产装配的存储引擎配置（自适应容量 + 内置 GC 常规节奏）
fn store_config() -> StoreConfig {
  let mut config = StoreConfig::auto();
  config.gc.enabled = true;
  config.gc.scan_interval_ms = DEFAULT_GC_SCAN_INTERVAL_MS;
  config.gc.compaction_interval_ms = DEFAULT_GC_COMPACTION_INTERVAL_MS;
  config
}

/// 集合项经纪 + 向量集合管理器装配对（open_node 与恢复装配共用产物）
type NodeComponents = (
  Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  Arc<VectorManager>,
);

/// 集合项经纪 + 向量集合管理器（随存储句柄派生的装配对；open 与恢复
/// 装配共用，经纪/向量各持独立存储会话）
fn node_components(store: &SharedStore<SegmentedDevice>) -> crate::Result<NodeComponents> {
  let broker_session = store.new_session()?;
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(broker_session),
  ))));

  let vector_session = Arc::new(store.new_session()?);
  let vector_callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(vector_session)));
  let vector_manager = Arc::new(VectorManager::new(
    VectorManagerOptions::default(),
    vector_callbacks,
  ));

  Ok((broker, vector_manager))
}

/// 检查点目录口径（C# CheckpointBaseDirectory 缺省时回落数据目录的
/// Store/checkpoints，对标 GetStoreCheckpointDirectory(0)）
fn checkpoint_dir_of(data_path: &Path) -> PathBuf {
  data_path
    .parent()
    .unwrap_or_else(|| Path::new("."))
    .join("Store")
    .join("checkpoints")
}

/// 打开单文件存储设备，装配存储引擎、共享经纪与向量集合管理器
///
/// 单机/集群功能一致的统一节点装配三件套（存储域初始化 + 经纪 +
/// 向量管理器，对标 C# GarnetServer InitializeServer）
pub fn open_node(data_path: impl AsRef<Path>) -> crate::Result<DefaultNodeHandles> {
  open_node_with_config(store_config(), data_path)
}

/// [`open_node`] 的显式配置变体（测试/嵌入式用小预算配置注入）
pub fn open_node_with_config(
  config: StoreConfig,
  data_path: impl AsRef<Path>,
) -> crate::Result<DefaultNodeHandles> {
  let device = SegmentedDevice::single_file(data_path.as_ref())?;
  let store = WedbStore::open_shared(config, Arc::new(device))?;
  let (broker, vector_manager) = node_components(&store)?;
  Ok((store, broker, vector_manager))
}

/// 从最新检查点恢复存储句柄（wdatabase 恢复面宿主段）
///
/// libs/server/StoreWrapper.cs:RecoverCheckpointAsync
///
/// 以空库为恢复宿主（GarnetDatabase 契约：版本基线对齐 + 恢复设备源），
/// 经 wdatabase 的 recover_database_checkpoint_async 执行恢复
///（C# DatabaseManagerBase.RecoverDatabaseCheckpointAsync 真身），
/// 恢复出的全新 [`WedbStore`] 优先采用并补启 GC（`open_shared` 仅覆盖
/// 宿主段）；目录无有效快照时返回宿主空库（冷启动语义）
async fn recover_checkpoint_store(
  checkpoint_dir: &Path,
  device: Arc<SegmentedDevice>,
  config: StoreConfig,
) -> crate::Result<SharedStore<SegmentedDevice>> {
  let bootstrap = WedbStore::open_shared(config, Arc::clone(&device))?;
  let db = Arc::new(GarnetDatabase::<SegmentedDevice, ()>::new(
    0,
    Arc::clone(&bootstrap),
    device,
    checkpoint_dir.to_path_buf(),
    None,
  ));
  let mgr = SingleDatabaseManager::new(checkpoint_dir.to_path_buf(), Arc::clone(&db));
  let Some(store) = mgr
    .base
    .recover_database_checkpoint_async(&db, None)
    .await?
  else {
    return Ok(bootstrap);
  };
  store.start_gc();
  log::info!("Recovered checkpoint: {}", checkpoint_dir.display());
  Ok(store)
}

/// WAL 物理日志装配（优先显式 wal_dir，未指定时默认 `<data>/wal`），
/// 返回（解析后的目录，日志句柄）
fn open_wal(
  data_path: &Path,
  wal_dir: Option<&Path>,
) -> crate::Result<(PathBuf, Arc<WalLog<SegmentedDevice>>)> {
  let wal_dir = match wal_dir {
    Some(dir) => dir.to_path_buf(),
    None => data_path
      .parent()
      .unwrap_or_else(|| Path::new("."))
      .join("wal"),
  };
  create_dir_all(&wal_dir)?;
  let wal_file_path = wal_dir.join("wal.log");
  OpenOptions::new()
    .create(true)
    .append(true)
    .open(&wal_file_path)?;
  let wal_device = Arc::new(SegmentedDevice::single_file(&wal_file_path)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  Ok((wal_dir, wal))
}

/// 存储执行域会话提供者基座（单机/集群共用装配流程，模板方法模式）
///
/// 固化公共流程：new_session → StoreGarnetApi（挂向量集合管理器）→
/// 差异钩子 → set_item_broker → set_runtime_config；单机/集群功能一致，
/// 形态差异仅 [`Self::open`] 注入的 `decorate` 钩子——单机构造
/// `RespSessionConsumer::new`（无集群切面），集群构造 `with_cluster_session`
///（挂 ClusterSession 切面，maxDatabases = 2）
pub struct StorageSessionProvider<F> {
  /// 共享存储引擎（每连接 new_session 派生独立纪元参与者；集群 CLUSTER
  /// RESET 的 HasKeysInSlots 扫描与 HARD 清库经 set_store 下达同一实例）
  pub store: SharedStore<SegmentedDevice>,
  /// 集合项经纪（服务器级共享；阻塞命令挂起/唤醒仲裁，对标 C#
  /// storeWrapper.itemBroker，经纪持独立取件会话）
  pub broker: Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  /// 向量集合管理器（服务器级共享，统一物理键存储回调落盘；向量命令经会话
  /// 级 can_serve_slot 槽位门——VADD/VREM/VSETATTR 带 wait_for_stable_slot
  /// 特判——通过后才入专用会话直写存储，与 C# CanServeSlot 一致，
  /// C# VectorManager 亦持独立 getTempSession）
  pub vector_manager: Arc<VectorManager>,
  /// WATCH 版本表（libs/server/Storage/StoreWrapper.cs:129 watchversionMap）
  pub watch_version_map: Arc<WatchVersionMap>,
  /// 服务器级运行时配置（CONFIG GET/SET 与 OBJECT_SCAN_COUNT_LIMIT 热更源）
  pub runtime_config: Arc<RuntimeServerConfig>,
  /// 慢日志容器（服务器级共享，对标 C# StoreWrapper.cs:164/243
  /// slowLogContainer；容量 = SlowLogMaxEntries）
  pub slow_log_container: Arc<SlowLogContainer>,
  /// 检查点目录（SAVE/BGSAVE 落点，C# GetStoreCheckpointDirectory 口径：
  /// 数据目录下 Store/checkpoints）
  pub checkpoint_dir: PathBuf,
  /// 最近成功检查点时刻（Unix 毫秒，服务器级共享，LASTSAVE 数据源）
  pub last_save_ms: Arc<AtomicI64>,
  /// 活跃消费者注册表（C# GarnetServerBase.activeHandlers 域；CLIENT
  /// LIST/KILL 与监视器枚举面）
  pub registry: Arc<ConsumerRegistry>,
  /// 发布订阅中枢（服务器级共享单例，C# GarnetServer.cs:277
  /// `!opts.DisablePubSub → new SubscribeBroker(...)`；None = --disable-pubsub
  /// 关闭形态，命令面按同款禁用文案回错）
  pub pubsub: Option<Arc<SubscribeBroker>>,
  /// 发布订阅后台消费任务已拉起标志（C# broker.Initialize 首次订阅拉起
  /// StartAsync 后台消费循环的对译；随首个会话建立惰性启动，幂等）
  pubsub_consume_started: AtomicBool,
  /// 量化消费者协程已拉起数量（上限 quantization_task_count，跨 worker runtime 分摊）
  quantization_started: AtomicUsize,
  /// AOF 门面（C# storeWrapper.appendOnlyFile；EnableAOF 门控——
  /// [`Self::open`] 默认关闭恒 None，[`Self::open_with_aof`] 点亮）
  aof: Option<Arc<GarnetAppendOnlyFile>>,
  /// 物理日志句柄（[`Self::open_with_aof`] / [`Self::open_recovered_with_aof`]
  /// 点亮；宿主据此装配集群复制数据面——副本落盘目标与主端推流数据源）
  wal: Option<Arc<WalLog<SegmentedDevice>>>,
  /// --recover 重放后的 AOF 尾地址（仅 [`Self::open_recovered_with_aof`]
  /// 形态点亮；对标 C# RecoverCheckpointAndAOFAsync 的 `replayedUntil`，
  /// 宿主装配尾段据此回填复制域位点 replicationOffset.SetValue）
  recovered_aof_tail: Option<AofAddress>,
  /// 访问控制列表（requirepass 认证源，对标 C# storeWrapper.serverOptions.AuthSettings）
  pub acl: Option<Arc<AccessControlList>>,
  /// 自定义命令注册表（C# storeWrapper.customCommandManager；模块 on_load
  /// 注册面，服务器级共享）
  command_manager: SharedCustomCommandManager,
  /// 差异钩子：按发送端装配会话消费者（None = 拒绝建连）
  decorate: F,
}

impl<F> StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>,
{
  /// 统一装配体：打开单文件存储引擎、共享经纪与向量集合管理器后构造基座
  ///（run 闭包一行装配；store 句柄经公开字段供集群 set_store 下达）。
  /// 注册表随装配进程级安装（CLIENT 族命令/dispose 归并直取）；
  /// AOF 门控默认关闭（`aof = None`，行为与历史逐字节一致）
  pub fn open(data_path: impl AsRef<Path>, decorate: F) -> crate::Result<Self> {
    Self::open_with_config(store_config(), data_path, decorate)
  }

  /// [`Self::open`] 的显式配置变体（测试/嵌入式用小预算配置注入）
  pub fn open_with_config(
    config: StoreConfig,
    data_path: impl AsRef<Path>,
    decorate: F,
  ) -> crate::Result<Self> {
    let data_path = data_path.as_ref();
    let (store, broker, vector_manager) = open_node_with_config(config, data_path)?;
    Self::from_parts(
      store,
      broker,
      vector_manager,
      checkpoint_dir_of(data_path),
      decorate,
    )
  }

  /// 基础装配尾段（注册表进程级安装 + 运行时配置/ACL 缺省；open 与
  /// 恢复装配共用，AOF 字段由 `open_with_aof` 形态事后点亮）
  fn from_parts(
    store: SharedStore<SegmentedDevice>,
    broker: Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
    vector_manager: Arc<VectorManager>,
    checkpoint_dir: PathBuf,
    decorate: F,
  ) -> crate::Result<Self> {
    let registry = Arc::new(ConsumerRegistry::new());
    registry.install_global();
    let watch_version_map = Arc::new(WatchVersionMap::default());
    let mut command_manager = CustomCommandManager::new();
    #[cfg(feature = "roaring")]
    wext_roaring::RoaringBitmapCommands::register(&mut command_manager)
      .map_err(|e| io::Error::other(format!("roaring commands register failed: {e}")))?;
    let command_manager = Arc::new(RwLock::new(command_manager));
    // 服务器级运行时配置（对标 C# RuntimeServerConfig 持 owner 的静态枚举
    // 分发替代：CONFIG SET 经 try_set 产出 ConfigReconcile 调停消息，由
    // CONFIG 命令域按 [`config_owner::apply_config_reconcile`] 就地送达存储
    // 引擎后台任务域（expired-key-deletion-scan-freq → 内置 GC 扫描循环
    // 启停/调频，libs/server/StoreWrapper.cs:ReconcilePrimaryTask））
    let runtime_config = Arc::new(RuntimeServerConfig::new(RuntimeServerOptions::default()));
    // 慢日志容器（对标 C# StoreWrapper.cs:243 无条件构造，容量 =
    // SlowLogMaxEntries 默认 128；main 层经 with_runtime_server_options 覆盖）
    let slow_log_container =
      new_slow_log_container(RuntimeServerOptions::default().slow_log_max_entries);
    // 发布订阅中枢默认装配（C# DisablePubSub = false 默认启用；页大小
    // 默认 4k = C# PubSubPageSize "4k"），main 层按 NodeArgs 经
    // with_pubsub_config 覆盖
    let pubsub = Some(Arc::new(SubscribeBroker::new(
      wconf::DEFAULT_PUBSUB_PAGE_SIZE,
    )));
    Ok(Self {
      store,
      broker,
      vector_manager,
      watch_version_map,
      runtime_config,
      slow_log_container,
      checkpoint_dir,
      last_save_ms: Arc::new(AtomicI64::new(0)),
      registry,
      pubsub,
      pubsub_consume_started: AtomicBool::new(false),
      quantization_started: AtomicUsize::new(0),
      aof: None,
      wal: None,
      recovered_aof_tail: None,
      acl: None,
      command_manager,
      decorate,
    })
  }

  /// 覆盖发布订阅装配（main 层按 NodeArgs 调用：--disable-pubsub 关闭 /
  /// --pubsub-page-size 调页；须在端点 accept 之前调用——首个连接建立后
  /// 已 attach 的会话持旧中枢）
  pub fn with_pubsub_config(mut self, disabled: bool, page_size: usize) -> Self {
    if let Some(old) = self.pubsub.take() {
      old.dispose();
    }
    if !disabled {
      self.pubsub = Some(Arc::new(SubscribeBroker::new(page_size)));
    }
    self
  }

  /// 覆盖运行时配置与慢日志装配（main 层按 NodeArgs 调用：
  /// [`NodeArgs::runtime_server_options`] 投影 —— --slowlog-log-slower-than /
  /// --slowlog-max-len / --object-scan-count-limit / --aof-commit-ms /
  /// --max-databases 播种 RuntimeServerConfig；须在端点 accept 之前调用）
  pub fn with_runtime_server_options(mut self, options: RuntimeServerOptions) -> Self {
    self.slow_log_container = new_slow_log_container(options.slow_log_max_entries);
    self.runtime_config = Arc::new(RuntimeServerConfig::new(options));
    self
  }

  /// AOF 门控点亮装配（C# StoreWrapper.EnableAOF 语义）：在 [`Self::open`]
  /// 基础上建独立 WAL 设备 → [`WalLog`] → [`single_log_aof`] 工厂 →
  /// [`NodeService::with_node_args`] 注册全部 AOF 写监听端口。
  ///
  /// `wal_dir`：自定义 WAL 日志目录（None 默认使用 `<data>/wal`）。
  /// `aof_commit_ms`：周期提交毫秒数（None 用 RuntimeServerOptions 默认）。
  /// 返回的基座 `aof()` / `wal()` 在场，宿主据此注入集群面（set_aof /
  /// set_replica_replication_session / set_primary_replication / set_wal）
  pub fn open_with_aof(
    data_path: impl AsRef<Path>,
    wal_dir: Option<&Path>,
    aof_commit_ms: Option<u8>,
    decorate: F,
  ) -> crate::Result<Self> {
    Self::open_with_config_and_aof(store_config(), data_path, wal_dir, aof_commit_ms, decorate)
  }

  /// [`Self::open_with_aof`] 的显式配置变体（测试/嵌入式用小预算配置注入）
  pub fn open_with_config_and_aof(
    config: StoreConfig,
    data_path: impl AsRef<Path>,
    wal_dir: Option<&Path>,
    aof_commit_ms: Option<u8>,
    decorate: F,
  ) -> crate::Result<Self> {
    let mut provider = Self::open_with_config(config, data_path.as_ref(), decorate)?;
    let (wal_dir, wal) = open_wal(data_path.as_ref(), wal_dir)?;
    let args = NodeArgs {
      wal_dir: Some(wal_dir),
      aof: true,
      aof_commit_ms,
      ..NodeArgs::default()
    };
    // 写监听端口注册 + 服务级会话（对标 C# EnableAOF 构造段）
    let node = NodeService::with_node_args(&args, Arc::clone(&provider.store), Arc::clone(&wal))
      .map_err(|e| io::Error::other(e.to_string()))?;
    // 向量域 AOF 直推装配：生产端注入端口（VADD/VREM/VSETATTR 合成写）+
    // 重放端承接面（AofProcessor 向量分支重放重建索引）
    let aof = Arc::clone(node.aof());
    provider
      .vector_manager
      .set_aof_sink(Arc::new(VectorAofSink::new(
        &aof,
        Arc::clone(provider.store.current_version_atomic()),
      )));
    aof.set_vector_manager(Arc::clone(&provider.vector_manager));
    provider.aof = Some(aof);
    provider.wal = Some(wal);
    Ok(provider)
  }

  /// --recover 恢复装配（无 AOF 形态）：从最新检查点恢复存储句柄后装配基座
  ///
  /// C# RecoverAsync（无 AOF 分支）的变体：仅承接 checkpoint 恢复段，
  /// 完整恢复（checkpoint + AOF 重放）见 [`Self::open_recovered_with_aof`]；
  /// 恢复在端点 accept 之前完成的时序由
  /// [`crate::server::ServerBootstrap::run_async`] 的装配回调承接
  ///（对标 C# Start 的 `Provider.RecoverAsync()` 同步完成后才
  /// `servers[i].Start()`）。
  ///
  /// 检查点目录无有效快照时回退冷启动空库（对标 C# RecoverAsync 对空
  /// 检查点目录的静默语义）
  pub async fn open_recovered(data_path: impl AsRef<Path>, decorate: F) -> crate::Result<Self> {
    Self::open_recovered_with_config(store_config(), data_path, decorate).await
  }

  /// [`Self::open_recovered`] 的显式配置变体（测试/嵌入式用小预算配置
  /// 注入；恢复装配的 `index_size` 预检要求与快照 StoreMeta 一致，
  /// 两代装配必须传同一 config）
  pub async fn open_recovered_with_config(
    config: StoreConfig,
    data_path: impl AsRef<Path>,
    decorate: F,
  ) -> crate::Result<Self> {
    let data_path = data_path.as_ref();
    let checkpoint_dir = checkpoint_dir_of(data_path);
    let device = Arc::new(SegmentedDevice::single_file(data_path)?);
    let store = recover_checkpoint_store(&checkpoint_dir, device, config).await?;
    let (broker, vector_manager) = node_components(&store)?;
    Self::from_parts(store, broker, vector_manager, checkpoint_dir, decorate)
  }

  /// --recover 恢复装配（AOF 点亮形态）：检查点恢复 + WAL 设备面恢复 + 全量重放
  ///
  /// libs/server/StoreWrapper.cs:RecoverAsync（Recover 分支：
  /// RecoverCheckpointAsync → RecoverAOFAsync → ReplayAOF）。重放以恢复
  /// store 的版本基线过滤（[`wkv`] checkpoint token 版本——跳过检查点已
  /// 覆盖的旧代条目，未从检查点恢复时版本 0 = 全量重放）；生产写路径
  /// AOF 监听注册在恢复出的存储句柄上（增量继续镜像 WAL）
  pub async fn open_recovered_with_aof(
    data_path: impl AsRef<Path>,
    wal_dir: Option<&Path>,
    aof_commit_ms: Option<u8>,
    decorate: F,
  ) -> crate::Result<Self> {
    Self::open_recovered_with_config_and_aof(
      store_config(),
      data_path,
      wal_dir,
      aof_commit_ms,
      decorate,
    )
    .await
  }

  /// [`Self::open_recovered_with_aof`] 的显式配置变体（测试/嵌入式用小
  /// 预算配置注入；恢复装配的 `index_size` 预检要求与快照 StoreMeta
  /// 一致，两代装配必须传同一 config）
  pub async fn open_recovered_with_config_and_aof(
    config: StoreConfig,
    data_path: impl AsRef<Path>,
    wal_dir: Option<&Path>,
    aof_commit_ms: Option<u8>,
    decorate: F,
  ) -> crate::Result<Self> {
    let data_path = data_path.as_ref();
    let checkpoint_dir = checkpoint_dir_of(data_path);
    let device = Arc::new(SegmentedDevice::single_file(data_path)?);
    let store = recover_checkpoint_store(&checkpoint_dir, Arc::clone(&device), config).await?;
    let (resolved_wal_dir, wal) = open_wal(data_path, wal_dir)?;
    let args = NodeArgs {
      wal_dir: Some(resolved_wal_dir),
      aof: true,
      aof_commit_ms,
      ..NodeArgs::default()
    };
    // 写监听端口注册 + 服务级会话（挂到恢复出的存储句柄）
    let node = NodeService::with_node_args(&args, Arc::clone(&store), Arc::clone(&wal))
      .map_err(|e| io::Error::other(e.to_string()))?;
    // 向量域先于 AOF 重放装配（重放的 VADD/VREM/VSETATTR 条目经 AOF 门面的
    // 向量承接面重建索引；vm 的存储回调绑恢复出的存储句柄，元素数据随
    // 重放落盘）
    let (broker, vector_manager) = node_components(&store)?;
    let aof = Arc::clone(node.aof());
    vector_manager.set_aof_sink(Arc::new(VectorAofSink::new(
      &aof,
      Arc::clone(store.current_version_atomic()),
    )));
    aof.set_vector_manager(Arc::clone(&vector_manager));
    // AOF 设备面恢复 + 全量重放（版本基线过滤）
    let db = Arc::new(GarnetDatabase::with_garnet_aof(
      0,
      Arc::clone(&store),
      device,
      checkpoint_dir.clone(),
      Some(Arc::clone(&aof)),
    ));
    let mgr = SingleDatabaseManager::new(checkpoint_dir.clone(), db);
    let replayed = mgr.recover_aof().await?;
    log::info!("Recovered AOF: replayed {replayed} entries");
    // 重放后的 AOF 尾地址（对标 C# ReplayAOF 返回值 replayedUntil；宿主
    // 装配尾段据此回填 rm 复制位点——gossip 广播与 failover 判定基线）
    let recovered_aof_tail = aof.log().tail_address();
    let mut provider = Self::from_parts(store, broker, vector_manager, checkpoint_dir, decorate)?;
    provider.aof = Some(aof);
    provider.wal = Some(wal);
    provider.recovered_aof_tail = Some(recovered_aof_tail);
    Ok(provider)
  }

  /// 注册表句柄（RegisterApi 装配源，对标 storeWrapper.customCommandManager）
  pub fn command_manager(&self) -> SharedCustomCommandManager {
    Arc::clone(&self.command_manager)
  }

  /// AOF 门面（AOF 门控未点亮时 None）
  pub fn aof(&self) -> Option<&Arc<GarnetAppendOnlyFile>> {
    self.aof.as_ref()
  }

  /// 物理日志句柄（AOF 门控未点亮时 None；宿主据此装配集群复制数据面：
  /// 副本接收会话落盘目标与主端推流数据源共用同一日志实例）
  pub fn wal(&self) -> Option<&Arc<WalLog<SegmentedDevice>>> {
    self.wal.as_ref()
  }

  /// --recover 重放后的 AOF 尾地址（仅 [`Self::open_recovered_with_aof`]
  /// 形态点亮；对标 C# RecoverCheckpointAndAOFAsync 尾段
  /// `replicationOffset.SetValue(ref replayedUntil)` 的回填值来源）
  pub fn recovered_aof_tail(&self) -> Option<AofAddress> {
    self.recovered_aof_tail
  }

  /// 配置 requirepass 认证口令（对标 C# Options.Password / GetAuthenticationSettings）
  pub fn with_requirepass(mut self, pass: Option<&str>) -> Self {
    self.acl = pass.filter(|p| !p.is_empty()).map(|p| {
      // 纯内存创建默认用户，无配置文件 I/O，绝不 panic
      let acl = AccessControlList::new(p, None)
        .expect("failed to create AccessControlList with requirepass");
      Arc::new(acl)
    });
    self
  }
}

impl<F> SessionProviderFace for StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync,
{
  type Consumer = RespSessionConsumer;

  /// 模板方法：公共装配流程 + 差异钩子（存储会话创建失败拒绝建连）
  fn get_session(
    &self,
    _wire_format: WireFormat,
    network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    // 量化消费者协程随首个 worker runtime 惰性拉起（C# 宿主启动序列
    // VectorManager.StartQuantizationTasks(QuantizationTaskCount)；
    // 逐 worker 分摊直至配额用尽，避免单 runtime 独扛全部量化负载）
    let quota = self.vector_manager.quantization_task_count.max(1);
    if self.quantization_started.fetch_add(1, Ordering::Relaxed) < quota {
      self.vector_manager.start_quantization_tasks(1);
    }

    // 发布订阅后台消费任务随首个会话惰性拉起（C# SubscribeBroker.Initialize
    // 首次订阅拉起 StartAsync 后台消费循环的对译；compio spawn 与本调用
    // 同 runtime，wait_pending 事件驱动零空转轮询）
    if let Some(broker) = &self.pubsub
      && !self.pubsub_consume_started.swap(true, Ordering::Relaxed)
    {
      spawn_pubsub_consume_task(Arc::clone(broker));
    }

    let session = self.store.new_session().ok()?;
    let checkpoint = CheckpointCtx {
      dir: self.checkpoint_dir.clone(),
      last_save_ms: Arc::clone(&self.last_save_ms),
      aof: self.aof.clone(),
    };
    let api = StoreGarnetApi::new(session)
      .with_vector_manager(Arc::clone(&self.vector_manager))
      .with_checkpoint_ctx(checkpoint)
      .with_custom_command_manager(Arc::clone(&self.command_manager));
    let mut consumer = (self.decorate)(network_sender_id, api)?;
    consumer.attach_transaction_components(Arc::clone(&self.watch_version_map));
    consumer.set_item_broker(self.broker.clone());
    consumer.set_runtime_config(self.runtime_config.clone());
    // 慢日志容器接线（C# StoreWrapper.cs:243 构造 + 会话构造传入；
    // SLOWLOG 记录/查询面共享同一实例）
    consumer.set_slow_log_container(Arc::clone(&self.slow_log_container));
    consumer.set_custom_command_manager(Arc::clone(&self.command_manager));
    if let Some(acl) = &self.acl {
      let auth = Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(
        Arc::clone(acl),
      ))));
      consumer.attach_acl(auth, None);
    }
    // 发布订阅中枢接线（C# StoreWrapper 构造把 subscribeBroker 传入会话；
    // None = --disable-pubsub，会话命令面按禁用文案回错）
    if let Some(broker) = &self.pubsub {
      consumer.attach_pubsub(Arc::clone(broker));
    }
    Some(consumer)
  }

  /// 活跃消费者注册表（网络泵建连/注册、释放/注销的入口）
  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }
}
