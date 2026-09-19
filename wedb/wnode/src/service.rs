//! 节点服务编排：存储引擎（wkv）与 AOF 日志层（GarnetLog + waof 磁盘子日志）
//! 的统一收口
//!
//! 对标 Garnet 的单一 AOF 机制：写入端统一经 [`crate::aof::garnet_log::GarnetLog::enqueue`]
//! （唯一条目编码定义），重放端统一经 [`AofProcessor`]（唯一重放分发），
//! 磁盘承载为 [`WaofSublog`]（waof `WalLog` 的 GarnetLog 后端适配），
//! 域装配唯一入口 [`single_log_aof`]（库管理面与数据写入面共享同一物理
//! 日志实例）。协议载荷为 [`ReplayInput`]（C# StringInput 的
//! 序列化形态），apply → log 顺序由 wkv 写监听端口固化，条目 store_version
//! 取写入时存储版本（checkpoint token，重放端跳过低版本条目）。

use std::{
  fs::{OpenOptions, create_dir_all},
  future::Future,
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
use waof::{AofAddress, AofEntryType, WalConfig, WalLog};
use wbase::{align::prev_power_of2, convert::unix_time_in_milliseconds_from_ticks};
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub, StorageBackendType, TreeTuning};
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
use wkv::{Error, StoreConfig, StoreEvent, StoreEventSink, StoreSession, WedbStore};
use wlua::LuaTimeoutManager;
use wmetric::{SessionMetricsHandle, SlowLogContainer};
use wpubsub::subscribe_broker::SubscribeBroker;
use wresp::command::RespCommand;
use wtxn::{TxnLockTable, WatchVersionMap};
use wval::{KeyTag, NO_ETAG, NamespaceDbCodec, TaggedKeyBuf};
use wvector::Callbacks;

use crate::{
  aof::{
    AofProcessor, AofWriteContext,
    aof_processor::ReplayTarget,
    garnet_append_only_file::GarnetAppendOnlyFile,
    readconsistency::replica_read_session_context::ReadSessionState,
    recover::aof_recover::AofRecover,
    replay_input::{EMPTY_REPLAY_INPUT_BYTES, ReplayInputSlice},
    replaycoordinator::stored_proc_replay::StoredProcRegistryReplayer,
    waof_sublog::single_log_aof,
  },
  config_owner::apply_config_reconcile,
  database::{GarnetDatabase, IDatabaseManager, SingleDatabaseManager},
  primary_tasks::PrimaryTasks,
  rangeindex::range_index_manager_replication::{
    RangeIndexManagerReplication, RangeIndexStreamArgs,
  },
  resp::{
    RespSessionConsumer, SessionDependencies,
    garnet_api::{CheckpointCtx, CollectionNotify, StoreGarnetApi},
    metrics_commands::new_slow_log_container,
    objects::collection_item_source::CollectionItemSource,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_replication::VectorAofSink,
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
  servers::consumer_registry::ConsumerRegistry,
  storage::session::storage_session::{StorageSession, version_map_watch_hook},
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
  /// 本引擎实例锁表句柄（对标 C# 重放会话经所属 store 取 `LockTable`：
  /// `SessionFunctionsWrapper.cs:30`；AOF 存储过程重放与在线事务同面互斥）
  lock_table: TxnLockTable,
}

/// 原始底层条目入队（失败上抛拒绝该命令：主存写入已生效，AOF 缺条目即
/// 主从发散，对标 C# 会话转 GarnetException 失败回客户端）
#[inline]
fn enqueue_raw(
  aof: &GarnetAppendOnlyFile,
  op_type: AofEntryType,
  version: i64,
  key: &[u8],
  value: &[u8],
  input: &[u8],
) -> wkv::Result<i64> {
  aof
    .enqueue_raw(op_type, version, key, value, input)
    .map_err(|e| Error::AofEnqueue(e.to_string()))
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
) -> wkv::Result<i64> {
  aof
    .enqueue_slices(op_type, version, key, value, input)
    .map_err(|e| Error::AofEnqueue(e.to_string()))
}

/// 物理键编码（栈上分配避免堆分配，TaggedKeyBuf 最多内联 62 字节）
///
/// 唯一入账键编码器：ns/db 取事件携带的会话真值（杜绝伪造 0,0 致跨租界
/// 落错库），tag 按记录实际驻留域选择——TTL/ETag/字符串删为 String 域，
/// RI 族（含就地升阶流）为 Meta 域，与 `doc/zh/db.md` 刚性前缀同构。
#[inline]
fn physical_key(ns: u64, db: u64, tag: KeyTag, user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(ns, db, tag, user_key)
}

/// AOF 写事件监听器上下文（静态分发环境）
struct AofSinkContext {
  aof: Arc<GarnetAppendOnlyFile>,
  ver_atomic: Arc<AtomicI64>,
  /// RI AOF 复制面：集合升阶树数据经既有 RangeIndexStreamChunk 通道灌入
  /// （与回放面 replay_into_session 各持一实例，同一引擎句柄派生）
  ri: Arc<RangeIndexManagerReplication>,
}

fn on_aof_store_event(ctx: &AofSinkContext, event: StoreEvent<'_>) -> wkv::Result<()> {
  let ver = ctx.ver_atomic.load(Ordering::Acquire);
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
          return Ok(());
        }
        enqueue_raw(
          &ctx.aof,
          AofEntryType::StoreDelete,
          ver,
          key,
          val,
          &EMPTY_REPLAY_INPUT_BYTES,
        )?;
        return Ok(());
      }
      // ACL 用户规则旁路标签（0x0D）：与 String 域同为整值写/墓碑删，
      // 经 StoreUpsert/StoreDelete 条目镜像（从库 AOF 回放据此重建用户表）
      //
      // DbMeta 系统元数据（0x0E）：映射体系镜像通道（doc/zh/db.md「物理日志
      // 复制与 Checkpoint 直接镜像主库的 KeyTag::DbMeta 与数据记录；从库完全
      // 继承主库的映射体系，不进行本地二次映射」）——主库全部换号批/首映射/
      // SWAPDB 记录与 GC 墓碑注销经此镜像，从库回放面交
      // WedbStore::apply_dbmeta_record / apply_dbmeta_tombstone 应用，换号虚号
      // 主从同源；条目即完整记录（键载荷 + 定长值），无需第二套映射同步机制
      if tag != Some(KeyTag::String) && tag != Some(KeyTag::Acl) && tag != Some(KeyTag::DbMeta) {
        return Ok(());
      }
      let op = if tombstone {
        AofEntryType::StoreDelete
      } else {
        AofEntryType::StoreUpsert
      };
      enqueue_raw(&ctx.aof, op, ver, key, val, &EMPTY_REPLAY_INPUT_BYTES)?;
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
        .with_deterministic()
        .with_args_num(arg1, 0, 0);
      enqueue_slices(
        &ctx.aof,
        AofEntryType::StoreRMW,
        ver,
        &physical_key(ns, db, KeyTag::String, key),
        &[],
        &input,
      )?;
    }
    StoreEvent::EtagWrite { ns, db, key, etag } => {
      let empty_args: [&[u8]; 0] = [];
      let input = ReplayInputSlice::new(RespCommand::Setwithetag, &empty_args)
        .with_deterministic()
        .with_args_num(etag.unwrap_or(NO_ETAG), 0, 0);
      enqueue_slices(
        &ctx.aof,
        AofEntryType::StoreRMW,
        ver,
        &physical_key(ns, db, KeyTag::String, key),
        &[],
        &input,
      )?;
    }
    StoreEvent::ObjectRmw(notif) => {
      let input = ReplayInputSlice::new(RespCommand::None, notif.args)
        .with_deterministic()
        .with_sub_id(notif.op_code)
        .with_obj_type(notif.obj_type)
        .with_args_num(notif.arg1 as i64, notif.arg2 as i64, 0);
      enqueue_slices(
        &ctx.aof,
        AofEntryType::ObjectStoreRMW,
        ver,
        notif.key,
        &[],
        &input,
      )?;
    }
    StoreEvent::EnvelopeUpsert { key, val } => {
      enqueue_raw(
        &ctx.aof,
        AofEntryType::ObjectStoreUpsert,
        ver,
        key,
        val,
        &EMPTY_REPLAY_INPUT_BYTES,
      )?;
    }
    // RI.SET/RI.DEL 的 AOF 记录单点：C# 经 functionsState 显式调用
    // libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ReplicateRangeIndexSet
    // 与 libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ReplicateRangeIndexDel
    // 两对口，rust 由本 StoreEvent 通道一处承接
    StoreEvent::RangeIndexWrite {
      ns,
      db,
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
      let input = ReplayInputSlice::new(cmd, args).with_deterministic();
      enqueue_slices(
        &ctx.aof,
        AofEntryType::StoreRMW,
        ver,
        &physical_key(ns, db, KeyTag::Meta, key),
        &[],
        &input,
      )?;
    }
    StoreEvent::RangeIndexCreate {
      ns,
      db,
      key,
      backend,
      tuning,
    } => {
      let stub = RangeIndexStub::from_tuning(0, &tuning, *backend);
      let mut stub_bytes = [0u8; RANGE_INDEX_STUB_SIZE];
      if let Err(e) = stub.encode_into(&mut stub_bytes) {
        log::error!("RI.CREATE AOF 存根编码失败: {e}");
        return Ok(());
      }
      let stub_args = [&stub_bytes[..]];
      let input = ReplayInputSlice::new(RespCommand::Ricreate, &stub_args).with_deterministic();
      enqueue_slices(
        &ctx.aof,
        AofEntryType::StoreRMW,
        ver,
        &physical_key(ns, db, KeyTag::Meta, key),
        &[],
        &input,
      )?;
    }
    StoreEvent::RangeIndexDrop { ns, db, key } => {
      enqueue_raw(
        &ctx.aof,
        AofEntryType::StoreDelete,
        ver,
        &physical_key(ns, db, KeyTag::Meta, key),
        &[],
        &EMPTY_REPLAY_INPUT_BYTES,
      )?;
    }
    StoreEvent::RangeIndexStream {
      ns,
      db,
      key,
      obj_type,
      stub,
      file_path,
    } => {
      // 集合就地升阶的树数据通道：复用 RI 迁移流，把快照文件分块灌入
      // RangeIndexStreamChunk。条目键取 Meta 域物化键（回放真值=物理键，
      // KeyContextGuard 据此直设事件携带的虚拟域 (vns, vdb) 并以用户键重组发
      // 布），与其余 RI 臂同走物理键编码器 `physical_key(.., Meta, ..)` 单一
      // 口径。发布判别类型 obj_type 随首块 ReplayInput 携带，副本据此重建
      // MetaValue。
      let meta_key = physical_key(ns, db, KeyTag::Meta, key);
      ctx
        .ri
        .replicate_range_index_stream(
          RangeIndexStreamArgs {
            key: &meta_key,
            obj_type,
            stub: &stub,
            file_path,
            ctx: AofWriteContext::from_version(ver),
            chunk_size: ctx.ri.aof_stream_chunk_size(),
          },
          Some(&ctx.aof),
        )
        .map_err(|e| Error::AofEnqueue(e.to_string()))?;
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
        &ctx.aof,
        AofEntryType::StoreRMW,
        ver,
        &physical_key(ns, db, KeyTag::String, key),
        &[],
        &input,
      )?;
    }
  }
  Ok(())
}

impl<D: Device> NodeService<D> {
  /// 组装节点服务（AOF 域由调用方装配后注入——单物理日志域唯一实例，
  /// 经 [`single_log_aof`] 工厂构造，与库管理面共享同一物理日志）
  ///
  /// 按运行态 GC 配置补启内置后台循环（幂等）：`gc.enabled`（默认禁用，
  /// 对标 C# ExpiredKeyDeletionScanFrequencySecs = -1）为真时拉起，全关
  /// 即 no-op——显式传配置或热更新打开的嵌入式形态在此承接；服务端形态
  /// 按槽位的启动期注册在 StorageSessionProvider::get_session 惰性段，
  /// 升主恢复点 resume_primary_tasks → start_gc 承接重拉
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
    let ctx = Arc::new(AofSinkContext {
      aof: Arc::clone(&aof),
      ver_atomic: Arc::clone(store.current_version_atomic()),
      ri: Arc::new(RangeIndexManagerReplication::new(Arc::clone(
        store.range_index(),
      ))),
    });
    let event_sink = StoreEventSink::new(ctx, on_aof_store_event);
    if !store.set_event_sink(event_sink) {
      log::warn!("存储事件处理器重复注册");
    }
    store.start_gc();
    let session = store.new_session()?;
    // 锁源注入本引擎 store 的当前索引装载闭包：事务键锁走 windex 哈希桶内嵌闩，
    //        每笔事务现取 HashIndex 版本，粒度随索引规模与 split 扩容联动（对标 C# 持 store 引用）
    let lock_table = TxnLockTable::from_loader({
      let store = Arc::clone(&store);
      move || store.index.load_full()
    });
    Ok(Self {
      session,
      aof,
      lock_table,
    })
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
    let storage = StorageSession::new(batch);
    let mut processor = AofProcessor::new(Arc::clone(&self.aof));
    let ri_manager = Arc::new(RangeIndexManagerReplication::new(Arc::clone(
      target_session.store.range_index(),
    )));
    processor.set_range_index_manager(ri_manager);
    // 存储过程回放执行面装配（C# `RunCustomTxnProcAtReplica` 经重放会话所属
    // store 的 LockTable 取锁；rust 侧同口径下发本服务实例锁表句柄，
    // 重放事务与在线事务同面互斥）
    processor.set_stored_proc_replayer(Arc::new(StoredProcRegistryReplayer::new(
      self.lock_table.clone(),
    )));
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
    let aof = single_log_aof(wal, &args.runtime_server_options())?;
    Self::assemble(store, aof)
  }
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
/// 在臂内，宿主不再绕过 trait 面内联第二套门控。单次失败仅记录日志不退出
/// 循环（C# catch 仅 LogError 同语义）。弱引用捕获，宿主释放后自然退出。
pub fn spawn_aof_size_limit_task(
  database_manager: Arc<SingleDatabaseManager<SegmentedDevice>>,
  aof_size_limit: u64,
  runtime_config: Option<Arc<RuntimeServerConfig>>,
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
  spawn(async move {
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
  })
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
/// 单次失败仅记录日志不退出循环。弱引用捕获，宿主释放后自然退出。
pub fn spawn_index_auto_grow_task(
  database_manager: Arc<SingleDatabaseManager<SegmentedDevice>>,
  index_max_size: usize,
  resize_threshold: i64,
  frequency_secs: u64,
) {
  let interval = Duration::from_secs(frequency_secs.max(1));
  let weak_dm = Arc::downgrade(&database_manager);
  drop(database_manager);
  spawn(async move {
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
  })
  .detach();
}

/// 发布订阅后台消费任务
///
/// libs/server/PubSub/SubscribeBroker.cs:StartAsync（ConsumeAllAsync 循环）
/// 的宿主驱动：等待待发队列新元素（事件驱动，零空转轮询）→ 批量分发
/// 广播到各订阅邮箱；中枢 dispose 关闭队列后自然退出。任务体首尾经
/// consumer_start/consumer_finish 回报生命周期（C# Initialize 的 done.Reset
/// 与 StartAsync finally 的 done.Set），供 [`SubscribeBroker::dispose`]
/// 的收口等待承接 done.WaitOne 语义
fn spawn_pubsub_consume_task(broker: Arc<SubscribeBroker>) {
  spawn(async move {
    broker.consumer_start();
    while broker.wait_pending().await {
      broker.consume_pending();
    }
    broker.consumer_finish();
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

/// 单机/集群统一节点装配句柄：存储引擎 + 集合项经纪 + 向量集合管理器
pub type DefaultNodeHandles = (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  Arc<VectorManager>,
);

/// 生产装配的存储引擎配置（自适应容量；内置 GC 默认禁用，对标 C#
/// GarnetServerOptions.ExpiredKeyDeletionScanFrequencySecs = -1：装配不越权
/// 开后台任务，启停一律走槽位——启动期 `--expired-key-deletion-scan-freq`
/// 经 `NodeArgs → RuntimeServerOptions` 播种槽位（对标 C# Options.cs:1033 →
/// RuntimeServerConfig.cs:264），运行期 CONFIG SET 同槽经
/// [`apply_config_reconcile`] 调停启停；本函数的 `config.gc` 仅作引擎初值，
/// 首轮会话即被槽位投影覆盖，不构成第二套启停真值源）
fn store_config() -> StoreConfig {
  StoreConfig::auto()
}

/// hlog 配置段覆盖项（`wconf::HlogOptions::validated` 投影）
type HlogOverrides = HlogProjection;

/// hlog 环形缓冲最小页数（对齐 wkv 内存预算规划器的页数下限）
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
///   （wedb ReadCache 页容量绑定主存页，见 wkv `StoreConfig::enable_read_cache`；
///   预算未显式配置取 `wconf::DEFAULT_READ_CACHE_MEMORY_SIZE`，16MB 主存页下
///   推导 64 页，与引擎默认页数一致）。
fn apply_hlog_overrides(config: &mut StoreConfig, overrides: HlogOverrides) -> crate::Result<()> {
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
    config.read_cache_num_pages = prev_power_of2((budget / config.page_size) as u64) as usize;
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
fn store_config_from_node(node: &NodeArgs) -> crate::Result<StoreConfig> {
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
/// 单机/集群功能一致的节点装配三件套唯一入口（存储域初始化 + 经纪 +
/// 向量管理器，对标 C# GarnetServer InitializeServer）：`config` 由调用方
/// 投影——生产经 `store_config_from_node`（[`NodeArgs`] hlog 段覆盖自适应
/// 基线），测试与嵌入式显式注入小预算 [`StoreConfig`]
pub fn open_node_with_config(
  config: StoreConfig,
  data_path: impl AsRef<Path>,
) -> crate::Result<DefaultNodeHandles> {
  let device = SegmentedDevice::single_file(data_path.as_ref())?;
  let store = WedbStore::open_shared(config, Arc::new(device))?;
  let (broker, vector_manager) = node_components(&store)?;
  Ok((store, broker, vector_manager))
}

/// 从最新检查点恢复存储句柄（database 恢复面宿主段）
///
/// libs/server/StoreWrapper.cs:RecoverCheckpointAsync
///
/// 以空库为恢复宿主（GarnetDatabase 契约：版本基线对齐 + 恢复设备源），
/// 经 DatabaseManagerBase 的 recover_database_checkpoint_async 执行恢复
///（C# DatabaseManagerBase.RecoverDatabaseCheckpointAsync 真身），
/// 恢复出的全新 [`WedbStore`] 优先采用并补启 GC（`open_shared` 仅覆盖
/// 宿主段）；目录无有效快照时返回宿主空库（冷启动语义）
async fn recover_checkpoint_store(
  checkpoint_dir: &Path,
  device: Arc<SegmentedDevice>,
  config: StoreConfig,
) -> crate::Result<SharedStore<SegmentedDevice>> {
  let bootstrap = WedbStore::open_shared(config, Arc::clone(&device))?;
  let db = Arc::new(GarnetDatabase::<SegmentedDevice>::new(
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
/// 差异钩子 → inject_dependencies（统一注入会话依赖）；单机/集群功能一致，
/// 在线引擎置换槽（共享状态句柄，对标 C# storeWrapper 引擎原位重构——
/// 持当前在线引擎一份状态，宿主与集群反查同一槽，无第二份拷贝）
#[derive(Clone, Default)]
pub struct StoreSwapSlot {
  inner: Arc<RwLock<Option<SharedStore<SegmentedDevice>>>>,
}

impl StoreSwapSlot {
  /// 创建空槽
  pub fn new() -> Self {
    Self::default()
  }

  /// 当前引擎（未播种且未置换时 None）
  #[inline]
  pub fn get(&self) -> Option<SharedStore<SegmentedDevice>> {
    self.inner.read().clone()
  }

  /// 换持新引擎并返回被换下的旧引擎（若有）；首次调用即为播种
  pub fn swap(&self, store: SharedStore<SegmentedDevice>) -> Option<SharedStore<SegmentedDevice>> {
    let mut w = self.inner.write();
    let old = w.clone();
    *w = Some(store);
    old
  }
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
  /// 发布订阅后台消费任务已拉起标志（C# broker.Initialize 首次订阅拉起
  /// StartAsync 后台消费循环的对译；随首个会话建立惰性启动，幂等）
  pubsub_consume_started: AtomicBool,
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
  /// AOF 体积限额任务已拉起标志（随首个会话建立惰性启动，幂等）
  aof_size_limit_started: AtomicBool,
  /// 索引自动扩容任务参数（上限桶数, 阈值百分比, 检查周期秒；None = 未启用。
  /// C# StartGenericNodeTasks 注册条件 AdjustedIndexMaxCacheLines > 0）
  index_auto_grow: Option<(usize, i64, u64)>,
  /// 索引自动扩容任务已拉起标志（随首个会话建立惰性启动，幂等）
  index_auto_grow_started: AtomicBool,
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
  /// 差异钩子：按发送端装配会话消费者（None = 拒绝建连）
  decorate: F,
}

impl<F> StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>,
{
  /// 冷启动基础装配体（无 AOF 形态）：打开单文件存储引擎、共享经纪与向量
  /// 集合管理器后构造基座（生产无调用方——启动一律经 [`Self::open_from_args`]
  /// 按 (recover, aof) 分派；测试与嵌入式显式注入小预算 [`StoreConfig`]）。
  /// 注册表随装配进程级安装（CLIENT 族命令/dispose 归并直取）；
  /// AOF 门控此形恒 `aof = None`
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

  /// 基础装配尾段（注册表进程级安装 + 运行时配置/慢日志/PubSub 缺省；
  /// 冷启动与恢复装配共用，AOF 字段由 `open_with_config_and_aof` /
  /// `open_recovered_with_config_and_aof` 装配口点亮）
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
    // 引擎在线置换槽须先于锁表构造：供锁源闭包读取「当前引擎」（副本检查点导入换持新引擎）
    let store_swap = StoreSwapSlot::new();
    // 引擎实例锁表（C# LockTable 随 store 构造：Tsavorite.cs:228
    // `LockTable = new OverflowBucketLockTable<TStoreFunctions, TAllocator>(this)`，
    // 一处构造、随本实例的会话句柄共享，无进程级静态表）
    // 锁源注入「当前引擎索引装载闭包」：事务键锁走 windex 哈希桶内嵌闩（与 wkv TTL 读改写
    //        同一把锁、同一份内存），每笔事务现取当前引擎的 HashIndex 版本——粒度随 split 在线扩容
    //        细化，并经 store_swap 跟随引擎在线置换（对标 C# LockTable 逐次现取 store.state[version]）
    let lock_table = {
      let base_store = Arc::clone(&store);
      let store_swap = store_swap.clone();
      TxnLockTable::from_loader(move || {
        store_swap
          .get()
          .unwrap_or_else(|| Arc::clone(&base_store))
          .index
          .load_full()
      })
    };
    // WATCH 写面收口接线（对齐 C# functionsState.watchVersionMap 与引擎同实例
    // 共享）：EXEC 校验表与本表合一，wkv 用户键写入口统一按键推进
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&watch_version_map)));
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
    Ok(Self {
      store,
      store_swap,
      broker,
      vector_manager,
      watch_version_map,
      lock_table,
      runtime_config,
      slow_log_container,
      checkpoint_dir,
      database_manager,
      registry,
      pubsub,
      pubsub_consume_started: AtomicBool::new(false),
      gc_scan_started: AtomicBool::new(false),
      primary_tasks,
      aof_size_limit: None,
      aof_size_limit_started: AtomicBool::new(false),
      index_auto_grow: None,
      index_auto_grow_started: AtomicBool::new(false),
      quantization_started: AtomicUsize::new(0),
      aof: None,
      wal: None,
      recovered_aof_tail: None,
      acl: None,
      metrics_sampling_frequency_secs: 0,
      decorate,
    })
  }

  /// 覆盖发布订阅装配（main 层按 NodeArgs 调用：--disable-pubsub 关闭；须在端点
  /// accept 之前调用——首个连接建立后已 attach 的会话持旧中枢）
  pub fn with_pubsub_config(mut self, disabled: bool) -> Self {
    if disabled && let Some(old) = self.pubsub.take() {
      // 关闭时析构默认装配的中枢（C# DisablePubSub = true 不启动 broker）
      old.dispose();
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

  /// AOF 门控点亮装配（C# StoreWrapper.EnableAOF 语义）：在 [`Self::open_with_config`]
  /// 基础上建独立 WAL 设备 → [`WalLog`] → [`single_log_aof`] 工厂 →
  /// [`NodeService::with_node_args`] 注册全部 AOF 写监听端口；
  /// [`Self::open_from_args_with_config`] 的 `(false, true)` 臂即走本口。
  ///
  /// `config`：存储引擎配置（生产经 [`Self::open_from_args`] 由 NodeArgs 推导）。
  /// `wal_dir`：自定义 WAL 日志目录（None 默认使用 `<data>/wal`）。
  /// `aof_commit_ms`：周期提交毫秒数（None 用 RuntimeServerOptions 默认）。
  /// 返回的基座 `aof()` / `wal()` 在场，宿主据此注入集群面（set_aof /
  /// set_replica_replication_session / set_primary_replication / set_wal）
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
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&provider.store),
      Arc::clone(&provider.store.device),
      provider.checkpoint_dir.clone(),
      Some(Arc::clone(&aof)),
    ));
    provider.database_manager = Arc::new(SingleDatabaseManager::new(
      provider.checkpoint_dir.clone(),
      db,
    ));
    // 换装点同步注入角色域：AOF 超限臂在任意装配形态下读同一角色位
    provider
      .database_manager
      .attach_primary_tasks(Arc::clone(&provider.primary_tasks));
    // FLUSH 族登记表域回收联动注入
    provider
      .database_manager
      .attach_vector_manager(Arc::clone(&provider.vector_manager));
    provider.aof = Some(aof);
    provider.wal = Some(wal);
    if let Some(ms) = aof_commit_ms {
      let opts = RuntimeServerOptions {
        commit_frequency_ms: i32::from(ms),
        ..Default::default()
      };
      provider = provider.with_runtime_server_options(opts);
    }
    Ok(provider)
  }

  /// --recover 恢复装配（无 AOF 形态）：从最新检查点恢复存储句柄后装配基座
  ///
  /// C# RecoverAsync（无 AOF 分支）的变体：仅承接 checkpoint 恢复段，
  /// 完整恢复（checkpoint + AOF 重放）见
  /// [`Self::open_recovered_with_config_and_aof`]；
  /// 恢复在端点 accept 之前完成的时序由
  /// [`crate::server::ServerBootstrap::run_async`] 的装配回调承接
  ///（对标 C# Start 的 `Provider.RecoverAsync()` 同步完成后才
  /// `servers[i].Start()`）。生产无调用方（启动走 [`Self::open_from_args`]
  /// 的 `(true, false)` 臂），恢复装配的 `index_size` 预检要求与快照
  /// StoreMeta 一致，两代装配必须传同一 config
  ///
  /// 检查点目录无有效快照时回退冷启动空库（对标 C# RecoverAsync 对空
  /// 检查点目录的静默语义）
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

  /// --recover 恢复装配（AOF 点亮形态）：检查点恢复 + WAL 设备面恢复 + 全量重放；
  /// [`Self::open_from_args_with_config`] 的 `(true, true)` 臂即走本口。
  ///
  /// libs/server/StoreWrapper.cs:RecoverAsync（Recover 分支：
  /// RecoverCheckpointAsync → RecoverAOFAsync → ReplayAOF）。重放以恢复
  /// store 的版本基线过滤（[`wkv`] checkpoint token 版本——跳过检查点已
  /// 覆盖的旧代条目，未从检查点恢复时版本 0 = 全量重放）；生产写路径
  /// AOF 监听注册在恢复出的存储句柄上（增量继续镜像 WAL）。恢复装配的
  /// `index_size` 预检要求与快照 StoreMeta 一致，两代装配必须传同一 config
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
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      device,
      checkpoint_dir.clone(),
      Some(Arc::clone(&aof)),
    ));
    let mgr = Arc::new(SingleDatabaseManager::new(checkpoint_dir.clone(), db));
    mgr.attach_vector_manager(Arc::clone(&vector_manager));
    let replayed = mgr.recover_aof().await?;
    log::info!("Recovered AOF: replayed {replayed} entries");
    // 重放后的 AOF 尾地址（对标 C# ReplayAOF 返回值 replayedUntil；宿主
    // 装配尾段据此回填 rm 复制位点——gossip 广播与 failover 判定基线）
    let recovered_aof_tail = aof.log().tail_address();
    let mut provider = Self::from_parts(store, broker, vector_manager, checkpoint_dir, decorate)?;
    provider.database_manager = mgr;
    // 换装点同步注入角色域：AOF 超限臂在任意装配形态下读同一角色位
    provider
      .database_manager
      .attach_primary_tasks(Arc::clone(&provider.primary_tasks));
    provider.aof = Some(aof);
    provider.wal = Some(wal);
    provider.recovered_aof_tail = Some(recovered_aof_tail);
    if let Some(ms) = aof_commit_ms {
      let opts = RuntimeServerOptions {
        commit_frequency_ms: i32::from(ms),
        ..Default::default()
      };
      provider = provider.with_runtime_server_options(opts);
    }
    Ok(provider)
  }

  /// 根据 [`NodeArgs`] 自动分派恢复与 AOF 策略，并串联 requirepass、PubSub 与运行时选项装配
  ///
  /// 封装 (recover, aof) 四路状态分派（对标 GarnetServer 启动时序，四臂均走
  /// 带 [`StoreConfig`] 的装配口，配置由 [`Self::open_from_args`] 一次推导）：
  /// - `(true, true)`: [`Self::open_recovered_with_config_and_aof`] 检查点恢复 + WAL 重放
  /// - `(true, false)`: [`Self::open_recovered_with_config`] 仅检查点恢复
  /// - `(false, true)`: [`Self::open_with_config_and_aof`] 冷启动 + AOF 挂载
  /// - `(false, false)`: [`Self::open_with_config`] 基础冷启动
  ///
  /// 装配完成后依次链式注入认证口令、PubSub 规格与运行时动态配置。
  pub async fn open_from_args(
    node: &NodeArgs,
    data_path: impl AsRef<Path>,
    decorate: F,
  ) -> crate::Result<Self> {
    Self::open_from_args_with_config(store_config_from_node(node)?, node, data_path, decorate).await
  }

  /// [`Self::open_from_args`] 的显式配置变体（测试/嵌入式注入自定义 [`StoreConfig`]）
  pub async fn open_from_args_with_config(
    config: StoreConfig,
    node: &NodeArgs,
    data_path: impl AsRef<Path>,
    decorate: F,
  ) -> crate::Result<Self> {
    let data_path = data_path.as_ref();
    let provider = match (node.recover, node.aof) {
      (true, true) => {
        Self::open_recovered_with_config_and_aof(
          config,
          data_path,
          node.wal_dir.as_deref(),
          node.aof_commit_ms,
          decorate,
        )
        .await?
      }
      (true, false) => Self::open_recovered_with_config(config, data_path, decorate).await?,
      (false, true) => Self::open_with_config_and_aof(
        config,
        data_path,
        node.wal_dir.as_deref(),
        node.aof_commit_ms,
        decorate,
      )?,
      (false, false) => Self::open_with_config(config, data_path, decorate)?,
    }
    .with_requirepass(node.requirepass.as_deref())
    .with_pubsub_config(node.disable_pubsub)
    .with_runtime_server_options(node.runtime_server_options())
    .with_metrics_sampling_frequency_secs(node.metrics_sampling_frequency_secs)
    .with_vector_set_preview(node.enable_vector_set_preview);
    // 后台维护任务装配（对标 StoreWrapper.Start → StartPrimaryTasks /
    // StartGenericNodeTasks：AofSizeLimitTask / IndexAutoGrowTask 注册条件）
    Self::wire_background_tasks(provider, node)
  }

  /// 置换槽句柄（供集群节点对齐 C# storeWrapper 同步在线置换引擎）
  pub fn store_swap_slot(&self) -> StoreSwapSlot {
    self.store_swap.clone()
  }

  /// Primary 类后台任务生命周期域句柄（集群装配期注入 ClusterProvider，
  /// 角色切换点批量挂起/恢复 Primary 类任务）
  pub fn primary_tasks(&self) -> Arc<PrimaryTasks> {
    Arc::clone(&self.primary_tasks)
  }

  /// 当前在线引擎（本结构引擎取口唯一入口：置换槽优先、回落装配期初值；
  /// 对标 C# libs/server/StoreWrapper.cs:41 单计算属性——副本检查点导入
  /// 置换后，新会话装配与集群侧经此取到的必为同一新引擎实例）
  pub fn store(&self) -> SharedStore<SegmentedDevice> {
    self
      .store_swap
      .get()
      .unwrap_or_else(|| Arc::clone(&self.store))
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

  /// --recover 重放后的 AOF 尾地址（仅
  /// [`Self::open_recovered_with_config_and_aof`] 形态点亮；对标 C#
  /// RecoverCheckpointAndAOFAsync 尾段
  /// `replicationOffset.SetValue(ref replayedUntil)` 的回填值来源）
  pub fn recovered_aof_tail(&self) -> Option<AofAddress> {
    self.recovered_aof_tail
  }

  /// 上次保存时间（毫秒 Unix 时间戳，对标 C# StoreWrapper.lastSaveTime）
  pub fn last_save_ms(&self) -> u64 {
    self.database_manager.last_save_ms()
  }

  /// 配置 requirepass 认证口令（对标 C# Options.Password / GetAuthenticationSettings）
  pub fn with_requirepass(mut self, pass: Option<&str>) -> Self {
    self.acl = pass.filter(|p| !p.is_empty()).map(|p| {
      // 纯内存创建默认用户，无配置文件 I/O，绝不 panic
      let acl =
        AccessControlList::new(p).expect("failed to create AccessControlList with requirepass");
      Arc::new(acl)
    });
    self
  }

  /// 注入自定义 ACL 访问控制列表
  pub fn with_acl(mut self, acl: Arc<AccessControlList>) -> Self {
    self.acl = Some(acl);
    self
  }

  /// 注入指标采样频率秒数（对标 C# ServerOptions.MetricsSamplingFrequency →
  /// storeWrapper.trackStats 门控；仅装配链尾段注入，与监视器任务同源）
  pub fn with_metrics_sampling_frequency_secs(mut self, secs: u64) -> Self {
    self.metrics_sampling_frequency_secs = secs;
    self
  }

  /// 注入 Vector Set 预览开关（装配链尾段单次定值；开关投影进向量管理器
  /// `is_enabled`，命令面与量化/清理后台链均以此门控）。
  ///
  /// libs/server/Resp/Vector/VectorManager.cs:VectorManager
  ///（构造器 `IsEnabled = serverOptions.EnableVectorSetPreview` 注入位）
  pub fn with_vector_set_preview(self, enabled: bool) -> Self {
    self
      .vector_manager
      .is_enabled
      .store(enabled, Ordering::Relaxed);
    self
  }

  /// 启动 AOF 周期提交后台任务（C# StoreWrapper.cs:TryStartCommitTask 对标实现）
  ///
  /// 若配置了 commit_frequency_ms > 0 且存在 AOF 句柄，首次调用拉起后台周期
  /// 循环（任务常驻，副本角色由 [`PrimaryTasks`] 角色位门检挂起）。
  pub fn try_start_commit_task(&self) {
    if let Some(aof) = &self.aof {
      let ms = self.runtime_config.get_int(ServerConfigType::AofCommitFreq);
      self
        .primary_tasks
        .try_start_commit_task(aof, ms.max(0) as u64);
    }
  }

  /// 配置 AOF 体积限额（C# StartPrimaryTasks 的 AofSizeLimitTask 注册条件：
  /// limit_bytes > 0；须在端点 accept 之前调用）。检查周期不在此设——运行期
  /// 真值源为 wconf 槽 aof-size-limit-enforce-frequency（node 侧经
  /// runtime_server_options 播种），任务循环每轮现取，CONFIG SET 即时生效
  pub fn with_aof_size_limit(mut self, limit_bytes: u64) -> Self {
    self.aof_size_limit = (limit_bytes > 0).then_some(limit_bytes);
    self
  }

  /// 配置索引自动扩容上限、阈值与周期（C# StartGenericNodeTasks 的
  /// IndexAutoGrowTask 注册条件：max_buckets > 0；须在端点 accept 之前调用）
  pub fn with_index_auto_grow(
    mut self,
    max_buckets: usize,
    resize_threshold: i64,
    frequency_secs: u64,
  ) -> Self {
    self.index_auto_grow =
      (max_buckets > 0).then_some((max_buckets, resize_threshold, frequency_secs));
    self
  }

  /// 拉起 AOF 体积限额后台任务（C# StartPrimaryTasks 的 AofSizeLimitTask 注册段；
  /// 首个会话建立时惰性触发，幂等）
  fn try_start_aof_size_limit_task(&self) {
    if let Some(limit) = self.aof_size_limit
      && !self.aof_size_limit_started.swap(true, Ordering::Relaxed)
    {
      spawn_aof_size_limit_task(
        Arc::clone(&self.database_manager),
        limit,
        Some(Arc::clone(&self.runtime_config)),
      );
    }
  }

  /// 拉起索引自动扩容后台任务（C# StartGenericNodeTasks 的 IndexAutoGrowTask
  /// 注册段；首个会话建立时惰性触发，幂等）
  fn try_start_index_auto_grow_task(&self) {
    if let Some((max_buckets, threshold, freq)) = self.index_auto_grow
      && !self.index_auto_grow_started.swap(true, Ordering::Relaxed)
    {
      spawn_index_auto_grow_task(
        Arc::clone(&self.database_manager),
        max_buckets,
        threshold,
        freq,
      );
    }
  }

  /// 按节点参数装配后台维护任务（AOF 体积限额 / 索引自动扩容）
  ///
  /// libs/server/StoreWrapper.cs:StartGenericNodeTasks
  ///（注册条件对齐：AofSizeLimit 配置且 EnableAOF；IndexMaxMemorySize 配置；
  /// StartPrimaryTasks 的严格映射单点在 ClusterProvider::resume_primary_tasks，
  /// 周期任务角色门控见 primary_tasks 域）
  fn wire_background_tasks(provider: Self, node: &NodeArgs) -> crate::Result<Self> {
    // libs/host/Configuration/Options.cs:869（AofSizeLimit 不能与禁用 AOF 同启）
    if node.aof_size_limit.is_some() && !node.aof {
      return Err(crate::Error::InvalidArgument(
        "aof_size_limit cannot be enforced with disabled AOF!".into(),
      ));
    }
    let mut provider = provider;
    if let Some(limit) = node.aof_size_limit_bytes() {
      provider = provider.with_aof_size_limit(limit);
    }
    if let Some(max_buckets) = node.index_max_size_buckets() {
      provider = provider.with_index_auto_grow(
        max_buckets,
        node.index_resize_threshold,
        node.index_resize_frequency_secs,
      );
    }
    Ok(provider)
  }

  /// 提取当前服务基座持有的会话共享依赖集合（对标 C# StoreWrapper 共享依赖组）
  pub fn session_dependencies(&self) -> SessionDependencies {
    let acl_authenticator = self
      .acl
      .as_ref()
      .map(|acl| Arc::new(Mutex::new(GarnetAclAuthenticator::new(Arc::clone(acl)))));
    SessionDependencies {
      watch_version_map: Arc::clone(&self.watch_version_map),
      lock_table: self.lock_table.clone(),
      item_broker: Arc::clone(&self.broker),
      runtime_config: Arc::clone(&self.runtime_config),
      slow_log_container: Arc::clone(&self.slow_log_container),
      acl_authenticator,
      pubsub: self.pubsub.as_ref().map(Arc::clone),
      primary_tasks: Some(Arc::clone(&self.primary_tasks)),
      aof: self.aof.as_ref().map(Arc::clone),
    }
  }
}

impl<F> SessionProviderFace for StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync,
{
  type Consumer = RespSessionConsumer;

  /// libs/server/Providers/GarnetProvider.cs:GetSession
  ///
  /// 模板方法：公共装配流程 + 差异钩子（存储会话创建失败拒绝建连）
  fn get_session(
    &self,
    _wire_format: WireFormat,
    network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    // 向量后台链随首会话惰性拉起且整体受 Vector Set 预览开关门控（对标 C#
    // VectorManager.Initialize 的 !IsEnabled 早退与 StoreWrapper.cs:1054
    // StartReplicaTasks 条件装配；C# 构造器无条件 fire 的三个空转清理协程
    // 在此形态下不拉起——命令面全臂 disabled 无人投递，等价省协程）
    if self.vector_manager.is_enabled() {
      // 量化消费者协程随首个 worker runtime 惰性拉起（C# 宿主启动序列
      // VectorManager.StartQuantizationTasks(QuantizationTaskCount)；
      // 逐 worker 分摊直至配额用尽，避免单 runtime 独扛全部量化负载）
      let quota = self.vector_manager.quantization_task_count.max(1);
      if self.quantization_started.fetch_add(1, Ordering::Relaxed) < quota {
        self.vector_manager.start_quantization_tasks(1);
      }

      // 向量清理三常驻协程随首个 worker runtime 惰性拉起一次并托管（对标 C#
      // VectorManager 构造器启动 RunCleanupTaskAsync/RunRequestCleanupTaskAsync/
      // RunRequestDropTaskAsync；Rust 构造在 compio runtime 外，故与量化协程同处
      // 首会话拉起，spawn 与本调用同 runtime。JoinHandle 收进 CleanupRuntime 托管，
      // 停机时由 dispose_vector_cleanup 收敛释放）。
      self.vector_manager.ensure_cleanup_tasks_started();
    }

    // 发布订阅后台消费任务随首个会话惰性拉起（C# SubscribeBroker.Initialize
    // 首次订阅拉起 StartAsync 后台消费循环的对译；compio spawn 与本调用
    // 同 runtime，wait_pending 事件驱动零空转轮询）
    if let Some(broker) = &self.pubsub
      && !self.pubsub_consume_started.swap(true, Ordering::Relaxed)
    {
      spawn_pubsub_consume_task(Arc::clone(broker));
    }

    self.try_start_commit_task();
    self.try_start_aof_size_limit_task();
    self.try_start_index_auto_grow_task();
    // 启动期过期键删除任务注册段（C# StoreWrapper.Start → StartPrimaryTasks
    // → TryStartExpiredKeyDeletionTask 对译：读 expired-key-deletion-scan-freq
    // 槽位，经唯一调停入口 apply_config_reconcile 走 CONFIG SET 同一分支——
    // 槽位即唯一启停真值源；副本角色不拉起对标 Start 按角色分派，升主恢复点
    // resume_primary_tasks → start_gc 承接重拉）。首个会话惰性触发，幂等。
    if !self.gc_scan_started.swap(true, Ordering::Relaxed) {
      // 紧缩旋钮启动投影（C# DoCompactionAsync 每轮 GetInt/GetEnum 现取
      // runtimeConfig 的同效前置：槽位初值一次性投影进 GcConfig，此后 CONFIG
      // SET 经调停消息增量同步，GcManager 每轮重读快照达成「每轮现取」语义；
      // 副本角色同样投影，升主恢复点即持最新实效值）
      let store = self.store();
      let runtime_config = &self.runtime_config;
      store.update_gc_config(|c| {
        c.compaction_max_segments = runtime_config
          .get_int(ServerConfigType::CompactionMaxSegments)
          .max(0) as usize;
        c.compaction_type = runtime_config
          .get_enum(ServerConfigType::CompactionType)
          .unwrap_or(c.compaction_type);
      });
      if !self.primary_tasks.is_replica() {
        apply_config_reconcile(
          Some(&self.primary_tasks),
          &store,
          self.aof.as_ref(),
          None,
          ConfigReconcile::ExpiredKeyDeletionScan {
            scan_frequency_secs: i64::from(
              self
                .runtime_config
                .get_int(ServerConfigType::ExpiredKeyDeletionScanFreq),
            ),
          },
        );
      }
    }
    // 周期对象收集任务随首个会话惰性拉起（C# StoreWrapper.Start →
    // StartPrimaryTasks 的 ObjectCollectTask 注册段对译：执行域绑定装配
    // 终态引擎与配置后按 expired-object-collection-freq 槽位拉起，副本
    // 角色不拉起，升主恢复点 resume_primary_tasks 重拉）
    {
      let store = self.store();
      self
        .primary_tasks
        .bind_object_collect_env(&store, Some(&self.runtime_config));
      self.primary_tasks.try_start_object_collect_task();
    }

    let mut session = self.store().new_session().ok()?;
    // 副本一致读会话装配（C# RespServerSession.cs:298-300 建会话时按
    // EnableCluster + EnableAOF + MultiLogEnabled + appendOnlyFile 创建
    // consistentReadDBSession 的对位——rust 以 aof 的读一致性管理器在场为门，
    // 管理器仅 multi_log_enabled 拓扑才建）；角色动态性（C# EnforceConsistentRead
    // = enforceConsistentRead && clusterProvider.IsReplica()，StoreWrapper.cs:903-904）
    // 由 ReadSessionState pre 入口 role_gate 判定承接：主库/单机读路径零协议
    // 开销直通，晋升副本即时生效，快慢路径经连接级附着态自动共享
    if let Some(manager) = self
      .aof
      .as_ref()
      .and_then(|aof| aof.read_consistency_manager())
    {
      let state = ReadSessionState::attach(manager, Some(Arc::clone(&self.primary_tasks)));
      session = session.with_read_session_state(Some(Arc::new(state)));
    }
    // 会话指标共享句柄门控（C# StoreWrapper.trackStats =
    // MetricsSamplingFrequency > 0 方置 sessionMetrics，采样关闭会话与存储
    // 两侧均为 null；本处逐连接创建一个，与会话执行域共持同一对象）
    let session_metrics =
      (self.metrics_sampling_frequency_secs > 0).then(|| Arc::new(SessionMetricsHandle::default()));
    // 连接会话置严格上下文态：冷租户/冷库映射未装载时 set_context 拒绝
    // 盲分配，AUTH/HELLO/SELECT 据此挂起磁盘点查装载（冷租户 0 内存常驻条款；
    // 内部/重放/统计会话维持纯内存原语语义不受影响）
    session.set_strict_context(true);
    let checkpoint = CheckpointCtx::new(Arc::clone(&self.database_manager));
    // 集合更新唤醒：慢路径写回后唤醒阻塞观察者（C# itemBroker
    // HandleCollectionUpdate 的存储执行域可达面），move 闭包捕获经纪句柄注入
    let notify_broker = Arc::clone(&self.broker);

    let api = StoreGarnetApi::new(session)
      .with_vector_manager(Arc::clone(&self.vector_manager))
      .with_checkpoint_ctx(checkpoint)
      .with_collection_notify(Some(Arc::new(move |key: &[u8]| {
        notify_broker.handle_collection_update(key);
      }) as CollectionNotify))
      // 会话指标共享句柄（对标 C# GarnetProvider/GarnetServer 装配链
      // trackStats 门控下创建 sessionMetrics 并同传 RespServerSession 与
      // storageSession：rust 以 provider 为单一创建点，执行域与会话共持；
      // 采样关闭 None 与 C# null 会话指标同形）
      .with_session_metrics(session_metrics.clone());
    let mut consumer = (self.decorate)(network_sender_id, api)?;
    consumer.attach_session_metrics(session_metrics);
    consumer.inject_dependencies(self.session_dependencies());
    Some(consumer)
  }

  /// 活跃消费者注册表（网络泵建连/注册、释放/注销的入口）
  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }

  /// AOF 门面（inherent `aof()` 转发；EnableAOF 门控——未点亮为 None）
  fn aof(&self) -> Option<&Arc<GarnetAppendOnlyFile>> {
    self.aof.as_ref()
  }

  /// libs/server/StoreWrapper.cs:WaitForCommitAsync
  ///
  /// WAIT-FOR-COMMIT 档的存储侧等待口：`!EnableAOF` 直返 false（C# 同款
  /// 门），否则经 `IDatabaseManager::wait_for_commit_to_aof_async` 下达
  /// 全部活跃库 AOF 提交落盘（C# `databaseManager.WaitForCommitToAofAsync`
  /// 的接口面调用）
  fn wait_for_commit_async(&self) -> impl Future<Output = bool> {
    let aof_enabled = self.aof.is_some();
    let database_manager = self.database_manager.as_ref();
    async move {
      if !aof_enabled {
        return false;
      }
      IDatabaseManager::wait_for_commit_to_aof_async(database_manager)
        .await
        .is_ok()
    }
  }

  /// 向量清理协程停机收敛（对标 C# `VectorManager.Dispose`）：转发
  /// [`VectorManager::dispose_cleanup`]，由宿主 `stop()` 在停 coordinator 前
  /// 于主线程驱动，确保 worker 运行时仍在排空三条清理通道。
  fn dispose_vector_cleanup(&self) -> bool {
    self.vector_manager.dispose_cleanup()
  }

  /// pubsub 中枢停机收口（对标 C# InternalDispose 的
  /// `subscribeBroker?.Dispose()`）：停机链经本门面单点取用 broker，
  /// `--disable-pubsub` 形态（None）直返 true
  fn dispose_pubsub(&self) -> bool {
    self.pubsub.as_ref().is_none_or(|b| b.dispose())
  }
}

#[cfg(test)]
mod hlog_assembly_tests {
  use wconf::HlogOptions;
  use wkv::StoreConfig as WkvStoreConfig;

  use super::*;

  /// 构造 hlog 覆盖投影（ReadCache 与 reviv 三旋钮默认关闭透传）
  fn proj(
    page_size: Option<usize>,
    memory_size: Option<usize>,
    mutable_fraction: Option<f64>,
  ) -> HlogProjection {
    HlogProjection {
      page_size,
      memory_size,
      mutable_fraction,
      read_cache: false,
      read_cache_memory_size: None,
      reviv: false,
      reviv_fraction: None,
      copy_reads_to_tail: false,
    }
  }

  /// hlog 配置段投影：显式项覆盖自适应基线（对标 C# GetSettings → KVSettings）
  #[test]
  fn hlog_overrides_apply_to_store_config() {
    let mut config = store_config();
    apply_hlog_overrides(
      &mut config,
      proj(Some(8 * 1024 * 1024), Some(256 * 1024 * 1024), Some(0.6)),
    )
    .expect("合法覆盖项");
    assert_eq!(config.page_size, 8 * 1024 * 1024);
    // 256MB / 8MB = 32 页（向下取 2 的幂）
    assert_eq!(config.num_pages, 32);
    assert!((config.mutable_fraction - 0.6).abs() < f64::EPSILON);
    // GC 默认禁用态不受 hlog 覆盖影响（装配不越权开后台任务，对标 C#
    // ExpiredKeyDeletionScanFrequencySecs = -1）
    assert!(!config.gc.enabled);

    // 全 None：保持自适应基线
    let mut config = store_config();
    let baseline_page = config.page_size;
    let baseline_pages = config.num_pages;
    let baseline_fraction = config.mutable_fraction;
    apply_hlog_overrides(&mut config, proj(None, None, None)).expect("空覆盖项");
    assert_eq!(config.page_size, baseline_page);
    assert_eq!(config.num_pages, baseline_pages);
    assert!((config.mutable_fraction - baseline_fraction).abs() < f64::EPSILON);
  }

  /// 非法覆盖项必须被拦截（页容量非 2 的幂）
  #[test]
  fn hlog_overrides_reject_invalid_page_size() {
    let mut config = store_config();
    let err = apply_hlog_overrides(&mut config, proj(Some(4095), None, None));
    assert!(err.is_err(), "非 2 的幂页容量必须被 validate 拦截");
  }

  /// memory_size 预算配不足最小页数时装配期显式拒绝（不静默钳制超配内存：
  /// 100MB 预算 + 16MB 页 = 6 页 < 16 页下限，旧逻辑钳 16 页实配 256MB）
  #[test]
  fn hlog_overrides_reject_memory_size_below_min_pages() {
    let mut config = store_config();
    let err = apply_hlog_overrides(
      &mut config,
      proj(Some(16 * 1024 * 1024), Some(100 * 1024 * 1024), None),
    )
    .expect_err("预算不足最小页数必须拒绝");
    assert!(
      matches!(&err, crate::Error::InvalidArgument(msg)
        if msg.contains("104857600") && msg.contains("16777216")),
      "报错须给出预算与页大小: {err}"
    );
    // 恰好 16 页为合法下界（prev_power_of2(16) = 16）
    let mut config = store_config();
    apply_hlog_overrides(
      &mut config,
      proj(Some(16 * 1024 * 1024), Some(256 * 1024 * 1024), None),
    )
    .expect("恰好最小页数为合法下界");
    assert_eq!(config.num_pages, 16);
  }

  /// NodeArgs 全链投影：hlog 配置段 → 生产 StoreConfig（大值通道默认打通）
  #[test]
  fn node_args_hlog_section_reaches_store_config() {
    let node = NodeArgs {
      hlog: wconf::HlogOptions {
        page_size: Some(16 * 1024 * 1024),
        memory_size: Some(512 * 1024 * 1024),
        mutable_percent: Some(50),
        read_cache: true,
        read_cache_memory_size: Some(256 * 1024 * 1024),
        ..HlogOptions::default()
      },
      ..NodeArgs::default()
    };
    let config = store_config_from_node(&node).expect("合法 hlog 配置段");
    assert_eq!(config.page_size, 16 * 1024 * 1024);
    assert_eq!(config.num_pages, 32);
    assert!((config.mutable_fraction - 0.5).abs() < f64::EPSILON);
    // ReadCache 开关 + 页数推导（256MB / 16MB = 16 页）
    assert!(config.enable_read_cache);
    assert_eq!(config.read_cache_num_pages, 16);
  }

  /// ReadCache 开关关闭时页数保持引擎默认（不参与推导）
  #[test]
  fn node_args_read_cache_disabled_keeps_default_pages() {
    let node = NodeArgs {
      hlog: wconf::HlogOptions {
        read_cache: false,
        read_cache_memory_size: Some(256 * 1024 * 1024),
        ..HlogOptions::default()
      },
      ..NodeArgs::default()
    };
    let config = store_config_from_node(&node).expect("合法 hlog 配置段");
    assert!(!config.enable_read_cache);
    assert_eq!(
      config.read_cache_num_pages,
      WkvStoreConfig::default().read_cache_num_pages
    );
  }
}
