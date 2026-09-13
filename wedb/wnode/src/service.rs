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
  path::Path,
  result,
  sync::{Arc, atomic::Ordering},
};

use thiserror::Error;
use waof::{AofEntryType, WalLog};
use wbase::convert::unix_time_in_milliseconds_from_ticks;
use wbftree::{RangeIndexStub, StorageBackend, StorageBackendType, TreeTuning};
use wconf::{RuntimeServerConfig, RuntimeServerOptions};
use wdatabase::DEFAULT_VERSION_MAP_SIZE;
use wdev::{Device, SegmentedDevice};
use wkv::{
  ObjectRmwListenerFn, RangeIndexCreateListenerFn, RangeIndexDropListenerFn, RangeIndexError,
  RangeIndexListenerFn, StoreSession, TtlPurgeListenerFn, TtlWriteListenerFn, WedbStore,
  WriteListenerFn,
};
use wobject::{
  RespInputFlags,
  itembroker::{collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker},
};
use wresp::RespCommand;
use wtxn::WatchVersionMap;
use wval::{KeyTag, NamespaceDbCodec, TaggedKeyBuf};
use wvector::Callbacks;

use crate::{
  NodeArgs,
  aof::{
    AofProcessor, AofReplayError, ReplayInput, ReplayInputSlice, aof_processor::ReplayTarget,
    garnet_append_only_file::GarnetAppendOnlyFile, garnet_log::RecordShape,
    recover::aof_recover::AofRecover, waof_sublog::single_log_aof,
  },
  resp::{
    RespSessionConsumer,
    garnet_api::StoreGarnetApi,
    objects::{
      collection_item_source::CollectionItemSource, object_store_utils::is_object_envelope,
    },
    rangeindex::range_index_manager_replication::RangeIndexManagerReplication,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
  storage::session::storage_session::StorageSession,
  traits::{SessionProviderFace, WireFormat},
};

#[derive(Error, Debug)]
pub enum Error {
  /// 存储引擎错误（含会话创建失败）
  #[error(transparent)]
  Store(#[from] wkv::Error),
  /// 范围索引操作错误
  #[error(transparent)]
  RangeIndex(#[from] RangeIndexError),
  /// WAL 物理层错误
  #[error(transparent)]
  Wal(#[from] waof::Error),
  /// AOF 重放错误
  #[error(transparent)]
  Aof(#[from] AofReplayError),
}

pub type Result<T> = result::Result<T, Error>;

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
  pub fn new(store: SharedStore<D>, aof: Arc<GarnetAppendOnlyFile>) -> Result<Self>
  where
    D: 'static,
  {
    Self::assemble(store, aof)
  }

  /// 公共装配体：注册全部 AOF 写监听端口并拉起会话。
  fn assemble(store: SharedStore<D>, aof: Arc<GarnetAppendOnlyFile>) -> Result<Self>
  where
    D: 'static,
  {
    // 各端口版本源快照（原子指针捕获消除循环引用与多余开销；对标 C# storeWrapper.store.CurrentVersion）
    let ver_atomic = Arc::clone(store.current_version_atomic());

    // RangeIndex 写监听端口：字段写/删成功后同栈入队 StoreRMW 条目
    // （对标 C# RangeIndexManager.Replication.ReplicateRangeIndexSet/Del）
    let sink = Arc::clone(&aof);
    let ver_ri = Arc::clone(&ver_atomic);
    if !store.set_range_listener(RangeIndexListenerFn::new(
      move |key, field, value, delete| {
        let (cmd, args): (RespCommand, &[&[u8]]) = if delete {
          (RespCommand::Ridel, &[field])
        } else {
          (RespCommand::Riset, &[field, value])
        };
        let input =
          ReplayInputSlice::new(cmd, args).with_flags(RespInputFlags::DETERMINISTIC.bits());
        enqueue_slices(
          &sink,
          AofEntryType::StoreRMW,
          ver_ri.load(Ordering::Acquire),
          &physical_key(0, 0, key),
          &[],
          &input,
        );
      },
    )) {
      log::warn!("RangeIndex 写监听端口重复注册");
    }

    // RangeIndex 创建监听端口：索引创建并完成存根落盘后入队 RICREATE
    // StoreRMW 条目（存根字节随 parseState 携带，对标 C# RI.CREATE RMW 帧）
    let sink = Arc::clone(&aof);
    let ver_ri_create = Arc::clone(&ver_atomic);
    if !store.set_range_create_listener(RangeIndexCreateListenerFn::new(
      move |key, backend, tuning| {
        let stub = RangeIndexStub::from_tuning(0, &tuning, storage_backend_type(backend));
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
          ver_ri_create.load(Ordering::Acquire),
          &physical_key(0, 0, key),
          &[],
          &input,
        );
      },
    )) {
      log::warn!("RangeIndex 创建监听端口重复注册");
    }

    // RangeIndex 删除监听端口：索引被显式删除并完成整树清理后入队
    // StoreDelete 条目（对标 C# 主存 delete → WriteLogDelete）
    let sink = Arc::clone(&aof);
    let ver_ri_drop = Arc::clone(&ver_atomic);
    if !store.set_range_drop_listener(RangeIndexDropListenerFn::new(move |key| {
      enqueue_raw(
        &sink,
        AofEntryType::StoreDelete,
        ver_ri_drop.load(Ordering::Acquire),
        &physical_key(0, 0, key),
        &[],
        &EMPTY_REPLAY_INPUT_BYTES,
      );
    })) {
      log::warn!("RangeIndex 删除监听端口重复注册");
    }

    // 普通 KV 写监听端口：仅对普通字符串键（KeyTag::String）同栈入队
    // StoreUpsert/StoreDelete 条目，key 为 wkv 物理键（自带 ns/db 域）。
    // 忽略内部 Meta 与 TTL 旁路记录，并按命令级路线排除集合对象信封
    // （Hash/Set/ZSet/List 走增量 ObjectStoreRMW）。
    // TTL 旁路记录（KeyTag::Ttl）不经本端口：随键 TTL 由 TTL 写端口单独
    // 入队 PEXPIREAT/PERSIST RMW 条目（见下方 ttl_write_listener）。
    let sink = Arc::clone(&aof);
    let ver_write = Arc::clone(&ver_atomic);
    if !store.set_write_listener(WriteListenerFn::new(move |key, val, tombstone| {
      if NamespaceDbCodec::decode_tag(key) != Some(KeyTag::String) {
        return;
      }
      if !tombstone && is_object_envelope(val) {
        return;
      }
      let op = if tombstone {
        AofEntryType::StoreDelete
      } else {
        AofEntryType::StoreUpsert
      };
      enqueue_raw(
        &sink,
        op,
        ver_write.load(Ordering::Acquire),
        key,
        val,
        &EMPTY_REPLAY_INPUT_BYTES,
      );
    })) {
      log::warn!("普通 KV 写监听端口重复注册");
    }

    // 对象 RMW 增量日志监听端口：集合操作成功修改数据后同栈入队
    // ObjectStoreRMW 增量条目（对标 C# ObjectSessionFunctions.WriteLogRMW，
    // obj_type/op_code 显式判别 + Deterministic 标志；notif.key 为物理键）
    let sink = Arc::clone(&aof);
    let ver_rmw = Arc::clone(&ver_atomic);
    if !store.set_object_rmw_listener(ObjectRmwListenerFn::new(move |notif| {
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
        ver_rmw.load(Ordering::Acquire),
        notif.key,
        &[],
        &input,
      );
    })) {
      log::warn!("对象 RMW 监听端口重复注册");
    }

    // TTL 写监听端口：随键 TTL 旁路记录（SET k v EX / EXPIRE / PERSIST）写入或
    // 删除成功后同栈入队 StoreRMW 条目——设值入队 PEXPIREAT（arg1 = 绝对
    // Unix 毫秒）、清除入队 PERSIST（对标 C# PEXPIREAT/PERSIST 命令层 RMW
    // 形态）。C# 的 SET EX 为单条 StoreUpsert 随行 expiration（StringInput
    // arg1），rust 侧随键 TTL 为旁路记录（无命令上下文经物理写端口），以
    // 「StoreUpsert 值条目 + PEXPIREAT RMW 条目」两跳等价闭环，重放端
    // PEXPIREAT/PERSIST 分支已有支持。
    let sink = Arc::clone(&aof);
    let ver_ttl = Arc::clone(&ver_atomic);
    if !store.set_ttl_write_listener(TtlWriteListenerFn::new(
      move |ns: u64, db: u64, key: &[u8], expire_at_ticks: Option<i64>| {
        let (cmd, arg1) = match expire_at_ticks {
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
          ver_ttl.load(Ordering::Acquire),
          &physical_key(ns, db, key),
          &[],
          &input,
        );
      },
    )) {
      log::warn!("TTL 写监听端口重复注册");
    }

    // TTL 过期 purge 端口：端口在场即令 wkv 侧 purge 链抑制物理墓碑镜像，
    // 改为入队单条 DELIFEXPIM StoreRMW 条目（对标 C# ExpiredKeyDeletionTask
    // 的 DELIFEXPIM RMW + Expired|Deterministic 标志：主端已判定到期，
    // 重放端确定性执行统一 DEL；expire_at_ticks 随 arg1 携带供审计）。
    let sink = Arc::clone(&aof);
    let ver_purge = ver_atomic;
    if !store.set_ttl_purge_listener(TtlPurgeListenerFn::new(
      move |ns: u64, db: u64, key: &[u8], expire_at_ticks: i64| {
        let empty_args: [&[u8]; 0] = [];
        let input = ReplayInputSlice::new(RespCommand::Delifexpim, &empty_args)
          .with_flags((RespInputFlags::DETERMINISTIC | RespInputFlags::EXPIRED).bits())
          .with_args_num(expire_at_ticks, 0, 0);
        enqueue_slices(
          &sink,
          AofEntryType::StoreRMW,
          ver_purge.load(Ordering::Acquire),
          &physical_key(ns, db, key),
          &[],
          &input,
        );
      },
    )) {
      log::warn!("TTL 过期 purge 端口重复注册");
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
    storage_backend: StorageBackend,
    tuning: TreeTuning,
  ) -> Result<()> {
    self
      .session
      .range_index_create(key, storage_backend, tuning)
      .await?;
    Ok(())
  }

  /// 设置范围索引字段并预写 WAL
  pub async fn ri_set(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<()> {
    self.session.range_index_set(key, field, value).await?;
    Ok(())
  }

  /// 删除范围索引字段并预写 WAL
  ///
  /// 与 Garnet `RangeIndexDel`"字段不存在则不写 AOF"的刻意差异：
  /// bf-tree 墓碑删除不区分字段是否存在（`BfTreeDeleteResult` 无
  /// NotFound 语义），故删除恒落日志，回放端按幂等删除处理
  pub async fn ri_del(&self, key: &[u8], field: &[u8]) -> Result<bool> {
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
  ) -> Result<u64> {
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
  ) -> Result<Self> {
    Self::with_node_args(&NodeArgs::default(), store, wal)
  }

  /// 基于通用 NodeArgs 节点参数与存储引擎组装单机服务
  pub fn with_node_args(
    args: &NodeArgs,
    store: SharedStore<SegmentedDevice>,
    wal: Arc<WalLog<SegmentedDevice>>,
  ) -> Result<Self> {
    let mut opts = RuntimeServerOptions::default();
    if let Some(commit_ms) = args.aof_commit_ms {
      opts.commit_frequency_ms = commit_ms as i32;
    }
    let aof = single_log_aof(wal, &opts);
    Self::assemble(store, aof)
  }
}

/// StorageBackend → 存根后端类型（wbftree StorageBackendType 判别值）
fn storage_backend_type(backend: &StorageBackend) -> StorageBackendType {
  match backend {
    StorageBackend::Std => StorageBackendType::Disk,
    StorageBackend::Memory => StorageBackendType::Memory,
  }
}

pub const DEFAULT_DATA_FILE: &str = "wedb.data";

/// 单机/集群统一节点装配句柄：存储引擎 + 集合项经纪 + 向量集合管理器
pub type DefaultNodeHandles = (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  Arc<VectorManager>,
);

/// 打开单文件存储设备，装配存储引擎、共享经纪与向量集合管理器
///
/// 单机/集群功能一致的统一节点装配三件套（存储域初始化 + 经纪 +
/// 向量管理器，对标 C# GarnetServer InitializeServer）
pub fn open_node(data_path: impl AsRef<Path>) -> crate::Result<DefaultNodeHandles> {
  let device = SegmentedDevice::single_file(data_path.as_ref())
    .map_err(|e| crate::Error::Custom(format!("打开存储设备失败: {e}")))?;
  let mut config = wkv::StoreConfig::auto();
  config.gc.enabled = true;
  let store = WedbStore::open_shared(config, Arc::new(device))
    .map_err(|e| crate::Error::Custom(format!("打开存储引擎失败: {e}")))?;

  let broker_session = store
    .new_session()
    .map_err(|e| crate::Error::Custom(format!("创建经纪取件会话失败: {e}")))?;
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(broker_session),
  ))));

  let vector_session = Arc::new(
    store
      .new_session()
      .map_err(|e| crate::Error::Custom(format!("创建向量存储会话失败: {e}")))?,
  );
  let vector_callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(vector_session)));
  let vector_manager = Arc::new(VectorManager::new(
    VectorManagerOptions::default(),
    vector_callbacks,
  ));

  Ok((store, broker, vector_manager))
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
  /// 服务器级运行时配置（CONFIG GET/SET 与 OBJECT_SCAN_COUNT_LIMIT 热更源）
  pub runtime_config: Arc<RuntimeServerConfig>,
  /// 差异钩子：按发送端装配会话消费者（None = 拒绝建连）
  decorate: F,
}

impl<F> StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>,
{
  /// 统一装配体：打开单文件存储引擎、共享经纪与向量集合管理器后构造基座
  ///（run 闭包一行装配；store 句柄经公开字段供集群 set_store 下达）
  pub fn open(data_path: impl AsRef<Path>, decorate: F) -> crate::Result<Self> {
    let (store, broker, vector_manager) = open_node(data_path)?;
    Ok(Self {
      store,
      broker,
      vector_manager,
      runtime_config: RuntimeServerConfig::shared_default(),
      decorate,
    })
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
    let session = self.store.new_session().ok()?;
    let api = StoreGarnetApi::new(session).with_vector_manager(Arc::clone(&self.vector_manager));
    let mut consumer = (self.decorate)(network_sender_id, api)?;
    consumer.set_item_broker(self.broker.clone());
    consumer.set_runtime_config(self.runtime_config.clone());
    Some(consumer)
  }
}
