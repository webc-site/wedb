//! wnode 集成测试存储环境夹具单源（对标 wedb/tests/common 收口先例）
//!
//! 收口 wnode/tests 各集成测试逐字同形的装配与泵面样板：
//! - 存储环境族：无钩子单机环境、WATCH 版本表环境、存储 + WAL 对、
//!   NodeService 节点、单机 AOF 节点全量装配；
//! - 会话 / 泵族：指定 API 会话、环境便捷泵、会话帧泵；
//! - 向量族：向量管理器生产同形态装配、内存 AOF；
//! - 断言 / 解析族：整数回执、bulk 帧组帧解析、cmdstat 行断言、应答字节断言。
//!
//! 差异点（tag、页容缓存、WAL 配置、断言文案）一律参数暴露，禁全局状态。

use std::{
  fs::create_dir_all,
  future::{poll_fn, ready},
  mem::{forget, take},
  path::{Path, PathBuf},
  pin::pin,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, Ordering},
  },
  task::Poll,
  time::Duration,
};

use compio::{buf::BufResult, io::AsyncRead, net::TcpStream, runtime::Runtime};
use tempfile::{TempDir, tempdir};
use waof::{WalConfig, WalLog, WalRecord};
use wbase::{align::DEFAULT_SECTOR_SIZE, crc64::hash};
use wcol::types::member_ttl::encode_member;
use wconf::{DEFAULT_RESP_VERSION, RuntimeServerOptions};
use wdev::SegmentedDevice;
use wkv::{SessionLocking, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wmetric::GarnetServerMonitor;
use wnode::{
  GarnetAppendOnlyFile, MessageConsumerFace, RespSessionConsumer,
  aof::{garnet_log::GarnetLog, waof_sublog::single_log_aof},
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{
    TtlResume,
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    slow_path::SlowWait,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_locking::CreateIndexParams,
      vector_manager_replication::VectorAofSink,
      vector_store_callbacks::{
        ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
      },
    },
  },
  servers::consumer_registry::ConsumerRegistry,
  service::{NodeService, StorageSessionProvider},
  storage::session::storage_session::{StorageSession, version_map_watch_hook},
};
use wresp::{command::RespCommand, length::try_write_length};
use wtest_base::{resp_frame, test_store_config};
use wtxn::{
  DEFAULT_VERSION_MAP_SIZE, TransactionManager, TxnKeyEntryComparison, TxnLockTable,
  WatchVersionMap,
};
use wval::{GarnetObjectType, SessionPrefixBuf};
use wvector::{Callbacks, VectorDistanceMetricType, VectorQuantType};

use super::{
  SessionFactory, TestEnv, TestStore, auto_exec, consumer_on, drain_output, drive_pending_parks,
  drive_pending_parks_consumer, feed, pump, pump_slow, session_factory, test_sublogs,
};

// ---------------------------------------------------------------------------
// 单机存储环境族
// ---------------------------------------------------------------------------

/// 指定目录与配置的存储装配单源（单文件设备即建；对标各册定制
/// open_env/env_with_* 构建器中的 device+store 两连收口）
#[must_use]
pub fn store_open(dir: impl AsRef<Path>, tag: &str, config: StoreConfig) -> Arc<TestStore> {
  let device = Arc::new(SegmentedDevice::single_file(dir.as_ref().join(tag)).unwrap());
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// 批量 bulk 数组帧单源（LRANGE 精确对照等批量体；对标持窗竞速族 3 册同形
/// `bulk_array` 收口）
#[must_use]
pub fn bulk_array(items: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", items.len()).into_bytes();
  for item in items {
    out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
    out.extend_from_slice(item);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 指定 [`StoreConfig`] 的单机环境装配单源：tempdir + 单文件设备 + 存储门面
/// API + Runtime（对标 wnode/tests 各册私义 `env(tag)` / `degrade_env(tag)`
/// 元组装配收口；配置即差异点，由各册调用点显式给定）
#[must_use]
fn env_with_config(
  tag: &str,
  config: StoreConfig,
) -> (Runtime, GarnetApi, Arc<TestStore>, TempDir) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap())).into();
  (Runtime::new().unwrap(), api, store, dir)
}

/// 无钩子元组环境（页容 2048 / 1MB 缓存 / 16 盘 / 0.5 预算；对标
/// collection_adaptive_tiering / set_cond_tiered_matrix / tiered_cmds_align /
/// tiered_hset_mixed_large_value / tiered_list_lrange_window /
/// tiered_stub_heal_write / zadd_options_error_order / zset_r15_parity /
/// geo_store_tiered_retire 9 册同形 `open_env` 收口；[`plain_env`] 的元组形态）
#[must_use]
pub fn open_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, TempDir) {
  env_with_config(tag, StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap())
}

/// 降级压力环境：16KB × 4 页小环形日志（持续写入回绕复用槽位必遇
/// PageNotReady，即 RMW 写回降级生产形态的确定性微缩；对标 bitfield /
/// etag_conditional / nx_conditional / set_keepttl_resume 4 册同形
/// `degrade_env` 收口）
#[must_use]
pub fn degrade_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, TempDir) {
  env_with_config(tag, StoreConfig::new(1024, 16 * 1024, 4, 0.5).unwrap())
}

/// 无钩子单机环境（页容 2048 / 1MB 缓存 / 16 盘 / 0.5 预算，[`TestEnv`] 形；
/// 对标 tiered_list_collect_expiry_exempt / tiered_list_export_member_codec /
/// tiered_promote_demote_ttl 三册同形 `fn env(tag)` 收口）
#[must_use]
pub fn plain_env(tag: &str) -> TestEnv {
  let (rt, api, store, _dir) =
    env_with_config(tag, StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap());
  TestEnv {
    rt,
    store,
    api,
    _dir,
  }
}

/// WATCH 族测试环境（字段直曝，对标 delempty/tiered_background_demote/
/// tiered_watch_fence 族同形 `Env` 副本：引擎级写面钩子挂共享版本表 +
/// 事务锁表；不用锁表 / 版本表的册忽略对应字段即可）
pub struct WatchEnv {
  pub rt: Runtime,
  pub store: Arc<TestStore>,
  pub map: Arc<WatchVersionMap>,
  pub lock_table: TxnLockTable,
  pub api: GarnetApi,
  pub _dir: TempDir,
}

/// WATCH 族环境装配单源：tempdir + 页容 2048 / 指定缓存 / 16 盘 / 0.5 预算 +
/// `WatchVersionMap::new(1 << 10)` 写面钩子首挂断言（断言文案逐字保留；
/// 对标 9 册同形 `fn env(tag)` 收口）
#[must_use]
pub fn watch_env_sized(tag: &str, page_cache: usize) -> WatchEnv {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, page_cache, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap())).into();
  WatchEnv {
    rt: Runtime::new().unwrap(),
    store,
    map,
    lock_table: TxnLockTable::new(),
    api,
    _dir: dir,
  }
}

/// 默认缓存（1MB）的 WATCH 族环境
#[must_use]
pub fn watch_env(tag: &str) -> WatchEnv {
  watch_env_sized(tag, 1024 * 1024)
}

/// WATCH 族会话装配：默认选项会话挂环境 API（id=1）
#[must_use]
pub fn watch_session(env: &WatchEnv) -> RespServerSession {
  session_on(&env.api)
}

// ---------------------------------------------------------------------------
// 存储节点族（存储 + WAL / NodeService / 单机 AOF 节点）
// ---------------------------------------------------------------------------

/// 存储 + WAL 对（无 NodeService 装配；checkpoint / 恢复域用）
pub struct StoreWalPair {
  pub _dir: TempDir,
  pub store: Arc<TestStore>,
  pub wal: Arc<WalLog<SegmentedDevice>>,
}

/// tempdir + 存储设备 + WAL 设备装配单源：`range_index_dir` 恒指临时目录，
/// `page_cache` 为引擎缓存字节数；`wal_segments` = `Some(段长)` 走段式 WAL
/// 设备（段长 + DEFAULT_SECTOR_SIZE），`None` 走单文件设备（对标
/// checkpoint 系 / aof_domain / service 各册同形 `open_node` 收口）
pub fn open_store_wal(
  tag: &str,
  page_cache: usize,
  wal_config: WalConfig,
  wal_segments: Option<u64>,
) -> wnode::Result<StoreWalPair> {
  let dir = tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let wal_device = match wal_segments {
    Some(seg) => Arc::new(SegmentedDevice::new(
      dir.path().join(format!("{tag}.wal")),
      seg,
      DEFAULT_SECTOR_SIZE,
    )?),
    None => Arc::new(SegmentedDevice::single_file(
      dir.path().join(format!("{tag}.wal")),
    )?),
  };
  let mut config = StoreConfig::new(2048, page_cache, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, wal_config)?);
  Ok(StoreWalPair {
    _dir: dir,
    store,
    wal,
  })
}

/// 主/副本各一套：存储 + WAL + NodeService（重放经 NodeService 统一闭环；
/// 对标 ttl_ticks / tiered 重放族 10 册同形私义 `open_node` 收口：1MB 缓存 +
/// 单文件 WAL 默认配置 + `NodeService::with_wal`）
pub struct StoreWalNode {
  pub store: Arc<TestStore>,
  pub service: NodeService<SegmentedDevice>,
  pub wal: Arc<WalLog<SegmentedDevice>>,
  pub _dir: TempDir,
}

/// 独立存储节点（db + wal 各一文件，页容 2048 / 1MB 缓存 / 16 盘 / 0.5 预算，
/// range_index 指临时目录）
pub fn open_node(tag: &str) -> wnode::Result<StoreWalNode> {
  let StoreWalPair { _dir, store, wal } =
    open_store_wal(tag, 1024 * 1024, WalConfig::default(), None)?;
  let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;
  Ok(StoreWalNode {
    store,
    service,
    wal,
    _dir,
  })
}

/// 单机 AOF 节点全量装配（设备 + 存储 + 段式 WAL + single_log_aof +
/// NodeService + 检查点目录；对标 aof_replay_domain / swapdb 系 4 册同形
/// 私义 `open_node` 收口）
pub struct AofNode {
  pub _dir: TempDir,
  pub device: Arc<SegmentedDevice>,
  pub store: Arc<TestStore>,
  pub aof: Arc<GarnetAppendOnlyFile>,
  pub cp_dir: PathBuf,
  pub service: NodeService<SegmentedDevice>,
}

/// 起一套临时目录内的单机 AOF 节点（存储 `test_store_config` + 64KB 段 WAL
/// 1MB 环 + `ckpt/` 检查点目录）
pub fn open_aof_node(tag: &str) -> wnode::Result<AofNode> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::new(
    dir.path().join(format!("{tag}.wal")),
    64 * 1024,
    DEFAULT_SECTOR_SIZE,
  )?);
  let cp_dir = dir.path().join("ckpt");
  create_dir_all(&cp_dir)?;
  let store = Arc::new(WedbStore::open(test_store_config(), Arc::clone(&device))?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::new(1 << 20))?);
  let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default())
    .expect("装配 single_log_aof");
  let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
  Ok(AofNode {
    _dir: dir,
    device,
    store,
    aof,
    cp_dir,
    service,
  })
}

// ---------------------------------------------------------------------------
// 单机 AOF 服务器 provider 装配族
// ---------------------------------------------------------------------------

/// 单机 AOF provider 装配单源：`test_store_config` + 默认运行时选项 + 单连接
/// 会话工厂（对标 wnode/tests 各 AOF 服务器册 42 处同形内联装配收口）
#[must_use]
pub fn open_aof_provider(data_path: &Path) -> Arc<StorageSessionProvider<SessionFactory>> {
  open_aof_provider_opts(data_path, RuntimeServerOptions::default())
}

/// 指定运行时选项的 provider 装配变体（周期提交窗等差异点经 `options` 透传）
#[must_use]
pub fn open_aof_provider_opts(
  data_path: &Path,
  options: RuntimeServerOptions,
) -> Arc<StorageSessionProvider<SessionFactory>> {
  Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      data_path,
      None,
      options,
      session_factory as SessionFactory,
    )
    .expect("open with aof"),
  )
}

/// 停机重开恢复 provider 装配单源：`test_store_config` + 默认运行时选项 +
/// 不重放尾帧 + 单连接会话工厂（对标各册 17 处同形
/// `open_recovered_with_config_and_aof` 内联装配收口）
///
/// 调用方在异步域内 `rt.block_on(open_recovered_provider(&data_path))` 承接
#[must_use]
pub async fn open_recovered_provider(
  data_path: &Path,
) -> Arc<StorageSessionProvider<SessionFactory>> {
  Arc::new(
    StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      data_path,
      None,
      RuntimeServerOptions::default(),
      false,
      session_factory as SessionFactory,
    )
    .await
    .expect("open recovered"),
  )
}

// ---------------------------------------------------------------------------
// 会话 / 泵族
// ---------------------------------------------------------------------------

/// 指定 API 的单机会话（id=1、默认选项；对标 38 册同形
/// `fn session_with(api: &GarnetApi)` 收口）
#[must_use]
pub fn session_on(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 热键同步泵单源：无降级形态的命令必须同步闭环而非挂起（非空断言；断言
/// 文案经 `no_degrade_tmpl` 逐册保留，`{}` 占位符替换为命令名；对标热键族
/// 4 册同形 `sync_exec` 收口）
pub fn sync_exec(
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
  no_degrade_tmpl: &str,
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  let out = take(&mut s.output);
  let msg = no_degrade_tmpl.replace("{}", cmd.as_ref());
  assert!(!out.is_empty(), "{msg}");
  out
}

/// 降级泵单源：快路径应答直取并回 `(应答, 是否同步闭环)`，零应答必须挂起
/// SlowWait（`expect("降级必须挂起 SlowWait")`）后 block_on 闭环（对标降级
/// 形态族 5 册同形 `exec` 收口；`(Vec<u8>, bool)` 形）
pub fn exec_degraded(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> (Vec<u8>, bool) {
  s.output.clear();
  api.exec(s, cmd, args);
  if s.output.is_empty() {
    let slow = s.take_slow_wait().expect("降级必须挂起 SlowWait");
    (rt.block_on(slow.resolve()), true)
  } else {
    (take(&mut s.output), false)
  }
}

/// 同步段慢臂并轨泵单源：同步段既有应答与慢臂应答按序拼接（`SET k v
/// KEEPTTL GET` 形态：sync 段可能已有输出），回 `(应答, 是否走了慢臂)`；
/// 对标 KEEPTTL 降级族同形 `exec` 收口
pub fn exec_slow_concat(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> (Vec<u8>, bool) {
  s.output.clear();
  api.exec(s, cmd, args);
  match s.take_slow_wait() {
    Some(slow) => {
      let mut out = take(&mut s.output);
      out.extend(rt.block_on(slow.resolve()));
      (out, true)
    }
    None => (take(&mut s.output), false),
  }
}

/// 冷键降级泵单源：快路径必须空应答挂起 SlowWait（绝不同步误答，双断言
/// 文案逐字保留），挂起体 block_on 闭环取慢路径应答（对标冷键降级族 4 册
/// 同形 `cold_exec` 收口）
pub fn cold_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  assert!(
    s.output.is_empty(),
    "冷键 {cmd} 必须降级挂起而非同步应答：{:?}",
    String::from_utf8_lossy(&s.output)
  );
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("冷键 {cmd} 降级未挂起 SlowWait"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// [`auto_exec`] 的异步原生孪生：慢路径 `resolve().await` 直接续跑，不嵌套
/// block_on（并发任务依赖同一 worker 轮转调度；对标并发计数族 2 册同形
/// `exec_cmd` 收口）
pub async fn auto_exec_async(
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = slow.resolve().await;
  s.output.clear();
  out
}

/// [`TestEnv`] 便捷泵：[`auto_exec`] 的环境入参形态（分层族 8 册同形私义
/// `fn auto_exec(env: &TestEnv, ...)` 包装收口）
pub fn auto_exec_env(
  env: &TestEnv,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 会话帧泵：喂一帧断言整帧消费完毕后冲出应答（慢路径不在射程；对标
/// resp_arg_frame_violation / resp_pubsub / server_monitor 3 册同形 `feed` 收口）
pub fn feed_session(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let consumed = s.try_consume_messages();
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  drain_output(s)
}

/// 会话帧泵停车臂变体：消费断言后经 [`drive_pending_parks`] 同步闭环
/// ACL/AUTH 停车臂，应答并回（对标 txn 族 6 册同形 `feed` 收口）
pub fn feed_session_parked(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  let consumed = s.try_consume_messages();
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  s.take_output_into(&mut resp_buf, true);
  Runtime::new()
    .unwrap()
    .block_on(drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  drain_output(s)
}

/// 异步域单命令往返（融合轮询注入臂专用：可 poll 的 future，绝不起嵌套
/// block_on；对标 rmw 持窗竞速族 7 册同形 `deliver` 收口）
pub async fn deliver(store: Arc<TestStore>, args: Vec<Vec<u8>>) -> Vec<u8> {
  let slices: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
  let mut c = consumer_on(&store);
  let mut out = feed(&mut c, &slices);
  if let Some(slow) = c.take_slow_wait() {
    out.extend_from_slice(&slow.resolve().await);
  }
  out
}

/// 窗口在手判据单源：同键第二窗取闩失败（对面 DEL/SET 按物理记录键取闩、
/// 根本不取本窗，判据只反映 victim 臂持窗事实；对标持窗竞速族 7 册同形
/// `rmw_window_held` 收口）
#[must_use]
pub fn rmw_window_held(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("判据会话");
  let batch = sess.enter_batch();
  batch.try_rmw_window(key).is_none()
}

/// 交叠驱动计划：victim 慢路径命令与对面注入命令的定参一处收敛
///
/// - `intruder_kind`：对面命令称谓（断言文案），如 `"ZADD"` / `"RMW 命令"`
/// - `window_stage`：持窗串行化失效断言尾注，如 `"覆写窗串行化失效"`
/// - `hold_stage`：持窗中断断言的臂程描述，如 `"域快照-落笔"` / `"装载-写回"`
/// - `block_intruder`：true = 对面为同窗 RMW 命令（至多两轮 poll 必被挡）；
///   false = 对面为 DEL/SET（独立桶闩），须在 victim 闭环前完整 ACK
pub struct DrivePlan<'a> {
  /// victim 慢路径命令实参
  pub victim_args: &'a [&'a [u8]],
  /// victim 目标键（持窗判据探测键）
  pub victim_key: &'a [u8],
  /// 对面注入命令实参
  pub intruder: Vec<Vec<u8>>,
  /// 对面命令称谓（断言文案）
  pub intruder_kind: &'a str,
  /// 持窗串行化失效断言尾注
  pub window_stage: &'a str,
  /// 持窗中断断言的臂程描述
  pub hold_stage: &'a str,
  /// true = 对面同窗 RMW 命令（必挡）；false = 对面 DEL/SET（窗内完整 ACK）
  pub block_intruder: bool,
}

impl DrivePlan<'_> {
  /// 装载臂定参形：对面为同窗 RMW 命令族（称谓 / 尾注 / 臂程文案单源）
  pub fn load_arm<'a>(
    victim_args: &'a [&'a [u8]],
    victim_key: &'a [u8],
    intruder: Vec<Vec<u8>>,
    block_intruder: bool,
  ) -> DrivePlan<'a> {
    DrivePlan {
      victim_args,
      victim_key,
      intruder,
      intruder_kind: "RMW 命令",
      window_stage: "窗口串行化失效",
      hold_stage: "装载-写回",
      block_intruder,
    }
  }

  /// STORE 覆写臂定参形：对面域快照-落笔全程持窗，注入必挡（`kind` 为对面称谓）
  pub fn store_arm<'a>(
    intruder_kind: &'a str,
    victim_args: &'a [&'a [u8]],
    victim_key: &'a [u8],
    intruder: Vec<Vec<u8>>,
  ) -> DrivePlan<'a> {
    DrivePlan {
      victim_args,
      victim_key,
      intruder,
      intruder_kind,
      window_stage: "覆写窗串行化失效",
      hold_stage: "域快照-落笔",
      block_intruder: true,
    }
  }
}

/// victim 慢路径臂挂起 → 持窗判据成立 → 对面命令注入 → victim 闭环 →
/// 对面补齐（对标持窗竞速族 4 册同形 `drive_interleaved` 收口，定参经
/// [`DrivePlan`] 收敛）
pub fn drive_interleaved(
  rt: &Runtime,
  store: &Arc<TestStore>,
  plan: DrivePlan<'_>,
) -> (bool, Vec<u8>, Vec<u8>) {
  let DrivePlan {
    victim_args,
    victim_key,
    intruder,
    intruder_kind,
    window_stage,
    hold_stage,
    block_intruder,
  } = plan;
  let mut c = consumer_on(store);
  let sync_out = feed(&mut c, victim_args);
  assert!(
    sync_out.is_empty(),
    "冷键 victim 应挂慢路径，实际同步段直出 {:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("冷键装载必挂慢路径");
  let mut rmw = pin!(slow.resolve());
  let inject_store = Arc::clone(store);
  let mut inject = pin!(deliver(inject_store, intruder));
  let probe_store = Arc::clone(store);
  let probe_key = victim_key.to_vec();
  let mut held = false;
  let mut blocked_polls = 0usize;
  let mut inject_out: Option<Vec<u8>> = None;
  let mut victim_early: Option<Vec<u8>> = None;
  let victim_reply = rt.block_on(poll_fn(|cx| {
    loop {
      if !held {
        // 交叠判据未成立的轮次只推 victim：判据成立前对面命令绝不放行；
        // 未成立即让出（禁忙等活锁）——开窗点在目标键落笔之前
        match rmw.as_mut().poll(cx) {
          Poll::Ready(out) => {
            victim_early = Some(out);
            return Poll::Ready(Vec::new());
          }
          Poll::Pending => {
            if rmw_window_held(&probe_store, &probe_key) {
              held = true;
            } else {
              return Poll::Pending;
            }
          }
        }
      }
      if inject_out.is_none() {
        if block_intruder {
          if blocked_polls < 2 {
            blocked_polls += 1;
            assert!(
              matches!(inject.as_mut().poll(cx), Poll::Pending),
              "对面 {intruder_kind} 竟在 victim 持窗期内落地：{window_stage}"
            );
            assert!(
              rmw_window_held(&probe_store, &probe_key),
              "victim 持窗中断：窗口应在{hold_stage}全程在手"
            );
            continue;
          }
          // victim 闭环后再补齐对面（让核等待臂对位，不与其抢预算）
          return match rmw.as_mut().poll(cx) {
            Poll::Ready(out) => Poll::Ready(out),
            Poll::Pending => Poll::Pending,
          };
        }
        // DEL/SET 形：取独立桶闩，victim 持窗期内必须完整 ACK
        match inject.as_mut().poll(cx) {
          Poll::Ready(out) => inject_out = Some(out),
          Poll::Pending => return Poll::Pending,
        }
      }
      return match rmw.as_mut().poll(cx) {
        Poll::Ready(out) => Poll::Ready(out),
        Poll::Pending => Poll::Pending,
      };
    }
  }));
  let victim_reply = match victim_early {
    Some(out) => {
      // victim 在判据成立前即闭环：交叠不成立，应答作废由调用方炸出
      let _ = victim_reply;
      out
    }
    None => victim_reply,
  };
  let intruder_reply = match inject_out {
    Some(out) => out,
    None => rt.block_on(inject.as_mut()),
  };
  (held, victim_reply, intruder_reply)
}

/// 慢臂直驱单源（与降级快照投递同径，不经会话快路径；RESP2 + Basic 锁；
/// 对标读面记账族 6 册同形 `slow_direct` 收口）
pub fn slow_direct(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  rt.block_on(async {
    SlowWait::for_command(
      api,
      cmd,
      snapshot,
      DEFAULT_RESP_VERSION,
      SessionLocking::Basic,
    )
    .resolve()
    .await
  })
}

/// 慢臂直驱续跑标记变体：快照尾参恒带续跑标记（exec 契约；对标 TTL 续跑族
/// 3 册同形 `slow_direct` 收口）
pub fn slow_direct_resume(
  rt: &Runtime,
  api: &GarnetApi,
  cmd: RespCommand,
  args: &[&[u8]],
  resume: TtlResume,
) -> Vec<u8> {
  let mut snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  snapshot.push(resume.tail_bytes());
  rt.block_on(
    SlowWait::for_command(
      api,
      cmd,
      snapshot,
      DEFAULT_RESP_VERSION,
      SessionLocking::Basic,
    )
    .resolve(),
  )
}

// ---------------------------------------------------------------------------
// 向量族
// ---------------------------------------------------------------------------

/// 向量管理器测试装配单源：生产同形态——回调无状态（会话按执行域绑定），
/// 后台处理项经专用会话工厂自备会话，直调臂用
/// [`VectorManager::bind_dedicated_session`]（对标 wnode/tests 各向量册 15 处
/// 同形私义 `vector_manager(_of)` 收口）
#[must_use]
pub fn vector_manager_of(store: &Arc<TestStore>) -> Arc<VectorManager> {
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new())),
  ));
  let s = Arc::clone(store);
  vm.attach_dedicated_session_factory(Arc::new(move || {
    s.new_session()
      .ok()
      .map(OwnedActiveVectorSession::new)
      .map(ActiveDedicatedVectorSession::from_bound)
  }));
  vm
}

/// 内存态单日志 AOF 装配单源（`test_sublogs` 单后端；对标 aof_store_rmw_replay
/// 形态——重放面与磁盘拓扑同路径；对标 16 册同形私义 `memory_aof` 收口）
#[must_use]
pub fn memory_aof(tag: &str) -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = test_sublogs(tag, 1);
          backends
        },
        None,
      )
      .expect("构造 GarnetLog"),
    ),
    &options,
    None,
  ))
}

/// 合法 RESTORE 载荷单源（类型 0x00 + 长度前缀 + 值 + rdb 版本 11 + crc64，
/// crc 含类型字节起算的 rust 口径；对标 restore 族 5 册同形 `restore_payload`
/// 收口）
#[must_use]
pub fn restore_payload(val: &[u8]) -> Vec<u8> {
  let mut encoded_len = [0u8; 5];
  let written = try_write_length(val.len() as u32, &mut encoded_len).unwrap();
  let mut payload = Vec::with_capacity(1 + written + val.len() + 2 + 8);
  payload.push(0x00);
  payload.extend_from_slice(&encoded_len[..written]);
  payload.extend_from_slice(val);
  payload.extend_from_slice(&11u16.to_le_bytes());
  let crc = hash(&payload);
  payload.extend_from_slice(&crc);
  payload
}

/// 解析 SCAN 族应答帧单源 → (游标, 条目字节列表；null 项以空 Vec 占位)
/// （对标 scan 族 4 册同形 `parse_scan` 收口）
#[must_use]
pub fn parse_scan(frame: &[u8]) -> (i64, Vec<Vec<u8>>) {
  let text = String::from_utf8_lossy(frame);
  let mut parts = text.split("\r\n");
  assert_eq!(parts.next(), Some("*2"), "外层应为 *2: {text}");
  let cursor_hdr = parts.next().unwrap();
  assert!(cursor_hdr.starts_with('$'), "游标 bulk 头: {cursor_hdr}");
  let cursor: i64 = parts.next().unwrap().parse().unwrap();
  let arr_hdr = parts.next().unwrap();
  assert!(arr_hdr.starts_with('*'), "条目数组头: {arr_hdr}");
  let n: usize = arr_hdr[1..].parse().unwrap();
  let mut items = Vec::with_capacity(n);
  for _ in 0..n {
    let hdr = parts.next().unwrap();
    if hdr == "$-1" {
      items.push(Vec::new());
    } else {
      let len: usize = hdr[1..].parse().unwrap();
      let val = parts.next().unwrap().as_bytes().to_vec();
      assert_eq!(val.len(), len);
      items.push(val);
    }
  }
  (cursor, items)
}

/// 向量域命令面消费者单源：直挂命令面 API（携向量管理器；默认选项 id=1；
/// 对标向量 RENAME/drop 族 6 册同形 `consumer_of` 收口）
#[must_use]
pub fn vector_consumer_of(store: &Arc<TestStore>, vm: &Arc<VectorManager>) -> RespSessionConsumer {
  let api = StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(vm));
  RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api))
}

/// 直调域向量装配单源：显式 [`OwnedActiveVectorSession`] 绑定本执行域 +
/// 回调无状态 [`VectorManager`]（不经专用会话工厂，单任务同步段形态；
/// 对标 8 册同形内联装配收口；`domain` 由调用方持至用例结束）
#[must_use]
pub fn bound_vector_manager(
  store: &Arc<TestStore>,
) -> (
  OwnedActiveVectorSession<SegmentedDevice>,
  Arc<VectorManager>,
) {
  let domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new())),
  ));
  (domain, vm)
}

/// 常驻单库数据库消费者环境单源：真存储执行域 + DB 0 常驻单库管理器 +
/// 命令面消费者（对标 INFO/ADMIN 网络会话族 9 册同形 `consumer()` 装配收口；
/// 会话级差异项——定制选项 / 端点回显 / 独立运行时配置——由调用方对返回
/// consumer 追加设置）
#[must_use]
pub fn database_consumer_env(tag: &str) -> (Runtime, RespSessionConsumer) {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  let session = store.new_session().unwrap();
  // 常驻单库管理器（DB 0，对标 wnode service.rs 装配形态）
  let db = Arc::new(GarnetDatabase::<SegmentedDevice>::new(
    0,
    Arc::clone(&store),
    Arc::clone(&device),
    dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(dir, db));
  let api = StoreGarnetApi::new(session).with_database_manager(mgr);
  (
    rt,
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api)),
  )
}

/// VADD 两维 FP32 向量经 RESP 命令面全链写入并断言成功（对标 RENAME 失败族
/// 3 册同形 `vadd` 收口）
pub async fn vadd_fp32(consumer: &mut RespSessionConsumer, key: &[u8], element: &[u8]) {
  let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64];
  let out = pump_slow(
    consumer,
    &resp_frame(&[
      b"VADD",
      key,
      b"FP32",
      values.as_slice(),
      element,
      b"NOQUANT",
    ]),
  )
  .await;
  assert_eq!(out, b":1\r\n", "VADD {key:?}/{element:?} 应成功");
}

/// 归一化随机 FP32 向量单源（LCG 填充 `dim` 维后按模归一；对标量化任务族
/// 3 册同形 `vec_bytes` 收口）
#[must_use]
pub fn vec_bytes(seed: u64, dim: usize) -> Vec<u8> {
  let mut s = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
  let mut v = vec![0f32; dim];
  let mut n = 0f64;
  for e in &mut v {
    s = s
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
    *e = (((s >> 33) % 2000) as f32 / 1000.0) - 1.0;
    n += (*e as f64) * (*e as f64);
  }
  let norm = n.sqrt().max(1e-9) as f32;
  let mut out = Vec::with_capacity(dim * 4);
  for e in v {
    out.extend_from_slice(&(e / norm).to_le_bytes());
  }
  out
}

/// 默认建索引参数单源（dims=2 / L2 / NoQuant / num_links=8 / bef=64；对标
/// 向量锁竞速族 3 册同形 `create_params` 收口）
#[must_use]
pub fn create_index_params() -> CreateIndexParams {
  CreateIndexParams {
    hash_slot: 0,
    dims: 2,
    reduce_dims: 0,
    quant: VectorQuantType::NoQuant,
    build_exploration_factor: 64,
    num_links: 8,
    distance_metric: VectorDistanceMetricType::L2,
  }
}

/// 执行域绑定测试装配单源（新契约：进入 manager 命令臂前本执行域须已绑定
/// 向量存储会话；直调用例按仓内唯一机制自持——真 wkv 会话经
/// [`OwnedActiveVectorSession`] 后台自持形态绑定，对标 5 册同形
/// `bound_domain` 收口）
#[must_use]
pub fn bound_domain() -> (
  TempDir,
  Arc<TestStore>,
  OwnedActiveVectorSession<SegmentedDevice>,
) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("bind.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
  (dir, store, bound)
}

// ---------------------------------------------------------------------------
// 分层族判据
// ---------------------------------------------------------------------------

/// 分层态判据单源：BfTree 元记录存根在册（wkv `load_collection_stub` 权威读；
/// 对标分层族 9 册同形 `is_tiered` 收口；环境形态解耦，凡持 `rt`/`store`
/// 字段的环境均可直引）
#[must_use]
pub fn is_tiered(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().unwrap();
  rt.block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// [`is_tiered`] 的 [`TestEnv`] 环境形
#[must_use]
pub fn is_tiered_env(env: &TestEnv, key: &[u8]) -> bool {
  is_tiered(&env.rt, &env.store, key)
}

/// [`is_tiered`] 的 [`WatchEnv`] 环境形
#[must_use]
pub fn watch_is_tiered(env: &WatchEnv, key: &[u8]) -> bool {
  is_tiered(&env.rt, &env.store, key)
}

/// [`promote_at`] 的 [`TestEnv`] 环境形
pub fn promote_env(
  env: &TestEnv,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  next_expiry: i64,
) {
  promote_at(&env.rt, &env.store, key, obj_type, entries, next_expiry);
}

/// 分层 zset 双成员升层单源（m1=1.0 / m2=2.0，f64 BE member_ttl 编码无 TTL；
/// 对标分层族 4 册同形 `promote_zset2` 收口）
pub fn promote_env_zset2(env: &TestEnv, key: &[u8]) {
  promote_env_max(
    env,
    key,
    GarnetObjectType::SortedSet,
    vec![
      (b"m1".to_vec(), encode_member(&1.0f64.to_be_bytes(), None)),
      (b"m2".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
    ],
  );
}

/// 分层 hash 三字段升层单源（f1/f2/f3，member_ttl 编码无 TTL；对标分层族
/// 5 册同形 `promote_hash3` 收口）
pub fn promote_env_hash3(env: &TestEnv, key: &[u8]) {
  promote_env_max(
    env,
    key,
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
  );
}

/// 小容量单文件存储环境单源（页 1024 / 64KB 缓存 / 16 盘 / 0.5 预算，无钩子；
/// 对标驱逐族 2 册同形 `open_env` 收口）
#[must_use]
pub fn small_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, TempDir) {
  env_with_config(tag, StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap())
}

/// [`promote`] 的 [`TestEnv`] 环境形（永不过期）
fn promote_env_max(
  env: &TestEnv,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
) {
  promote_at(&env.rt, &env.store, key, obj_type, entries, i64::MAX);
}

/// 集合升层装配单源：直驱 `promote_collection_to_bftree` + 断言存根在册
/// （对标分层族 8 册同形 `promote` 收口；环境形态解耦同 [`is_tiered`]）
pub fn promote_at(
  rt: &Runtime,
  store: &Arc<TestStore>,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  next_expiry: i64,
) {
  let sess = store.new_session().unwrap();
  rt.block_on(sess.promote_collection_to_bftree(key, obj_type, entries, next_expiry, false))
    .unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(key))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
}

/// 永不过期升层（[`promote_at`] 的 `i64::MAX` 便捷形，对标 4 参 `promote` 册）
pub fn promote(
  rt: &Runtime,
  store: &Arc<TestStore>,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
) {
  promote_at(rt, store, key, obj_type, entries, i64::MAX);
}

// ---------------------------------------------------------------------------
// 断言 / 解析族
// ---------------------------------------------------------------------------

/// `:N\r\n` 整数回执解析单源（对标 19 册同形 `reply_int` 收口）
#[must_use]
pub fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// `$N\r\n…\r\n` bulk string 回执解析单源（nil 回 None；对标 restore/rmw
/// 键管族 8 册同形 `reply_bulk` 收口）
#[must_use]
pub fn reply_bulk(resp: &[u8]) -> Option<Vec<u8>> {
  let body = resp.strip_prefix(b"$")?;
  let nl = body.iter().position(|b| *b == b'\r')?;
  let len: usize = from_utf8(&body[..nl]).ok()?.parse().ok()?;
  let val = body.get(nl + 2..nl + 2 + len)?;
  if body.get(nl + 2 + len..nl + 4 + len)? != b"\r\n" {
    return None;
  }
  Some(val.to_vec())
}

/// SCAN 族应答帧组装单源：`*2\r\n` + 游标 bulk + 条目数组头 + 逐条目帧
///（对标 scan 族 3 册同形 `scan_frame` 收口）
#[must_use]
pub fn scan_frame(cursor: i64, items: &[Vec<u8>]) -> Vec<u8> {
  let mut out = b"*2\r\n".to_vec();
  out.extend_from_slice(&bulk_bytes(cursor.to_string().as_bytes()));
  out.extend_from_slice(&format!("*{}\r\n", items.len()).into_bytes());
  for item in items {
    out.extend_from_slice(item);
  }
  out
}

/// 非提交帧条目排取单源（AOF 面观测口：先 commit 闭日志尾再全量扫描，
/// 剔除提交指纹帧；对标向量 AOF 族同形 `aof_records` 收口）
pub fn aof_records(aof: &GarnetAppendOnlyFile) -> Vec<WalRecord> {
  aof.log().commit();
  let mut records = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload) {
      records.push(rec.clone());
    }
    true
  });
  records
}

/// 共享版本表 WATCH 挂键单源（根域事务管理器，读面判据与 wtxn 校验同面；
/// 对标 WATCH 族 4 册同形 `watched` 收口）
#[must_use]
pub fn watched(map: &Arc<WatchVersionMap>, key: &[u8]) -> wtxn::TransactionManager {
  let mut txn = TransactionManager::new(TxnLockTable::new(), Arc::clone(map), None);
  txn.watch(SessionPrefixBuf::ROOT.as_slice(), key);
  txn
}

/// 共享版本表版本读点单源（与 wtxn 校验同一哈希面：根域 scoped 键哈希读；
/// 对标 WATCH 族 4 册同形 `ver` 收口）
#[must_use]
pub fn ver(map: &Arc<WatchVersionMap>, key: &[u8]) -> u64 {
  map.read_version(
    TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64,
  )
}

/// RESP bulk 帧组帧单源：`$len\r\n<v>\r\n`（对标 6 册同形 `bulk` 收口）
#[must_use]
pub fn bulk_bytes(v: &[u8]) -> Vec<u8> {
  let mut out = format!("${}\r\n", v.len()).into_bytes();
  out.extend_from_slice(v);
  out.extend_from_slice(b"\r\n");
  out
}

/// RESP 数组帧内 bulk 串逐条解析单源（对标 resp_hash/resp_set/resp_sorted_set
/// 3 册同形 `parse_bulk_array` 收口；非 bulk 首字节即停）
#[must_use]
pub fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut pos = match frame.iter().position(|&b| b == b'\n') {
    Some(p) => p + 1,
    None => return items,
  };
  while pos < frame.len() {
    if frame[pos] != b'$' {
      break;
    }
    let len_end = frame[pos..].iter().position(|&b| b == b'\n').unwrap() + pos;
    let len: usize = str::from_utf8(&frame[pos + 1..len_end - 1])
      .unwrap()
      .parse()
      .unwrap();
    let start = len_end + 1;
    items.push(frame[start..start + len].to_vec());
    pos = start + len + 2;
  }
  items
}

/// cmdstat 行断言单源（INFO COMMANDSTATS 段内的 Redis 约定格式；对标 3 册
/// 同形 `assert_cmdstat` 收口）
pub fn assert_cmdstat(info: &[u8], cmd: &str, calls: u64, rejected: u64, failed: u64) {
  let text = from_utf8(info).unwrap();
  let line = text
    .split("\r\n")
    .find(|l| l.starts_with(&format!("cmdstat_{cmd}:")))
    .unwrap_or_else(|| panic!("缺少 cmdstat_{cmd} 条目: {text}"));
  assert_eq!(
    line,
    format!(
      "cmdstat_{cmd}:calls={calls},usec=0,usec_per_call=0.00,rejected_calls={rejected},failed_calls={failed}"
    ),
  );
}

/// 应答字节断言辅助单源（按 UTF-8 lossy 逐字比对；对标 3 册同形
/// `AssertBytes` trait 收口）
pub trait AssertBytes {
  /// 断言应答字节与期望一致（失配 panic 带双方 lossy 文本）
  fn assert_eq_bytes(&self, expected: &[u8]);
}

impl AssertBytes for Vec<u8> {
  fn assert_eq_bytes(&self, expected: &[u8]) {
    assert_eq!(
      String::from_utf8_lossy(self),
      String::from_utf8_lossy(expected),
      "应答不匹配"
    );
  }
}

/// 消费者单命令同步往返单源：整帧消费断言后取同步段应答（对标网络泵 4 册
/// 同形 `roundtrip` 收口）
pub fn roundtrip_frame(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 慢命令往返单源：同步段消费 → block_on 承担网络泵闭环慢路径（对标 4 册
/// 同形 `slow_roundtrip` 收口）
pub fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// 期望字节数读完单源（累计读，容忍 TCP 分段；对标 3 册同形 `read_until` 收口）
pub async fn read_until(stream: &mut TcpStream, acc: &mut Vec<u8>, expect: usize) {
  let mut chunk = vec![0u8; 8192];
  while acc.len() < expect {
    let BufResult(res, ret) = stream.read(chunk).await;
    chunk = ret;
    let n = res.expect("对端提前关闭");
    assert!(n > 0, "对端提前关闭（已收 {} 字节）", acc.len());
    acc.extend_from_slice(&chunk[..n]);
  }
}

/// 单命令原始帧往返单源：同步段消费（挂起慢路径由 block_on 承担闭环）
/// （对标向量读面 3 册同形 `roundtrip` 收口）
pub fn raw_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, req: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, req);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {req:?}");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// ACL 族消费者命令泵单源：RESP 数组帧手工组帧投喂 + 停车臂 async 域内联
/// 闭环（对标 ACL 3 册同形 `send_cmd` 收口）
pub async fn send_consumer_args(consumer: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut cmd_frame = Vec::new();
  cmd_frame.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
  for arg in args {
    cmd_frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
    cmd_frame.extend_from_slice(arg);
    cmd_frame.extend_from_slice(b"\r\n");
  }
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(&cmd_frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  consumer.try_consume_messages_into(&mut resp);
  // 停车臂（AUTH/ACL 族存储点查）直接在调用方 async 域内联闭环
  drive_pending_parks_consumer(consumer, &mut resp).await;
  resp
}

/// 批纪元内存储会话装配单源：独立版本表实例并同步接线引擎写面钩子——WATCH
/// 写面推进统一走 wkv 引擎收口（生产由装配层以 NodeService.watch_version_map
/// 同构接线；对标 4 册同形 `storage_session` 收口）
#[must_use]
pub fn storage_session<'s, D: wdev::Device>(
  session: &'s wkv::StoreSession<D>,
) -> StorageSession<'s, D> {
  let version_map = Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE));
  let _ = session
    .store
    .set_watch_hook(version_map_watch_hook(Arc::clone(&version_map)));
  StorageSession::new(session.enter_batch())
}

/// 事件入账存储环境单源：`test_store_config` 引擎 + `StoreEventSink` 计数
/// 接线（事件处理器由调用方给定，支持各册差异化拦查；对标 3 册同形
/// `open_store` 收口）
#[must_use]
pub fn open_store_with_sink(
  tag: &str,
  on_store_event: fn(&AtomicU64, i64, i32, StoreEvent<'_>) -> wkv::Result<()>,
) -> (Arc<TestStore>, TempDir, Arc<AtomicU64>) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof_rmw_count = Arc::new(AtomicU64::new(0));
  assert!(
    store.set_event_sink(StoreEventSink::new(
      Arc::clone(&aof_rmw_count),
      on_store_event
    )),
    "事件分发器注入失败（重复注入或时机过晚）"
  );
  (store, dir, aof_rmw_count)
}

/// 存储门面 API 装配单源（独立会话经 `?` 上抛；对标 NodeService 族 10 册同形
/// `api_of` 收口）
pub fn api_of(store: &Arc<TestStore>) -> wnode::Result<GarnetApi> {
  Ok(Arc::new(StoreGarnetApi::new(store.new_session()?)).into())
}

/// 嵌套数组 RESP 组帧单源：外层每命令一数组、内层逐 bulk（对标 txn/竞速族
/// 3 册同形 `frame` 收口）
#[must_use]
pub fn nested_frame(cmds: &[&[&[u8]]]) -> Vec<u8> {
  let mut out = Vec::new();
  for c in cmds {
    out.extend_from_slice(format!("*{}\r\n", c.len()).as_bytes());
    for a in c.iter() {
      out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
      out.extend_from_slice(a);
      out.extend_from_slice(b"\r\n");
    }
  }
  out
}

/// AOF 逻辑数据条目排取单源（不先 commit，仅按重放消费面同一单点滤除提交
/// 元数据帧；对标向量 RENAME 族 3 册同形 `data_records` 收口）
pub fn aof_data_records(aof: &GarnetAppendOnlyFile) -> Vec<WalRecord> {
  let mut records = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload) {
      records.push(rec.clone());
    }
    true
  });
  records
}

/// 小预算测试存储单源（页 16384 / 64KB 缓存 / 64 盘 / 0.5 预算，GC 关闭；
/// 目录经 forget 交 RAII 兜底；对标向量 drop/rename 族 3 册同形 `test_store`
/// 收口）
#[must_use]
pub fn small_budget_store(tag: &str) -> Arc<TestStore> {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  forget(dir);
  store
}

/// 最小回声消费者单源：PING → +PONG（容量门/优雅停机测试同一协议面；
/// 对标 2 册同形私义 `EchoConsumer` 收口）
pub struct EchoConsumer {
  pub buf: Vec<u8>,
  pub head: usize,
}

impl EchoConsumer {
  /// 空消费者
  pub fn new() -> Self {
    Self {
      buf: Vec::new(),
      head: 0,
    }
  }
}

impl Default for EchoConsumer {
  fn default() -> Self {
    Self::new()
  }
}

impl MessageConsumerFace for EchoConsumer {
  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    while self.buf[self.head..].starts_with(b"PING\r\n") {
      self.head += 6;
      resp_buf.extend_from_slice(b"+PONG\r\n");
    }
    if self.head >= self.buf.len() {
      self.buf.clear();
      self.head = 0;
      return Some(0);
    }
    Some(self.buf.len() - self.head)
  }
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.buf)
  }
  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }
  fn dispose(&mut self) {}
}

/// 独立连接装配单源（[`conn_pair`] 的 `Conn` 形）
#[must_use]
pub fn conn_on_store(store: &Arc<TestStore>) -> Conn {
  let (api, s) = conn_pair(store);
  Conn { api, s }
}

/// 独立连接（生产 thread-per-core 形态：并发各方各持一份会话；对标扫描
/// 降级族 4 册同形 `Conn` 收口）
pub struct Conn {
  /// 存储门面 API（会话绑定同源）
  pub api: GarnetApi,
  /// 本连接独立会话
  pub s: RespServerSession,
}

impl Conn {
  /// 单命令往返：同步段无输出且挂起慢路径时，以 await 承担网络泵角色闭环
  pub async fn exec(&mut self, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
    self.s.output.clear();
    self.api.exec(&mut self.s, cmd, args);
    if !self.s.output.is_empty() {
      return take(&mut self.s.output);
    }
    let slow = self
      .s
      .take_slow_wait()
      .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
    slow.resolve().await
  }
}

/// 独立连接装配对单源（生产 thread-per-core 形态：各方各持一份会话；api 与
/// 已绑定会话成对返回，对标扫描降级族 4 册同形 `Conn` 装配头收口）
#[must_use]
fn conn_pair(store: &Arc<TestStore>) -> (GarnetApi, RespServerSession) {
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap())).into();
  let s = session_on(&api);
  (api, s)
}

/// 根域事务提交单源（禁序/并发参数固定，零超时；对标 WATCH 族 3 册同形
/// `exec(txn)` 收口）
pub fn txn_exec(txn: &mut wtxn::TransactionManager) -> bool {
  txn.run(
    SessionPrefixBuf::ROOT.as_slice(),
    false,
    false,
    Duration::ZERO,
  )
}

/// 并行恢复拓扑 AOF 装配单源（单物理子日志 + 指定回放任务数；WAL 配置差异
/// 经 `wal_config` 透传；对标并行恢复族 4 册同形装配收口）
pub fn parallel_aof(
  case: &str,
  replay_task_count: i32,
  wal_config: WalConfig,
) -> waof::Result<Arc<GarnetAppendOnlyFile>> {
  let (_dirs, backends) = super::test_sublogs_with_config(case, 1, wal_config);
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: replay_task_count,
    ..RuntimeServerOptions::default()
  };
  Ok(Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  )))
}

/// ACL 单机默认会话工厂单源（default 用户 + 16 库上限；对标 ACL 族 4 册
/// 同形 `default_consumer` 装饰钩子收口）
pub fn acl_session_factory(
  sender_id: u64,
  api: StoreGarnetApi<SegmentedDevice>,
) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    sender_id,
    RespServerSessionOptions {
      default_user: "default".into(),
      max_databases: 16,
      ..RespServerSessionOptions::default()
    },
    Arc::new(api),
  ))
}

/// 向量 AOF 直推接线单源：sink 挂当前版本原子位 + AOF 反挂管理器（对标
/// swapdb/重放族同形接线段收口；专用会话守卫由调用方
/// [`VectorManager::bind_dedicated_session`] 自持）
pub fn wire_vector_aof(
  store: &Arc<TestStore>,
  aof: &Arc<GarnetAppendOnlyFile>,
  vm: &Arc<VectorManager>,
) {
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    aof,
    Arc::clone(store.current_version_atomic()),
  )));
  aof.set_vector_manager(Arc::clone(vm));
}

/// 专用向量会话守卫绑定单源（测试单任务段持至用例尾，直调回调臂经线程槽
/// 取会话；断言文案逐字保留）
pub fn bind_vector_domain(vm: &Arc<VectorManager>) -> ActiveDedicatedVectorSession {
  vm.bind_dedicated_session()
    .expect("专用向量会话工厂应已注入")
}

/// 无管理面网络会话环境单源：tempdir keep + `test_store_config` 存储 + api +
/// 消费者成组返回（对标慢路径网络族 4 册同形 `consumer_with_handles` 收口）
#[must_use]
pub fn plain_handles_env(tag: &str) -> (RespSessionConsumer, GarnetApi, Arc<TestStore>) {
  let dir = tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session)).into();
  (
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone()),
    api,
    store,
  )
}

/// 二维默认建索引参数单源（NoQuant / L2 / bef=64 / num_links=8；对标向量
/// 检索族 3 册同形 `IndexConfig{dims:2,..}` 字面量收口）
#[must_use]
pub fn index_config_dims2() -> wvector::IndexConfig {
  wvector::IndexConfig {
    dims: 2,
    reduce_dims: 0,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

/// 监视器采样驱动单源：驱动 N 轮监视器采样（复位回调与宿主装配同构：
/// `ConsumerRegistry::monitor_iteration_inputs`；对标 3 册同形
/// `run_sampling_rounds` / `run_iterations` 收口）
pub async fn run_sampling_rounds(
  monitor: &GarnetServerMonitor,
  registry: &Arc<ConsumerRegistry>,
  rounds: u32,
) {
  let done = Arc::new(AtomicU32::new(0));
  let done_cancel = Arc::clone(&done);
  monitor
    .main_monitor_task_async(
      |_duration| ready(()),
      move || done_cancel.load(Ordering::Relaxed) >= rounds,
      || {
        done.fetch_add(1, Ordering::Relaxed);
        registry.monitor_iteration_inputs(|| {}, || {})
      },
    )
    .await;
}

/// 恢复落点装配宏：`open_test_store` 目标库 + 批纪元 [`StorageSession`] +
/// 引擎版本推进 + [`ReplayTarget`] 借用装配。storage/target 为借用自持结构，
/// 必须在同一栈帧内联展开（宏声明绑定到调用方作用域），禁函数化返回。
///
/// 展开绑定：`_dir` / `store` / `storage` / `target`（对标 AOF 恢复族 7 册
/// 同形五连装配收口；`$floor` 传 `vec![]` 或恢复基线）
#[macro_export]
macro_rules! replay_target {
  ($db_name:expr, $version:expr, $floor:expr, $store:ident, $storage:ident, $target:ident) => {
    let (_dir, $store) = wtest_base::open_test_store($db_name)?;
    let session = $store.new_session()?;
    let $storage =
      wnode::storage::session::storage_session::StorageSession::new(session.enter_batch());
    $store.set_current_version($version);
    let $target = wnode::aof::aof_processor::ReplayTarget {
      session: &$storage,
      store: ::std::sync::Arc::clone(&$store),
      aof_floor: $floor,
    };
  };
  // 在手存储形：目标库已由调用方装配（自定义预算 / 共享拓扑）
  ($store:ident, $version:expr, $floor:expr, $storage:ident, $target:ident) => {
    let session = $store.new_session()?;
    let $storage =
      wnode::storage::session::storage_session::StorageSession::new(session.enter_batch());
    $store.set_current_version($version);
    let $target = wnode::aof::aof_processor::ReplayTarget {
      session: &$storage,
      store: ::std::sync::Arc::clone(&$store),
      aof_floor: $floor,
    };
  };
}
