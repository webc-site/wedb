use std::{
  fmt::Write as _,
  fs::{create_dir_all, read, write},
  io,
  path::{Path, PathBuf},
  str::from_utf8,
  sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering},
  },
  time::Duration,
};

use async_lock::{RwLock as AsyncLockRwLock, RwLockWriteGuard as AsyncLockWriteGuard};
use compio::{runtime::spawn, time::sleep};
use futures_util::future::join_all;
use log::trace;
use parking_lot::{Mutex, RwLock};
use wbase::{
  hex::{hex_encode, hex_str_u128},
  map::{ConcurrentMap, new_concurrent_map},
  time::now_secs,
};
use wresp::{command::RespCommand, metrics::MetricsItem};

// 槽位门机制拆分至 cluster_manager_slot_gate.rs（对标 C# ClusterManager
// partial class 的文件级拆分拓扑），再导出保持既有引用路径兼容
pub use crate::server::cluster_manager_slot_gate::{
  GateVerdict, IterativeGate, SlotVerifyRequest, SlotWaitMemo,
};
use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{ClusterConfig, LOCAL_WORKER_ID},
    cluster_provider::ClusterProvider,
    connection_info::ConnectionInfo,
    hash_slot::SlotState,
    wait_async,
    worker::{LocalWorkerSpec, NodeRole},
  },
};

/// 生成 40 字符十六进制随机串（对标 C# Generator.CreateHexId(40)）；
/// 仅供复制 replid（主从复制身份，Redis 协议惯例 40 hex）使用，
/// 集群节点 ID 已收敛 u128，走 [`create_node_id`]
pub fn create_hex_id() -> String {
  let mut buf = [0u8; 20];
  fastrand::fill(&mut buf);
  hex_encode(&buf)
}

/// 生成 128 位随机节点 ID（对标 C# Generator.CreateHexId(40)；身份内部
/// 收敛为 u128 纯二进制，hex 形态仅在协议渲染处出现）
pub fn create_node_id() -> u128 {
  u128::from(fastrand::u64(..)) << 64 | u128::from(fastrand::u64(..))
}

/// 宣告主机名单源解析（C# ClusterManager.cs:201 读配置 + :212/:223 的
/// `string.IsNullOrEmpty(hostname) ? Format.GetHostName() : hostname` 收口
/// 为一处）：配置值非空即直取，空则回退一次 OS 主机名。[`ClusterManager::init_local`]
/// 两臂共用本结果，杜绝各臂独立重复取系统主机名
fn resolve_announce_hostname(announce: &str) -> String {
  if announce.is_empty() {
    os_hostname()
  } else {
    announce.to_string()
  }
}

/// OS 主机名探测（C# Format.GetHostName() 的 rust 等价，取一次 gethostname
/// 系统调用）：探测失败或返回值非 UTF-8 时回退空串（与「未宣告」同义，
/// 下游端点/反查臂据此退化为 IP）。pub(crate)：announce 宣告 IP 解析的
/// 机器名等价门共用本单源
pub(crate) fn os_hostname() -> String {
  // 缓冲 256 覆盖 POSIX HOST_NAME_MAX 各平台取值（含 macOS 255）；gethostname
  // 不保证结尾 NUL，故按首个 NUL 截断，无 NUL 则整段取用
  let mut buf = [0u8; 256];
  let ptr = buf.as_mut_ptr().cast::<libc::c_char>();
  // 平台差异仅长度形参类型（unix size_t / windows c_int），故分型传参
  #[cfg(not(windows))]
  let rc = unsafe { libc::gethostname(ptr, buf.len()) };
  #[cfg(windows)]
  let rc = unsafe { libc::gethostname(ptr, buf.len() as libc::c_int) };
  if rc != 0 {
    return String::new();
  }
  let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
  from_utf8(&buf[..end])
    .map(str::to_owned)
    .unwrap_or_default()
}

/// libs/cluster/Server/ClusterUtils.cs:WriteInto 的 rust 形态（模块内落盘原语）
fn write_into(path: &Path, bytes: &[u8]) -> io::Result<()> {
  if let Some(parent) = path.parent()
    && !parent.as_os_str().is_empty()
  {
    create_dir_all(parent)?;
  }
  write(path, bytes)
}

/// libs/cluster/Server/ClusterUtils.cs:ReadDevice 的 rust 形态（模块内读盘原语）
pub(crate) fn read_device(path: &Path) -> io::Result<Vec<u8>> {
  read(path)
}

/// 集群核心管理器（libs/cluster/Server/ClusterManager.cs）
pub struct ClusterManager {
  pub current_config: RwLock<ClusterConfig>,
  pub cluster_provider: Arc<ClusterProvider>,
  pub flush_count: AtomicI32,
  /// 配置演化版本号：flush_config 统一出口递增。C# 以 CurrentConfig 对象
  /// 引用变化（每次演化替换新对象）供 gossip 增量判定，rust 配置为 RwLock
  /// 原地改写，以此计数器等价对标（Gossip.cs:FlushConfig 调用域）
  pub config_version: AtomicI64,
  /// 拓扑落盘文件路径（未装配为 None，写盘静默跳过；C# clusterConfigDevice，
  /// 装配期由宿主经 ClusterProvider::initialize_cluster_config 注入）
  pub cluster_config_path: RwLock<Option<PathBuf>>,
  /// 刷盘频率毫秒数（-1 纯内存 / 0 即时 / >0 周期；C#
  /// serverOptions.ClusterConfigFlushFrequencyMs，装配期注入，未装配 -1）
  pub flush_frequency_ms: AtomicI32,
  /// 写盘互斥（C# FlushConfig 即时分支 lock(this)）
  flush_disk_lock: Mutex<()>,
  /// 周期刷盘循环在跑旗标（C# ctsGossip 的 flush 消费面；停机经
  /// dispose_background_tasks 落旗收尾）
  flush_running: AtomicBool,
  /// 节点封禁表（节点 id → 解禁时刻秒），对位 C# Gossip/Gossip.cs:33
  /// `readonly ConcurrentDictionary<string, long> workerBanList`：C# 侧本就是
  /// 无外加锁的并发字典（逐操作原子，GetBanList/TryMerge/
  /// DisposeBannedWorkerConnections 各取一次操作），故此处同为 papaya 无锁表
  /// 经 wbase::map 单点构造（继承进程级随机种子注记），不再套读写锁。
  /// 调用方不得依赖「持锁跨多次操作」的原子性——判据一律压成单次
  /// get/contains_key/insert/remove_if
  pub worker_ban_list: ConcurrentMap<u128, i64>,
  /// 配置合并挂起门（C# Gossip.cs:27 `private readonly
  /// common.ReaderWriterLock activeMergeLock`——SemaphoreSlim 基读写锁，
  /// 等待方以 await 让出线程）：异步感知读写锁，私有收口对标 C# private
  /// readonly，唯一入口写侧 suspend_config_merge、读侧 try_merge
  active_merge_lock: AsyncLockRwLock<()>,
}

impl ClusterManager {
  /// 管理器骨架构造（rust 构造拆分面：装配期数据目录未知，仅建空配置与
  /// 内存字段；C# 构造函数的落盘与恢复段见
  /// [`ClusterProvider::initialize_cluster_config`]）
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    let current_config = RwLock::new(ClusterConfig::new());
    Self {
      current_config,
      cluster_provider,
      flush_count: AtomicI32::new(0),
      config_version: AtomicI64::new(0),
      cluster_config_path: RwLock::new(None),
      flush_frequency_ms: AtomicI32::new(-1),
      flush_disk_lock: Mutex::new(()),
      flush_running: AtomicBool::new(false),
      worker_ban_list: new_concurrent_map(),
      active_merge_lock: AsyncLockRwLock::new(()),
    }
  }

  /// 注入落盘路径与刷盘频率（装配期一次；rust 结构差异的字段注入口，
  /// C# 构造期直读 serverOptions 无需本面）
  pub(crate) fn set_persist_options(&self, path: PathBuf, flush_frequency_ms: i32) {
    *self.cluster_config_path.write() = Some(path);
    self
      .flush_frequency_ms
      .store(flush_frequency_ms, Ordering::Release);
  }

  /// 拉起周期刷盘任务（对标 C# 构造尾 `ClusterConfigFlushFrequencyMs > 0`
  /// 的 `Task.Run(FlushTaskAsync)`）；须在 compio 运行时内调用
  pub(crate) fn start_flush_task(self: &Arc<Self>, period: Duration) {
    if self.flush_frequency_ms.load(Ordering::Acquire) <= 0
      || self.flush_running.swap(true, Ordering::AcqRel)
    {
      return;
    }
    let manager = Arc::downgrade(self);
    spawn(async move { ClusterManager::flush_task_async(manager, period).await }).detach();
  }

  /// 当前配置演化版本号（gossip 增量判定键，对标 GarnetServerNode.lastConfig
  /// 引用比较语义）
  #[inline]
  pub fn config_version(&self) -> i64 {
    self.config_version.load(Ordering::Acquire)
  }

  /// 获取当前集群配置读句柄
  #[inline]
  pub fn current_config(&self) -> parking_lot::RwLockReadGuard<'_, ClusterConfig> {
    self.current_config.read()
  }

  /// 惰性更新本地复制偏移量（委托至当前集群配置）
  #[inline]
  pub fn lazy_update_local_replication_offset(&self, offset: i64) {
    self
      .current_config
      .write()
      .lazy_update_local_replication_offset(offset);
  }

  /// libs/cluster/Server/ClusterManager.cs:InitLocal
  ///
  /// `announce_hostname` 对应 C# `serverOptions.ClusterAnnounceHostname`：
  /// 配置非空即直取，空则回退一次 OS 主机名（[`resolve_announce_hostname`]
  /// 单源解析，两臂共用同一结果，系统调用至多一次）。本地位恒有宣告值，
  /// 仅 0 号保留位为空（C# ClusterConfig.cs:112 口径）
  pub fn init_local(
    &self,
    address: &str,
    port: i32,
    recover_config: bool,
    announce_hostname: &str,
  ) {
    let mut config = self.current_config.write();
    // 配置优先、空则一次 OS 主机名取值（对标 C#:201 + :212/:223 三元式）
    let hostname = resolve_announce_hostname(announce_hostname);
    if recover_config {
      // 先摘取本地字段再原地改写，避免 &mut 与读借用冲突
      let (node_id, config_epoch, role, primary_id) = {
        let c = &*config;
        (
          c.local_node_id().unwrap_or_default(),
          c.local_node_config_epoch(),
          c.local_node_role(),
          c.local_node_primary_id(),
        )
      };
      config.initialize_local_worker(LocalWorkerSpec {
        node_id,
        address,
        port,
        config_epoch,
        role,
        replica_of_node_id: primary_id,
        hostname: Some(hostname.as_str()),
      });
    } else {
      let node_id = create_node_id();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id,
        address,
        port,
        config_epoch: 0,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: Some(hostname.as_str()),
      });
    }
  }

  /// libs/cluster/Server/ClusterManager.cs:Dispose
  pub fn dispose(&self) {
    self.dispose_background_tasks();
  }

  /// libs/cluster/Server/ClusterManager.cs:DisposeBackgroundTasks
  pub fn dispose_background_tasks(&self) {
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      gm.dispose();
    }
    self.flush_running.store(false, Ordering::Release);
  }

  /// libs/cluster/Server/ClusterManager.cs:Start
  pub fn start(&self) {
    self.try_start_gossip_tasks();
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryStartGossipTasks
  pub fn try_start_gossip_tasks(&self) {
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      gm.start();
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GetConnectionInfo
  pub fn get_connection_info(&self, node_id: u128) -> ConnectionInfo {
    self
      .cluster_provider
      .gossip_manager()
      .and_then(|gm| gm.connection_store.get_connection_info(node_id))
      .unwrap_or_default()
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GetPrimaryLinkStatus
  pub fn get_primary_link_status(&self, config: &ClusterConfig) -> [MetricsItem; 2] {
    let info = config
      .local_node_primary_id()
      .map(|primary_id| self.get_connection_info(primary_id))
      .unwrap_or_default();
    [
      MetricsItem::new(
        "master_link_status",
        if info.connected { "up" } else { "down" },
      ),
      MetricsItem::new("master_last_io_seconds_ago", info.last_io.to_string()),
    ]
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryClusterPublishAsync
  pub async fn try_cluster_publish_async(&self, cmd: RespCommand, channel: &[u8], message: &[u8]) {
    let Some(gm) = self.cluster_provider.gossip_manager() else {
      return;
    };
    let node_entries = {
      let conf = self.current_config();
      if cmd == RespCommand::Publish {
        conf.get_all_node_ids()
      } else {
        conf.get_node_ids_for_shard()
      }
    };

    if node_entries.is_empty() {
      return;
    }

    let is_spublish = cmd != RespCommand::Publish;
    let channel: Arc<[u8]> = Arc::from(channel);
    let message: Arc<[u8]> = Arc::from(message);

    for (node_id, endpoint) in node_entries {
      let conn = match gm.connection_store.get_connection(node_id) {
        Some(conn) => conn,
        None => {
          let ip_str = endpoint.ip().to_string();
          gm.connection_store.get_or_add(
            node_id,
            &ip_str,
            endpoint.port() as i32,
            &self.cluster_provider,
          )
        }
      };
      let ch = Arc::clone(&channel);
      let msg = Arc::clone(&message);
      spawn(async move {
        conn.try_cluster_publish_async(is_spublish, &ch, &msg).await;
      })
      .detach();
    }
  }

  /// 全租户秒清 FLUSHALL 跨主节点换号广播（doc/zh/db.md 4.5 Cluster Bus
  /// Broadcast，本端口多租户扩展；C# 无 ns 维度无对应函数）
  ///
  /// 收令节点即协调者：本地换号成功后进入，枚举 current_config 全部
  /// Primary（排除本机与封禁期节点），复用 gossip connection_store 的每节点
  /// 客户端并行扇出 CLUSTER FLUSHALL_NS 帧并 await +OK ack（聚合形态同
  /// replication_snapshot_iterator.rs:fan_out_send 的 join_all：全部在途
  /// 并发终结后收账；超时 cluster_node_timeout，0 = 无限不挂计时器，同
  /// MEET 超时先例）；任一节点失败即 Err 上抛，调用方据此拒绝向客户端回
  /// +OK，严禁先应答再异步广播
  pub async fn flushall_broadcast_async(&self, ns: u64) -> Result<()> {
    let (origin_hex, epoch, targets) = {
      let conf = self.current_config();
      let local_id = conf.local_node_id().unwrap_or_default();
      let targets: Vec<_> = conf
        .get_primary_node_ids()
        .into_iter()
        .filter(|(id, _)| *id != local_id)
        .collect();
      (
        hex_str_u128(local_id),
        conf.local_node_config_epoch(),
        targets,
      )
    };
    if targets.is_empty() {
      return Ok(());
    }
    let Some(gm) = self.cluster_provider.gossip_manager() else {
      return Err(Error::Gossip("gossip manager not initialized".into()));
    };
    let wait = self.cluster_provider.cluster_node_timeout();
    // 单遍取/建连接（封禁过滤同扇出口径，get_or_add 仅构造，I/O 惰性在
    // try_flushall_ns_async 内）
    let conns: Vec<_> = targets
      .into_iter()
      .filter(|(id, _)| !self.is_banned(*id))
      .map(|(node_id, endpoint)| {
        let conn = match gm.connection_store.get_connection(node_id) {
          Some(conn) => conn,
          None => gm.connection_store.get_or_add(
            node_id,
            &endpoint.ip().to_string(),
            endpoint.port() as i32,
            &self.cluster_provider,
          ),
        };
        (node_id, conn)
      })
      .collect();
    // 并行扇出等全部终结（不用 try_join_all 早退，避免丢弃在途换号帧任务），
    // 首错上抛：应答字节口径与串行版逐字节一致；origin 降 &str 供每个
    // future 复制引用零分配
    let origin_hex = origin_hex.as_str();
    let outcomes = join_all(conns.into_iter().map(|(node_id, conn)| async move {
      match wait_async(wait, conn.try_flushall_ns_async(ns, origin_hex, epoch)).await {
        Some(r) => r,
        None => Err(Error::Gossip(format!(
          "node {node_id} FLUSHALL_NS timed out"
        ))),
      }
    }))
    .await;
    if let Some(e) = outcomes.into_iter().find_map(Result::err) {
      return Err(e);
    }
    Ok(())
  }

  /// libs/cluster/Server/ClusterManager.cs:FlushConfig
  ///
  /// 全部配置演化路径的统一出口，递增 config_version 驱动 gossip 增量判定；
  /// 落盘三分支对标 C#：-1 纯内存、0 即时写盘、>0 置脏由周期任务刷盘
  pub fn flush_config(&self) {
    let frequency = self.flush_frequency_ms.load(Ordering::Acquire);
    if frequency > 0 {
      self.flush_count.fetch_add(1, Ordering::SeqCst);
    } else if frequency == 0 {
      self.write_config_to_device();
    }
    self.config_version.fetch_add(1, Ordering::Release);
  }

  /// 当前配置落盘（写盘原语见 [`write_into`]；路径未装配时跳过）
  ///
  /// rust 结构差异：C# 即时刷盘分支在 FlushConfig 内联 lock + WriteInto、
  /// 周期分支在 FlushTaskAsync 内联 WriteInto，两分支共用同一写盘体，
  /// 按「一个机制一处定义」提取为本方法
  pub(crate) fn write_config_to_device(&self) {
    let Some(path) = self.cluster_config_path.read().clone() else {
      return;
    };
    let bytes = self.current_config.read().to_byte_array();
    let _guard = self.flush_disk_lock.lock();
    if let Err(e) = write_into(&path, &bytes) {
      log::error!("Failed to flush cluster config to {path:?}: {e}");
    }
  }

  /// libs/cluster/Server/ClusterManager.cs:FlushTaskAsync
  ///
  /// 周期刷盘任务（对标 C# 构造尾 `ClusterConfigFlushFrequencyMs > 0` 拉起的
  /// Task.Run 循环）：`flushCount != 0` 即写盘（C# CompareExchange 只读不清零，
  /// 置脏后每周期重写），直至 `flush_running` 落旗（对标 ctsGossip 取消）
  pub(crate) async fn flush_task_async(manager: Weak<ClusterManager>, period: Duration) {
    while let Some(cm) = manager.upgrade() {
      if !cm.flush_running.load(Ordering::Acquire) {
        break;
      }
      if cm.flush_count.load(Ordering::SeqCst) > 0 {
        cm.write_config_to_device();
      }
      drop(cm);
      sleep(period).await;
    }
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryInitializeLocalWorker
  pub fn try_initialize_local_worker(&self, spec: LocalWorkerSpec) {
    let mut config = self.current_config.write();
    config.initialize_local_worker(spec);
  }

  /// libs/cluster/Server/ClusterManager.cs:GetInfo
  pub fn get_info(&self) -> String {
    // 持读锁直接统计，不克隆整份配置；单遍扫描取全部状态计数
    let current = self.current_config.read();
    let counts = current.slot_state_counts();
    let (stable, fail) = (
      counts[SlotState::Stable as usize],
      counts[SlotState::Fail as usize],
    );
    format!(
      "cluster_state:ok\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:{}\r\n\
             cluster_slots_fail:{}\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_sent:0\r\n\
             cluster_stats_messages_received:0\r\n",
      stable,
      stable,
      fail,
      fail,
      current.num_workers(),
      current.get_primary_count(),
      current.get_max_config_epoch(),
      current.local_node_config_epoch(),
    )
  }

  /// libs/cluster/Server/ClusterManager.cs:GetRange
  ///
  /// 输入须升序；连续槽合并为 `start-end` 区间，其余逐个列出
  pub fn get_range(slots: &[usize]) -> String {
    let mut range = String::from("> ");
    // 哨兵值保证末区间在扫描内闭合，免去循环后重复收尾代码
    let mut prev = None;
    for s in slots.iter().copied().chain([usize::MAX]) {
      match prev {
        Some((start, end)) if s == end + 1 => prev = Some((start, s)),
        Some((start, end)) => {
          let _ = write!(range, "{start}-{end} ");
          prev = Some((s, s));
        }
        None => prev = Some((s, s)),
      }
    }
    range
  }

  /// libs/cluster/Server/ClusterManager.cs:TrySetLocalConfigEpoch
  ///
  /// 错误集中定义于 [`crate::error`]，不再用裸字节串
  pub fn try_set_local_config_epoch(&self, config_epoch: i64) -> Result<()> {
    {
      let mut current = self.current_config.write();
      if current.num_workers() == 0 {
        return Err(Error::NoWorkers);
      }
      if !current.set_local_worker_config_epoch(config_epoch) {
        return Err(Error::EpochNotSet);
      }
    }
    self.flush_config();
    trace!("SetConfigEpoch {}", config_epoch);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManager.cs:TryBumpClusterEpoch
  pub fn try_bump_cluster_epoch(&self) -> bool {
    {
      let mut current = self.current_config.write();
      current.bump_local_node_config_epoch();
    }
    self.flush_config();
    true
  }

  /// libs/cluster/Server/ClusterManager.cs:TrySetLocalNodeRole
  pub fn try_set_local_node_role(&self, role: NodeRole) {
    {
      let mut current = self.current_config.write();
      current
        .set_local_worker_role(role)
        .bump_local_node_config_epoch();
    }
    self.flush_config();
  }

  /// libs/cluster/Server/ClusterManager.cs:TryResetReplica
  pub fn try_reset_replica(&self) {
    {
      let mut current = self.current_config.write();
      current
        .make_replica_of(None)
        .set_local_worker_role(NodeRole::Primary)
        .bump_local_node_config_epoch();
    }
    self.flush_config();
  }

  /// libs/cluster/Server/ClusterManager.cs:TryStopWrites
  pub fn try_stop_writes(&self, replica_id: u128) {
    {
      let mut current = self.current_config.write();
      let slots = current.get_slot_list(LOCAL_WORKER_ID as u16);
      let worker_id = current.get_worker_id_from_node_id(replica_id);
      current
        .make_replica_of(Some(replica_id))
        .assign_slots(&slots, worker_id, SlotState::Stable);
    }
    self.flush_config();
  }

  /// libs/cluster/Server/ClusterManager.cs:TryTakeOverForPrimary
  pub fn try_take_over_for_primary(&self) -> bool {
    {
      let mut current = self.current_config.write();
      if !current.is_replica() || current.local_node_primary_id().is_none() {
        return false;
      }
      current
        .take_over_from_primary()
        .bump_local_node_config_epoch();
    }
    self.flush_config();
    true
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:SuspendConfigMerge
  ///
  /// 挂起配置合并（写锁，挡下并发 merge）。返回异步感知守卫：可跨 await
  /// 持有（C# 即 WriteLock 贯穿 await TryMeetAsync / HasKeysInSlots），等待方
  /// 以 await 让出而非占死线程；守卫 drop 等价 C# finally 中的
  /// ResumeConfigMerge
  pub async fn suspend_config_merge(&self) -> AsyncLockWriteGuard<'_, ()> {
    self.active_merge_lock.write().await
  }

  /// 检查节点是否处于封禁期（按秒级时间戳判定）
  ///
  /// 单次点查即完整判据（C# Gossip.cs:394 `workerBanList.ContainsKey` 同形），
  /// 只判活不摘除——摘除归 [`Self::cleanup_ban_list`] 一处，勿在此留第二把刀
  pub fn is_banned(&self, node_id: u128) -> bool {
    let now = now_secs() as i64;
    self
      .worker_ban_list
      .pin()
      .get(&node_id)
      .is_some_and(|&expiry| expiry > now)
  }

  /// 封禁节点指定秒数（覆盖式单操作插入，等价 C# 的
  /// `workerBanList[key] = expiry`）
  pub fn ban_node(&self, node_id: u128, expiry_seconds: u64) {
    let expiry = (now_secs() + expiry_seconds) as i64;
    self.worker_ban_list.pin().insert(node_id, expiry);
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GetBanList
  ///
  /// 列出全部封禁条目，格式 `{node_id hex} : {diff}`；
  /// 过期项 diff 为负秒，与 C# 负秒语义一致，不做过滤
  pub fn get_ban_list(&self) -> Vec<String> {
    use wbase::hex::hex_str_u128;
    let now = now_secs() as i64;
    // 迭代快照（C# `foreach (var w in workerBanList)` 同为无锁快照枚举）
    self
      .worker_ban_list
      .pin()
      .iter()
      .map(|(id, &expiry)| format!("{} : {}", hex_str_u128(*id), expiry - now))
      .collect()
  }

  /// 清理已过期的封禁条目
  ///
  /// 迭代快照 + 就地条件删除（对位 C# Gossip.cs:421-434
  /// `foreach (var w in workerBanList)` 内 `Expired` 判据 + `TryRemove`）：
  /// `remove_if` 在 CAS 点以**当时**的值复判，故并发 ban_node 续期/重封的条目
  /// 绝不被本轮摘除——原写锁 retain 的「判与删同处锁内」原子性由这一次条件
  /// 删除承接，不残留第二套锁序
  pub fn cleanup_ban_list(&self) {
    let now = now_secs() as i64;
    let ban_list = self.worker_ban_list.pin();
    for (node_id, expiry) in ban_list.iter() {
      if *expiry > now {
        continue;
      }
      // 仅需过期时间戳判定，键为被封禁节点 ID 忽略；条件不成立（值已被并发
      // 改写）即放弃本轮，留给下一轮清理
      let _ = ban_list.remove_if(node_id, |_, current| *current <= now);
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryMerge
  ///
  /// 合并远端 Gossip 配置，带纪元冲突仲裁与变更落盘。读锁为异步等待：
  /// 挂起窗口内让出线程而非冻死本线程（C# Gossip.cs:121/:142
  /// `if (acquireLock) activeMergeLock.ReadLock()/ReadUnlock()` 的 try/
  /// finally 收口，rust 以守卫作用域等价承载）
  pub async fn try_merge(&self, sender_config: &ClusterConfig, acquire_lock: bool) -> bool {
    // acquire_lock:false 供已持挂起写锁的调用方（迁移收尾汇聚）透传，
    // 同任务先写后读会自死锁
    let _guard = if acquire_lock {
      Some(self.active_merge_lock.read().await)
    } else {
      None
    };

    // 顶层封禁门与 merge 内层（ClusterConfig::merge 的 contains_key）同一
    // 判据：在册即拒（C# Gossip.cs:TryMerge 的 BanList.ContainsKey）；时间
    // 判活的 is_banned 仅保留给 CLUSTER BANLIST 展示与连接摘除面
    if let Some(sender_id) = sender_config.local_node_id()
      && self.worker_ban_list.pin().contains_key(&sender_id)
    {
      trace!(
        "Cannot merge node <{}> because still in ban list",
        hex_str_u128(sender_id)
      );
      return false;
    }

    // 封禁表以并发容器直传 merge（C# Gossip.cs:132 `Merge(senderConfig,
    // workerBanList, logger)` 同形）：本侧只剩 current_config 一把锁，
    // 原「config 写锁 + ban 读锁」的锁序随外层锁一并消解
    let mut current = self.current_config.write();
    let merged_config = current.merge(sender_config, &self.worker_ban_list);

    if let Some(mut next) = merged_config {
      next.handle_config_epoch_collision(sender_config);
      *current = next;
      drop(current);
      self.flush_config();
      true
    } else if current.handle_config_epoch_collision(sender_config) {
      drop(current);
      self.flush_config();
      true
    } else {
      false
    }
  }
}
