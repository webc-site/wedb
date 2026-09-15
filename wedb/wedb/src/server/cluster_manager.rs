use std::{
  fmt::Write as _,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering},
  },
  thread,
  time::Duration,
};

use compio::time::sleep;
use gxhash::HashMap;
use log::trace;
use parking_lot::{Mutex, RwLock};
use wbase::{
  hash_slot::hash_slot as cluster_slot,
  time::{now_ms, now_secs},
};
use wmetric::MetricsItem;
use wnode::{StorageSession, storage::session::common::ttl_sync::probe_alive, types::GarnetStatus};
use wresp::RespCommand;

use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{ClusterConfig, ClusterPreferredEndpointType, LOCAL_WORKER_ID},
    cluster_provider::ClusterProvider,
    connection_info::ConnectionInfo,
    hash_slot::SlotState,
    slot_verify::{
      ClusterSlotVerificationState, IterativeSlotVerifyCache, SlotVerifySessionState,
      iterative_slot_verify_step, multi_key_slot_verify, single_key_slot_verify,
    },
    worker::{LocalWorkerSpec, NodeRole},
  },
};

/// 生成标准 40 字符十六进制节点 ID（对标 Garnet Generator.CreateHexId(40)）
pub fn create_hex_id() -> String {
  const HEX: &[u8; 16] = b"0123456789abcdef";
  let mut bytes = [0u8; 40];
  for b in &mut bytes {
    *b = HEX[fastrand::usize(..16)];
  }
  // SAFETY: 仅填充了合法 ASCII 十六进制小写字符
  unsafe { String::from_utf8_unchecked(bytes.to_vec()) }
}

/// 集群核心管理器（libs/cluster/Server/ClusterManager.cs）
pub struct ClusterManager {
  pub current_config: RwLock<ClusterConfig>,
  pub cluster_provider: Arc<ClusterProvider>,
  pub(crate) flush_count: AtomicI32,
  /// 配置演化版本号：flush_config 统一出口递增。C# 以 CurrentConfig 对象
  /// 引用变化（每次演化替换新对象）供 gossip 增量判定，rust 配置为 RwLock
  /// 原地改写，以此计数器等价对标（Gossip.cs:FlushConfig 调用域）
  config_version: AtomicI64,
  pub(crate) worker_ban_list: RwLock<gxhash::HashMap<String, i64>>,
  pub(crate) active_merge_lock: RwLock<()>,
}

impl ClusterManager {
  /// libs/cluster/Server/ClusterManager.cs:ClusterManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    let current_config = RwLock::new(ClusterConfig::new());
    Self {
      current_config,
      cluster_provider,
      flush_count: AtomicI32::new(0),
      config_version: AtomicI64::new(0),
      worker_ban_list: RwLock::new(HashMap::default()),
      active_merge_lock: RwLock::new(()),
    }
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
  pub fn init_local(&self, address: &str, port: i32, recover_config: bool) {
    let mut config = self.current_config.write();
    if recover_config {
      // 先摘取本地字段再原地改写，避免 &mut 与读借用冲突
      let (node_id, config_epoch, role, primary_id) = {
        let c = &*config;
        (
          c.local_node_id().unwrap_or_default().to_string(),
          c.local_node_config_epoch(),
          c.local_node_role(),
          c.local_node_primary_id().map(String::from),
        )
      };
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: &node_id,
        address,
        port,
        config_epoch,
        role,
        replica_of_node_id: primary_id.as_deref(),
        hostname: None, // Format.GetHostName() equivalent
      });
    } else {
      let node_id = create_hex_id(); // equivalent to Generator.CreateHexId(40)
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: &node_id,
        address,
        port,
        config_epoch: 0,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
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
  pub fn get_connection_info(&self, node_id: &str) -> ConnectionInfo {
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

    let is_spublish = cmd != RespCommand::Publish;
    for (node_id, endpoint) in node_entries {
      let conn = match gm.connection_store.get_connection(&node_id) {
        Some(conn) => conn,
        None => {
          let ip_str = endpoint.ip().to_string();
          gm.connection_store
            .get_or_add(&node_id, &ip_str, endpoint.port() as i32)
        }
      };
      conn
        .try_cluster_publish_async(is_spublish, channel, message)
        .await;
    }
  }

  /// libs/cluster/Server/ClusterManager.cs:FlushConfig
  ///
  /// 全部配置演化路径的统一出口，递增 config_version 驱动 gossip 增量判定
  pub fn flush_config(&self) {
    self.flush_count.fetch_add(1, Ordering::SeqCst);
    self.config_version.fetch_add(1, Ordering::Release);
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryInitializeLocalWorker
  pub fn try_initialize_local_worker(&self, spec: LocalWorkerSpec<'_>) {
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
  pub fn try_stop_writes(&self, replica_id: &str) {
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
  /// 挂起配置合并（写锁，阻塞并发 merge）
  pub fn suspend_config_merge(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
    self.active_merge_lock.write()
  }

  /// 检查节点是否处于封禁期（按秒级时间戳判定）
  pub fn is_banned(&self, node_id: &str) -> bool {
    let now = now_secs() as i64;
    let ban_list = self.worker_ban_list.read();
    if let Some(&expiry) = ban_list.get(node_id) {
      expiry > now
    } else {
      false
    }
  }

  /// 封禁节点指定秒数
  pub fn ban_node(&self, node_id: &str, expiry_seconds: u64) {
    let expiry = (now_secs() + expiry_seconds) as i64;
    self
      .worker_ban_list
      .write()
      .insert(node_id.to_string(), expiry);
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GetBanList
  ///
  /// 列出全部封禁条目，格式 `{node_id} : {diff}`；
  /// 过期项 diff 为负秒，与 C# 负秒语义一致，不做过滤
  pub fn get_ban_list(&self) -> Vec<String> {
    let now = now_secs() as i64;
    let ban_list = self.worker_ban_list.read();
    ban_list
      .iter()
      .map(|(id, &expiry)| format!("{id} : {}", expiry - now))
      .collect()
  }

  /// 清理已过期的封禁条目
  pub fn cleanup_ban_list(&self) {
    let now = now_secs() as i64;
    // 仅需过期时间戳判定，键为被封禁节点 ID 忽略
    self
      .worker_ban_list
      .write()
      .retain(|_node_id, &mut expiry| expiry > now);
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryMerge
  ///
  /// 合并远端 Gossip 配置，带纪元冲突仲裁与变更落盘
  pub fn try_merge(&self, sender_config: &ClusterConfig, acquire_lock: bool) -> bool {
    let _guard = if acquire_lock {
      Some(self.active_merge_lock.read())
    } else {
      None
    };

    if let Some(sender_id) = sender_config.local_node_id()
      && self.is_banned(sender_id)
    {
      trace!(
        "Cannot merge node <{}> because still in ban list",
        sender_id
      );
      return false;
    }

    let mut current = self.current_config.write();
    let ban_list = self.worker_ban_list.read();
    let merged_config = current.merge(sender_config, &ban_list);
    drop(ban_list);

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

/// 槽位校验挂起等待请求束（挂起体持有，等待轮询期间参数自持）
#[derive(Clone)]
pub struct SlotVerifyRequest {
  /// 校验键集（owned：等待期间网络缓冲可复用）
  pub keys: Vec<Vec<u8>>,
  /// 命令只读标志
  pub read_only: bool,
  /// 会话标志快照（ASKING / READONLY / 内部写）
  pub session: SlotVerifySessionState,
  /// 向量集写命令的槽位稳定等待要求
  pub wait_for_stable: bool,
  /// 端点偏好（重定向地址形态）
  pub pref_type: ClusterPreferredEndpointType,
}

impl SlotVerifyRequest {
  /// 键集切片视图
  fn key_slices(&self) -> Vec<&[u8]> {
    self.keys.iter().map(Vec::as_slice).collect()
  }
}

/// 槽位校验门裁决（C# CanServeSlot 门 + CanOperateOnKey 自旋在 compio
/// 协作调度下的投影：Serve/Redirect 同步终评，Wait 由宿主挂起重评）
#[derive(Debug)]
pub enum GateVerdict {
  /// 放行，执行命令分派
  Serve,
  /// 重定向/错误终态（MOVED/ASK/CLUSTERDOWN/CROSSSLOT/TRYAGAIN）
  Redirect(ClusterSlotVerificationState),
  /// 键迁移推进中或槽位未稳定：须挂起等待后重评；`undecided` 为存活性
  /// 待异步裁决的键下标（None = 纯迁移推进等待，短睡重轮即可）
  Wait { undecided: Option<usize> },
}

/// 单键可操作性裁决（C# CanOperateOnKey 分步投影：自旋等待外提至宿主）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyOperable {
  /// 可访问且键仍存在 → MIGRATING 臂放行（C# CanOperateOnKey true）
  Operable,
  /// 键已迁走 / 等待超时强制 / 存储故障 → ASK（C# Exists false）
  NotOperable,
  /// 键正被传输/删除：须轮询迁移推进（C# Caller responsible for spin-wait）
  AccessPending,
  /// 存活性待异步裁决（磁盘候选 / 到期须物理清除）：须 await 存储探测
  ExistsPending,
}

/// 槽位校验等待轮询间隔毫秒数（compio 任务短睡让出，对齐 C#
/// Thread.Yield 自旋的协作让步语义，禁占线程）
const SLOT_VERIFY_POLL_MS: u64 = 1;

/// 槽位等待交接记忆（等待体与下一次同步门评之间的确定性交接，防
/// 挂起-重评活锁）
pub struct SlotWaitMemo {
  /// 等待超时旗标：置位后下一次门评压制全部等待点，按超时终评
  ///（C# 无限自旋无此臂；rust 侧要求超时后按 C# 语义失败或 ASK，
  /// 不得永久挂起）
  exhausted: AtomicBool,
  /// 异步存在性裁决缓存（键下标 → 裁决）：同步快评仅内存探测，磁盘候选
  /// 键经等待体内 `exists().await` 裁决后入缓存，门评重评据此终评
  exists: Mutex<Vec<Option<bool>>>,
}

impl SlotWaitMemo {
  /// 按键集规模建空记忆
  pub fn new(key_count: usize) -> Self {
    Self {
      exhausted: AtomicBool::new(false),
      exists: Mutex::new(vec![None; key_count]),
    }
  }

  /// 等待是否已超时
  fn exhausted(&self) -> bool {
    self.exhausted.load(Ordering::Acquire)
  }

  /// 等待是否已超时（测试与诊断面）
  pub fn is_exhausted(&self) -> bool {
    self.exhausted()
  }

  /// 取异步存在性裁决缓存
  fn exists_decided(&self, idx: usize) -> Option<bool> {
    *self.exists.lock().get(idx)?
  }

  /// 写入异步存在性裁决
  fn insert_exists(&self, idx: usize, alive: bool) {
    if let Some(slot) = self.exists.lock().get_mut(idx) {
      *slot = Some(alive);
    }
  }
}

/// 门评上下文（命令级校验参数束：会话标志 + 端点偏好 + 挂起重评记忆）
struct GateCtx<'a> {
  /// 命令只读标志
  read_only: bool,
  /// 会话标志快照（ASKING / READONLY / 内部写）
  session: SlotVerifySessionState,
  /// 向量集写命令的槽位稳定等待要求
  wait_for_stable: bool,
  /// 端点偏好（重定向地址形态）
  pref_type: ClusterPreferredEndpointType,
  /// 挂起重评交接记忆（首评 None）：超时旗标压制等待点，存在性缓存承接
  /// 异步裁决结果
  memo: Option<&'a SlotWaitMemo>,
}

impl GateCtx<'_> {
  /// 等待是否已超时强制（压制全部等待点按超时终评）
  fn force(&self) -> bool {
    self.memo.is_some_and(SlotWaitMemo::exhausted)
  }

  /// 副本读放行标志（C# IsLocal enableReplicaReads：读路径取 READONLY
  /// 会话态，写路径取内部写会话态）
  fn replica_reads(&self) -> bool {
    if self.read_only {
      self.session.read_only_session
    } else {
      self.session.internal_write
    }
  }
}

/// 槽位校验等待门（门评 + 挂起等待体的宿主实现面）
impl ClusterManager {
  /// 门评单键（ClusterSlotVerify.cs:SingleKeySlotVerify + CanOperateOnKey
  /// 的同步投影；C# 内联自旋在此改为 Wait 裁决交宿主挂起）
  ///
  /// 取 `req` 首键校验
  pub fn evaluate_key_gate(
    &self,
    req: &SlotVerifyRequest,
    memo: Option<&SlotWaitMemo>,
  ) -> GateVerdict {
    let Some(key) = req.keys.first() else {
      return GateVerdict::Serve;
    };
    let ctx = GateCtx {
      read_only: req.read_only,
      session: req.session,
      wait_for_stable: req.wait_for_stable,
      pref_type: req.pref_type,
      memo,
    };
    self.evaluate_single_key(key, cluster_slot(key), &ctx)
  }

  /// 单键门评内核（单键与多键门评共用）
  fn evaluate_single_key(&self, key: &[u8], slot: u16, ctx: &GateCtx) -> GateVerdict {
    let force = ctx.force();
    let config = self.current_config();
    let is_recovering = self.is_recovering();

    // C# waitForStableSlot 门（WaitForSlotToStabalize 循环谓词：向量集写
    // 命令要求槽位先脱离 IMPORTING/MIGRATING）；超时强制则交状态机按当前
    // 状态自然降级（MIGRATING → ASK / IMPORTING → CLUSTERDOWN）
    let state = config.get_state(slot);
    if ctx.wait_for_stable && !force && matches!(state, SlotState::Importing | SlotState::Migrating)
    {
      return GateVerdict::Wait { undecided: None };
    }

    // can_operate 组装：仅 MIGRATING 本地臂消费（C# CanOperateOnKey 只在
    // MIGRATING 分支调用；Stable 热路径零探测开销）
    let can_operate = if state == SlotState::Migrating && config.is_local(slot, ctx.replica_reads())
    {
      match self.resolve_can_operate(key, slot, ctx, 0) {
        KeyOperable::Operable => true,
        KeyOperable::NotOperable => false,
        KeyOperable::AccessPending => return GateVerdict::Wait { undecided: None },
        KeyOperable::ExistsPending => return GateVerdict::Wait { undecided: Some(0) },
      }
    } else {
      // 非本地臂不经 CanOperateOnKey（状态机走 MOVED/CLUSTERDOWN）
      true
    };

    match single_key_slot_verify(
      &config,
      slot,
      ctx.read_only,
      ctx.session,
      is_recovering,
      can_operate,
      ctx.pref_type,
    ) {
      ClusterSlotVerificationState::Ok => GateVerdict::Serve,
      redirect => GateVerdict::Redirect(redirect),
    }
  }

  /// 门评多键（ClusterSlotVerify.cs:MultiKeySlotVerify 逐键 CanOperateOnKey
  /// 外提形态：任一键须等待则整体挂起重评）
  pub fn evaluate_multi_key_gate(
    &self,
    keys: &[&[u8]],
    read_only: bool,
    session: SlotVerifySessionState,
    wait_for_stable: bool,
    pref_type: ClusterPreferredEndpointType,
    memo: Option<&SlotWaitMemo>,
  ) -> GateVerdict {
    let ctx = GateCtx {
      read_only,
      session,
      wait_for_stable,
      pref_type,
      memo,
    };

    // 单键命令（GET/SET 等绝大多数流量）直入单键内核，免逐键装配分配
    if let [key] = keys {
      let slot = cluster_slot(key);
      return self.evaluate_single_key(key, slot, &ctx);
    }

    let force = ctx.force();
    let config = self.current_config();
    let is_recovering = self.is_recovering();
    let slots: Vec<u16> = keys.iter().map(|k| cluster_slot(k)).collect();

    // 稳定等待门（谓词与单键一致；多键同槽位前置校验）
    if wait_for_stable
      && !force
      && let Some(&slot) = slots.first()
    {
      let state = config.get_state(slot);
      if matches!(state, SlotState::Importing | SlotState::Migrating) {
        return GateVerdict::Wait { undecided: None };
      }
    }

    // 逐键可操作性裁决（MIGRATING 本地臂）
    let mut operable = vec![true; keys.len()];
    for (idx, (&key, &slot)) in keys.iter().zip(&slots).enumerate() {
      if config.get_state(slot) != SlotState::Migrating
        || !config.is_local(slot, ctx.replica_reads())
      {
        continue;
      }
      operable[idx] = match self.resolve_can_operate(key, slot, &ctx, idx) {
        KeyOperable::Operable => true,
        KeyOperable::NotOperable => false,
        KeyOperable::AccessPending => return GateVerdict::Wait { undecided: None },
        KeyOperable::ExistsPending => {
          return GateVerdict::Wait {
            undecided: Some(idx),
          };
        }
      };
    }

    match multi_key_slot_verify(
      &config,
      &slots,
      read_only,
      session,
      is_recovering,
      pref_type,
      |idx| operable[idx],
    ) {
      ClusterSlotVerificationState::Ok => GateVerdict::Serve,
      redirect => GateVerdict::Redirect(redirect),
    }
  }

  /// libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
  ///
  /// 迭代式槽位校验的单键验证步（事务 Prepare 同步上下文专用）：批量门评的
  /// Wait 挂起重评在此不可用，CanOperateOnKey 自旋内联为线程让出重试（对标
  /// C# Thread.Yield 自旋），超时上限取 cluster_node_timeout_ms，超时按
  /// NotOperable 终评（MIGRATING → ASK）；磁盘候选键（同步探测 None、异步
  /// 裁决不可达）同沿超时终评，登记差异——C# Exists 同步等 IO 完成即时裁决
  pub fn evaluate_iterative_key_gate(
    &self,
    key: &[u8],
    read_only: bool,
    session: SlotVerifySessionState,
    pref_type: ClusterPreferredEndpointType,
  ) -> ClusterSlotVerificationState {
    let slot = cluster_slot(key);
    let config = self.current_config();
    let is_recovering = self.is_recovering();

    // can_operate 组装（同门评内核：仅 MIGRATING 本地臂消费；同步自旋至
    // 裁决或超时）
    let can_operate = if config.get_state(slot) == SlotState::Migrating
      && config.is_local(
        slot,
        if read_only {
          session.read_only_session
        } else {
          session.internal_write
        },
      ) {
      let ctx = GateCtx {
        read_only,
        session,
        wait_for_stable: false,
        pref_type,
        memo: None,
      };
      let deadline = now_ms().saturating_add(self.cluster_provider.cluster_node_timeout_ms());
      loop {
        match self.resolve_can_operate(key, slot, &ctx, 0) {
          KeyOperable::Operable => break true,
          KeyOperable::NotOperable => break false,
          // 迁移推进 / 磁盘候选裁决未落：让出线程等推进方，超时强制终评
          KeyOperable::AccessPending | KeyOperable::ExistsPending => {
            if now_ms() >= deadline {
              break false;
            }
            thread::yield_now();
          }
        }
      }
    } else {
      true
    };

    single_key_slot_verify(
      &config,
      slot,
      read_only,
      session,
      is_recovering,
      can_operate,
      pref_type,
    )
  }

  /// 迭代式槽位校验整批步进（C# NetworkIterativeSlotVerify 逐键循环形态：
  /// 缓存步进 + 单键验证，供集群会话迭代入口复用）
  pub fn iterative_slot_verify(
    &self,
    cache: &mut IterativeSlotVerifyCache,
    key: &[u8],
    read_only: bool,
    session: SlotVerifySessionState,
    pref_type: ClusterPreferredEndpointType,
  ) -> bool {
    let slot = cluster_slot(key);
    let verdict = self.evaluate_iterative_key_gate(key, read_only, session, pref_type);
    iterative_slot_verify_step(cache, verdict, slot)
  }

  /// 挂起等待体：轮询至终评或超时（ClusterSlotVerify.cs:CanOperateOnKey /
  /// WaitForSlotToStabalize 自旋循环的 compio 投影）
  ///
  /// C# 以「释放纪元 + Thread.Yield + 重取配置」让出网络线程；rust 迁移
  /// 驱动与请求处理同在 compio 任务池，内联自旋会饿死推进方，改为短睡
  /// 让出（`SLOT_VERIFY_POLL_MS`）。存在性待裁决键经 `exists().await`
  /// 异步裁决入缓存后立即重评。超时置位 `memo.exhausted`，下一次门评
  /// 按超时终评（MIGRATING → ASK / IMPORTING → CLUSTERDOWN），杜绝永久
  /// 挂起。应答恒为空集：终评落地由消费循环重驱门评承接，此处不写重定向
  pub async fn wait_key_gate(&self, req: SlotVerifyRequest, memo: Arc<SlotWaitMemo>) {
    let deadline = now_ms().saturating_add(self.cluster_provider.cluster_node_timeout_ms());
    let keys = req.key_slices();
    loop {
      match self.evaluate_multi_key_gate(
        &keys,
        req.read_only,
        req.session,
        req.wait_for_stable,
        req.pref_type,
        Some(&memo),
      ) {
        GateVerdict::Serve | GateVerdict::Redirect(_) => return,
        GateVerdict::Wait { undecided } => {
          if let Some(idx) = undecided {
            // 磁盘候选 / 到期清除须异步裁决（C# Exists 经 Tsavorite pending
            // IO 异步阶段同等闭环）；裁决入缓存后立即重评
            let alive = self.probe_key_alive_async(&req.keys[idx]).await;
            memo.insert_exists(idx, alive);
            continue;
          }
          if now_ms() >= deadline {
            memo.exhausted.store(true, Ordering::Release);
            return;
          }
          sleep(Duration::from_millis(SLOT_VERIFY_POLL_MS)).await;
        }
      }
    }
  }

  /// 单键可操作性裁决（CanOperateOnKey 本体：迁移可访问性 + 存在性判定）
  ///
  /// `key_idx` 为存在性缓存下标（多键命令逐键独立裁决）
  fn resolve_can_operate(
    &self,
    key: &[u8],
    slot: u16,
    ctx: &GateCtx,
    key_idx: usize,
  ) -> KeyOperable {
    let force = ctx.force();
    // 1. 迁移可访问性（MigrateSessionKeyAccess.cs:CanAccessKey——调用方负责
    // 自旋等待；INITIALIZING/MIGRATED 双向放行，TRANSMITTING 仅读放行，
    // DELETING 全等待）
    let accessible = self
      .cluster_provider
      .migration_manager()
      .map(|mm| mm.can_access_key(key, slot as i32, ctx.read_only))
      .unwrap_or(true);
    if !accessible {
      return if force {
        KeyOperable::NotOperable
      } else {
        KeyOperable::AccessPending
      };
    }

    // 2. 存在性判定（C# `return Exists(key)`：键仍在本地 → OK，已迁走 →
    // ASK）。同步内存探测优先（String / ObjectEnvelope 双域 + TTL 裁决），
    // 磁盘候选/到期清除交异步裁决
    match self.probe_key_alive(key) {
      Ok(Some(alive)) if alive => KeyOperable::Operable,
      Ok(Some(_)) => KeyOperable::NotOperable,
      Ok(None) => match ctx.memo.and_then(|m| m.exists_decided(key_idx)) {
        Some(alive) if alive => KeyOperable::Operable,
        Some(_) => KeyOperable::NotOperable,
        None if force => KeyOperable::NotOperable,
        None => KeyOperable::ExistsPending,
      },
      Err(_) => KeyOperable::NotOperable,
    }
  }

  /// 同步存活探测（内存命中即时裁决；磁盘候选返回 None 交异步）
  fn probe_key_alive(&self, key: &[u8]) -> wkv::Result<Option<bool>> {
    let Some(store) = self.cluster_provider.try_store() else {
      return Ok(None);
    };
    let session = store.new_session()?;
    let batch = session.enter_batch();
    probe_alive(&batch, key)
  }

  /// 异步存活探测（StorageSession EXISTS：双域 + TTL 完整裁决）
  async fn probe_key_alive_async(&self, key: &[u8]) -> bool {
    let Some(store) = self.cluster_provider.try_store() else {
      return false;
    };
    let Ok(session) = store.new_session() else {
      return false;
    };
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    storage
      .exists(key)
      .await
      .map(|status| status == GarnetStatus::Ok)
      .unwrap_or(false)
  }

  /// 恢复态快照
  fn is_recovering(&self) -> bool {
    self
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.is_recovering())
      .unwrap_or(false)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::server::{
    hash_slot::HashSlot,
    worker::{LocalWorkerSpec, Worker},
  };

  /// 从条目字符串 `{node_id} : {diff}` 提取秒数
  fn seconds_of(entry: &str, node_id: &str) -> i64 {
    entry[node_id.len() + 3..].parse().unwrap()
  }

  /// 装配本地主节点拓扑（按键哈希槽位配置归属与状态）
  fn manager_with_key(key: &[u8], state: SlotState) -> ClusterManager {
    let cm = ClusterManager::new(Arc::new(ClusterProvider::default()));
    let slot = cluster_slot(key);
    {
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: "node_local",
        address: "127.0.0.1",
        port: 7000,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some("node_remote".to_string()),
        address: "127.0.0.1".to_string(),
        port: 7001,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
      config.slot_map[slot as usize] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state,
      };
    }
    cm
  }

  #[test]
  fn test_ban_list_includes_expired_entries_with_negative_seconds() {
    let cm = ClusterManager::new(Arc::new(ClusterProvider::default()));
    cm.ban_node("node_active", 100);
    // 直接注入已过期封禁条目（早于当前 50 秒）
    let now = now_secs() as i64;
    cm.worker_ban_list
      .write()
      .insert("node_expired".to_string(), now - 50);

    let list = cm.get_ban_list();
    assert_eq!(list.len(), 2);

    // 过期条目仍列出且为负秒（对标 C# GetBanList 全量输出）
    let expired = list.iter().find(|e| e.starts_with("node_expired")).unwrap();
    assert_eq!(seconds_of(expired, "node_expired"), -50);

    // 未过期条目秒数落在 (0, 100]
    let active = list.iter().find(|e| e.starts_with("node_active")).unwrap();
    assert!((1..=100).contains(&seconds_of(active, "node_active")));
  }

  /// 门评基础形态：STABLE 本地槽放行、远端槽 MOVED（热路径零探测零等待）
  #[test]
  fn test_gate_verdict_stable_and_remote() {
    let local_key = b"gate_local_key";
    let cm = manager_with_key(local_key, SlotState::Stable);
    let session = SlotVerifySessionState::default();
    let req = |key: &'static [u8]| SlotVerifyRequest {
      keys: vec![key.to_vec()],
      read_only: false,
      session,
      wait_for_stable: false,
      pref_type: ClusterPreferredEndpointType::Ip,
    };

    // 本地 STABLE → Serve
    assert!(matches!(
      cm.evaluate_key_gate(&req(local_key), None),
      GateVerdict::Serve
    ));

    // 远端槽（构造远端 STABLE）
    let remote_key = b"gate_remote_key";
    let remote_slot = cluster_slot(remote_key);
    let mut config = cm.current_config.write();
    config.slot_map[remote_slot as usize] = HashSlot {
      worker_id: 2,
      state: SlotState::Stable,
    };
    drop(config);
    match cm.evaluate_key_gate(&req(remote_key), None) {
      GateVerdict::Redirect(ClusterSlotVerificationState::Moved { slot, port, .. }) => {
        assert_eq!((slot, port), (remote_slot, 7001));
      }
      other => panic!("远端槽应 MOVED，实得 {other:?}"),
    }
  }

  /// 门评超时强制形态：MIGRATING + memo 超时旗标 → 不再等待，按 ASK 终评
  ///（迁移会话管辖的键即使 can_access 判定未决也压制等待点）
  #[test]
  fn test_gate_verdict_forced_migrating_redirects() {
    let key = b"gate_mig_key";
    let cm = manager_with_key(key, SlotState::Migrating);
    let memo = SlotWaitMemo::new(1);
    memo.exhausted.store(true, Ordering::Release);

    // 无迁移会话管辖（can_access = true）+ 键存在性无从探测（无 store）：
    // 超时强制下存活性未决按 NotOperable → ASK
    let verdict = cm.evaluate_key_gate(
      &SlotVerifyRequest {
        keys: vec![key.to_vec()],
        read_only: true,
        session: SlotVerifySessionState::default(),
        wait_for_stable: false,
        pref_type: ClusterPreferredEndpointType::Ip,
      },
      Some(&memo),
    );
    assert!(matches!(
      verdict,
      GateVerdict::Redirect(ClusterSlotVerificationState::Ask { .. })
    ));
  }

  /// 门评 wait_for_stable：IMPORTING/MIGRATING 未超时时挂起等待
  #[test]
  fn test_gate_verdict_wait_for_stable_defers() {
    let key = b"gate_vset_key";
    let slot = cluster_slot(key);
    let cm = manager_with_key(key, SlotState::Importing);
    let req = SlotVerifyRequest {
      keys: vec![key.to_vec()],
      read_only: false,
      session: SlotVerifySessionState::default(),
      wait_for_stable: true,
      pref_type: ClusterPreferredEndpointType::Ip,
    };
    let verdict = cm.evaluate_key_gate(&req, None);
    assert!(matches!(verdict, GateVerdict::Wait { undecided: None }));

    // 非稳定等待命令不受影响（IMPORTING 非本地 → MOVED 至源属主）
    let mut config = cm.current_config.write();
    config.slot_map[slot as usize].worker_id = 2;
    drop(config);
    let verdict = cm.evaluate_key_gate(
      &SlotVerifyRequest {
        wait_for_stable: false,
        ..req.clone()
      },
      None,
    );
    assert!(matches!(
      verdict,
      GateVerdict::Redirect(ClusterSlotVerificationState::Moved { .. })
    ));
  }
}
