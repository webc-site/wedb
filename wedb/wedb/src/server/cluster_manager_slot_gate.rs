//! 槽位校验等待门（门评 + 挂起等待体 + 可操作性裁决）
//!
//! 文件级拆分对标 garnet libs/cluster/Server/ClusterManager.cs 与
//! ClusterManagerSlotState.cs / ClusterManagerWorkerState.cs 的 partial class
//! 拆分拓扑（rust 侧以跨文件 impl ClusterManager 块等价投影，与既有
//! cluster_manager_slot_state.rs / cluster_manager_worker_state.rs 对称）；
//! 门评状态机本体见 slot_verify.rs（Session/ClusterSlotVerify.cs），本文件
//! 是宿主挂起外提层（C# 内联自旋在 compio 协作调度下的投影）

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::time::sleep;
use parking_lot::Mutex;
use wbase::time::now_ms;
use wnode::{StorageSession, storage::session::common::ttl_sync::probe_alive, types::GarnetStatus};

use crate::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  hash_slot::SlotState,
  slot_verify::{
    ClusterSlotVerificationState, IterativeSlotVerifyCache, SlotVerifiedState,
    SlotVerifySessionState, iterative_slot_verify_step, single_key_slot_verify,
  },
};

/// `cluster_node_timeout` 的毫秒 deadline 投影：无限（0 哨兵，None）取
/// u64::MAX，saturating_add 后 deadline 钳至最大值、永不到点（对标 C#
/// RuntimeServerConfig.GetTimeSpan 非正 → Timeout.InfiniteTimeSpan）。
///
/// 无限上界只允许用于异步等待臂（[`ClusterManager::wait_key_gate`] 挂起
/// 轮询，compio 挂起不占线程，即 C# Thread.Yield 无限自旋的异步投影）；
/// 同步门评面不得设 deadline 自旋等待——compio「一线程一 CPU」下同步
/// 让出饿死同线程的迁移驱动推进方，两互等键成活锁
fn node_timeout_ms(provider: &ClusterProvider) -> u64 {
  provider
    .cluster_node_timeout()
    .map_or(u64::MAX, |d| d.as_millis() as u64)
}

/// 槽位校验挂起等待请求束（挂起体持有，等待轮询期间参数自持）
#[derive(Clone)]
pub struct SlotVerifyRequest {
  /// 会话库级槽位：`Mixer(namespace, active_db)`（doc/zh/db.md 4.1），
  /// 等待期重评沿用同一槽位（等待体不持会话，故随请求自持）
  pub slot: u16,
  /// 校验键集（owned：等待期间网络缓冲可复用）
  pub keys: Vec<Vec<u8>>,
  /// 命令只读标志
  pub read_only: bool,
  /// 会话标志快照（ASKING / READONLY / 内部写）
  pub session: SlotVerifySessionState,
  /// 向量集写命令的槽位稳定等待要求
  pub wait_for_stable: bool,
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
  /// 重定向/错误终态（MOVED/ASK/CLUSTERDOWN/TRYAGAIN）
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

/// 迭代门评裁决（RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
/// bool 面的挂起扩展）
#[derive(Debug)]
pub enum IterativeGate {
  /// 终评（缓存已步进）：bool 同 C# 返回语义，true = 放行
  Done(bool),
  /// 迁移推进 / 存活性裁决未落：C# CanOperateOnKey 内 `Thread.Yield` 同步
  /// 自旋的挂起投影——同步面不得等待（compio 一线程一 CPU 下自旋饿死
  /// 迁移驱动），宿主登记 [`ClusterManager::wait_key_gate`] 等待体，迁移
  /// 推进或超时后重驱门评；缓存不步进（非终态）
  Pending,
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
  pub exhausted: AtomicBool,
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

/// 门评上下文（命令级校验参数束：会话标志 + 挂起重评记忆；端点偏好属
/// 渲染面，裁决不感知）
struct GateCtx<'a> {
  /// 命令只读标志
  read_only: bool,
  /// 会话标志快照（ASKING / READONLY）
  session: SlotVerifySessionState,
  /// 向量集写命令的槽位稳定等待要求
  wait_for_stable: bool,
  /// 挂起重评交接记忆（首评 None）：超时旗标压制等待点，存在性缓存承接
  /// 异步裁决结果
  memo: Option<&'a SlotWaitMemo>,
}

impl GateCtx<'_> {
  /// 等待是否已超时强制（压制全部等待点按超时终评）
  fn force(&self) -> bool {
    self.memo.is_some_and(SlotWaitMemo::exhausted)
  }

  /// 副本读放行标志（C# IsLocal enableReplicaReads：读路径取 READONLY 会话
  /// 态；写路径 C# 取内部写会话态，rust 回放不经 RESP 分派、直达存储域，
  /// 本门仅拦客户端会话，写路径恒不放行）
  fn replica_reads(&self) -> bool {
    self.read_only && self.session.read_only_session
  }
}

/// 槽位校验等待门（门评 + 挂起等待体的宿主实现面）
impl ClusterManager {
  /// 门评单键（宿主挂起外提层：等待与存在性裁决汇聚后转调
  /// slot_verify::single_key_slot_verify 状态机，映射注释在该层；
  /// C# 内联自旋在此改为 Wait 裁决交宿主挂起）
  ///
  /// 槽位取 `req.slot`（会话库级定槽），键仅参与 MIGRATING 窗口的可操作性
  /// 与存在性判定
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
      memo,
    };
    self.evaluate_single_key(key, req.slot, &ctx)
  }

  /// 单键门评内核（单键与多键门评共用；`slot` 恒为会话库级槽位）
  fn evaluate_single_key(&self, key: &[u8], slot: u16, ctx: &GateCtx) -> GateVerdict {
    let force = ctx.force();
    let config = self.current_config();

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

    self.finish_slot_gate(&config, slot, can_operate, ctx)
  }

  /// 槽位级终评（单键与多键门评共用尾段：可操作性裁决已定，转调
  /// slot_verify 状态机并把裁决映射为门裁决三态）
  fn finish_slot_gate(
    &self,
    config: &ClusterConfig,
    slot: u16,
    can_operate: bool,
    ctx: &GateCtx,
  ) -> GateVerdict {
    match single_key_slot_verify(
      config,
      slot,
      ctx.read_only,
      ctx.session,
      self.is_recovering(),
      can_operate,
    ) {
      redirect if redirect.is_ok() => GateVerdict::Serve,
      redirect => GateVerdict::Redirect(redirect),
    }
  }

  /// 门评多键（宿主挂起外提层：任一键须等待则整体挂起重评）
  ///
  /// 库级定槽形态（doc/zh/db.md 4.1/4.4）：同一命令的各键恒共会话槽位
  /// `slot`，C# `ClusterSlotVerify.cs:MultiKeySlotVerify` 的键间槽位比较与
  /// CROSSSLOT 裁决在架构层消失；键间唯一可漂移的是 MIGRATING 传输窗口的
  /// 逐键可操作性（C# 逐键比较状态枚举、混合态落 TRYAGAIN），此处以布尔
  /// 累计等价，终评仍走单键状态机
  pub fn evaluate_multi_key_gate(
    &self,
    keys: &[&[u8]],
    slot: u16,
    read_only: bool,
    session: SlotVerifySessionState,
    wait_for_stable: bool,
    memo: Option<&SlotWaitMemo>,
  ) -> GateVerdict {
    let ctx = GateCtx {
      read_only,
      session,
      wait_for_stable,
      memo,
    };

    // 单键命令（GET/SET 等绝大多数流量）直入单键内核，免逐键装配分配
    if let [key] = keys {
      return self.evaluate_single_key(key, slot, &ctx);
    }

    let force = ctx.force();
    let config = self.current_config();
    let state = config.get_state(slot);

    // 稳定等待门（谓词与单键一致；整命令共一槽，无键间槽位校验）
    if wait_for_stable && !force && matches!(state, SlotState::Importing | SlotState::Migrating) {
      return GateVerdict::Wait { undecided: None };
    }

    // 逐键可操作性裁决（CanOperateOnKey 只在 MIGRATING 本地臂调用，同单键
    // 内核；Stable 热路径零探测开销）
    let mut can_operate = true;
    if state == SlotState::Migrating && config.is_local(slot, ctx.replica_reads()) {
      let (mut any_operable, mut any_moved_away) = (false, false);
      for (idx, &key) in keys.iter().enumerate() {
        match self.resolve_can_operate(key, slot, &ctx, idx) {
          KeyOperable::Operable => any_operable = true,
          KeyOperable::NotOperable => any_moved_away = true,
          KeyOperable::AccessPending => return GateVerdict::Wait { undecided: None },
          KeyOperable::ExistsPending => {
            return GateVerdict::Wait {
              undecided: Some(idx),
            };
          }
        }
      }
      // 键间可操作性混合（部分键已迁走、部分键仍在本地）→ C# 键间状态
      // 枚举不一致，TRYAGAIN 交客户端重试
      if any_operable && any_moved_away {
        return GateVerdict::Redirect(ClusterSlotVerificationState::new(
          SlotVerifiedState::TryAgain,
          slot,
        ));
      }
      can_operate = !any_moved_away;
    }

    self.finish_slot_gate(&config, slot, can_operate, &ctx)
  }

  /// 迭代式槽位校验的单键验证步内核（事务 Prepare 同步上下文专用）
  ///
  /// libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
  ///
  /// C# 在网络线程内联 `Thread.Yield` 自旋等待迁移推进（C# 网络线程与
  /// 迁移驱动不同线程亲和，让出即有效）；compio 一线程一 CPU 下同步自旋
  /// 会饿死同线程的迁移驱动推进方（两互等键成活锁），故同步面单次快评：
  /// 迁移推进 / 存活性裁决未落即回 [`IterativeGate::Pending`] 交宿主登记
  /// 等待体挂起重驱（同门另两套形态 evaluate_single_key /
  /// evaluate_multi_key_gate 的 `GateVerdict::Wait` 同口径）。
  ///
  /// `memo` 为挂起重评交接记忆（首评 None）：等待体超时置位 `exhausted`
  /// 后重驱本门评，等待点全部压制、按超时终评 NotOperable（MIGRATING →
  /// ASK），杜绝挂起-重评活锁；磁盘候选键（同步探测 None）经等待体
  /// `exists().await` 裁决入缓存后重评终评——C# Exists 同步等 IO 即时
  /// 裁决，rust 同步面不可达异步探测
  ///
  /// `slot` 为会话库级槽位（整批事务键共槽，键内容不参与定槽）
  fn evaluate_iterative_key_gate(
    &self,
    key: &[u8],
    slot: u16,
    read_only: bool,
    session: SlotVerifySessionState,
    memo: Option<&SlotWaitMemo>,
  ) -> Option<ClusterSlotVerificationState> {
    let config = self.current_config();
    let is_recovering = self.is_recovering();

    // can_operate 组装（同门评内核：仅 MIGRATING 本地臂消费；单次快评
    // 不等待，Pending 交宿主挂起）
    let can_operate = if config.get_state(slot) == SlotState::Migrating
      && config.is_local(slot, read_only && session.read_only_session)
    {
      let ctx = GateCtx {
        read_only,
        session,
        wait_for_stable: false,
        memo,
      };
      match self.resolve_can_operate(key, slot, &ctx, 0) {
        KeyOperable::Operable => true,
        KeyOperable::NotOperable => false,
        // 迁移推进 / 磁盘候选裁决未落：不在同步面等待（自旋饿死迁移
        // 驱动），交宿主登记等待体挂起重驱
        KeyOperable::AccessPending | KeyOperable::ExistsPending => return None,
      }
    } else {
      true
    };

    Some(single_key_slot_verify(
      &config,
      slot,
      read_only,
      session,
      is_recovering,
      can_operate,
    ))
  }

  /// 迭代式槽位校验整批步进（C# 网络迭代校验逐键循环形态：缓存步进 +
  /// 单键验证，供集群会话迭代入口复用）；Pending 不步进缓存（非终态，
  /// 重驱后从头重评）
  pub fn iterative_slot_verify(
    &self,
    cache: &mut IterativeSlotVerifyCache,
    key: &[u8],
    slot: u16,
    read_only: bool,
    session: SlotVerifySessionState,
    memo: Option<&SlotWaitMemo>,
  ) -> IterativeGate {
    match self.evaluate_iterative_key_gate(key, slot, read_only, session, memo) {
      None => IterativeGate::Pending,
      Some(verdict) => IterativeGate::Done(iterative_slot_verify_step(cache, verdict)),
    }
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
    let deadline = now_ms().saturating_add(node_timeout_ms(&self.cluster_provider));
    let keys = req.key_slices();
    loop {
      match self.evaluate_multi_key_gate(
        &keys,
        req.slot,
        req.read_only,
        req.session,
        req.wait_for_stable,
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
