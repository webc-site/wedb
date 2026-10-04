//! Primary 类后台任务的生命周期域
//!
//! 对标 libs/server/StoreWrapper.cs 的 StartPrimaryTasks /
//! SuspendPrimaryOnlyTasksAsync / ReconcilePrimaryTask 家族与
//! libs/server/TaskManager/TaskPlacementCategory.cs 的 Primary 类约束：
//! Primary 类任务（周期提交 / 周期对象收集 / 体积限额检查 / 过期键 GC 扫描）
//! 仅应在主角色运行，角色切换时批量挂起或恢复。
//!
//! 机制（一处定义）：[`PrimaryTasks`] 持有副本角色位与任务幂等启动位，
//! 周期任务循环每轮门检角色位——挂起 = 置位后轮次轮空（任务常驻空转，
//! 升主自动恢复，对标 C# 副本角色下任务体仅 Delay 的空转形态），禁用 =
//! 频率槽位 <= 0 时循环自退出（CONFIG SET 调停重拉）。与 C#「类别取消 +
//! StartPrimaryTasks 重启」的等价性：C# 重启后重读 RuntimeServerConfig
//! 采纳新间隔；rust 循环每轮重读槽位，同一终态少一次任务销毁重建。
//! GC 扫描/紧缩任务的挂起与恢复由 [`wkv::WedbStore`] 的 stop_gc /
//! start_gc 承接（见 wedb 集群层 suspend/resume 接线点）。
//!
//! 生命周期单轨判定（双轨归一；在册载体为
//! js/check/ignore/garnet/libs/server/TaskManager/TaskManager.yml 理由行，原落册票
//! task/done/task-manager-single-lifecycle-track.md 不在五池存活、判据不可回收，论证正文随本头自注存活）：
//! C# TaskManager.cs 的注册表托管面（RegisterAndRun/CancelAsync/Dispose）判定
//! 不移植——其单一注册表成立的前提是 .NET 全局线程池可跨线程注册/取消，rust
//! compio 为一线程一运行时，任务控制点（首会话惰性拉起 / CONFIG SET / 角色切换 /
//! 停机 join）均在任意 worker 或集群线程，线程局部注册表无法覆盖，强行收编需
//! 新造跨线程取消机制，超出 C# 复杂度。后台任务启停/取消/观测的唯一轨由本域
//! 角色位 + 频率槽位每轮重读 + 弱引用自退出承接；ExpiredKeyDeletion/Compaction
//! 由 wkv 引擎 GC 域（stop_gc/start_gc/reconcile_gc_scan）承接；pubsub 消费与
//! lua timeout tick 在 C# 本就不入注册表（SubscribeBroker/LuaTimeoutManager 各
//! 持专属取消位），随宿主 runtime 析构收敛。
//!
//! 注意：被挂起的是主动 GC 扫描与周期对象收集，副本读路径的惰性过期
//! （ttl probe_alive）与确定性的 TtlPurge 重放不受影响——TTL 裁决不在
//! 本域。

// TaskExitGate 测试闸门仅 debug 装配（release 剔除防 unused imports）
#[cfg(debug_assertions)]
use std::time::Instant;
use std::{
  sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use log::{error, warn};
#[cfg(debug_assertions)]
use parking_lot::Condvar;
use parking_lot::Mutex;
use wbase::supervise::supervise_resumable;
use wconf::{RuntimeServerConfig, ServerConfigType};
use wdev::SegmentedDevice;
use wkv::WedbStore;

use crate::{
  aof::GarnetAppendOnlyFile,
  resp::{
    garnet_api::{CollectLockGuard, object_collect_all},
    objects::tiered_demote::tiered_demote_round,
  },
};

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const AOF_COMMIT_TASK: &str = "aof_commit";
/// 同上（周期对象收集任务）
const OBJECT_COLLECT_TASK: &str = "object_collect";

/// 对象收集任务执行域（首个拉起点绑定；在线引擎置换后经
/// [`PrimaryTasks::bind_object_collect_env`] 刷新指向）
struct CollectTaskEnv {
  /// 存储引擎弱引用（弱引用自退出模式：引擎释放任务自然收敛）
  store: Weak<WedbStore<SegmentedDevice>>,
  /// 运行时配置（装配终态 Arc：expired-object-collection-freq 槽位每轮重读）
  runtime_config: Arc<RuntimeServerConfig>,
}

/// AOF 周期提交任务执行域
struct CommitTaskEnv {
  /// AOF 门面弱引用
  aof: Weak<GarnetAppendOnlyFile>,
  /// 运行时配置（aof-commit-freq 槽位每轮重读）
  runtime_config: Arc<RuntimeServerConfig>,
}

/// Primary 类后台任务生命周期域（服务级共享单例；对标 StoreWrapper 的
/// taskLifecycleLock 串行化域 + Primary 类任务注册表）
pub struct PrimaryTasks {
  /// 副本角色位（true = 当前副本角色，Primary 类任务挂起；对标 C#
  /// clusterProvider.IsReplica 的任务域投影，角色切换点批量翻转）
  replica: AtomicBool,
  /// 周期提交任务已拉起标志（幂等重拉；对标 TryStartCommitTask；Arc 供
  /// [`wbase::supervise::supervise_resumable`] panic 臂复位收口）
  commit_started: Arc<AtomicBool>,
  /// AOF 周期提交任务执行域（首个拉起点绑定）
  commit_env: Mutex<Option<CommitTaskEnv>>,
  /// 周期对象收集任务已拉起标志（幂等重拉；Arc 同上）
  object_collect_started: Arc<AtomicBool>,
  /// 对象收集任务执行域（首个拉起点绑定；None = 装配后尚无会话触发）
  object_collect_env: Mutex<Option<CollectTaskEnv>>,
  /// 周期对象收集的 HCOLLECT 单写位（对标 C# 周期收集专用会话
  /// StoreCollectionDbStorageSession._hcollectTaskLock：周期任务与各连接
  /// 会话的 HCOLLECT 互斥粒度一致——跨会话不互斥，C# 同）
  pub hcollect_in_progress: AtomicBool,
  /// 周期对象收集的 ZCOLLECT 单写位（对标 _zcollectTaskLock，与 HCOLLECT
  /// 独立两把，C# 同）
  pub zcollect_in_progress: AtomicBool,
  /// 测试注入槽（debug 断言形态专用，doc(hidden) 非公开契约）：AOF 提交
  /// 任务退出臂闸门
  #[cfg(debug_assertions)]
  #[doc(hidden)]
  pub commit_exit_gate: TaskExitGate,
  /// 同上：周期对象收集任务退出臂闸门
  #[cfg(debug_assertions)]
  #[doc(hidden)]
  pub object_collect_exit_gate: TaskExitGate,
}

impl Default for PrimaryTasks {
  fn default() -> Self {
    Self {
      replica: AtomicBool::new(false),
      commit_started: Arc::new(AtomicBool::new(false)),
      commit_env: Mutex::new(None),
      object_collect_started: Arc::new(AtomicBool::new(false)),
      object_collect_env: Mutex::new(None),
      hcollect_in_progress: AtomicBool::new(false),
      zcollect_in_progress: AtomicBool::new(false),
      #[cfg(debug_assertions)]
      commit_exit_gate: TaskExitGate::default(),
      #[cfg(debug_assertions)]
      object_collect_exit_gate: TaskExitGate::default(),
    }
  }
}

impl PrimaryTasks {
  /// 当前是否处于副本角色（Primary 类任务挂起中）
  #[inline]
  pub fn is_replica(&self) -> bool {
    self.replica.load(Ordering::Acquire)
  }

  /// 翻转副本角色位（角色切换点调用；true = 挂起 Primary 类任务）
  #[inline]
  pub fn set_replica(&self, replica: bool) {
    self.replica.store(replica, Ordering::Release);
  }

  /// 降副本挂起 Primary 类任务（对标 StoreWrapper.SuspendPrimaryOnlyTasksAsync
  /// 的角色位投影：置位后周期任务轮次轮空；GC 扫描由调用方 stop_gc 承接）
  #[inline]
  pub fn suspend(&self) {
    self.set_replica(true);
  }

  /// 升主恢复 Primary 类任务（对标 StoreWrapper.StartPrimaryTasks 的恢复
  /// 语义：角色位翻转 + 执行域刷新 + 幂等重拉周期对象收集；GC 扫描由
  /// 调用方 start_gc 承接，周期提交任务常驻门检自恢复）
  pub fn resume(self: &Arc<Self>, store: &Arc<WedbStore<SegmentedDevice>>) {
    self.set_replica(false);
    self.bind_object_collect_env(store, None);
    self.try_start_object_collect_task();
    self.try_start_commit_task();
  }

  /// 周期对象收集任务是否在跑（对标 C# TaskManager.IsRunning 的观测面；
  /// 禁用退出或引擎释放后为 false）
  #[inline]
  pub fn object_collect_running(&self) -> bool {
    self.object_collect_started.load(Ordering::Acquire)
  }

  /// AOF 周期提交任务是否在跑（对标 C# TaskManager.IsRunning 的观测面；
  /// 禁用退出后为 false）
  #[inline]
  pub fn commit_running(&self) -> bool {
    self.commit_started.load(Ordering::Acquire)
  }

  /// 绑定/刷新 AOF 提交任务执行域
  pub fn bind_commit_env(
    &self,
    aof: &Arc<GarnetAppendOnlyFile>,
    runtime_config: &Arc<RuntimeServerConfig>,
  ) {
    let mut env = self.commit_env.lock();
    *env = Some(CommitTaskEnv {
      aof: Arc::downgrade(aof),
      runtime_config: Arc::clone(runtime_config),
    });
  }

  /// 拉起 AOF 周期提交任务（幂等）
  ///
  /// libs/server/StoreWrapper.cs:TryStartCommitTask（`commit_ms > 0` 注册）
  /// 的 rust 形态：从 RuntimeServerConfig 槽位重读 AofCommitFreq，
  /// 若 `freq <= 0` 或处于副本角色则不拉起。
  /// 退出判定、标志翻转、重拉判定三点同在 commit_env 锁内串行（C# taskLifecycleLock 对位）。
  pub fn try_start_commit_task(self: &Arc<Self>) -> bool {
    if self.is_replica() {
      return false;
    }
    let should_spawn = {
      let env = self.commit_env.lock();
      let Some(e) = env.as_ref() else {
        return false;
      };
      let freq = e.runtime_config.get_int(ServerConfigType::AofCommitFreq);
      if freq <= 0 {
        return false;
      }
      !self.commit_started.swap(true, Ordering::AcqRel)
    };
    if should_spawn {
      spawn_aof_commit_task(Arc::clone(self));
      true
    } else {
      false
    }
  }

  /// 绑定/刷新对象收集任务执行域（首个会话惰性触发；装配终态的配置与
  /// 引擎。在线引擎置换后传入新引擎引用即刷新指向）
  pub fn bind_object_collect_env(
    &self,
    store: &Arc<WedbStore<SegmentedDevice>>,
    runtime_config: Option<&Arc<RuntimeServerConfig>>,
  ) {
    let mut env = self.object_collect_env.lock();
    match env.as_mut() {
      Some(e) => {
        e.store = Arc::downgrade(store);
        if let Some(rc) = runtime_config {
          e.runtime_config = Arc::clone(rc);
        }
      }
      None => {
        let Some(rc) = runtime_config else { return };
        *env = Some(CollectTaskEnv {
          store: Arc::downgrade(store),
          runtime_config: Arc::clone(rc),
        });
      }
    }
  }

  /// 拉起周期对象收集任务（幂等；执行域自 [`Self::bind_object_collect_env`]
  /// 绑定取用）
  ///
  /// libs/server/StoreWrapper.cs:TryStartObjectCollectTask（频率 <= 0 禁用）
  /// 的 rust 形态。执行域未绑定（装配后尚无会话触发，由下一会话惰性绑定
  /// 后重拉）或当前副本角色（对标 C# ReconcilePrimaryTask「副本上不启动，
  /// 升主经 StartPrimaryTasks 重启」，由 [`Self::resume`] 承接）时不拉起。
  /// 退出判定、标志翻转、重拉判定三点同在 object_collect_env 锁内串行（C# taskLifecycleLock 对位）。
  ///
  /// 注：本任务兼分层键后台降阶评估轮（[`tiered_demote_round`]）唯一生产宿主——
  /// 槽位 <= 0（缺省 0）时过期收集与降阶评估一并不跑，旋钮兼职登记见
  /// doc/zh/deviations.md §120。
  ///
  /// 返回 true 表示本次调用拉起了任务。
  pub fn try_start_object_collect_task(self: &Arc<Self>) -> bool {
    if self.is_replica() {
      return false;
    }
    let should_spawn = {
      let env = self.object_collect_env.lock();
      let Some(e) = env.as_ref() else {
        return false;
      };
      let freq = e
        .runtime_config
        .get_int(ServerConfigType::ExpiredObjectCollectionFreq);
      if freq <= 0 {
        return false;
      }
      !self.object_collect_started.swap(true, Ordering::AcqRel)
    };
    if should_spawn {
      spawn_object_collect_task(Arc::clone(self));
      true
    } else {
      false
    }
  }
}

/// AOF 周期提交后台任务
///
/// libs/server/StoreWrapper.cs:CommitTaskAsync 的宿主驱动：每轮循环从
/// `RuntimeServerConfig` 重读 `AofCommitFreq` 槽位，按其节拍驱动
/// `GarnetLog::commit_async`。
///
/// 频率槽位 <= 0 时自退出并将 `commit_started` 置为 false（对标 C# 取消任务）；
/// CONFIG SET 经调停消息调用 [`PrimaryTasks::try_start_commit_task`] 重新拉起。
/// 副本角色位挂起时仅 Delay 不提交（与 C# CommitTaskAsync 副本分支逐行同构）。
/// 任务体经 wbase [`supervise_task`] 顶层监督（单点一次成型）：panic 臂落
/// log::error + 监督快照计数后复位 `commit_started`，由既有 CONFIG SET 调停与
/// `resume` 重拉通路自然复活（对标 C# 异常必经 catch 落日志后 IsRunning 可查）。
fn spawn_aof_commit_task(tasks: Arc<PrimaryTasks>) {
  spawn(supervise_resumable(
    AOF_COMMIT_TASK,
    Arc::clone(&tasks.commit_started),
    aof_commit_loop(Arc::clone(&tasks)),
  ))
  .detach();
}

/// 后台任务退出臂测试闸门（debug 断言形态专用注入槽；doc(hidden) 非公开
/// 契约，release 形态编译消除）。
///
/// 语义：任务循环退出臂在环境锁（commit_env / object_collect_env）内、
/// `started` 标志翻转前命中 [`Self::hit`]——通知测试已到达并挂起至
/// [`Self::release`]，锁测据此断言「持锁挂起期间标志未翻转、并发重拉在
/// 锁外排队」。原 `Arc<dyn Fn>` 全局静态钩子（全仓唯一 dyn 测试钩子）随
/// 测试体迁 tests/ 收编为本非 dyn 注入槽。
#[cfg(debug_assertions)]
#[doc(hidden)]
#[derive(Default)]
pub struct TaskExitGate {
  /// 闸门状态机：Idle（未注入/直通）→ Entered（任务命中退出臂）→ Released。
  state: Mutex<GateState>,
  /// 状态翻转唤醒器。
  signal: Condvar,
}

/// [`TaskExitGate`] 状态机。
#[cfg(debug_assertions)]
#[doc(hidden)]
#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum GateState {
  /// 未注入（任务侧直通，零阻塞；生产 debug 构建缺省态）。
  #[default]
  Idle,
  /// 测试已注入，等待任务命中退出臂。
  Armed,
  /// 任务已命中退出臂并在环境锁内挂起。
  Entered,
  /// 测试已放行（终态，闸门再注入经 [`TaskExitGate::reset`]）。
  Released,
}

#[cfg(debug_assertions)]
#[doc(hidden)]
impl TaskExitGate {
  /// 测试侧：注入闸门（置 Armed 待任务命中；单实例可重复注入）
  pub fn reset(&self) {
    *self.state.lock() = GateState::Armed;
  }

  /// 测试侧：等待任务命中退出臂（未注入即直通态超时返回 false）
  pub fn wait_entered(&self, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut guard = self.state.lock();
    while *guard == GateState::Armed {
      if self
        .signal
        .wait_while_until(&mut guard, |s| *s == GateState::Armed, deadline)
        .timed_out()
        && *guard == GateState::Armed
      {
        return false;
      }
    }
    true
  }

  /// 测试侧：放行命中退出臂的任务（循环继续收尾翻转 started 并退锁）
  pub fn release(&self) {
    *self.state.lock() = GateState::Released;
    self.signal.notify_all();
  }

  /// 任务侧：命中退出臂——注入态（Armed）转 Entered 通知测试并在环境锁内
  /// 挂起至放行，未注入（Idle/终态）零阻塞直通
  fn hit(&self) {
    let mut guard = self.state.lock();
    if *guard != GateState::Armed {
      return;
    }
    *guard = GateState::Entered;
    self.signal.notify_all();
    self
      .signal
      .wait_while(&mut guard, |s| *s != GateState::Released);
  }
}

/// [`spawn_aof_commit_task`] 的循环体（监督对象）
async fn aof_commit_loop(tasks: Arc<PrimaryTasks>) {
  loop {
    let next = {
      let env = tasks.commit_env.lock();
      match env.as_ref() {
        Some(e) => {
          let freq = e.runtime_config.get_int(ServerConfigType::AofCommitFreq);
          if freq > 0 {
            if let Some(aof) = e.aof.upgrade() {
              Some((freq as u64, aof))
            } else {
              tasks.commit_started.store(false, Ordering::Release);
              None
            }
          } else {
            // 退出臂：频率 <= 0 禁用，标志翻转与判定在 commit_env 锁内同锁串行
            #[cfg(debug_assertions)]
            tasks.commit_exit_gate.hit();
            tasks.commit_started.store(false, Ordering::Release);
            None
          }
        }
        None => {
          tasks.commit_started.store(false, Ordering::Release);
          None
        }
      }
    };
    let Some((freq_ms, aof)) = next else {
      break;
    };
    // 副本角色：仅 Delay 不提交（C# CommitTaskAsync 副本分支同构）
    // 主分支：先 commit 后 sleep（C# CommitTaskAsync 先 CommitToAofAsync 后 Delay 同构）
    if !tasks.is_replica() {
      aof.log().commit_async().await;
    }
    sleep(Duration::from_millis(freq_ms)).await;
  }
}

/// 周期对象收集后台任务（兼分层键后台降阶评估轮宿主）
///
/// libs/server/StoreWrapper.cs:ObjectCollectTaskAsync 的宿主驱动：进循环
/// 先执行一轮再按 expired-object-collection-freq（秒，最小 1s）间隔收集
/// （C# 循环体先 ExecuteObjectCollection 后 Delay，注册后首轮立即执行，
/// rust 对齐），对全库 Hash/ZSet 对象执行过期字段收集，随后跑一轮分层键
/// 降阶评估（[`tiered_demote_round`] 单内核，承接 doc/zh/collection.md
/// 3.3 后台降阶评估轮承诺的唯一执行体；命令面无第二管理入口，旧「与手动
/// 驱动双入口」宣称系失实已订正，本任务兼降阶宿主之旋钮兼职登记见
/// doc/zh/deviations.md §120）。收集本体复用
/// [`object_collect_all`] 唯一机制（与
/// HCOLLECT/ZCOLLECT `*` 命令路径同源，不另设第二套收集逻辑），互斥沿
/// [`PrimaryTasks`] 的 h/zcollect 单写位（与 C# 周期收集专用会话的
/// collectLock 粒度一致）。频率槽位每轮重读（禁用即退出，CONFIG SET 调停
/// 经 [`PrimaryTasks::try_start_object_collect_task`] 重拉，对标 C#
/// ReconcilePrimaryTask 取消 + 重启采纳新间隔）；副本角色位挂起时轮空
/// （C# 副本不注册该任务）。引擎弱引用每轮升格：在线引擎置换后收集自然
/// 作用于新引擎，引擎释放任务自然退出。
fn spawn_object_collect_task(tasks: Arc<PrimaryTasks>) {
  spawn(supervise_resumable(
    OBJECT_COLLECT_TASK,
    Arc::clone(&tasks.object_collect_started),
    object_collect_loop(Arc::clone(&tasks)),
  ))
  .detach();
}

/// [`spawn_object_collect_task`] 的循环体（监督对象）
async fn object_collect_loop(tasks: Arc<PrimaryTasks>) {
  loop {
    // 进循环先读间隔与禁用位并升格引擎弱引用（单次加锁避免竞争）
    let next = {
      let env = tasks.object_collect_env.lock();
      match env.as_ref() {
        Some(e) => {
          let freq = e
            .runtime_config
            .get_int(ServerConfigType::ExpiredObjectCollectionFreq);
          if freq <= 0 {
            // 退出臂 1：频率 <= 0 禁用，标志翻转与判定在 object_collect_env 锁内同锁串行
            #[cfg(debug_assertions)]
            tasks.object_collect_exit_gate.hit();
            tasks.object_collect_started.store(false, Ordering::Release);
            None
          } else {
            match e.store.upgrade() {
              Some(store) => Some((freq as u64, store)),
              None => {
                // 退出臂 2：引擎释放自退出，标志翻转同在 object_collect_env 锁内
                tasks.object_collect_started.store(false, Ordering::Release);
                None
              }
            }
          }
        }
        None => {
          tasks.object_collect_started.store(false, Ordering::Release);
          None
        }
      }
    };
    let Some((freq_secs, store)) = next else {
      break;
    };
    // 副本角色挂起：轮空（任务常驻，升主自动恢复）
    if tasks.is_replica() {
      sleep(Duration::from_secs(freq_secs.max(1))).await;
      continue;
    }
    // 先收集后睡（C# ObjectCollectTaskAsync 循环体先 ExecuteObjectCollection
    // 后 Delay，注册后首轮立即执行，rust 对齐）；Hash 与 ZSet 两族先后全库
    // 收集（C# ExecuteObjectCollection 的 ExecuteHashCollect +
    // ExecuteSortedSetCollect 顺序）。单族失败仅 warn 留痕进入下一轮——
    // 刻意差异于 C#（未知异常记 CRITICAL 后整个任务退出不再重启）：rust
    // 失败粒度到单族，瞬时存储错误不放大为周期收集永久停摆
    collect_family(&tasks, &store, true).await;
    collect_family(&tasks, &store, false).await;
    // 分层键后台懒降阶评估轮（doc/zh/collection.md 3.3「后台降阶评估轮」）：
    // 冷分层键（升阶后再无前台写触碰）的唯一降阶评估点，复用本任务频率节拍、
    // 副本挂起与弱引用自退出闸门，不放第二套调度器、不新增配置旋钮；缺省
    // freq=0 任务不启动即本轮不跑（旋钮兼职登记见 doc/zh/deviations.md §120）；
    // 单轮限批、零命中静默（观测与判定纪律见 [`tiered_demote_round`] 模块头）
    tiered_demote_round(&store).await;
    sleep(Duration::from_secs(freq_secs.max(1))).await;
  }
}

/// 单族全库收集轮次（逐活跃域对齐：单写位每域一取一放 → 独立批处理纪元内收集 → 释放）
pub async fn collect_family(
  tasks: &PrimaryTasks,
  store: &Arc<WedbStore<SegmentedDevice>>,
  is_hash: bool,
) {
  let in_progress = if is_hash {
    &tasks.hcollect_in_progress
  } else {
    &tasks.zcollect_in_progress
  };
  let domains = store.vdb.list_active_virtual_dbs();
  for (vns, vdb) in domains {
    if tasks.is_replica() {
      break;
    }
    let Some(_guard) = CollectLockGuard::try_acquire(in_progress) else {
      continue;
    };
    // 每域独立会话（域对齐活跃虚拟域快照；批窗口由 object_collect_all 逐批自管，
    // 批间纪元让步；对标 slow.rs 慢路径收集会话形态；冷域跳过待首访装载）
    let res = match store.new_session() {
      Ok(session) => {
        // 直设域须显式携逻辑域（版本轨=逻辑域种子）：活跃虚拟域经
        // version_domain_of 一次换算即映射真值，固着进会话逻辑槽
        let (lns, ldb) = store.vdb.version_domain_of(vns, vdb);
        session.set_virtual_context(vns, vdb, lns, ldb);
        object_collect_all(session, is_hash).await
      }
      Err(e) => {
        error!("周期对象收集存储会话创建失败: {e}");
        continue;
      }
    };
    drop(_guard);
    if let Err(err) = res {
      warn!(
        "周期对象收集失败（{cmd} 族，域 ({vns}, {vdb})），留待下轮: {err}",
        cmd = if is_hash { "HCOLLECT" } else { "ZCOLLECT" }
      );
    }
  }
}
