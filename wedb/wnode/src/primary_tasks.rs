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
//! 生命周期单轨判定（双轨归一，见 task/done/task-manager-single-lifecycle-track.md）：
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

use std::{
  sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use log::{error, warn};
use parking_lot::Mutex;
use wconf::{RuntimeServerConfig, ServerConfigType};
use wdev::SegmentedDevice;
use wkv::WedbStore;

use crate::{
  aof::GarnetAppendOnlyFile,
  resp::{garnet_api::object_collect_all, objects::tiered_demote::tiered_demote_round},
  storage::session::storage_session::StorageSession,
};

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
  /// 周期提交任务已拉起标志（幂等重拉；对标 TryStartCommitTask）
  commit_started: AtomicBool,
  /// AOF 周期提交任务执行域（首个拉起点绑定）
  commit_env: Mutex<Option<CommitTaskEnv>>,
  /// 周期对象收集任务已拉起标志（幂等重拉）
  object_collect_started: AtomicBool,
  /// 对象收集任务执行域（首个拉起点绑定；None = 装配后尚无会话触发）
  object_collect_env: Mutex<Option<CollectTaskEnv>>,
  /// 周期对象收集的 HCOLLECT 单写位（对标 C# 周期收集专用会话
  /// StoreCollectionDbStorageSession._hcollectTaskLock：周期任务与各连接
  /// 会话的 HCOLLECT 互斥粒度一致——跨会话不互斥，C# 同）
  hcollect_in_progress: AtomicBool,
  /// 周期对象收集的 ZCOLLECT 单写位（对标 _zcollectTaskLock，与 HCOLLECT
  /// 独立两把，C# 同）
  zcollect_in_progress: AtomicBool,
}

impl Default for PrimaryTasks {
  fn default() -> Self {
    Self {
      replica: AtomicBool::new(false),
      commit_started: AtomicBool::new(false),
      commit_env: Mutex::new(None),
      object_collect_started: AtomicBool::new(false),
      object_collect_env: Mutex::new(None),
      hcollect_in_progress: AtomicBool::new(false),
      zcollect_in_progress: AtomicBool::new(false),
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
  pub fn try_start_commit_task(self: &Arc<Self>) -> bool {
    let freq = {
      let env = self.commit_env.lock();
      let Some(e) = env.as_ref() else {
        return false;
      };
      e.runtime_config.get_int(ServerConfigType::AofCommitFreq)
    };
    if freq <= 0 || self.is_replica() {
      return false;
    }
    if self.commit_started.swap(true, Ordering::Relaxed) {
      return false;
    }
    spawn_aof_commit_task(Arc::clone(self));
    true
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
  ///
  /// 返回 true 表示本次调用拉起了任务。
  pub fn try_start_object_collect_task(self: &Arc<Self>) -> bool {
    let freq = {
      let env = self.object_collect_env.lock();
      let Some(e) = env.as_ref() else {
        return false;
      };
      e.runtime_config
        .get_int(ServerConfigType::ExpiredObjectCollectionFreq)
    };
    if freq <= 0 || self.is_replica() {
      return false;
    }
    if self.object_collect_started.swap(true, Ordering::Relaxed) {
      return false;
    }
    spawn_object_collect_task(Arc::clone(self));
    true
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
fn spawn_aof_commit_task(tasks: Arc<PrimaryTasks>) {
  spawn(async move {
    loop {
      let (freq_ms, aof) = {
        let env = tasks.commit_env.lock();
        match env.as_ref() {
          Some(e) => {
            let freq = e.runtime_config.get_int(ServerConfigType::AofCommitFreq);
            let freq = (freq > 0).then_some(freq as u64);
            let aof = e.aof.upgrade();
            (freq, aof)
          }
          None => (None, None),
        }
      };
      let (Some(freq_ms), Some(aof)) = (freq_ms, aof) else {
        tasks.commit_started.store(false, Ordering::Relaxed);
        break;
      };
      // 副本角色：仅 Delay 不提交（C# CommitTaskAsync 副本分支同构）
      // 主分支：先 commit 后 sleep（C# CommitTaskAsync 先 CommitToAofAsync 后 Delay 同构）
      if !tasks.is_replica() {
        aof.log().commit_async().await;
      }
      sleep(Duration::from_millis(freq_ms)).await;
    }
  })
  .detach();
}

/// 周期对象收集后台任务（兼分层键后台降阶评估轮宿主）
///
/// libs/server/StoreWrapper.cs:ObjectCollectTaskAsync 的宿主驱动：进循环
/// 先执行一轮再按 expired-object-collection-freq（秒，最小 1s）间隔收集
/// （C# 循环体先 ExecuteObjectCollection 后 Delay，注册后首轮立即执行，
/// rust 对齐），对全库 Hash/ZSet 对象执行过期字段收集，随后跑一轮分层键
/// 降阶评估（[`tiered_demote_round`] 单内核，承接 doc/zh/collection.md
/// 3.3 后台异步降阶承诺，与手动驱动双入口共享同一执行体）。收集本体复用
/// [`object_collect_all`] 唯一机制（与
/// HCOLLECT/ZCOLLECT `*` 命令路径同源，不另设第二套收集逻辑），互斥沿
/// [`PrimaryTasks`] 的 h/zcollect 单写位（与 C# 周期收集专用会话的
/// collectLock 粒度一致）。频率槽位每轮重读（禁用即退出，CONFIG SET 调停
/// 经 [`PrimaryTasks::try_start_object_collect_task`] 重拉，对标 C#
/// ReconcilePrimaryTask 取消 + 重启采纳新间隔）；副本角色位挂起时轮空
/// （C# 副本不注册该任务）。引擎弱引用每轮升格：在线引擎置换后收集自然
/// 作用于新引擎，引擎释放任务自然退出。
fn spawn_object_collect_task(tasks: Arc<PrimaryTasks>) {
  spawn(async move {
    loop {
      // 进循环先读间隔与禁用位并升格引擎弱引用（单次加锁避免竞争）
      let (freq_secs, store) = {
        let env = tasks.object_collect_env.lock();
        match env.as_ref() {
          Some(e) => {
            let freq = e
              .runtime_config
              .get_int(ServerConfigType::ExpiredObjectCollectionFreq);
            let freq = (freq > 0).then_some(freq as u64);
            let store = e.store.upgrade();
            (freq, store)
          }
          None => (None, None),
        }
      };
      let Some(freq_secs) = freq_secs else {
        // 退出决定与标志翻转同点生效：先落 started 再退出。若翻转滞后到
        // 任务体收尾，disable→enable 快速连续操作会在窗口内读到 started=true
        // 使幂等重拉 swap 失败，随后旧任务落 false 退出——任务永久丢失
        tasks.object_collect_started.store(false, Ordering::Relaxed);
        break;
      };
      // 副本角色挂起：轮空（任务常驻，升主自动恢复）
      if tasks.is_replica() {
        sleep(Duration::from_secs(freq_secs.max(1))).await;
        continue;
      }
      let Some(store) = store else {
        // 同上：退出决定点先落标志（引擎释放自退出让位重拉）
        tasks.object_collect_started.store(false, Ordering::Relaxed);
        break;
      };
      // 先收集后睡（C# ObjectCollectTaskAsync 循环体先 ExecuteObjectCollection
      // 后 Delay，注册后首轮立即执行，rust 对齐）；Hash 与 ZSet 两族先后全库
      // 收集（C# ExecuteObjectCollection 的 ExecuteHashCollect +
      // ExecuteSortedSetCollect 顺序）。单族失败仅 warn 留痕进入下一轮——
      // 刻意差异于 C#（未知异常记 CRITICAL 后整个任务退出不再重启）：rust
      // 失败粒度到单族，瞬时存储错误不放大为周期收集永久停摆
      collect_family(&tasks, &store, true).await;
      collect_family(&tasks, &store, false).await;
      // 分层键后台懒降阶评估轮（doc/zh/collection.md 3.3「后台紧缩异步降阶」）：
      // 冷分层键（升阶后再无前台写触碰）的唯一降阶评估点，复用本任务频率节拍、
      // 副本挂起与弱引用自退出闸门，不放第二套调度器、不新增配置旋钮；
      // 单轮限批、零命中静默（观测与判定纪律见 [`tiered_demote_round`] 模块头）
      tiered_demote_round(&store).await;
      sleep(Duration::from_secs(freq_secs.max(1))).await;
    }
  })
  .detach();
}

/// 单族全库收集轮次（单写位 CAS 抢占 → 独立批处理纪元内收集 → 释放）
async fn collect_family(
  tasks: &PrimaryTasks,
  store: &Arc<WedbStore<SegmentedDevice>>,
  is_hash: bool,
) {
  let in_progress = if is_hash {
    &tasks.hcollect_in_progress
  } else {
    &tasks.zcollect_in_progress
  };
  if in_progress
    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
    .is_err()
  {
    return;
  }
  // 每轮独立会话 + 只读批处理纪元（对标 C# 慢路径收集会话形态；引擎置换
  // 后下轮自然作用于新引擎）
  let res = match store.new_session() {
    Ok(session) => {
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      object_collect_all(&storage, is_hash).await
    }
    Err(e) => {
      error!("周期对象收集存储会话创建失败: {e}");
      in_progress.store(false, Ordering::Release);
      return;
    }
  };
  in_progress.store(false, Ordering::Release);
  if let Err(err) = res {
    warn!(
      "周期对象收集失败（{cmd} 族），留待下轮: {err}",
      cmd = if is_hash { "HCOLLECT" } else { "ZCOLLECT" }
    );
  }
}
