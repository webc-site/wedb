use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::runtime::spawn;
use parking_lot::RwLock;

use crate::server::{
  cluster_provider::ClusterProvider,
  failover::{
    failover_option::FailoverOption, failover_session::FailoverSession,
    failover_status::FailoverStatus,
  },
};

/// libs/cluster/Server/Failover/FailoverManager.cs:FailoverManager
///
/// 同一时刻至多一个 failover 会话在跑：任务锁为原子标志（C#
/// SingleWriterMultiReaderLock 的 try-lock 语义），在发起时同步获取、由
/// 后台会话任务完成时释放。会话经 compio 后台任务驱动，终态回写
/// `last_failover_status`——对齐 C# Task.Run 中
/// `lastFailoverStatus = success ? FAILOVER_COMPLETED : FAILOVER_ABORTED`。
pub struct FailoverManager {
  cluster_provider: Arc<ClusterProvider>,
  current_failover_session: RwLock<Option<Arc<FailoverSession>>>,
  failover_task_lock: AtomicBool,
  pub last_failover_status: RwLock<FailoverStatus>,
}

impl FailoverManager {
  /// libs/cluster/Server/Failover/FailoverManager.cs:FailoverManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      current_failover_session: RwLock::new(None),
      failover_task_lock: AtomicBool::new(false),
      last_failover_status: RwLock::new(FailoverStatus::NoFailover),
    }
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:Dispose
  pub fn dispose(&self) {
    self.reset();
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryAbortReplicaFailover
  pub fn try_abort_replica_failover(&self) {
    self.reset();
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:Reset
  ///
  /// 差异：C# 在此同时解锁任务锁（跨线程解锁），本实现的任务锁由后台任务
  /// 完成时释放——abort 只摘除会话，不放行新 failover，杜绝 C# 旧任务
  /// 尚在跑时新会话并发启动的竞态
  fn reset(&self) {
    if let Some(s) = self.current_failover_session.write().take() {
      s.dispose();
    }
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:GetFailoverStatus
  pub fn get_failover_status(&self) -> String {
    let session = self.current_failover_session.read();
    session
      .as_ref()
      .map_or(FailoverStatus::NoFailover, |s| s.status())
      .get_failover_status()
      .to_string()
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:GetLastFailoverStatus
  pub fn get_last_failover_status(&self) -> String {
    self
      .last_failover_status
      .read()
      .get_failover_status()
      .to_string()
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryStartReplicaFailover
  pub fn try_start_replica_failover(
    self: &Arc<Self>,
    option: FailoverOption,
    failover_timeout: Duration,
  ) -> bool {
    if self.failover_task_lock.swap(true, Ordering::Acquire) {
      return false;
    }

    *self.last_failover_status.write() = FailoverStatus::BeginFailover;

    let cluster_timeout = Duration::from_secs(60); // mock
    let session = Arc::new(FailoverSession::new(
      self.cluster_provider.clone(),
      option,
      cluster_timeout,
      failover_timeout,
      true,
      "",
      -1,
    ));
    *self.current_failover_session.write() = Some(Arc::clone(&session));

    let this = Arc::clone(self);
    spawn(async move {
      let success = session.begin_async_replica_failover_async().await;
      *this.last_failover_status.write() = if success {
        FailoverStatus::FailoverCompleted
      } else {
        FailoverStatus::FailoverAborted
      };
      session.dispose();
      *this.current_failover_session.write() = None;
      this.failover_task_lock.store(false, Ordering::Release);
    })
    .detach();
    true
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryStartPrimaryFailover
  pub fn try_start_primary_failover(
    self: &Arc<Self>,
    replica_address: &str,
    replica_port: i32,
    option: FailoverOption,
    timeout: Duration,
  ) -> bool {
    if self.failover_task_lock.swap(true, Ordering::Acquire) {
      return false;
    }

    let cluster_timeout = Duration::from_secs(60); // mock
    let session = Arc::new(FailoverSession::new(
      self.cluster_provider.clone(),
      option,
      cluster_timeout,
      timeout,
      false,
      replica_address,
      replica_port,
    ));
    *self.current_failover_session.write() = Some(Arc::clone(&session));

    let this = Arc::clone(self);
    spawn(async move {
      let _ = session.begin_async_primary_failover_async().await;
      session.dispose();
      *this.current_failover_session.write() = None;
      this.failover_task_lock.store(false, Ordering::Release);
    })
    .detach();
    true
  }
}

#[cfg(test)]
mod tests {
  use compio::{runtime::Runtime, time::sleep};

  use super::*;

  /// 轮询等待后台会话任务收敛（mock 会话数毫秒内完成）
  async fn wait_until(m: &FailoverManager, expect: &str) {
    for _ in 0..200 {
      if m.get_last_failover_status() == expect {
        return;
      }
      sleep(Duration::from_millis(5)).await;
    }
    panic!("后台会话未收敛: last={}", m.get_last_failover_status());
  }

  /// DEFAULT 副本 failover 全流程：终态 completed、会话摘除、任务锁释放
  #[test]
  fn replica_failover_completes_and_releases_lock() -> aok::Void {
    Runtime::new()?.block_on(async {
      let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider {})));
      assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::ZERO));
      wait_until(&m, "failover-completed").await;

      // 会话结束后状态归位 no-failover，任务锁已释放可再次发起
      assert_eq!(m.get_failover_status(), "no-failover");
      assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
      wait_until(&m, "failover-completed").await;
      aok::OK
    })
  }

  /// 主端发起的 failover：C# 主路径不回写 lastFailoverStatus
  #[test]
  fn primary_failover_leaves_last_status() -> aok::Void {
    Runtime::new()?.block_on(async {
      let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider {})));
      assert!(m.try_start_primary_failover(
        "10.0.0.2",
        7000,
        FailoverOption::Takeover,
        Duration::ZERO
      ));
      // 等会话跑完（状态归位 no-failover 即任务已收敛）
      for _ in 0..200 {
        if m.get_failover_status() == "no-failover"
          && m
            .last_failover_status
            .read()
            .eq(&FailoverStatus::NoFailover)
        {
          break;
        }
        sleep(Duration::from_millis(5)).await;
      }
      assert_eq!(m.get_failover_status(), "no-failover");
      assert_eq!(m.get_last_failover_status(), "no-failover");
      aok::OK
    })
  }

  /// 运行中互斥：任务锁在发起时同步获取，后台任务收敛释放前二次启动被拒
  #[test]
  fn concurrent_start_rejected_until_task_finishes() -> aok::Void {
    Runtime::new()?.block_on(async {
      let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider {})));
      assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
      // spawn 的后台任务尚未被调度，任务锁必被持有：二次启动确定性被拒
      assert!(
        !m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO),
        "任务运行期间互斥"
      );
      // 被拒路径不回写终态，仍停在发起态
      assert_eq!(m.get_last_failover_status(), "begin-failover");

      wait_until(&m, "failover-completed").await;
      // 锁释放后可再次发起
      assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
      wait_until(&m, "failover-completed").await;
      aok::OK
    })
  }

  /// 中止：abort 摘除会话引用（状态查询立归 no-failover），后台任务仍收敛
  /// 并释放任务锁，管理器回到可服务状态
  #[test]
  fn abort_detaches_session_and_lock_recovers() -> aok::Void {
    Runtime::new()?.block_on(async {
      let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider {})));
      assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));

      // abort 在任务调度前调用：会话被同步摘除
      m.try_abort_replica_failover();
      assert_eq!(m.get_failover_status(), "no-failover", "会话摘除后状态归位");

      // 后台任务持有会话 Arc 照常收敛：终态回写、任务锁释放
      wait_until(&m, "failover-completed").await;
      assert!(
        m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO),
        "abort 后任务锁最终释放"
      );
      wait_until(&m, "failover-completed").await;
      aok::OK
    })
  }
}
