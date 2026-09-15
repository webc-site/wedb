//! Group Commit 公共流水线：协商 / Follower 登记 / Leader 级联循环骨架
//!
//! 对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:CommitTask
//! 与 ongoingCommitRequests：单 Leader 串行物理刷盘，并发提交者折叠为 Follower
//! 挂起等待，批量唤醒，杜绝重复 I/O。领域特有部分（批次目标收集、物理持久化、
//! 水位归属）经 [`GroupCommitStep`] 静态泛型注入，WAL 提交链与 KV 页刷盘链复用。

use std::future::Future;

use crossfire::oneshot::{RxOneshot, TxOneshot, oneshot};
use parking_lot::Mutex;

/// Follower 提交中断哨兵（Leader 物理刷盘失败时批量广播；真实错误由 Leader 侧返回）
#[derive(Debug, thiserror::Error)]
#[error("group commit pipeline broken")]
pub struct Broken;

/// Follower 等待接收端（达标的提交位点或中断哨兵）
pub type CommitRx = RxOneshot<Result<u64, Broken>>;

/// Group Commit 挂起等待者（Follower 登记通道）
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:ongoingCommitRequests
struct Waiter {
  target: u64,
  tx: Option<TxOneshot<Result<u64, Broken>>>,
}

/// 流水线互斥内状态
struct Waiters {
  leading: bool,
  waiters: Vec<Waiter>,
}

/// 协商结果
pub enum Enter {
  /// 目标位点已被当前水位覆盖，直接完成（携带当前水位）
  Done(u64),
  /// 已登记为 Follower，须以 [`GroupCommitPipeline::wait`] 挂起等待批量唤醒
  Follow(CommitRx),
  /// 升级为 Leader，接管物理刷盘管道
  Lead,
}

/// Group Commit 流水线（严格对标 Garnet TsavoriteLog Group Commit）
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:CommitTask
pub struct GroupCommitPipeline {
  waiters: Mutex<Waiters>,
}

impl Default for GroupCommitPipeline {
  fn default() -> Self {
    Self::new()
  }
}

impl GroupCommitPipeline {
  /// 创建空闲流水线（无 Leader、无挂起等待者）
  pub const fn new() -> Self {
    Self {
      waiters: Mutex::new(Waiters {
        leading: false,
        waiters: Vec::new(),
      }),
    }
  }

  /// 状态机协商：判定当前提交者成为 Leader 还是 Follower
  ///
  /// 持锁双重检查水位：目标位点已被覆盖则 [`Enter::Done`]（0 I/O）；
  /// 已有 Leader 在位则登记为 Follower 返回 [`Enter::Follow`]，绝不重复发起 I/O；
  /// 否则升级为 Leader 返回 [`Enter::Lead`]。
  /// `watermark` 在持锁状态下读取最新持久化水位。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:CommitAsync
  pub fn enter(&self, target: u64, watermark: impl FnOnce() -> u64) -> Enter {
    let mut lock = self.waiters.lock();
    let committed = watermark();
    if target <= committed {
      return Enter::Done(committed);
    }
    if lock.leading {
      let (tx, rx) = oneshot::<Result<u64, Broken>>();
      lock.waiters.push(Waiter {
        target,
        tx: Some(tx),
      });
      return Enter::Follow(rx);
    }
    lock.leading = true;
    Enter::Lead
  }

  /// Follower 挂起等待 Leader 批量唤醒（0 重复物理 I/O）
  ///
  /// 通道中断（Leader 异常退出未广播）时以最新水位兜底：目标位点已被并发
  /// 提交覆盖则成功，否则返回中断哨兵。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:WaitForCommitAsync
  pub async fn wait(
    &self,
    rx: CommitRx,
    target: u64,
    watermark: impl Fn() -> u64,
  ) -> Result<u64, Broken> {
    match rx.await {
      Ok(res) => res,
      Err(_) => {
        let committed = watermark();
        if target <= committed {
          Ok(committed)
        } else {
          Err(Broken)
        }
      }
    }
  }

  /// Leader 级联刷盘驱动主循环（Cascade Loop）
  ///
  /// 每轮批量收集领域尾地址与所有挂起 Follower 的最大需求，步进物理持久化，
  /// 原地 retain_mut 批量唤醒达标 Follower（消除额外堆分配）；级联排空后释放
  /// Leader 身份返回；物理错误向全部 Follower 广播中断哨兵并释放身份防死锁。
  pub async fn run_leader<S: GroupCommitStep>(&self, step: S) -> Result<u64, S::Error> {
    let mut last_committed;
    loop {
      // (1) 收集当前批次目标：领域尾地址与所有挂起 Follower 的最大需求
      let batch_target = {
        let lock = self.waiters.lock();
        let max_waiter_target = lock.waiters.iter().map(|w| w.target).max().unwrap_or(0);
        step.tail().max(max_waiter_target)
      };

      let watermark = step.watermark();
      let step_res = if batch_target > watermark {
        step.step(batch_target).await
      } else {
        Ok(watermark)
      };

      match step_res {
        Ok(committed) => {
          last_committed = committed;
          // (2) 原地 retain_mut 筛选批量唤醒达标 Follower
          let mut lock = self.waiters.lock();
          lock.waiters.retain_mut(|waiter| {
            if waiter.target <= committed {
              if let Some(tx) = waiter.tx.take() {
                tx.send(Ok(committed));
              }
              false
            } else {
              true
            }
          });
          // (3) 级联检查：仍有更高水位 Follower 积压或新写入超过已提交位点则继续下一轮
          let has_lagging_waiters = lock.waiters.iter().any(|w| w.target > last_committed);
          let has_new_tail = step.tail() > last_committed;
          if !has_lagging_waiters && !has_new_tail {
            // 管道完全排空，释放 Leader 身份并退出
            lock.leading = false;
            return Ok(last_committed);
          }
        }
        Err(e) => {
          // (4) 物理错误：广播中断哨兵唤醒所有挂起 Follower，释放 Leader 身份防死锁
          let mut lock = self.waiters.lock();
          lock.leading = false;
          for mut waiter in lock.waiters.drain(..) {
            if let Some(tx) = waiter.tx.take() {
              tx.send(Err(Broken));
            }
          }
          return Err(e);
        }
      }
    }
  }
}

/// 领域刷盘步进契约（Leader 级联循环中的领域特有物理持久化，静态泛型分发）
pub trait GroupCommitStep {
  /// 底层 I/O 错误类型
  type Error;

  /// 当前领域写入尾部地址（与挂起 Follower 最大需求合成批次目标）
  fn tail(&self) -> u64;

  /// 当前已持久化水位
  fn watermark(&self) -> u64;

  /// 执行一步物理持久化推进至 target，返回达成的新水位（不小于调用前水位）
  fn step(&self, target: u64) -> impl Future<Output = Result<u64, Self::Error>> + Send;
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicU64, Ordering},
    },
    thread,
  };

  use super::*;
  use crate::future::block_on;

  /// 模拟步进器：step 直接推进水位（无真实 I/O）
  #[derive(Default)]
  struct FakeStep {
    watermark: AtomicU64,
    steps: AtomicU64,
  }

  impl GroupCommitStep for Arc<FakeStep> {
    type Error = Broken;

    fn tail(&self) -> u64 {
      self.watermark.load(Ordering::Acquire)
    }

    fn watermark(&self) -> u64 {
      self.watermark.load(Ordering::Acquire)
    }

    async fn step(&self, target: u64) -> Result<u64, Broken> {
      self.steps.fetch_add(1, Ordering::AcqRel);
      self.watermark.fetch_max(target, Ordering::AcqRel);
      Ok(self.watermark.load(Ordering::Acquire))
    }
  }

  #[test]
  fn test_enter_short_circuit_and_leadership() {
    let pipeline = GroupCommitPipeline::new();
    let watermark = AtomicU64::new(100);

    // 目标已被水位覆盖：Done 携带当前水位
    match pipeline.enter(80, || watermark.load(Ordering::Acquire)) {
      Enter::Done(v) => assert_eq!(v, 100),
      _ => panic!("须命中快速短路"),
    }

    // 未覆盖：升级 Leader
    assert!(matches!(
      pipeline.enter(120, || watermark.load(Ordering::Acquire)),
      Enter::Lead
    ));
  }

  #[test]
  fn test_batch_wake_and_site_consistency() {
    let pipeline = Arc::new(GroupCommitPipeline::new());
    let step = Arc::new(FakeStep::default());

    // 主线程先升级为 Leader（与并发提交者竞争时仅一个 Lead）
    assert!(matches!(pipeline.enter(300, || 0), Enter::Lead));

    // 4 个 Follower 线程登记（同步入队，无竞态）并挂起等待，目标各异
    let mut handles = Vec::new();
    for target in [100u64, 200, 250, 300] {
      let Enter::Follow(rx) = pipeline.enter(target, || step.watermark()) else {
        panic!("Leader 在位时须登记为 Follower");
      };
      let pipeline_bg = Arc::clone(&pipeline);
      let step_bg = Arc::clone(&step);
      handles.push(thread::spawn(move || {
        block_on(pipeline_bg.wait(rx, target, || step_bg.watermark()))
      }));
    }

    // Leader 级联一轮推进到 300：全部 Follower 达标唤醒，水位步进仅合并 1 次
    let last = block_on(pipeline.run_leader(Arc::clone(&step))).unwrap();
    assert_eq!(last, 300);
    assert_eq!(step.steps.load(Ordering::Acquire), 1);

    for handle in handles {
      assert_eq!(handle.join().unwrap().unwrap(), 300);
    }

    // Leader 身份已释放：后续提交可重新升级
    assert!(matches!(pipeline.enter(301, || step.watermark()), Enter::Lead));
  }

  #[test]
  fn test_error_broadcast_releases_leadership() {
    /// 步进恒失败的模拟器
    struct FailingStep;

    impl GroupCommitStep for FailingStep {
      type Error = Broken;

      fn tail(&self) -> u64 {
        500
      }

      fn watermark(&self) -> u64 {
        0
      }

      async fn step(&self, _target: u64) -> Result<u64, Broken> {
        Err(Broken)
      }
    }

    let pipeline = Arc::new(GroupCommitPipeline::new());

    assert!(matches!(pipeline.enter(500, || 0), Enter::Lead));
    let Enter::Follow(rx) = pipeline.enter(500, || 0) else {
      panic!("须登记为 Follower");
    };

    let pipeline_bg = Arc::clone(&pipeline);
    let handle =
      thread::spawn(move || block_on(pipeline_bg.wait(rx, 500, || 0)).unwrap_err());

    // Leader 步进失败：Follower 收中断哨兵，Leader 身份释放
    assert!(block_on(pipeline.run_leader(FailingStep)).is_err());
    let broken = handle.join().unwrap();
    assert_eq!(broken.to_string(), "group commit pipeline broken");
    assert!(matches!(pipeline.enter(1, || 0), Enter::Lead));
  }
}
