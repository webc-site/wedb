//! Group Commit 公共流水线：协商 / Follower 登记 / Leader 级联循环骨架
//!
//! 对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:CommitTask
//! 与 ongoingCommitRequests：单 Leader 串行物理刷盘，并发提交者折叠为 Follower
//! 挂起等待，批量唤醒，杜绝重复 I/O。领域特有部分（批次目标收集、物理持久化、
//! 水位归属）经 [`GroupCommitStep`] 静态泛型注入，WAL 提交链与 KV 页刷盘链复用。

use std::future::Future;

use crossfire::oneshot::{RxOneshot, TxOneshot, oneshot};
use parking_lot::Mutex;

use crate::future::yield_now;

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

/// Leader 身份守卫：[`GroupCommitPipeline::run_leader`] 入口构造，覆盖级联
/// 循环全部退出形态。panic unwind（工作区 panic 维持 unwind，drive.rs 泵层
/// catch_unwind 隔离单会话）或 future 被取消丢弃打断 Leader 刷盘链时，裸
/// `leading` 恒真——此后全部 `enter` 必入 Follow 臂，Follower 的 tx 存活于
/// waiters 永不 send，通道中断兜底臂永不触发，写平面永久砖化。Drop 收口
/// 复用错误臂广播单点：持锁复位 leading 并对 tx 存活 Follower 广播
/// [`Broken`]（经既有通道中断兜底臂按水位判定成功或回 Broken 上抛）。
/// 排空/错误两条正常臂收尾后 [`LeaderGuard::disarm`] 解除，防误复位后继
/// Leader 身份
struct LeaderGuard<'a> {
  pipeline: &'a GroupCommitPipeline,
  /// 正常臂已收尾（排空复位 / 错误广播），drop 为 no-op
  armed: bool,
}

impl Drop for LeaderGuard<'_> {
  fn drop(&mut self) {
    if !self.armed {
      return;
    }
    let mut lock = self.pipeline.waiters.lock();
    lock.leading = false;
    for mut waiter in lock.waiters.drain(..) {
      if let Some(tx) = waiter.tx.take() {
        tx.send(Err(Broken));
      }
    }
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
  /// Leader 升级时把自身 target 以 `tx=None` 登记为首个等待者（Leader 即第 0 等待
  /// 者），令其纳入 [`Self::run_leader`] 的批次目标与级联排空判定——否则 Leader
  /// 自身 target 高于被在途写入压低的 `step.tail()`（安全尾）时，级联循环零推进
  /// 即退出并回 `Ok(committed)`，committed 未覆盖 Leader 目标，wait-for-commit 档
  /// 即提前回 COMMIT_OK（未落盘字节获持久承诺）。对标 TsavoriteLog.cs
  /// WaitForCommitAsync:1874 与 CommitAsync:1997/2005 的 `while (CommittedUntilAddress
  /// < tail)` 达标循环：任一等待者（含发起提交的 Leader 自身）返回前其 target 必
  /// 已被持久水位覆盖。`tx=None` 使 retain 达标即移除 Leader 自身，无需回送通道。
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
    lock.waiters.push(Waiter { target, tx: None });
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
    watermark: impl FnOnce() -> u64,
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
  /// 每轮批量收集领域尾地址与所有挂起等待者（含 [`Self::enter`] 登记为首个等待者
  /// 的 Leader 自身）的最大需求，步进物理持久化，原地 retain_mut 批量唤醒达标
  /// Follower（消除额外堆分配）；级联排空后释放 Leader 身份返回；物理错误向全部
  /// Follower 广播中断哨兵并释放身份防死锁。panic unwind / future 取消打断由
  /// [`LeaderGuard`] 收口（复位身份 + 广播中断哨兵），零新增广播机制。
  ///
  /// 停滞让步：批次目标受在途写入压低的 `step.tail()`（安全尾）封顶、本轮水位零
  /// 推进时，让出调度权（[`yield_now`]）等待在途槽位释放、安全尾回升后补齐——对应
  /// C# WaitForCommit/CommitAsync 未达标即 await 让出后沿 NextTask 链重等的达标循环，
  /// 杜绝热轮询空转。取得推进则继续下一轮。
  pub async fn run_leader<S: GroupCommitStep>(&self, step: S) -> Result<u64, S::Error> {
    let mut guard = LeaderGuard {
      pipeline: self,
      armed: true,
    };
    let res = self.run_leader_cascade(step).await;
    guard.armed = false;
    res
  }

  /// 级联循环本体（[`Self::run_leader`] 守卫包裹的裸驱动，见其文档）
  async fn run_leader_cascade<S: GroupCommitStep>(&self, step: S) -> Result<u64, S::Error> {
    let mut last_committed;
    loop {
      // (1) 收集当前批次目标：领域尾地址与所有挂起等待者（含 Leader 自身）的最大需求
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
          let should_yield = {
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
            // (3) 级联检查：仍有更高水位等待者（含 Leader 自身 target）积压或新写入
            // 超过已提交位点则继续下一轮
            let has_lagging_waiters = lock.waiters.iter().any(|w| w.target > last_committed);
            let has_new_tail = step.tail() > last_committed;
            if !has_lagging_waiters && !has_new_tail {
              // 管道完全排空，释放 Leader 身份并退出（此时 last_committed 已覆盖
              // 全部入批 target，含 Leader 自身——回给客户端的确认点不越过已刷地址）
              lock.leading = false;
              return Ok(last_committed);
            }
            // (4) 未排空：本轮水位零推进（committed 未越过调用前水位），说明仍有等待者
            // target 高于可刷盘安全尾（在途写入压低 step.tail()），持提交锁的 Leader 无法
            // 单方推进——让锁并让出调度权，等待在途槽位释放、安全尾回升后补齐；取得推进
            // 则直接下一轮。
            committed <= watermark
          };
          if should_yield {
            yield_now().await;
          }
        }
        Err(e) => {
          // (5) 物理错误：广播中断哨兵唤醒所有挂起 Follower，释放 Leader 身份防死锁
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
  ///
  /// 刻意不加 Send 上界：运行时为 thread-per-core 单线程执行器，
  /// 刷盘步进的池缓冲（如对齐写缓冲）无需跨线程迁移
  fn step(&self, target: u64) -> impl Future<Output = Result<u64, Self::Error>>;
}
