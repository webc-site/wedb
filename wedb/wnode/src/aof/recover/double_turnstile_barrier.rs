//! 双闸栏同步机制（对标 libs/common/Synchronization/DoubleTurnstileBarrier.cs:DoubleTurnstileBarrier）
//!
//! 经典“双十字转门（double turnstile）”两阶段循环会合栅栏：用于 1 个 Leader 与 N-1 个 Worker
//! 协同逐页消费共享工作单元（AOF 回放页）。
//! 两个闸门（ready 与 completed）在每页起始与结束处门控所有参与者；第二闸门将计数归零，安全复用于下一页。

use std::{
  hint::spin_loop,
  sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use event_listener::Event;

/// libs/common/Synchronization/DoubleTurnstileBarrier.cs:DoubleTurnstileBarrier
///
/// 集中式两阶段循环会合栅栏。
pub struct DoubleTurnstileBarrier {
  /// 参与者总数（含 Leader 与全部 Worker）。
  participant_count: usize,
  /// 闸门轮次代际：偶数对应 Ready 闸门，奇数对应 Completed 闸门。
  phase: AtomicUsize,
  /// 当前代际到达参与者计数。
  arrived: AtomicUsize,
  /// 广播唤醒事件。
  event: Event,
  /// 停机/关闭标记。
  closed: AtomicBool,
}

impl DoubleTurnstileBarrier {
  /// libs/common/Synchronization/DoubleTurnstileBarrier.cs:DoubleTurnstileBarrier
  ///
  /// 构造双闸栏实例，`participant_count` 为包含 Leader 在内的总参与者数。
  pub fn new(participant_count: usize) -> Self {
    assert!(participant_count >= 1, "participant_count 必须至少为 1");
    Self {
      participant_count,
      phase: AtomicUsize::new(0),
      arrived: AtomicUsize::new(0),
      event: Event::new(),
      closed: AtomicBool::new(false),
    }
  }

  /// 参与者通过指定代际闸门（内部核心：单次递增到达计数，末位到达者翻转代际并广播放行）。
  ///
  /// C# 侧 SignalWorkReadyInternal（就绪闸门递增到 participantCount 后放行 participantCount-1）
  /// 与 SignalWorkCompletedInternal（完成闸门递减到 0 后放行）在此统一为「递增到 participant_count
  /// 即翻转代际并唤醒」的单一内部例程，二者语义等价（奇偶代际对应两扇闸门）。
  /// libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkReadyInternal
  /// libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkCompletedInternal
  ///
  /// 异步等待指定代际闸门。
  async fn wait_phase_async(&self, expected_phase: usize) {
    if self.participant_count <= 1 || self.closed.load(Ordering::Acquire) {
      return;
    }

    let count = self.arrived.fetch_add(1, Ordering::AcqRel) + 1;
    if count == self.participant_count {
      self.arrived.store(0, Ordering::Release);
      self.phase.fetch_add(1, Ordering::Release);
      self.event.notify(usize::MAX);
    } else {
      for _ in 0..64 {
        if self.phase.load(Ordering::Acquire) > expected_phase
          || self.closed.load(Ordering::Acquire)
        {
          return;
        }
        spin_loop();
      }

      while self.phase.load(Ordering::Acquire) <= expected_phase
        && !self.closed.load(Ordering::Acquire)
      {
        let listener = self.event.listen();
        if self.phase.load(Ordering::Acquire) > expected_phase
          || self.closed.load(Ordering::Acquire)
        {
          break;
        }
        listener.await;
      }
    }
  }

  /// libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkReadyWaitAsync
  ///
  /// 异步等待所有参与者到达就绪闸门。
  pub async fn signal_work_ready_wait_async(&self) {
    let p = self.phase.load(Ordering::Acquire);
    let target_phase = if p.is_multiple_of(2) { p } else { p + 1 };
    self.wait_phase_async(target_phase).await;
  }

  /// libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkCompletedWaitAsync
  ///
  /// 异步等待所有参与者到达完成闸门。
  pub async fn signal_work_completed_wait_async(&self) {
    let p = self.phase.load(Ordering::Acquire);
    let target_phase = if p.is_multiple_of(2) { p + 1 } else { p };
    self.wait_phase_async(target_phase).await;
  }

  /// 参与者总数。
  #[inline]
  pub fn participant_count(&self) -> usize {
    self.participant_count
  }

  /// 唤醒所有挂起的等待者以支持安全停机。
  pub fn notify_all(&self) {
    self.closed.store(true, Ordering::Release);
    self.event.notify(usize::MAX);
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{
    Arc,
    atomic::{self, Ordering},
  };

  use compio::runtime::{Runtime, spawn};

  use super::*;

  #[test]
  fn single_participant_never_blocks() -> aok::Result<()> {
    Runtime::new()?.block_on(async {
      let barrier = DoubleTurnstileBarrier::new(1);
      for _ in 0..100 {
        barrier.signal_work_ready_wait_async().await;
        barrier.signal_work_completed_wait_async().await;
      }
      Ok(())
    })
  }

  #[test]
  fn repeated_cycles_rendezvous_exactly_once() -> aok::Result<()> {
    Runtime::new()?.block_on(async {
      for worker_count in [1usize, 2, 4, 8] {
        let barrier = Arc::new(DoubleTurnstileBarrier::new(worker_count + 1));
        const CYCLES: usize = 100;
        let processed = Arc::new(atomic::AtomicUsize::new(0));

        for _ in 0..worker_count {
          let b = Arc::clone(&barrier);
          let p = Arc::clone(&processed);
          spawn(async move {
            for _ in 0..CYCLES {
              b.signal_work_ready_wait_async().await;
              p.fetch_add(1, Ordering::Relaxed);
              b.signal_work_completed_wait_async().await;
            }
          })
          .detach();
        }

        for i in 0..CYCLES {
          barrier.signal_work_ready_wait_async().await;
          barrier.signal_work_completed_wait_async().await;
          assert_eq!((i + 1) * worker_count, processed.load(Ordering::Acquire));
        }
      }
      Ok(())
    })
  }

  /// test/standalone/Garnet.test/DoubleTurnstileBarrierTests.cs:FastParticipantBlocksUntilCohortArrives
  ///
  /// 快参与者先到就绪闸门必须被挡住：全体到齐前不得放行（闸门正常时挂起
  /// 是无限期的，延时后的未完成断言是确定性判定，非竞速窗口）；Leader 到齐
  /// 后双方同时放行并顺利收尾完成闸门
  #[test]
  fn fast_participant_blocks_until_cohort_arrives() -> aok::Result<()> {
    use std::time::Duration;

    use compio::time::sleep;

    Runtime::new()?.block_on(async {
      let barrier = Arc::new(DoubleTurnstileBarrier::new(2));
      let done = Arc::new(AtomicBool::new(false));

      // 快参与者：过就绪闸门（挂起）→ 放行后过完成闸门 → 置完成标志
      let b = Arc::clone(&barrier);
      let flag = Arc::clone(&done);
      let fast = spawn(async move {
        b.signal_work_ready_wait_async().await;
        b.signal_work_completed_wait_async().await;
        flag.store(true, atomic::Ordering::Release);
      });

      sleep(Duration::from_millis(50)).await;
      assert!(
        !done.load(atomic::Ordering::Acquire),
        "快参与者不得在全体到齐前被放行"
      );

      // Leader 到齐 → 就绪闸门翻转放行双方；再跟进完成闸门 → fast 收尾
      barrier.signal_work_ready_wait_async().await;
      barrier.signal_work_completed_wait_async().await;
      fast.await.unwrap();
      assert!(done.load(atomic::Ordering::Acquire), "放行后快参与者应收尾");
      Ok(())
    })
  }

  /// test/standalone/Garnet.test/DoubleTurnstileBarrierTests.cs:ConstructorRejectsNonPositiveParticipantCount
  ///
  /// 构造拒绝非正参与者数（C# ArgumentOutOfRangeException 的 rust assert 臂；
  /// usize 入参下 0 与负数同收敛为 <1 面）
  #[test]
  #[should_panic(expected = "participant_count 必须至少为 1")]
  fn constructor_rejects_non_positive_participant_count() {
    let _ = DoubleTurnstileBarrier::new(0);
  }
}
