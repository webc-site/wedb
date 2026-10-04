//! 双闸栏同步集成测试（自 src/aof/recover/double_turnstile_barrier.rs 内联测迁入，
//! 断言与覆盖原样保留）
//!
//! 对标 test/standalone/Garnet.test/DoubleTurnstileBarrierTests.cs

use std::{
  sync::{
    Arc,
    atomic::{self, AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{
  runtime::{Runtime, spawn},
  time::{sleep, timeout},
};
use wnode::aof::recover::double_turnstile_barrier::DoubleTurnstileBarrier;

#[test]
fn single_participant_never_blocks() -> aok::Result<()> {
  Runtime::new()?.block_on(async {
    let barrier = DoubleTurnstileBarrier::new(1);
    // 活性判据收口：单参与者形态 100 轮双门应恒即时放行；回归态（门误关）
    // 以超时界确定性判红，而非无期挂起占死测试线程
    let done = timeout(Duration::from_secs(30), async {
      for _ in 0..100 {
        barrier.signal_work_ready_wait_async().await;
        barrier.signal_work_completed_wait_async().await;
      }
    })
    .await;
    assert!(done.is_ok(), "单参与者双门 30s 预算内未闭环，疑死锁回归");
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
