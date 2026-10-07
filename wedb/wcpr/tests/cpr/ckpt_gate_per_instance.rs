//! 检查点串行闸门的实例粒度契约（自 src/manager/mod.rs 内联测试迁入：
//! 依赖面全部为已导出的 `CkptGateState`/`acquire`，零 crate 私有项）
//!
//! 对标 C# GarnetDatabase.cs:75 CheckpointingLock per-instance 形态。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering as AtomicOrdering},
  },
  time::{Duration, Instant},
};

use compio::{runtime::Runtime, time::sleep};
use wcpr::{CkptGateState, acquire};

/// 持闸最短时长（异闸臂即时性断言窗远小于此，同闸挂起臂等待窗据此锚定）
const HOLD_MS: u64 = 120;

/// 实例闸粒度契约：闸门随宿主存储引擎实例持有——同实例（同一
/// `CkptGateState`）串行（挂起等待持闸者释放），不同实例（不同闸门状态）
/// 互不阻塞即时取得。对标 C# GarnetDatabase.CheckpointingLock per-instance 形态
#[test]
fn ckpt_gate_is_per_instance() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let gate_a = Arc::new(CkptGateState::default());
    let gate_b = Arc::new(CkptGateState::default());

    let a_acquired = Arc::new(AtomicBool::new(false));
    let b_acquired = Arc::new(AtomicBool::new(false));

    // H 臂：占 gate_a 闸并持 HOLD_MS
    let hold = {
      let gate = Arc::clone(&gate_a);
      let flag = Arc::clone(&a_acquired);
      rt.spawn(async move {
        let _gate = acquire(&gate).await;
        flag.store(true, AtomicOrdering::Release);
        sleep(Duration::from_millis(HOLD_MS)).await;
      })
    };
    while !a_acquired.load(AtomicOrdering::Acquire) {
      sleep(Duration::from_millis(1)).await;
    }

    // 异闸臂：gate_b 独立实例闸，必须即时取得（互不阻塞；跨实例互斥回归时
    // 此处将挂起至 H 臂释放，耗时 ≥ HOLD_MS 即暴露）
    let d_start = Instant::now();
    let waiter_d = {
      let gate = Arc::clone(&gate_b);
      let flag = Arc::clone(&b_acquired);
      rt.spawn(async move {
        let _gate = acquire(&gate).await;
        flag.store(true, AtomicOrdering::Release);
      })
    };
    while !b_acquired.load(AtomicOrdering::Acquire) {
      sleep(Duration::from_millis(1)).await;
    }
    assert!(
      d_start.elapsed() < Duration::from_millis(HOLD_MS / 2),
      "不同实例闸必须互不阻塞，实测等待 {:?}",
      d_start.elapsed()
    );

    // 同闸臂：gate_a 同实例，必须挂起至 H 臂持闸期满释放
    let c_start = Instant::now();
    let waiter_c = {
      let gate = Arc::clone(&gate_a);
      rt.spawn(async move {
        let _gate = acquire(&gate).await;
      })
    };
    waiter_c.await.unwrap();
    let c_waited = c_start.elapsed();
    assert!(
      c_waited >= Duration::from_millis(HOLD_MS / 2),
      "同实例闸门必须串行挂起等待持闸者释放，实测等待 {c_waited:?}"
    );

    waiter_d.await.unwrap();
    hold.await.unwrap();
  });
}
