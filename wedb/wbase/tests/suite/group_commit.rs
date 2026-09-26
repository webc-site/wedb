//! Group Commit 级联内核（wbase::group_commit）Leader 目标越界回归测试
//!
//! 对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs 的
//! WaitForCommitAsync:1874 与 CommitAsync:1997/2005 达标循环契约：任一等待者
//! （含发起刷盘的 Leader 自身）返回前，其 target 必已被持久水位覆盖。
//!
//! 测试以「安全尾封顶步进器」（CeilingStep）复现在途写入压低 safe_tail 的可观测
//! 语义：该步进器模拟 WAL 刷盘内核的诚实上界——物理刷盘的持久水位恒不越过其安全
//! 尾（被在途槽位压低的 step.tail()），随在途写入完成（本测试以步进轮次确定性地
//! 抬升安全尾，代替真实在途槽位释放的时间点）安全尾回升，水位方可补齐至目标。
//! 被测对象 run_leader 是生产内核本身，非假 mock。
//!
//! 自研依据: 分组提交原语（C# 对应 Tsavorite GroupCommit 语义，本仓以 crossfire 重写）

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread,
};

use wbase::{
  future::block_on,
  group_commit::{Broken, Enter, GroupCommitPipeline, GroupCommitStep},
};

/// 安全尾封顶步进器：持久水位（committed）恒不越过安全尾（safe_tail）。
///
/// step 内以步进轮次确定性地演示「在途写入释放 → 安全尾回升」：前 `hold` 轮
/// 安全尾冻结在低位（在途写入持槽），其后抬升至高位（在途写入完成）。
///
/// 以 `Arc<Shared>` 承载原子量，令 Clone 保持跨任务共享同一份位点状态；对本地类型
/// `CeilingStep` 实现外部 trait `GroupCommitStep`（规避集成测试的孤儿规则限制）。
struct Shared {
  safe_tail: AtomicU64,
  committed: AtomicU64,
  step_calls: AtomicU64,
  /// 安全尾冻结轮次（进入第 `hold + 1` 次 step 时抬升安全尾）
  hold: u64,
}

#[derive(Clone)]
struct CeilingStep {
  shared: Arc<Shared>,
}

impl CeilingStep {
  fn new(safe: u64, committed: u64, hold: u64) -> Self {
    Self {
      shared: Arc::new(Shared {
        safe_tail: AtomicU64::new(safe),
        committed: AtomicU64::new(committed),
        step_calls: AtomicU64::new(0),
        hold,
      }),
    }
  }

  fn safe(&self) -> u64 {
    self.shared.safe_tail.load(Ordering::Acquire)
  }

  fn durable(&self) -> u64 {
    self.shared.committed.load(Ordering::Acquire)
  }

  fn calls(&self) -> u64 {
    self.shared.step_calls.load(Ordering::Acquire)
  }
}

impl GroupCommitStep for CeilingStep {
  type Error = Broken;

  fn tail(&self) -> u64 {
    self.shared.safe_tail.load(Ordering::Acquire)
  }

  fn watermark(&self) -> u64 {
    self.shared.committed.load(Ordering::Acquire)
  }

  async fn step(&self, target: u64) -> Result<u64, Broken> {
    let calls = self.shared.step_calls.fetch_add(1, Ordering::AcqRel) + 1;
    // 越过冻结轮次后，模拟在途写入释放、安全尾回升至高位
    if calls > self.shared.hold {
      self.shared.safe_tail.fetch_max(HIGH_TAIL, Ordering::AcqRel);
    }
    // 诚实上界：本轮持久水位只推进到「目标」与「安全尾」的较小者，绝不刷未就绪字节
    let safe = self.shared.safe_tail.load(Ordering::Acquire);
    let durable = self
      .shared
      .committed
      .fetch_max(target.min(safe), Ordering::AcqRel);
    Ok(durable.max(target.min(safe)))
  }
}

/// 低位安全尾（被在途写入压低）、高位安全尾（在途释放后回升）、Leader/Follower 目标
const LOW_TAIL: u64 = 100;
const HIGH_TAIL: u64 = 200;
const BASE_COMMITTED: u64 = 50;

/// 核心回归：Leader 自身 target（200）高于被在途压低的 safe_tail（100）时，级联循环
/// 不得零推进即回 Ok。修复前 run_leader 首轮 batch_target=safe_tail=100，无 waiter、
/// 无 new_tail 即以 Ok(100) 退出——100 < Leader target 200，wait-for-commit 档提前回
/// COMMIT_OK（假确认）。修复后 Leader target 入 waiters，级联让步等在途释放、安全尾
/// 回升后补齐至 200 才退出。
#[test]
fn leader_target_above_safe_tail_waits_for_release_no_premature_ok() {
  let pipeline = GroupCommitPipeline::new();
  let step = CeilingStep::new(LOW_TAIL, BASE_COMMITTED, 2);

  // 升级 Leader，target=HIGH_TAIL > safe_tail=LOW_TAIL
  assert!(matches!(
    pipeline.enter(HIGH_TAIL, || step.watermark()),
    Enter::Lead
  ));

  let committed = block_on(pipeline.run_leader(step.clone())).unwrap();

  // 不变式①：返回的确认点覆盖 Leader 自身 target（绝不提前 Ok）
  assert!(
    committed >= HIGH_TAIL,
    "Leader target {HIGH_TAIL} 未落盘即回 Ok({committed})：wait-for-commit 假确认"
  );
  // 不变式②：确认点不超过步进器实际刷出的持久水位（不凭空承诺）
  assert_eq!(
    committed,
    step.durable(),
    "run_leader 返回值须等于步进器实际达成的持久水位"
  );
  // 不变式③：持久水位恒不越过安全尾（无刷半写字节）
  assert!(
    step.durable() <= step.safe(),
    "持久水位 {:?} 越过了安全尾 {:?}",
    step.durable(),
    step.safe()
  );
  // 证明 Leader 确实在低安全尾上多轮让步（在途未释放前未退出），而非首轮即返回
  assert!(
    step.calls() >= 2,
    "Leader 须在压低的 safe_tail 上停滞让步等待在途释放，实测 step 轮次 {:?}",
    step.calls()
  );

  // Leader 身份释放：排空后可重新升级
  assert!(matches!(
    pipeline.enter(HIGH_TAIL + 1, || step.watermark()),
    Enter::Lead
  ));
}

/// 达标快路径：Leader 自身 target 已被 safe_tail 覆盖（无在途压低）时，级联一轮即
/// 推进达标退出，不得无谓停滞让步（防止修复过度保守导致正常路径退化）。
#[test]
fn leader_target_within_safe_tail_commits_in_one_round() {
  let pipeline = GroupCommitPipeline::new();
  // 安全尾已达标（LOW_TAIL == target），无需等待在途释放
  let step = CeilingStep::new(LOW_TAIL, BASE_COMMITTED, u64::MAX);

  assert!(matches!(
    pipeline.enter(LOW_TAIL, || step.watermark()),
    Enter::Lead
  ));

  let committed = block_on(pipeline.run_leader(step.clone())).unwrap();

  assert_eq!(committed, LOW_TAIL);
  assert_eq!(step.calls(), 1, "安全尾已覆盖目标须单轮达标退出");
}

/// 级联批量唤醒（Leader 自身 target + Follower 目标并入同一批次）：多等待者目标各异
/// 且高于低位安全尾时，Leader 让步等在途释放后，单轮刷盘批量唤醒全部达标等待者，
/// 每位收到的确认位点不低于自身 target，Leader 身份释放。
#[test]
fn leader_and_followers_batched_targets_all_met() {
  let pipeline = Arc::new(GroupCommitPipeline::new());
  let step = CeilingStep::new(LOW_TAIL, BASE_COMMITTED, 2);

  // Leader 先升级（target=HIGH_TAIL 入 waiters 作第 0 等待者）
  assert!(matches!(
    pipeline.enter(HIGH_TAIL, || step.watermark()),
    Enter::Lead
  ));

  // 三名 Follower 登记，目标分别落在低/高位安全尾区间，挂起等待批量唤醒
  let mut handles = Vec::new();
  for target in [80u64, LOW_TAIL, HIGH_TAIL] {
    let Enter::Follow(rx) = pipeline.enter(target, || step.watermark()) else {
      panic!("Leader 在位时须登记为 Follower");
    };
    let pipeline_bg = Arc::clone(&pipeline);
    let step_bg = step.clone();
    handles.push(thread::spawn(move || {
      block_on(pipeline_bg.wait(rx, target, || step_bg.watermark()))
    }));
  }

  let committed = block_on(pipeline.run_leader(step.clone())).unwrap();

  assert!(committed >= HIGH_TAIL, "批量排空后确认点须覆盖最大 target");
  assert_eq!(committed, step.durable());
  for (idx, handle) in handles.into_iter().enumerate() {
    let woke = handle.join().unwrap().unwrap();
    assert!(
      woke >= [80u64, LOW_TAIL, HIGH_TAIL][idx],
      "Follower {idx} 唤醒位点 {woke} 不得低于其 target"
    );
  }
  // Leader 自身 target（HIGH_TAIL）已随批达标并被 retain 移除：可重新升级
  assert!(matches!(
    pipeline.enter(HIGH_TAIL + 1, || step.watermark()),
    Enter::Lead
  ));
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
  let handle = thread::spawn(move || block_on(pipeline_bg.wait(rx, 500, || 0)).unwrap_err());

  // Leader 步进失败：Follower 收中断哨兵，Leader 身份释放
  assert!(block_on(pipeline.run_leader(FailingStep)).is_err());
  let broken = handle.join().unwrap();
  assert_eq!(broken.to_string(), "group commit pipeline broken");
  assert!(matches!(pipeline.enter(1, || 0), Enter::Lead));
}

/// Leader 身份 Drop 守卫回归：step 闭包内 panic（unwind 打断 Leader 刷盘链，
/// 对位 drive.rs 泵层 catch_unwind 捕获后的 future 丢弃）——守卫 Drop 须复位
/// leading 并对挂起 Follower 广播中断哨兵（经通道中断兜底臂上抛），后续
/// enter 能再任 Leader（防单会话 panic 把写平面砖化为全域永久 Follow 挂起）。
#[test]
fn leader_panic_unwind_releases_leadership_and_broadcasts_broken() {
  use std::panic::{AssertUnwindSafe, catch_unwind};

  /// step 即 panic 的步进器（对位刷盘链内 panic 活源，如 bf-tree ENOSPC）
  struct PanickingStep;

  impl GroupCommitStep for PanickingStep {
    type Error = Broken;

    fn tail(&self) -> u64 {
      500
    }

    fn watermark(&self) -> u64 {
      0
    }

    async fn step(&self, _target: u64) -> Result<u64, Broken> {
      panic!("刷盘链内 panic（ENOSPC 等在案活源）");
    }
  }

  let pipeline = Arc::new(GroupCommitPipeline::new());

  assert!(matches!(pipeline.enter(500, || 0), Enter::Lead));
  let Enter::Follow(rx) = pipeline.enter(500, || 0) else {
    panic!("须登记为 Follower");
  };

  let pipeline_bg = Arc::clone(&pipeline);
  let handle = thread::spawn(move || block_on(pipeline_bg.wait(rx, 500, || 0)).unwrap_err());

  // Leader 刷盘链 panic：unwind 穿过 run_leader，守卫收口（隔离断言不外泄）
  let result = catch_unwind(AssertUnwindSafe(|| {
    block_on(pipeline.run_leader(PanickingStep))
  }));
  assert!(
    result.is_err(),
    "step panic 须原样上抛（单会话隔离由泵层承接）"
  );

  // 挂起 Follower 经守卫广播收中断哨兵，不被永久挂起
  let broken = handle.join().unwrap();
  assert_eq!(broken.to_string(), "group commit pipeline broken");

  // leading 复位：后续提交者能再任 Leader
  assert!(matches!(pipeline.enter(1, || 0), Enter::Lead));
}
