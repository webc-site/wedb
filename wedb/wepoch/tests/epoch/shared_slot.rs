//! `Participant` 会话句柄属主纪律测试（槽位重入计数、逐层退出与所有权转移）
//!
//! 对照 C# `libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:52-85`：槽位索引
//! `Metadata.Entries` 为 `[ThreadStatic]` 线程私有静态存储，同一槽位在语言层面
//! 不可能被两个线程同时进出；C# 亦无重入计数，`Acquire`/`Release` 严格单线程配对。
//!
//! rust 侧 `Participant` 已以 `PhantomData<Cell<()>>` 收紧为 `!Sync`
//! （对标 `[ThreadStatic]` 的语言层禁令），跨线程并发共享同一槽位在编译期即被
//! 拒绝（见 `ui_probe.rs` 的 `assert_not_impl_any!` 静态断言）；原「8 线程共享 `Arc<Participant>`
//! 风暴」运行时护栏用例随之降格删除。本文件保留单属主前提下必须成立的既有语义：
//! 逐层退出归还槽位、所有权跨线程转移后重绑新属主。
//!
//! 自研依据: Participant !Sync 收紧的运行时前身记录（已降格为 ui_probe.rs 编译期静态断言）

use std::{sync::Arc, thread::spawn};

use aok::{OK, Void};
use log::info;
use wbase::thread::current_thread_id;
use wepoch::LightEpoch;

/// `Participant` 逐层退出的槽位状态（行为原型为 C#
/// ProtectionTests.SuspendLeavesTheSlotCompletelyFree 的会话句柄侧同构用例，
/// 法定对位锚点由 wepoch::tests::epoch::protection 的
/// suspend_leaves_slot_completely_free 单点持有，本用例为 rust 自研衍生）
///
/// 锁定 `EpochEntry::exit` 改写为 CAS 递减后必须保持的三条既有语义：
/// 内层退出既不清公布纪元也不清线程绑定；末层退出三者全清；未受保护时的多余
/// 退出为无操作。
#[test]
fn participant_exit_leaves_slot_completely_free() -> Void {
  info!("验证 Participant 逐层退出至槽位完全归还");

  let epoch = Arc::new(LightEpoch::new(4));
  let participant = epoch.register()?;
  let idx = participant.entry_idx();
  let owner_tid = current_thread_id();

  let outer = participant.enter();
  let middle = participant.enter();
  let inner = participant.enter();
  assert_eq!(participant.reentrant_count(), 3);
  let announced = outer.protected_epoch();
  assert!(announced > 0);
  assert_eq!(middle.protected_epoch(), announced);
  assert_eq!(inner.protected_epoch(), announced);
  assert_eq!(epoch.test_hook_thread_id_at(idx + 1), owner_tid);

  // 内层退出：重入未清零前，公布纪元与线程绑定均须原样保留
  drop(inner);
  assert_eq!(participant.reentrant_count(), 2);
  assert!(participant.is_protected(), "内层退出提前解除了槽位保护");
  assert_eq!(participant.protected_epoch(), announced);
  assert_eq!(epoch.test_hook_thread_id_at(idx + 1), owner_tid);

  drop(middle);
  assert_eq!(participant.reentrant_count(), 1);
  assert!(participant.is_protected());

  // 末层退出：纪元、线程绑定、重入计数三者全清（对照 C# Release 不变量）
  drop(outer);
  assert_eq!(participant.reentrant_count(), 0);
  assert!(!participant.is_protected());
  assert_eq!(epoch.test_hook_announced_epoch_at(idx + 1), 0);
  assert_eq!(epoch.test_hook_thread_id_at(idx + 1), 0);

  // 未受保护时的多余退出为无操作，不得触碰槽位
  participant.exit();
  assert_eq!(participant.protected_epoch(), 0);
  assert_eq!(epoch.test_hook_thread_id_at(idx + 1), 0);

  // 同一槽位可复用：再入须公布现场读取的最新纪元
  let latest = epoch.current_epoch();
  let again = participant.enter();
  assert_eq!(again.protected_epoch(), latest);
  assert_eq!(participant.reentrant_count(), 1);

  OK
}

/// `Participant` 支持 `Send`：所有权跨线程转移后，槽位重新绑定新属主线程
///
/// 对照 README 既有契约（`Participant` 句柄可跨线程转移，enter 时绑定当时线程），
/// 与 C# `[ThreadStatic]`「任一时刻槽位只有一个线程在读写」保持一致：转移前后
/// 计数与线程绑定均须干净收敛。
#[test]
fn participant_ownership_migrates_to_other_thread() -> Void {
  info!("验证 Participant 所有权迁移后槽位重绑新属主线程");

  let epoch = Arc::new(LightEpoch::new(4));
  let participant = epoch.register()?;
  let creator_tid = current_thread_id();
  let entry_idx = participant.entry_idx();
  assert_eq!(participant.reentrant_count(), 0, "未进入的槽位计数应为 0");

  let ep = Arc::clone(&epoch);
  let handle = spawn(move || {
    let tid = current_thread_id();
    assert_ne!(tid, creator_tid, "子线程须为不同属主");
    let current = ep.current_epoch();
    let guard = participant.enter();
    assert_eq!(
      guard.protected_epoch(),
      current,
      "首层进入须公布现场读取的最新纪元"
    );
    assert!(participant.is_protected());
    assert_eq!(participant.reentrant_count(), 1);
    assert_eq!(ep.test_hook_thread_id_at(guard.entry_idx() + 1), tid);
    drop(guard);
    assert!(!participant.is_protected());
    participant
  });

  let participant = handle.join().unwrap();
  assert_eq!(participant.reentrant_count(), 0);
  assert_eq!(
    epoch.test_hook_thread_id_at(entry_idx + 1),
    0,
    "末层退出须清空线程绑定"
  );
  assert_eq!(epoch.test_hook_announced_epoch_at(entry_idx + 1), 0);

  OK
}
