#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 监督任务强取消路径的存活位收口锁测（独立测试目标：TASKS 注册表进程内
//! 共享，独立二进制规避与其它测试目标的 gc/monitor 环并行互扰）。
//!
//! 对标 C# TaskManager 的 CancelAsync 件（:139-154）的收口契约：取消路径下
//! registry.TryRemove 命中即 IsRunning（:30-41）翻假，绝无「任务已死、注册表
//! 仍报活」。rust 对位机制：执行器取消路径（JoinHandle Drop → cancel(true)，
//! compio task run 见 cancelled 位直接 return Ready）下未来体未经终局 poll
//! 即整图丢弃，存活位由 [`Supervised`] 的 Drop 臂确定性翻假——本测试以
//! 「首 poll 后丢弃未终局的监督未来体」等价复刻该路径（丢弃未来体图即
//! cancel 的最终效果，非 mock）。
//!
//! 在 garnet 中的相对路径: libs/server/TaskManager/TaskManager.cs:CancelAsync

#![cfg(feature = "supervise")]
use std::{
  future::pending,
  task::{Context, Poll, Waker},
};

use wbase::supervise::{snapshots, supervise_task};

/// 指定任务名的存活位（未注册返回 None）
fn alive_of(name: &str) -> Option<bool> {
  snapshots()
    .into_iter()
    .find(|s| s.name == name)
    .map(|s| s.alive)
}

/// 监督未来体完成首轮 poll（停泊 Pending）后未经终局即被整图丢弃（执行器
/// cancel(true) 强取消语义）：存活位必须经 Drop 臂翻假，不得卡真
#[test]
fn dropped_after_first_poll_clears_alive() {
  const NAME: &str = "t_cancel_drop";
  // 停泊体：首 poll 后恒 Pending（对标任务停泊 sleep/await 被强取消的形态）。
  // Box::pin 持有：drop 即整图丢弃未来体（pin! 投影引用 drop 不落底层 Drop 臂）
  let mut fut = Box::pin(supervise_task(NAME, pending::<()>()));
  let mut cx = Context::from_waker(Waker::noop());
  assert!(
    matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
    "停泊体首 poll 必须 Pending"
  );
  assert_eq!(alive_of(NAME), Some(true), "首 poll 后存活位置真");

  // 整图丢弃未来体：不经终局 poll（Ready 复位臂不触发），唯一收口即 Drop 臂
  drop(fut);
  assert_eq!(
    alive_of(NAME),
    Some(false),
    "强取消丢弃后存活位必须翻假（对标 CancelAsync TryRemove 即收口）"
  );
}

/// 零 poll 丢弃（任务入队即被取消、从未调度）同样收口：构造已置位，Drop 臂
/// 覆盖「从未 poll 过」的取消形态
#[test]
fn dropped_before_any_poll_clears_alive() {
  const NAME: &str = "t_cancel_zero_poll";
  let fut = supervise_task(NAME, pending::<()>());
  assert_eq!(alive_of(NAME), Some(true), "构造即置真（启动即真）");
  drop(fut);
  assert_eq!(alive_of(NAME), Some(false), "零 poll 丢弃同样翻假");
}
