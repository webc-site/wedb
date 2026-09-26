//! 停机信号监听收尾让渡回归（task/ing/zcode-r21-standalone）
//!
//! 缺陷：`wait_shutdown_signal` drop 未触发的监听任务后立即返回，而
//! `JoinHandle::drop` 只入队取消（compio `Task::cancel` = schedule +
//! set_cancelled），future 实际析构（触发 `SignalListener::drop` →
//! unregister → SigDfl 恢复）要等 executor tick 真正 run 到该取消任务；
//! 主运行时随后的 `GarnetServer::stop()` 同步长段（向量收敛 30s×2 /
//! pubsub 收敛 / join 排空无上界）占据单线程，取消任务得不到 poll，
//! SIG_DFL 迟迟不恢复——优雅停机窗口期二次信号（同种或异种）被仍在
//! slab 的 handler 吞掉，signal.rs 宣称的强杀逃生通道不成立。C# host
//! （main/GarnetServer/Program.cs Main）无任何信号注册，任何二次信号按
//! 默认处置必死，rust 窗口期劣于原型。
//!
//! 修复：drop 监听任务后 `yield_now` 让渡一轮（1:1 对标 C# Task.Yield 的
//! 协作让步原语，wbase::future），本任务重挂执行器队尾，调度 FIFO 保证
//! 取消任务先于本任务续跑被 tick 析构（`Task::run` 对取消任务直接
//! Ready，同轮 `Task::drop` 释放 future），SIG_DFL 恢复先于返回。
//!
//! 测试对标：C# 无镜像（信号注册为 rust 运行时自有机制），按票面验证点
//! 构建真链路：注册后 kill 自身 SIGINT 使 `wait_shutdown_signal` 胜出
//! 返回，断言 SIGINT 与 SIGTERM 双双恢复 SIG_DFL——SIG_DFL 即「二次信号
//! 按默认行为立即终止进程」，处置位即最硬证据，无需真杀进程。

use std::{process::id, time::Duration};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use nix::{
  sys::signal::{SigHandler, Signal, kill, signal},
  unistd::Pid,
};
use wnode::{SIGINT_LABEL, wait_shutdown_signal};

/// 注册 settling 余量（监听任务 spawn → compio slab 注册为微秒级，百毫秒
/// 足以覆盖调度抖动；kill 早于注册完成则胜出方为空，测试失去验证面）
const REGISTER_WAIT: Duration = Duration::from_millis(150);

/// 读当前信号处置并置回 SIG_DFL（返回值即改写前的 handler：SIG_DFL(0) 或
/// compio 的 `signal_handler` 地址）。此处在「应已恢复」的断言点调用，目标
/// 状态本就是 SIG_DFL，写操作幂等，不引入额外可观察副作用。
fn current_disposition(sig: Signal) -> SigHandler {
  unsafe { signal(sig, SigHandler::SigDfl) }.expect("signal 查询失败")
}

/// kill 自身 SIGINT 胜出返回后，SIGINT 与 SIGTERM 双双恢复默认处置
/// （修复前：SIGTERM 监听任务只入队取消、future 未析构，处置仍挂在
/// compio handler 上，窗口期二次信号被吞）
#[test]
fn listener_drop_restores_default_dispositions_before_return() {
  let rt = Runtime::new().unwrap();
  let label = rt.block_on(async {
    let waiter = spawn(wait_shutdown_signal());
    sleep(REGISTER_WAIT).await;
    // 首信号（SIGINT）：SIGINT/SIGTERM 两路并驱竞速的胜出面
    let pid = Pid::from_raw(id() as i32);
    kill(pid, Signal::SIGINT).expect("kill 自身失败");
    waiter.await.unwrap().expect("wait_shutdown_signal 出错")
  });

  assert_eq!(label, SIGINT_LABEL, "SIGINT 应为竞速胜出方");
  assert!(
    matches!(current_disposition(Signal::SIGINT), SigHandler::SigDfl),
    "SIGINT 处置未恢复默认"
  );
  assert!(
    matches!(current_disposition(Signal::SIGTERM), SigHandler::SigDfl),
    "SIGTERM 处置未恢复默认：取消监听任务未被 executor 实际析构，\
     停机窗口期二次信号会被吞掉"
  );
}
