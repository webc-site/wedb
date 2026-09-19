//! 业务侧长 I/O 窗口的纪元挂起守卫（RAII）
//!
//! 对照 C# Tsavorite 刷盘/驱逐路径的 `epoch.UnsafeSuspendThread`/`ResumeThread`
//! 协议（libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:OnPagesClosed
//! 等长 I/O 临界区）：进入长磁盘 I/O 与 safe_head 等待窗口前按当前重入深度逐层
//! 退出纪元保护区（解除对旧纪元的自钉，杜绝钉住纪元阻塞全系统页回收），守卫
//! Drop 时执行同深度重入协议，无论正常退出、异步 Future 被 Drop（取消/超时）
//! 还是 panic 展开均执行，保障取消安全。

use crate::Participant;

/// 纪元挂起守卫：构造时按当前重入深度逐层退出 [`Participant`] 保护区，Drop 时按原深度执行重入协议
pub struct EpochSuspendGuard<'a> {
  participant: &'a Participant,
  count: u32,
}

impl<'a> EpochSuspendGuard<'a> {
  /// 构造守卫并挂起保护区：按当前重入深度逐层 `exit`
  #[inline]
  pub fn new(participant: &'a Participant) -> Self {
    let count = participant.reentrant_count();
    for _ in 0..count {
      participant.exit();
    }
    Self { participant, count }
  }
}

impl Drop for EpochSuspendGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    // 与构造期逐层 exit 对称：按原深度 resume 恢复保护区（C# ResumeThread）。
    // 不可改用 enter() 的 RAII 守卫——守卫在本轮循环结束即 Drop 并 exit，净效果
    // 变成「解除保护」，长 I/O 之后的追加重试窗口会处于无保护态。
    for _ in 0..self.count {
      self.participant.resume();
    }
  }
}
