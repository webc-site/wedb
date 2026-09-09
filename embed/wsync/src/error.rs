use std::result;

use thiserror::Error;

/// 同步原语错误（屏障汇合 / 计数契约）
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
  /// 参与者数量非法（须至少为 1；对应 C# DoubleTurnstileBarrier 构造参数校验）
  #[error("参与者数量非法: {0}，须至少为 1")]
  InvalidParticipantCount(i32),

  /// LeaderBarrier 到达计数回拨为负（Signal 与 Release 配对失衡；对应 C# "Invalid count value < 0"）
  #[error("LeaderBarrier 到达计数回拨为负: {0}")]
  CountUnderflow(i32),

  /// 屏障汇合等待超时（对应 C# GarnetException 超时失败语义）
  #[error("屏障汇合等待超时")]
  Timeout,
}

pub type Result<T> = result::Result<T, Error>;
