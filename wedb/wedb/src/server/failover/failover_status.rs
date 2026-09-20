use strum::FromRepr;

/// libs/cluster/Server/Failover/FailoverStatus.cs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromRepr)]
#[repr(u8)]
pub enum FailoverStatus {
  #[default]
  NoFailover = 0,
  BeginFailover = 1,
  IssuingPauseWrites = 2,
  WaitingForSync = 3,
  FailoverInProgress = 4,
  TakingOverAsPrimary = 5,
  FailoverCompleted = 6,
  FailoverAborted = 7,
}

impl FailoverStatus {
  /// libs/cluster/Server/Failover/FailoverStatus.cs:GetFailoverStatus
  ///
  /// C# 侧为 static class FailoverUtils 的 `GetFailoverStatus(FailoverStatus?)`
  /// switch 表达式，非法值 `_ => throw`；rust 枚举变体穷尽 + `Option` 由调用侧
  /// `from_repr(...).unwrap_or_default()` 收编，故本件无不可达分支。
  pub fn get_failover_status(&self) -> &'static str {
    match self {
      Self::NoFailover => "no-failover",
      Self::BeginFailover => "begin-failover",
      Self::IssuingPauseWrites => "issuing-pause-writes",
      Self::WaitingForSync => "waiting-for-sync",
      Self::FailoverInProgress => "failover-in-progress",
      Self::TakingOverAsPrimary => "taking-over-as-primary",
      Self::FailoverCompleted => "failover-completed",
      Self::FailoverAborted => "failover-aborted",
    }
  }
}
