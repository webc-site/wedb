use strum::{FromRepr, IntoStaticStr};

/// libs/cluster/Server/Failover/FailoverStatus.cs:FailoverStatus
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromRepr, IntoStaticStr)]
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
  /// libs/cluster/Server/Failover/FailoverStatus.cs:FailoverUtils:GetFailoverStatus
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
