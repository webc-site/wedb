/// libs/cluster/Server/Failover/FailoverOption.cs:FailoverOption
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverOption {
  Default,
  Force,
  Takeover,
}
