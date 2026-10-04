/// libs/common/FailoverOption.cs:FailoverOption
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub enum FailoverOption {
  Default,
  Force,
  Takeover,
}
