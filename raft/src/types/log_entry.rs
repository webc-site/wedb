use std::fmt;

use super::Cmd;

#[derive(bitcode::Encode, bitcode::Decode, Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
  pub time_ms: Option<u64>,
  pub cmd: Cmd,
}

impl LogEntry {
  pub fn new(cmd: Cmd) -> Self {
    Self { time_ms: None, cmd }
  }

  pub fn new_with_time(cmd: Cmd, time_ms: Option<u64>) -> Self {
    Self { time_ms, cmd }
  }
}

impl fmt::Display for LogEntry {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let cmd = &self.cmd;
    write!(f, "{cmd}")
  }
}
