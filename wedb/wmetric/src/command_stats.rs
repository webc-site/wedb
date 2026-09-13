use wresp::RespCommand;

/// 单命令统计条目：调用 / 失败 / 拒绝计数（遵循 Redis COMMANDSTATS 约定）。
///（对标 libs/server/Metrics/CommandStats.cs:CommandStatsEntry）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommandStatsEntry {
  /// 该命令被调用的总次数。
  pub calls: u64,
  /// 该命令失败（返回错误响应）的总次数。
  pub failed_calls: u64,
  /// 该命令在执行前被拒绝的总次数（如 ACL 拒绝、OOM）。
  pub rejected_calls: u64,
}

/// 内置命令的逐命令使用统计：以 `RespCommand` 判别值为下标的数组，O(1) 访问。
/// 每个会话持有独立实例（单写者，无需加锁）。
///（对标 libs/server/Metrics/CommandStats.cs:CommandStats）
pub struct CommandStats {
  /// 以 `(int)RespCommand` 为下标的逐命令统计条目。
  pub entries: Vec<CommandStatsEntry>,
}

impl CommandStats {
  /// 统计数组容量：覆盖全部有效 `RespCommand` 值（最后一个有效成员为 Reset，
  /// Invalid = 65535 不占槽位）。
  ///（对齐 C# `(int)RespCommandExtensions.LastValidCommand + 1`）
  pub const ENTRY_COUNT: usize = RespCommand::Reset as u16 as usize + 1;

  /// libs/server/Metrics/CommandStats.cs:CommandStats（构造，条目清零）。
  pub fn new() -> Self {
    Self {
      entries: vec![CommandStatsEntry::default(); Self::ENTRY_COUNT],
    }
  }

  /// libs/server/Metrics/CommandStats.cs:IncrementCalls
  ///
  /// 指定命令的调用计数加一。
  #[inline]
  pub fn increment_calls(&mut self, cmd: RespCommand) {
    if let Some(entry) = self.entries.get_mut(cmd as u16 as usize) {
      entry.calls += 1;
    }
  }

  /// libs/server/Metrics/CommandStats.cs:IncrementFailed
  ///
  /// 指定命令的失败计数加一。
  #[inline]
  pub fn increment_failed(&mut self, cmd: RespCommand) {
    if let Some(entry) = self.entries.get_mut(cmd as u16 as usize) {
      entry.failed_calls += 1;
    }
  }

  /// libs/server/Metrics/CommandStats.cs:IncrementRejected
  ///
  /// 指定命令的拒绝计数加一。
  #[inline]
  pub fn increment_rejected(&mut self, cmd: RespCommand) {
    if let Some(entry) = self.entries.get_mut(cmd as u16 as usize) {
      entry.rejected_calls += 1;
    }
  }

  /// libs/server/Metrics/CommandStats.cs:GetEntry
  ///
  /// 读取指定命令的统计条目；越界返回默认值（对齐 C# `return default`）。
  #[inline]
  pub fn get_entry(&self, cmd: RespCommand) -> CommandStatsEntry {
    self
      .entries
      .get(cmd as u16 as usize)
      .copied()
      .unwrap_or_default()
  }

  /// libs/server/Metrics/CommandStats.cs:Add
  ///
  /// 聚合：将另一实例并入本实例。
  pub fn add(&mut self, other: &CommandStats) {
    let len = self.entries.len().min(other.entries.len());
    for (dst, src) in self.entries[..len]
      .iter_mut()
      .zip(other.entries[..len].iter())
    {
      dst.calls += src.calls;
      dst.failed_calls += src.failed_calls;
      dst.rejected_calls += src.rejected_calls;
    }
  }

  /// libs/server/Metrics/CommandStats.cs:Reset
  ///
  /// 全部条目清零。
  pub fn reset(&mut self) {
    self.entries.fill(CommandStatsEntry::default());
  }
}

impl Default for CommandStats {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use wresp::RespCommand;

  use super::CommandStats;

  #[test]
  fn counters_per_command() {
    let mut stats = CommandStats::new();
    assert_eq!(stats.entries.len(), RespCommand::Reset as u16 as usize + 1);

    stats.increment_calls(RespCommand::Get);
    stats.increment_calls(RespCommand::Get);
    stats.increment_failed(RespCommand::Get);
    stats.increment_rejected(RespCommand::Set);

    let get = stats.get_entry(RespCommand::Get);
    assert_eq!((get.calls, get.failed_calls, get.rejected_calls), (2, 1, 0));
    let set = stats.get_entry(RespCommand::Set);
    assert_eq!((set.calls, set.failed_calls, set.rejected_calls), (0, 0, 1));
    // 未触碰的命令为默认条目。
    assert_eq!(
      stats.get_entry(RespCommand::Ping),
      super::CommandStatsEntry::default()
    );
  }

  #[test]
  fn add_and_reset() {
    let mut a = CommandStats::new();
    let mut b = CommandStats::new();
    a.increment_calls(RespCommand::Incr);
    b.increment_calls(RespCommand::Incr);
    b.increment_calls(RespCommand::Incr);
    b.increment_failed(RespCommand::Decr);

    a.add(&b);
    assert_eq!(a.get_entry(RespCommand::Incr).calls, 3);
    assert_eq!(a.get_entry(RespCommand::Decr).failed_calls, 1);

    a.reset();
    assert_eq!(a.get_entry(RespCommand::Incr).calls, 0);
  }
}
