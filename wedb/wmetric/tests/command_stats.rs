use wmetric::{CommandStats, CommandStatsEntry};
use wresp::command::{LAST_VALID_COMMAND, RespCommand};

#[test]
fn counters_per_command() {
  let mut stats = CommandStats::new();
  assert_eq!(stats.entries.len(), LAST_VALID_COMMAND as u16 as usize + 1);

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
    CommandStatsEntry::default()
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
