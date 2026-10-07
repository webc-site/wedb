#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wmetric::{SlowLogContainer, SlowLogEntry};
use wresp::command::RespCommand;

fn entry() -> SlowLogEntry {
  SlowLogEntry {
    id: 0,
    timestamp: 1000,
    duration: 42,
    command: RespCommand::Get,
    arguments: None,
    client_ip_port: "127.0.0.1:1234".into(),
    client_name: "cli".into(),
  }
}

#[test]
fn add_assigns_ids_and_enforces_capacity() {
  let log = SlowLogContainer::new(3);
  for _ in 0..5 {
    log.add(entry());
  }
  assert_eq!(log.count(), 3);

  let entries = log.get_entries(-1);
  // 保留最新的 3 条，id 连续递增。
  assert_eq!(
    entries.iter().map(|e| e.id).collect::<Vec<_>>(),
    vec![2, 3, 4]
  );
}

#[test]
fn get_entries_tail_snapshot() {
  let log = SlowLogContainer::new(10);
  for _ in 0..4 {
    log.add(entry());
  }
  let latest_two = log.get_entries(2);
  assert_eq!(
    latest_two.iter().map(|e| e.id).collect::<Vec<_>>(),
    vec![2, 3]
  );
  assert_eq!(log.get_entries(-1).len(), 4);
  // 超出数量时返回全部。
  assert_eq!(log.get_entries(100).len(), 4);
}

#[test]
fn clear_empties_log() {
  let log = SlowLogContainer::new(4);
  log.add(entry());
  log.clear();
  assert_eq!(log.count(), 0);
}
