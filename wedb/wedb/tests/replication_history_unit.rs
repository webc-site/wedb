//! r320 迁出：replication_history 内联 tests 块（src/server/replication/replication_history.rs:144-188）
//! 复制历史 TOML 逐字段往返 / 故障转移轮转 / 未知字段前向兼容锁（私有 aof_address_toml 经 #[toml(with)] 间接覆盖）

use waof::AofAddress;
use wedb::server::replication::replication_history::ReplicationHistory;

#[test]
fn test_replication_history_roundtrip() {
  let hist = ReplicationHistory::new(1);
  let bytes = hist.to_byte_array();
  let decoded = ReplicationHistory::from_byte_array(&bytes).unwrap();
  assert_eq!(hist, decoded);
}

#[test]
fn test_failover_update() {
  let mut hist = ReplicationHistory::new(2);
  let orig_id = hist.primary_repl_id.clone();
  let failover_offset = AofAddress::create(2, 5000);
  hist.failover_update(failover_offset);
  assert_eq!(hist.primary_repl_id2, orig_id);
  assert_ne!(hist.primary_repl_id, orig_id);
  assert_eq!(hist.replication_offset2, failover_offset);
}

#[test]
fn test_ignore_unknown_fields_forward_compatibility() {
  let toml_str = r#"
      version = 1
      primary_repl_id = "05fc93097d961e0ceec3be16f96d9095a09b2a5d"
      primary_repl_id2 = "abc"
      replication_offset = 100
      replication_offset2 = [200, 300]
      unknown_future_field = "future_value"
      cluster_epoch = 999
    "#;
  let decoded = ReplicationHistory::from_byte_array(toml_str.as_bytes()).unwrap();
  assert_eq!(
    decoded.primary_repl_id,
    "05fc93097d961e0ceec3be16f96d9095a09b2a5d"
  );
  assert_eq!(decoded.primary_repl_id2, "abc");
  assert_eq!(decoded.replication_offset.get(0), Some(100));
  assert_eq!(decoded.replication_offset2.get(0), Some(200));
  assert_eq!(decoded.replication_offset2.get(1), Some(300));
}
