#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! r320 迁出：replication_history 内联 tests 块（src/server/replication/replication_history.rs:144-188）
//! 复制历史 TOML 逐字段往返 / 故障转移轮转 / 未知字段前向兼容锁（私有 aof_address_toml 经 #[toml(with)] 间接覆盖）
//! / 恢复合法性门四臂锁测：异代版本、长度不对账、越界数组、空数组一律落重建臂

use std::{fs, path::Path};

use waof::AofAddress;
use wedb::server::replication::replication_history::ReplicationHistory;

/// 种档用固定纪元 ID（重建断言以「换发 repl_id」为准绳）
const SEED_ID: &str = "05fc93097d961e0ceec3be16f96d9095a09b2a5d";

/// 手工铸造 replication.toml 载荷（版本与两形位点向量可控，模拟异代/编辑档）
fn history_toml(version: u32, offset: &str, offset2: &str) -> String {
  format!(
    "version = {version}\nprimary_repl_id = \"{SEED_ID}\"\nprimary_repl_id2 = \"abc\"\nreplication_offset = {offset}\nreplication_offset2 = {offset2}\n"
  )
}

/// 重建臂断言：换发 repl_id、版本回当前代、位点向量长度恒等装配 count、
/// 磁盘档案被重建档覆盖（对位 C# InitializeReplicationHistory 尾段 FlushConfig）
fn assert_rebuilt(path: &Path, count: usize) {
  let rebuilt = ReplicationHistory::recover_or_init(path, count);
  assert_ne!(rebuilt.primary_repl_id, SEED_ID, "非法档须换新纪元 ID");
  assert_eq!(rebuilt.version, 1);
  assert_eq!(rebuilt.replication_offset.length() as usize, count);
  assert_eq!(rebuilt.replication_offset2.length() as usize, count);
  let on_disk = ReplicationHistory::from_byte_array(&fs::read(path).expect("重建后磁盘档案须存在"))
    .expect("重建档须可解码");
  assert_eq!(on_disk, rebuilt, "重建臂须覆盖落盘，磁盘不留非法档");
}

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

/// 恢复合法性门四臂锁测（对标 C# ReplicationHistory.FromByteArray 版本闸
/// :68-74 与 RecoverReplicationHistory catch → InitializeReplicationHistory
/// :119-131 唯一出口）：异代档与编辑失真档一律视同损坏落重建臂
#[test]
fn test_recover_legality_gate_rebuilds_illegal_history() {
  let dir = tempfile::tempdir().expect("tempdir");
  let path = dir.path().join("replication.toml");

  // 臂 1：version=2 异代档——读侧 from_byte_array 代次闸拒回，recover 落重建
  fs::write(&path, history_toml(2, "100", "200")).expect("seed v2");
  assert!(
    ReplicationHistory::from_byte_array(&fs::read(&path).unwrap()).is_err(),
    "非当前版本一律拒绝恢复"
  );
  assert_rebuilt(&path, 1);

  // 臂 2：count=1 配 3 元数组——载荷本身解码合法（长度门在 recover 对账），
  // 位点向量长度不与装配子日志数对账即异代/编辑失真档，落重建
  fs::write(&path, history_toml(1, "[100, 200, 300]", "[1, 2, 3]")).expect("seed 3-elt");
  let parsed =
    ReplicationHistory::from_byte_array(&fs::read(&path).unwrap()).expect("3 元数组形本身可解");
  assert_eq!(parsed.replication_offset.length(), 3);
  assert_rebuilt(&path, 1);

  // 臂 3：6 元越界数组——toml 解码臂硬拒（弃静默截为 4 元），recover 落重建
  fs::write(&path, history_toml(1, "[1, 2, 3, 4, 5, 6]", "200")).expect("seed 6-elt");
  assert!(
    ReplicationHistory::from_byte_array(&fs::read(&path).unwrap()).is_err(),
    "元数超 MAX_SUBLOG_COUNT 须报 Failed，向 from_string/from_aof_binary 硬拒口径收口"
  );
  assert_rebuilt(&path, 1);

  // 臂 4：空数组——解码臂硬拒（弃 length=0 位点：any_lesser 恒假即静默放行），
  // recover 落重建
  fs::write(&path, history_toml(1, "[]", "200")).expect("seed empty");
  assert!(
    ReplicationHistory::from_byte_array(&fs::read(&path).unwrap()).is_err(),
    "空数组须报 Failed，不得铸 length=0 位点"
  );
  assert_rebuilt(&path, 1);

  // 对照臂：count=1 标量形档原样放行——生产 count 恒 1 面零行为变更
  fs::write(&path, history_toml(1, "100", "200")).expect("seed scalar");
  let recovered = ReplicationHistory::recover_or_init(&path, 1);
  assert_eq!(recovered.primary_repl_id, SEED_ID, "合法档不得换发 repl_id");
  assert_eq!(recovered.replication_offset.get(0), Some(100));
  assert_eq!(recovered.replication_offset2.get(0), Some(200));
}
