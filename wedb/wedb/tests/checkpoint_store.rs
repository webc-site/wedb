//! CheckpointStore 单测（自 src/server/replication/checkpoint_store.rs 内联
//! 测试迁出）：登记 / 淘汰链、读者闸门与孤儿快照物理清理
use waof::AofAddress;
use wedb::server::replication::{
  checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
  checkpoint_store::CheckpointStore,
};

#[test]
fn test_checkpoint_store_add_and_delete() {
  let mut store = CheckpointStore::new(true);
  let mut m1 = CheckpointMetadata::new(1);
  m1.store_version = 1;
  m1.store_hlog_token = 100;
  m1.store_index_token = 200;
  m1.store_checkpoint_covered_aof_address = AofAddress::create(1, 100);

  store.add_checkpoint_entry(CheckpointEntry::new(m1), true);
  assert_eq!(store.entry_count(), 1);

  let mut m2 = CheckpointMetadata::new(1);
  m2.store_version = 2;
  m2.store_hlog_token = 101;
  m2.store_index_token = 201;
  m2.store_checkpoint_covered_aof_address = AofAddress::create(1, 200);

  store.add_checkpoint_entry(CheckpointEntry::new(m2), true);
  // m1 不与 m2 共享 token 且无读者，安全淘汰 m1，剩余 m2
  assert_eq!(store.entry_count(), 1);
  let latest = store
    .try_get_latest_checkpoint_entry_from_memory()
    .expect("should get latest");
  assert_eq!(latest.metadata.store_version, 2);
  latest.remove_reader();

  // 验证从副本读取等待
  store.wait_for_replicas();

  assert!(
    store
      .get_latest_checkpoint_from_memory_info()
      .contains("storeVersion=2")
  );
}

/// 磁盘淘汰接通读者闸门：读者持有的条目其 token 文件不被 unlink，
/// 释放读者后淘汰链推进回收（C# DeleteOutdatedCheckpoints
/// TrySuspendReaders -> DeleteLogCheckpoint/DeleteIndexCheckpoint 对位）
#[test]
fn test_delete_outdated_purges_disk_except_reader_held() {
  use std::fs::{create_dir_all, write};

  use wcpr::{list_checkpoints, meta_filename};

  let dir = tempfile::tempdir().expect("tempdir");
  let ckpt_dir = dir.path().join("checkpoints");
  create_dir_all(&ckpt_dir).expect("mkdir");

  // 手工落 meta 占位文件（list/purge 的识别面，best-effort 删除即可）
  let seed = |token: u128| {
    write(ckpt_dir.join(meta_filename(token)), []).expect("seed meta");
  };

  let mut store = CheckpointStore::new(true);
  store.set_checkpoint_dir(ckpt_dir.clone());

  let entry = |version: i64, token: u128| {
    let mut m = CheckpointMetadata::new(1);
    m.store_version = version;
    m.store_hlog_token = token;
    m.store_index_token = token;
    CheckpointEntry::new(m)
  };

  // 三份磁盘快照 + 前两次登记（e1 无读者，随登记淘汰回收 t1）
  seed(1);
  seed(2);
  seed(3);
  store.add_checkpoint_entry(entry(1, 1), true);
  store.add_checkpoint_entry(entry(2, 2), true);
  assert_eq!(
    list_checkpoints(&ckpt_dir).expect("list"),
    vec![2, 3],
    "e1 无读者，随登记淘汰回收 t1"
  );

  // 传输会话持读者：下一次登记触发的淘汰停手，t2 不得 unlink
  let reader = store
    .try_get_latest_checkpoint_entry_from_memory()
    .expect("reader");
  store.add_checkpoint_entry(entry(3, 3), true);
  assert_eq!(
    list_checkpoints(&ckpt_dir).expect("list"),
    vec![2, 3],
    "读者持有期间快照 t2 不得被 unlink"
  );

  // 释放读者后淘汰链推进回收
  reader.remove_reader();
  seed(4);
  store.add_checkpoint_entry(entry(4, 4), true);
  assert_eq!(
    list_checkpoints(&ckpt_dir).expect("list"),
    vec![4],
    "释放读者后下一轮登记回收全部陈旧 token"
  );
}

/// purge_all_checkpoints_except_entry 物理清理段：保留 keep 条目 token，
/// 其余孤儿 token 连文件一并回收（C# :94/:104 对位；Initialize 期无读者）。
/// 内存链表与磁盘清理两轨分离：链上预登记的陈旧条目既不被出链，其磁盘快照
/// 也照样被回收
#[test]
fn test_purge_all_except_entry_cleans_orphan_files() {
  use std::fs::{create_dir_all, write};

  use wcpr::{list_checkpoints, meta_filename};

  let dir = tempfile::tempdir().expect("tempdir");
  let ckpt_dir = dir.path().join("checkpoints");
  create_dir_all(&ckpt_dir).expect("mkdir");

  let seed = |token: u128| {
    write(ckpt_dir.join(meta_filename(token)), []).expect("seed meta");
  };

  let mut store = CheckpointStore::new(true);
  store.set_checkpoint_dir(ckpt_dir.clone());

  let entry = |version: i64, token: u128| {
    let mut m = CheckpointMetadata::new(1);
    m.store_version = version;
    m.store_hlog_token = token;
    m.store_index_token = token;
    CheckpointEntry::new(m)
  };

  seed(1);
  seed(2);
  seed(9);
  // 预登记一条陈旧条目（单条目不触发登记期过期淘汰）
  store.add_checkpoint_entry(entry(9, 9), true);
  // keep 条目由调用方持有，purge 只看它的 token，与是否入链无关
  let keep = entry(2, 2);

  store.purge_all_checkpoints_except_entry(&keep);

  assert_eq!(store.entry_count(), 1, "purge 不改内存链表");
  assert_eq!(
    list_checkpoints(&ckpt_dir).expect("list"),
    vec![2],
    "除 keep 条目的 token 外孤儿快照应全部回收（含链上条目的 t9）"
  );
}

/// 共享索引下淘汰链持续推进（对标上游 c323bbf7a / #2144，
/// test/cluster/.../ClusterReplicationBaseTests.cs:
/// ClusterCheckpointCleanupRetiresEntriesWithSharedIndex 的单点对位）
///
/// 日志型检查点（full=false）增量继承链尾的 index token：e1 全量
/// (hlog=1,index=1)，e2/e3 日志型继承 index=1。旧形态在 index 共享判定处
/// `break` 断链——e1 永不淘汰、日志快照随打点单调累积；修订后 hlog 工件
/// 照常回收、共享 index 工件留待最终唯一持有者（e4 全量，index=4）淘汰 e3
/// 时统一回收。日志侧工件 = meta + token 子目录（RI 快照随日志检查点传输），
/// 索引侧工件 = index_<token>.ckpt（wcpr 分侧 purge 原语）
#[test]
fn test_delete_outdated_retires_entries_with_shared_index() {
  use std::fs::{create_dir_all, write};

  use wcpr::{index_filename, list_all_checkpoint_tokens, meta_filename};

  let dir = tempfile::tempdir().expect("tempdir");
  let ckpt_dir = dir.path().join("checkpoints");
  create_dir_all(&ckpt_dir).expect("mkdir");

  // 全量检查点 token 的完整文件集：meta + index + token 子目录（RI 快照占位）
  let seed_full = |token: u128| {
    write(ckpt_dir.join(meta_filename(token)), []).expect("seed meta");
    write(ckpt_dir.join(index_filename(token)), []).expect("seed index");
    let sub = ckpt_dir.join(wcpr::token_to_base32(token).as_str());
    create_dir_all(&sub).expect("seed token dir");
    write(sub.join("ri.bftree"), []).expect("seed ri snapshot");
  };

  let entry = |version: i64, hlog: u128, index: u128| {
    let mut m = CheckpointMetadata::new(1);
    m.store_version = version;
    m.store_hlog_token = hlog;
    m.store_index_token = index;
    CheckpointEntry::new(m)
  };

  let mut store = CheckpointStore::new(true);
  store.set_checkpoint_dir(ckpt_dir.clone());

  seed_full(1);
  store.add_checkpoint_entry(entry(1, 1, 1), true);

  // 两轮日志型检查点：继承 index=1，hlog 各自换代（日志侧工件只有 meta）
  for token in [2u128, 3] {
    write(ckpt_dir.join(meta_filename(token)), []).expect("seed meta");
    store.add_checkpoint_entry(entry(token as i64, token, 1), false);
  }

  assert_eq!(
    store.entry_count(),
    1,
    "共享索引不得阻断淘汰链：仅链尾 e3 存活"
  );
  // e1/e2 日志侧工件回收；共享 index 工件（index_1.ckpt）被 e3 引用，必须存活
  assert!(
    !ckpt_dir.join(meta_filename(1)).exists() && !ckpt_dir.join(meta_filename(2)).exists(),
    "被淘汰条目的 meta 必须回收"
  );
  assert!(
    !ckpt_dir.join(wcpr::token_to_base32(1).as_str()).exists(),
    "被淘汰条目的 RI 快照子目录（日志侧工件）必须回收"
  );
  assert!(
    ckpt_dir.join(index_filename(1)).exists(),
    "链尾仍引用的共享 index 工件不得删除（旧形态在此整链停摆）"
  );
  assert_eq!(
    list_all_checkpoint_tokens(&ckpt_dir).expect("list"),
    vec![1, 3],
    "磁盘仅剩链尾 meta 与其引用的共享 index"
  );

  // 最终持有者换代（e4 全量，index=4）：淘汰 e3 后共享 index_1 失去全部引用，统一回收
  seed_full(4);
  store.add_checkpoint_entry(entry(4, 4, 4), true);
  assert_eq!(store.entry_count(), 1, "仅链尾 e4 存活");
  assert_eq!(
    list_all_checkpoint_tokens(&ckpt_dir).expect("list"),
    vec![4],
    "共享 index 失去引用后随最终淘汰统一回收"
  );
}
