//! 全量快照传输中断孤儿残留清理回归
//!
//! 对位修复：`purge_checkpoint_files_except` 改用 `wcpr::list_all_checkpoint_tokens`
//! 物理实体枚举（对标 C# DeviceLogCommitCheckpointManager.GetLogCheckpointTokens/
//! GetIndexCheckpointTokens 直接 ListContents 物理目录、不以 meta 完整性过滤），
//! 使传输在 index 文件/RangeIndex 子目录落盘后、`.meta` 提交标记到达前中断所遗留的
//! 孤儿快照 Token 亦能纳入淘汰候选、被 `purge_checkpoint` 彻底物理回收，堵住从库
//! 磁盘随每次中断重试累积的永久静默泄漏。

use std::{fs, path::Path};

use wcpr::{
  index_filename, index_tmp_filename, list_all_checkpoint_tokens, list_checkpoints, meta_filename,
  meta_tmp_filename, token_to_base32,
};
use wedb::server::replication::{
  checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
  checkpoint_store::CheckpointStore,
};

/// 构造一份完整已提交快照：meta 提交标记 + index 文件 + Base32 rangeindex 子目录
fn seed_committed(dir: &Path, token: u128) {
  fs::write(dir.join(meta_filename(token)), b"meta").unwrap();
  fs::write(dir.join(index_filename(token)), b"index").unwrap();
  let b32 = token_to_base32(token);
  fs::create_dir_all(dir.join(b32.as_str()).join("rangeindex")).unwrap();
  fs::write(
    dir
      .join(b32.as_str())
      .join("rangeindex")
      .join("tree.bftree"),
    b"tree",
  )
  .unwrap();
}

/// 构造一份传输中断孤儿：index 文件与 Base32 子目录已落盘，`.meta` 提交标记始终未到
fn seed_interrupted_orphan(dir: &Path, token: u128) {
  fs::write(dir.join(index_filename(token)), b"index-half").unwrap();
  let b32 = token_to_base32(token);
  fs::create_dir_all(dir.join(b32.as_str()).join("rangeindex")).unwrap();
  fs::write(
    dir
      .join(b32.as_str())
      .join("rangeindex")
      .join("tree.bftree"),
    b"tree-half",
  )
  .unwrap();
  // 缺 meta 正是本票的引爆点：恢复选点面看不见它，清理面必须兜住它
  assert!(
    !dir.join(meta_filename(token)).exists(),
    "孤儿现场必须无 meta 提交标记"
  );
}

fn entry_for(token: u128) -> CheckpointEntry {
  let mut m = CheckpointMetadata::new(1);
  m.store_version = 1;
  m.store_hlog_token = token;
  m.store_index_token = token;
  CheckpointEntry::new(m)
}

/// 端到端：保留一份合法基准快照，经 CheckpointStore 淘汰路径清理传输中断孤儿
///
/// 反向注入判据：若把 `purge_checkpoint_files_except` 回退为 `list_checkpoints`
/// （仅认完整 .meta），本用例的孤儿 Token 无从枚举 → 其 index 文件与 Base32 子目录
/// 残留 → 下方全部断言必红。
#[test]
fn interrupted_transfer_orphan_is_purged_end_to_end() {
  let tmp = tempfile::tempdir().unwrap();
  let ckpt = tmp.path().join("checkpoints");
  fs::create_dir_all(&ckpt).unwrap();

  let keep_token: u128 = 0x1111_1111_1111_1111_u128;
  let orphan_token: u128 = 0x2222_2222_2222_2222_u128;

  seed_committed(&ckpt, keep_token);
  seed_interrupted_orphan(&ckpt, orphan_token);

  // 枚举语义分工（清理前的现场事实）：
  // - list_checkpoints 看不见缺 meta 的孤儿（保护恢复选点，语义不变）
  // - list_all_checkpoint_tokens 把孤儿物理痕迹一并纳入（清理候选面）
  assert_eq!(
    list_checkpoints(&ckpt).unwrap(),
    vec![keep_token],
    "有效快照枚举语义不变：缺 meta 的孤儿不得进入恢复选点候选"
  );
  assert_eq!(
    list_all_checkpoint_tokens(&ckpt).unwrap(),
    vec![keep_token, orphan_token],
    "全量物理枚举须把中断孤儿纳入清理候选"
  );

  let mut store = CheckpointStore::new(true);
  store.set_checkpoint_dir(ckpt.clone());
  store.purge_all_checkpoints_except_entry(&entry_for(keep_token));

  // 孤儿物理痕迹彻底移除：index 文件与 Base32 子目录双双消失
  let orphan_b32 = token_to_base32(orphan_token);
  assert!(
    !ckpt.join(index_filename(orphan_token)).exists(),
    "孤儿 index 文件必须被物理删除"
  );
  assert!(
    !ckpt.join(orphan_b32.as_str()).exists(),
    "孤儿 Base32 快照子目录必须被递归删除"
  );

  // 基准快照完整保留
  assert!(
    ckpt.join(meta_filename(keep_token)).is_file()
      && ckpt.join(index_filename(keep_token)).is_file()
      && ckpt
        .join(token_to_base32(keep_token).as_str())
        .join("rangeindex")
        .join("tree.bftree")
        .is_file(),
    "keep Token 的文件集须原样保留"
  );

  // 磁盘零残留：全量物理枚举只剩 keep（孤儿任何文件/目录若逃逸，list_all 仍会见到）
  assert_eq!(
    list_all_checkpoint_tokens(&ckpt).unwrap(),
    vec![keep_token],
    "清理后检查点目录不得残留任何孤儿物理痕迹"
  );
  assert_eq!(list_checkpoints(&ckpt).unwrap(), vec![keep_token]);
}

/// 枚举契约：list_all_checkpoint_tokens 覆盖全部四类物理文件命名 + Base32 子目录，
/// 而 list_checkpoints 仅认完整 .meta（两轨语义分工锁死，防回归）
#[test]
fn enumeration_covers_all_physical_forms_but_committed_only() {
  let tmp = tempfile::tempdir().unwrap();
  let ckpt = tmp.path();

  let t_meta: u128 = 1; // checkpoint_<b32>.meta —— 唯一已提交
  let t_index: u128 = 2; // index_<b32>.ckpt —— 中断孤儿索引文件
  let t_itmp: u128 = 3; // index_<b32>.ckpt.tmp —— 半截索引临时件
  let t_mtmp: u128 = 4; // checkpoint_<b32>.meta.tmp —— 半截元数据临时件
  let t_dir: u128 = 5; // <b32>/rangeindex —— 仅 Base32 子目录

  fs::write(ckpt.join(meta_filename(t_meta)), b"").unwrap();
  fs::write(ckpt.join(index_filename(t_index)), b"").unwrap();
  fs::write(ckpt.join(index_tmp_filename(t_itmp)), b"").unwrap();
  fs::write(ckpt.join(meta_tmp_filename(t_mtmp)), b"").unwrap();
  fs::create_dir_all(
    ckpt
      .join(token_to_base32(t_dir).as_str())
      .join("rangeindex"),
  )
  .unwrap();

  assert_eq!(
    list_all_checkpoint_tokens(ckpt).unwrap(),
    vec![t_meta, t_index, t_itmp, t_mtmp, t_dir],
    "全量枚举须含五种物理形态且升序去重"
  );
  assert_eq!(
    list_checkpoints(ckpt).unwrap(),
    vec![t_meta],
    "有效快照枚举仅认完整 .meta：.meta.tmp 与 index/tmp/目录均不得入选"
  );
}
