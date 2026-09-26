//! 集群错误文案单源映射锁测试（票 zcode-r135c 案三）
//!
//! `cluster_err_text` 收编旧 `slot_state_err_text` / `replicate_err_text`
//! 双套并行手抄为单一入口：逐变体 × 双语境应答文案必须与收编前逐字节
//! 一致（零帧漂移）；散点句词（know about node / MIGRATE to myself /
//! replicate myself）单一真源在 `wresp::cmd_strings::cluster`，业务源码
//! 不得再出现第二处字面量（防手抄漂移的静态锁）。

use std::{
  fs,
  path::{Path, PathBuf},
};

use wedb::{
  Error,
  error::{RespScope, cluster_err_text},
  server::cluster_manager_worker_state::ERR_RECOVERY_LOCK,
};

/// 槽位命令域逐变体文案逐字节锁（对位 C# ClusterManagerSlotState.cs 各
/// errorMessage 臂，收编自旧 slot_state_err_text）
#[test]
fn slot_state_scope_texts_are_byte_exact() {
  let cases: Vec<(Error, &str)> = vec![
    (
      Error::NodeNotFound("abc".into()),
      "ERR I don't know about node abc",
    ),
    (
      Error::TargetNotPrimary("n1".into()),
      "ERR Target node n1 is not a master node.",
    ),
    (Error::MigrateToMyself, "ERR Can't MIGRATE to myself"),
    (
      Error::SlotNotOwned(7),
      "ERR I'm not the owner of hash slot 7",
    ),
    (
      Error::SlotAlreadyScheduled {
        slot: 7,
        node_id: "nid".into(),
      },
      "ERR Slot already scheduled for migration from nid",
    ),
    (
      Error::InputNodeNotLocal {
        input: "A".into(),
        local: "B".into(),
      },
      "ERR Input nodeid A different from local nodeid B.",
    ),
    (
      Error::LocalSlotAlreadyImported(3),
      "ERR This is a local hash slot 3 and is already imported",
    ),
    (
      Error::SlotNotOwnedByNode {
        slot: 5,
        node_id: "x".into(),
      },
      "ERR Slot 5 is not owned by x",
    ),
    (
      Error::ImportingNodeNotPrimary("Slave".into()),
      "ERR Importing node Slave is not a master node.",
    ),
    (
      Error::SlotAlreadyScheduledForImport {
        slot: 5,
        node_id: "nid".into(),
      },
      "ERR Slot already scheduled for import from nid",
    ),
    (Error::NoWorkers, "ERR workers not initialized"),
  ];
  for (e, expect) in cases {
    assert_eq!(cluster_err_text(e, RespScope::SlotState), expect);
  }
}

/// 副本接入域逐变体文案逐字节锁（对位 C# ClusterManagerWorkerState.cs /
/// ReplicaOfCommand.cs 错误臂，收编自旧 replicate_err_text）
#[test]
fn worker_replica_scope_texts_are_byte_exact() {
  type ReplicaCase = (fn() -> Error, &'static str);
  let cases: Vec<ReplicaCase> = vec![
    (
      || Error::NodeNotFound("abc".into()),
      "ERR I don't know about node abc",
    ),
    (
      || Error::TargetNotPrimary("n1".into()),
      "ERR Target node n1 is not a master node.",
    ),
    (|| Error::MigrateToMyself, "ERR Can't replicate myself"),
    (
      || Error::AlreadyReplica("id".into()),
      "ERR I am already replica of id.",
    ),
    (
      || Error::ReplicateTargetNotPrimary("id".into()),
      "ERR Trying to replicate node (id) that is not a primary.",
    ),
    (
      || Error::PrimaryHasAssignedSlots,
      "ERR Primary has been assigned slots and cannot be a replica",
    ),
    (
      || Error::CannotAcquireRecoveryLock,
      "ERR Recovery in progress, could not acquire recoverLock",
    ),
  ];
  for (make, expect) in cases {
    assert_eq!(cluster_err_text(make(), RespScope::WorkerReplica), expect);
  }
  // 恢复锁句同源：登记臂即常量本体，杜绝第二处手抄
  assert_eq!(
    cluster_err_text(Error::CannotAcquireRecoveryLock, RespScope::WorkerReplica),
    ERR_RECOVERY_LOCK
  );
}

/// RespScope 分立语义锁：同变体两域异文；未登记臂两域各自兜底
/// Display 直通（与收编前两函数兜底行为逐字节一致，零帧漂移）
#[test]
fn scope_split_and_fallback_parity() {
  // MigrateToMyself：两域异文（RespScope 因这两臂而立）
  assert_eq!(
    cluster_err_text(Error::MigrateToMyself, RespScope::SlotState),
    "ERR Can't MIGRATE to myself"
  );
  assert_eq!(
    cluster_err_text(Error::MigrateToMyself, RespScope::WorkerReplica),
    "ERR Can't replicate myself"
  );
  // 域外变体落兜底 Display 臂（收编前各函数即此行为）
  assert_eq!(
    cluster_err_text(Error::AlreadyReplica("id".into()), RespScope::SlotState),
    Error::AlreadyReplica("id".into()).to_string()
  );
  assert_eq!(
    cluster_err_text(Error::SlotNotOwned(7), RespScope::WorkerReplica),
    Error::SlotNotOwned(7).to_string()
  );
  assert_eq!(
    cluster_err_text(Error::PayloadTooShort, RespScope::SlotState),
    "cluster config payload too short to contain a version"
  );
}

/// 收集目录下的 Rust 源码行（剔除注释行：文档注释允许引用文案作对拍凭据）
fn rust_code_lines(root: &Path) -> Vec<String> {
  let mut lines = Vec::new();
  let mut stack = vec![root.to_path_buf()];
  while let Some(dir) = stack.pop() {
    for entry in fs::read_dir(&dir).expect("目录可读") {
      let path = entry.expect("目录项").path();
      if path.is_dir() {
        stack.push(path);
      } else if path.extension().is_some_and(|x| x == "rs") {
        let text = fs::read_to_string(&path).expect("源码可读");
        lines.extend(
          text
            .lines()
            .map(str::trim)
            .filter(|l| !l.starts_with("//"))
            .map(str::to_string),
        );
      }
    }
  }
  lines
}

/// 句词单源静态锁：三句字面量真源仅在 wresp cmd_strings 模块各恰一处，
/// wedb 业务源码零出现（收编后防手抄复发）
#[test]
fn scatter_phrase_literals_have_single_source() {
  let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  let cmd_strings_text =
    fs::read_to_string(manifest.join("../wresp/src/cmd_strings.rs")).expect("cmd_strings 可读");
  let cmd_strings_lines: Vec<String> = cmd_strings_text
    .lines()
    .map(str::trim)
    .filter(|l| !l.starts_with("//"))
    .map(str::to_string)
    .collect();
  let wedb_src_lines = rust_code_lines(&manifest.join("src"));
  for phrase in [
    "ERR I don't know about node ",
    "ERR Can't MIGRATE to myself",
    "ERR Can't replicate myself",
  ] {
    let in_wresp = cmd_strings_lines
      .iter()
      .filter(|l| l.contains(phrase))
      .count();
    assert_eq!(
      in_wresp, 1,
      "句词 {phrase:?} 应在 wresp cmd_strings 单源恰一处，实测 {in_wresp} 处"
    );
    let leaked: Vec<&String> = wedb_src_lines
      .iter()
      .filter(|l| l.contains(phrase))
      .collect();
    assert!(
      leaked.is_empty(),
      "句词 {phrase:?} 不得在 wedb/src 再现（应经常量引用）: {leaked:?}"
    );
  }
}
