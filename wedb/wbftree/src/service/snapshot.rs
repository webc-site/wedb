//! CPR 快照与恢复 (1:1 对标 Garnet BfTreeService 的 cpr_snapshot / recover 系列)

use std::{fs, path::Path, sync::Arc};

use arc_swap::ArcSwapOption;
use bf_tree::BfTree;

use super::{BfTreeService, MIN_MAX_RECORD_SIZE, config_error_to_string, file_has_cpr_magic};
use crate::{
  error::{Error, Result},
  types::StorageBackendType,
};

/// 构造统一格式的快照恢复错误（零临时字符串分配）
fn format_recovery_err(prefix: &str, path: &Path) -> Error {
  use core::fmt::Write;
  let mut msg = String::with_capacity(prefix.len() + path.as_os_str().len());
  let _ = write!(msg, "{prefix}{}", path.display());
  Error::Recovery(msg)
}

/// 快照文件缺失错误
#[inline]
fn snapshot_missing(path: &Path) -> Error {
  format_recovery_err("快照文件不存在: ", path)
}

impl BfTreeService {
  /// 触发 CPR 快照 (非阻塞并发 CPR 快照语义，零锁直调)
  ///
  /// 与 insert/delete 并发安全：引擎 CPR 采用阶段协议 (REST → PREPARE →
  /// IN_PROGRESS → SWEEP)，在途写者按当前快照版本把触碰的页自行拷入快照文件，
  /// 快照不阻塞写、写不阻塞快照 (较旧版「屏障排空再快照」的改动：屏障会让
  /// 大树 checkpoint 期间全部写入停摆且在 compio 核上自旋空转)。
  /// 同一棵树的并发快照互斥由 [`crate::RangeIndexManager`] 的 per-tree claim
  /// 承担——引擎对并发快照静默 no-op，宿主必须串行化。
  ///
  /// 底层 bf-tree 在未启用 use_snapshot 等异常场景下直接 panic 而非返回错误，
  /// 此处 catch_unwind 拦截转换为 Err (1:1 对标 Garnet 原生互操作层的处理方式)。
  /// 注意：release 构建全局 `panic = "abort"`，panic 路径实际以进程终止收场
  /// （检查点元数据未发布，重启一致性不受影响）；catch_unwind 仅在
  /// unwind 构建（dev/test）下生效，且 panic 点位于任何写入之前，无部分写入副作用。
  pub fn cpr_snapshot(&self, snapshot_path: impl AsRef<Path>) -> Result<()> {
    let tree = self.tree_arc()?;
    if !self.enable_snapshots {
      return Err(Error::Snapshot(
        "底层引擎异常 (快照未启用或内部状态异常)".into(),
      ));
    }
    let p = snapshot_path.as_ref();
    if let Some(parent) = p.parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent)?;
    }
    tree.cpr_snapshot(p);
    Ok(())
  }

  /// 从 CPR 快照文件恢复创建全新的 BfTreeService (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:RecoverFromCprSnapshot)
  ///
  /// 调引擎前先做魔数预检：损坏/非快照文件走结构化 [`Error::Recovery`]，
  /// 不依赖 unwind 拦截 (release 构建 panic = "abort" 下 catch_unwind 无效)。
  /// catch_unwind 仅兜底引擎内部的断言异常 (1:1 对标 Garnet 原生互操作层
  /// bftree_new_from_cpr_snapshot 的处理方式)，dev/test 构建下生效。
  pub fn recover_from_cpr_snapshot(
    recovery_path: impl AsRef<Path>,
    enable_snapshots: bool,
    storage_backend: StorageBackendType,
  ) -> Result<Self> {
    let p = recovery_path.as_ref();
    if !p.exists() {
      return Err(snapshot_missing(p));
    }
    if !file_has_cpr_magic(p) {
      return Err(format_recovery_err(
        "快照文件损坏或格式非法 (魔数不匹配): ",
        p,
      ));
    }

    // 移除 catch_unwind 封装，改为严格返回 Result 处理
    match BfTree::new_from_cpr_snapshot(p, enable_snapshots, None, None, None) {
      Ok(tree) => {
        let max_record_size = tree
          .config()
          .get_cb_max_record_size()
          .max(MIN_MAX_RECORD_SIZE);
        Ok(Self {
          tree: ArcSwapOption::new(Some(Arc::new(tree))),
          storage_backend,
          file_path: Some(p.to_string_lossy().into_owned()),
          max_record_size,
          enable_snapshots,
        })
      }
      Err(e) => Err(Error::Recovery(config_error_to_string(e))),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{env::temp_dir, process::id};

  use bf_tree::Config;

  use super::*;
  use crate::types::BfTreeInsertResult;

  /// 未启用 use_snapshot 的树触发快照必须返回 Err，绝不 panic
  /// (new_with_backend 为 pub(crate)，Err 路径在 crate 内承接；
  /// 外部构造面经 RangeIndexManager，快照恒启用)
  #[test]
  fn test_cpr_snapshot_without_use_snapshot_returns_err() {
    let dir = temp_dir().join(format!(
      "wbftree_snap_disabled_{}_{}",
      id(),
      fastrand::u64(..)
    ));
    fs::create_dir_all(&dir).unwrap();
    let work = dir.join("work.bftree");
    let snap = dir.join("snap.bftree");

    let mut config = Config::default();
    config.file_path(&work).cb_min_record_size(4);
    let tree = BfTreeService::new_with_backend(
      config,
      StorageBackendType::Disk,
      Some(work.to_string_lossy().into_owned()),
      false, // use_snapshot
    )
    .unwrap();
    assert_eq!(tree.insert(b"k", b"val"), BfTreeInsertResult::Success);

    let res = tree.cpr_snapshot(&snap);
    assert!(res.is_err());
    assert!(!snap.exists());

    let _ = fs::remove_dir_all(&dir);
  }
}
