//! 检查点系独立存储节点单源（db + wal + 检查点目录 + 全接线角色 provider）
//!
//! 收口 checkpoint_import / receive_checkpoint_dir_fsync /
//! replica_receive_checkpoint_retry 三册逐字同形的 NodeStorage + open_node +
//! wired_provider 装配：公共核 db+wal 之上薄包装检查点目录；角色 provider 走
//! wedb_test::node_storage::provider_with_role 直通后补 set_checkpoint_dir。
//! 宿主册直挂（检查点目录装配属环境 fixture，留 tests/common/）：
//!
//! ```text
//! #[path = "common/ckpt_node.rs"]
//! mod ckpt_node;
//! ```
//!
//! 公共核单源见 `wedb_test::node_storage`（NodeStorage / open_node /
//! provider_with_role）。

use std::{fs::create_dir_all, ops::Deref, path::PathBuf, sync::Arc};

use wedb::server::{cluster_provider::ClusterProvider, worker::NodeRole};
use wedb_test::node_storage as common;

/// 检查点系独立存储节点（公共核 db+wal 加本地检查点目录薄包装）
pub struct CkptNodeStorage {
  /// 公共核（db 文件 + wal；临时目录守卫 Drop 即清理）
  pub base: common::NodeStorage,
  /// 检查点目录（接收面落盘 / 发送面读取的同一路径）
  pub checkpoint_dir: PathBuf,
}

impl Deref for CkptNodeStorage {
  type Target = common::NodeStorage;

  fn deref(&self) -> &common::NodeStorage {
    &self.base
  }
}

/// 开一套临时目录内的检查点系独立存储节点（db + wal + checkpoints 目录）
pub fn open_node(tag: &str) -> CkptNodeStorage {
  let base = common::open_node(tag);
  let checkpoint_dir = base._dir.path().join("checkpoints");
  create_dir_all(&checkpoint_dir).unwrap();
  CkptNodeStorage {
    base,
    checkpoint_dir,
  }
}

/// 角色 provider（对齐宿主装配：rm/store/wal 全接线 + 检查点目录接线；
/// 不填槽位图、不经攒批窗 setter——`provider_with_role` 直通形态）
pub fn wired_provider(
  node: &CkptNodeStorage,
  node_id: u128,
  port: i32,
  role: NodeRole,
  primary_id: u128,
) -> Arc<ClusterProvider> {
  let provider =
    common::provider_with_role(&node.base, node_id, port, role, primary_id, false, None);
  provider.set_checkpoint_dir(node.checkpoint_dir.clone());
  provider
}
