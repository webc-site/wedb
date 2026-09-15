//! store + resp 会话测试环境核心（tempdir + 数据文件 + 统一配置 + 引擎 + 会话）

use std::sync::Arc;

use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::{StoreSession, WedbStore};
use wnode::resp::resp_server_session::RespServerSession;

/// 构造测试环境：`with_range_index` 决定是否挂载 RangeIndex 目录
///
/// tempdir + 数据文件 + 统一小预算配置（wedb_test::test_store_config，对标
/// C# 测试基类 16MB 预算；GC 关闭保持历史语义）+ 引擎 + 会话，
/// with_batch 与 range_index 场景共用此一处 setup
pub fn test_env(
  with_range_index: bool,
) -> (
  tempfile::TempDir,
  StoreSession<SegmentedDevice>,
  RespServerSession,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  let mut config = test_store_config();
  config.gc.enabled = false;
  if with_range_index {
    config.range_index_dir = Some(dir.path().join("ri"));
  }
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let resp = RespServerSession::default();
  (dir, session, resp)
}
