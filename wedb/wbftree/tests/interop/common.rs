use std::sync::Arc;

use aok::Result;
use wbftree::{
  BfTreeInsertResult, BfTreeService, RangeIndexManager, StorageBackendType, TreeTuning,
};

pub use crate::guard::TestPathGuard;

/// 辅助函数：向 BfTree 批量写入测试键值对
pub fn insert_test_data(tree: &BfTreeService, count: usize) {
  for i in 0..count {
    let key = format!("key:{:04}", i).into_bytes();
    let val = format!("val:{}", i).into_bytes();
    assert_eq!(tree.insert(&key, &val), BfTreeInsertResult::Success);
  }
}

/// 构造管理器托管的树实例 (对标 C# BfTreeService 构造由 RangeIndexManager.CreateBfTree 托管，
/// 自定义调优经 TreeTuning 直达)
pub fn managed_tree(
  tag: &str,
  backend: StorageBackendType,
  tuning: TreeTuning,
) -> Result<(TestPathGuard, RangeIndexManager, Arc<BfTreeService>)> {
  let dir = TestPathGuard::new(tag, true);
  let manager = RangeIndexManager::new(&dir.path, dir.path.join("cpr"))?;
  let tree = manager.create_bftree(b"tree", backend, tuning)?;
  Ok((dir, manager, tree))
}
