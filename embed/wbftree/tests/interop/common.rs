use std::{
  env, fs,
  ops::Deref,
  path::{Path, PathBuf},
};

use wbftree::{BfTreeInsertResult, BfTreeService};

/// 测试临时 BfTree 文件 RAII 自动清理守卫
pub struct TempTreeGuard {
  path: PathBuf,
}

impl TempTreeGuard {
  pub fn new(name: &str) -> Self {
    let path = env::temp_dir().join(format!("bftree_test_{}_{}.bftree", name, fastrand::u64(..)));
    Self { path }
  }
}

impl Deref for TempTreeGuard {
  type Target = Path;
  fn deref(&self) -> &Self::Target {
    &self.path
  }
}

impl AsRef<Path> for TempTreeGuard {
  fn as_ref(&self) -> &Path {
    &self.path
  }
}

impl Drop for TempTreeGuard {
  fn drop(&mut self) {
    if self.path.exists() {
      let _ = fs::remove_file(&self.path);
    }
  }
}

/// 辅助函数：向 BfTree 批量写入测试键值对
pub fn insert_test_data(tree: &BfTreeService, count: usize) {
  for i in 0..count {
    let key = format!("key:{:04}", i).into_bytes();
    let val = format!("val:{}", i).into_bytes();
    assert_eq!(tree.insert(&key, &val), BfTreeInsertResult::Success);
  }
}
