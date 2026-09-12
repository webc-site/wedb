use std::{
  env, fs,
  ops::Deref,
  path::{Path, PathBuf},
};

use wbftree::TreeTuning;

/// 测试通用 BfTree 调优参数
pub const TUNE: TreeTuning = TreeTuning {
  cache_size: 16 * 1024 * 1024,
  min_record_size: 4,
  max_record_size: 1024,
  max_key_len: 32,
  leaf_page_size: 4096,
};

/// 管理器测试环境双目录 RAII 自动清理守卫
pub struct ManagerEnvGuard {
  pub ri_root: PathBuf,
  pub cpr_root: PathBuf,
}

impl ManagerEnvGuard {
  pub fn new(prefix: &str) -> Self {
    let temp_dir = env::temp_dir();
    let rand = fastrand::u64(..);
    let ri_root = temp_dir.join(format!("ri_{prefix}_{rand}"));
    let cpr_root = temp_dir.join(format!("cpr_{prefix}_{rand}"));
    Self { ri_root, cpr_root }
  }
}

impl Drop for ManagerEnvGuard {
  fn drop(&mut self) {
    if self.ri_root.exists() {
      let _ = fs::remove_dir_all(&self.ri_root);
    }
    if self.cpr_root.exists() {
      let _ = fs::remove_dir_all(&self.cpr_root);
    }
  }
}

/// 单文件 RAII 自动清理守卫
pub struct TestFileGuard {
  path: PathBuf,
}

impl TestFileGuard {
  pub fn new(prefix: &str, ext: &str) -> Self {
    let path = env::temp_dir().join(format!("{prefix}_{}.{ext}", fastrand::u64(..)));
    Self { path }
  }
}

impl Deref for TestFileGuard {
  type Target = Path;
  fn deref(&self) -> &Self::Target {
    &self.path
  }
}

impl AsRef<Path> for TestFileGuard {
  fn as_ref(&self) -> &Path {
    &self.path
  }
}

impl Drop for TestFileGuard {
  fn drop(&mut self) {
    if self.path.exists() {
      let _ = fs::remove_file(&self.path);
    }
  }
}
