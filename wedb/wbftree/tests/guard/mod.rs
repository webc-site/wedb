//! wbftree 测试公共守卫：temp_dir + fastrand 随机路径 + Drop 自动清理

use std::{
  env, fs,
  ops::Deref,
  path::{Path, PathBuf},
};

/// 测试临时路径 RAII 守卫：file 模式 Drop 删单文件；dir 模式预创建目录、Drop 递归删除
pub struct TestPathGuard {
  /// 守卫持有的临时路径（Deref 到 Path）
  pub path: PathBuf,
  dir: bool,
}

impl TestPathGuard {
  /// dir=true 预创建随机目录（收尾递归删除）；false 为随机 .bftree 文件（收尾删除）
  pub fn new(prefix: &str, dir: bool) -> Self {
    let path = env::temp_dir().join(if dir {
      format!("{prefix}_{}", fastrand::u64(..))
    } else {
      format!("{prefix}_{}.bftree", fastrand::u64(..))
    });
    if dir {
      if path.exists() {
        let _ = fs::remove_dir_all(&path);
      }
      fs::create_dir_all(&path).expect("创建测试临时目录失败");
    }
    Self { path, dir }
  }
}

impl Deref for TestPathGuard {
  type Target = Path;
  fn deref(&self) -> &Self::Target {
    &self.path
  }
}

impl AsRef<Path> for TestPathGuard {
  fn as_ref(&self) -> &Path {
    &self.path
  }
}

impl Drop for TestPathGuard {
  fn drop(&mut self) {
    let _ = if self.dir {
      fs::remove_dir_all(&self.path)
    } else {
      fs::remove_file(&self.path)
    };
  }
}
