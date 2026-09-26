use wbftree::TreeTuning;

pub use crate::guard::TestPathGuard;

/// 测试通用 BfTree 调优参数
pub const TUNE: TreeTuning = TreeTuning {
  cache_size: 16 * 1024 * 1024,
  min_record_size: 4,
  max_record_size: 1024,
  max_key_len: 32,
  leaf_page_size: 4096,
};

/// 管理器测试环境双目录（清理逻辑收敛于 TestPathGuard）
pub struct ManagerEnvGuard {
  pub ri_root: TestPathGuard,
  pub cpr_root: TestPathGuard,
}

impl ManagerEnvGuard {
  pub fn new(prefix: &str) -> Self {
    Self {
      ri_root: TestPathGuard::new(&format!("ri_{prefix}"), true),
      cpr_root: TestPathGuard::new(&format!("cpr_{prefix}"), true),
    }
  }
}
