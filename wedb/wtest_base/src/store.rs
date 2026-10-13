//! 测试存储实例装配（临时目录 + 小预算配置一键开库）
//!
//! 收敛各测试文件重复的 tempdir + SegmentedDevice::single_file +
//! StoreConfig + gc off + WedbStore::open 开库样板（对标 C#
//! AzureTestDirectory 的临时目录生命周期托管：目录随句柄存活，Drop 清理）

use std::sync::Arc;

use aok::Result;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::WedbStore;

use crate::config::{test_store_config, test_store_config_with_budget};

/// 在独立临时目录中打开小库存储实例（gc 关闭；目录随返回的 [`TempDir`]
/// 存活，Drop 自动清理数据文件）
///
/// `tag` 作为数据文件名片段，便于多实例场景下区分
pub fn open_test_store(tag: &str) -> Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  open_store_with(tag, test_store_config())
}

/// 在独立临时目录中打开指定内存预算的存储实例（gc 关闭）
///
/// 大值场景（对象信封整包内联，whlog 单记录不跨页）须以更大预算推导
/// 更大页容量：如承载 1MB 分块阈值值需 ≥128MB 预算（页 2MB）
pub fn open_test_store_with_budget(
  tag: &str,
  budget: u64,
) -> Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  open_store_with(tag, test_store_config_with_budget(budget))
}

fn open_store_with(
  tag: &str,
  mut config: wkv::StoreConfig,
) -> Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  // 小库 + gc off：与各存量测试文件「小索引、后台紧缩禁用」意图一致
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}
