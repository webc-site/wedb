//! 独立册 store 装配收口核（store 主套件之外的单册测试二进制直挂单源）
//!
//! 收口各独立测试二进制逐字同形的 open_store 装配段：挂 RangeIndex 旁表
//! 子目录的测试配置（16 页环、可变占比 0.5）+ 调用方临时目录内 single_file
//! 开箱。store 主套件走 support 聚合面（open_store_in / TestEnv 同款语义），
//! 单册按需直挂本文件，避免不消费册招 per-binary dead_code（replica_host /
//! ckpt_node 先例）：
//!
//! ```text
//! #[path = "store_open.rs"]
//! mod store_open;
//! ```

use std::sync::Arc;

use aok::Result;
use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

/// 测试库 RangeIndex 旁表子目录名（挂于调用方临时目录内，与 store 主套件
/// 支撑层目录组织一致）
const RANGE_INDEXES: &str = "range_indexes";

/// 挂载 RangeIndex 旁表子目录的测试配置（旁表目录落在调用方持有的临时
/// 目录内随其保活；紧缩/清退系用例的 GC 关闭由调用方对返回值置位）
pub fn range_index_config(
  dir: &TempDir,
  index_size: usize,
  page_size: usize,
) -> Result<StoreConfig> {
  Ok(
    StoreConfig::new(index_size, page_size, 16, 0.5)?
      .with_range_index_dir(dir.path().join(RANGE_INDEXES)),
  )
}

/// 在指定临时目录中打开存储实例（目录与数据文件路径由调用方持有，便于
/// 检查点目录挂载与崩溃后重开同一路径）
pub fn open_store_in(
  dir: &TempDir,
  name: &str,
  config: StoreConfig,
) -> Result<Arc<WedbStore<SegmentedDevice>>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(config, device)?))
}
