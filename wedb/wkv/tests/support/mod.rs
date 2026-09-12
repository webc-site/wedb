//! store 测试二进制公共支撑：临时目录环境、配置快捷构造与补零工具

use std::{iter::repeat_n, sync::Arc};

use aok::Result;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

/// 临时目录保活与存储实例绑定环境
pub struct TestEnv {
  /// 保活临时目录（Drop 时自动清理数据文件）
  pub _dir: TempDir,
  /// 存储引擎实例
  pub store: Arc<WedbStore<SegmentedDevice>>,
}

/// 快捷构造默认可变占比 0.5 的测试配置
pub fn config(index_size: usize, page_size: usize, num_pages: usize) -> Result<StoreConfig> {
  Ok(StoreConfig::new(index_size, page_size, num_pages, 0.5)?)
}

/// 在独立临时目录中打开存储实例（目录随 [`TestEnv`] 存活，Drop 自动清理）
pub fn open_store(name: &str, config: StoreConfig) -> Result<TestEnv> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok(TestEnv { _dir: dir, store })
}

/// 十进制补零到 width 位
pub fn pad(v: impl itoa::Integer, width: usize) -> String {
  let mut buf = itoa::Buffer::new();
  let digits = buf.format(v);
  let mut s = String::with_capacity(width.max(digits.len()));
  s.extend(repeat_n('0', width.saturating_sub(digits.len())));
  s.push_str(digits);
  s
}
