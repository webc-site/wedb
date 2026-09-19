//! store 测试二进制公共支撑：临时目录环境、配置快捷构造与补零工具

use std::{iter::repeat_n, sync::Arc};

use aok::Result;
use itoa::Buffer;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use windex::{HashBucketEntry, HashIndex};
use wkv::{StoreConfig, WedbStore};

/// 已从 windex 生产导出面收敛掉的索引便捷口，测试支撑层按生产入口等价复现：
/// 写入一律经唯一免查重追加口 [`HashIndex::insert_to_bucket`]，候选读出仍为
/// 零拷贝 [`HashIndex::lookup_candidates`]（`lookup_vec` 仅供断言做集合比较）
pub trait HashIndexTestOps {
  /// 免查重追加键对应逻辑地址（等价历史 `HashIndex::insert`）
  fn insert(&self, key: &[u8], address: u64) -> windex::Result<()>;

  /// 免查重追加指定哈希的逻辑地址（等价历史 `HashIndex::insert_by_hash`）
  fn insert_by_hash(&self, hash: u64, address: u64) -> windex::Result<()>;

  /// 候选地址 Vec 化读出（等价历史 `HashIndex::lookup`，仅测试断言用）
  fn lookup_vec(&self, key: &[u8]) -> Vec<u64>;
}

impl HashIndexTestOps for HashIndex {
  #[inline]
  fn insert_by_hash(&self, hash: u64, address: u64) -> windex::Result<()> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    self.insert_to_bucket(self.bucket_index_for_hash(hash), tag, address)
  }

  #[inline]
  fn insert(&self, key: &[u8], address: u64) -> windex::Result<()> {
    self.insert_by_hash(HashIndex::hash_key(key), address)
  }

  #[inline]
  fn lookup_vec(&self, key: &[u8]) -> Vec<u64> {
    self.lookup_candidates(key).to_vec()
  }
}

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

/// 在独立临时目录中打开存储实例（目录随 [`TestEnv`] 存活，Drop 自动清理）
pub fn open_store(name: &str, config: StoreConfig) -> Result<TestEnv> {
  let dir = tempdir()?;
  let store = open_store_in(&dir, name, config)?;
  Ok(TestEnv { _dir: dir, store })
}

/// 十进制补零到 width 位
pub fn pad(v: impl itoa::Integer, width: usize) -> String {
  let mut buf = Buffer::new();
  let digits = buf.format(v);
  let mut s = String::with_capacity(width.max(digits.len()));
  s.extend(repeat_n('0', width.saturating_sub(digits.len())));
  s.push_str(digits);
  s
}
