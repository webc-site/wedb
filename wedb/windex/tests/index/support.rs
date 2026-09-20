//! 测试辅助函数与夹具

use std::thread::yield_now;

use windex::{HashBucketEntry, HashEntryInfo, HashIndex, Result};

/// 已从生产导出面收敛掉的索引便捷口，在测试支撑层按生产入口等价复现。
///
/// 全部方法只由生产读/写入口组合实现，与被删实现逐语义等价：
/// - 写入一律经唯一的免查重追加口 [`HashIndex::insert_to_bucket`]（键/哈希形态在此补
///   `hash_key` 与 `tag_from_hash` 两步换算，与桶下标 `bucket_index_for_hash` 同源）；
/// - 查重语义写入一律经生产写入口 [`HashIndex::find_or_create_tag_by_hash_with_min_addr`]
///   + [`HashEntryInfo::try_cas`]；
/// - 读出候选一律为 [`HashIndex::lookup_candidates`] 的零拷贝小数组，`lookup_vec` 仅供
///   测试断言做集合比较，生产读路径不再出现堆分配形态。
pub trait HashIndexTestOps {
  /// 免查重追加键对应逻辑地址（等价历史 `HashIndex::insert`）
  fn insert(&self, key: &[u8], address: u64) -> Result<()>;

  /// 免查重追加指定哈希的逻辑地址（等价历史 `HashIndex::insert_by_hash`）
  fn insert_by_hash(&self, hash: u64, address: u64) -> Result<()>;

  /// 候选地址 Vec 化读出（等价历史 `HashIndex::lookup`，仅测试断言用）
  fn lookup_vec(&self, key: &[u8]) -> Vec<u64>;

  /// 单趟查找或试探性插入（等价历史 `HashIndex::find_tag_or_insert`）
  fn find_tag_or_insert(&self, key: &[u8], address: u64) -> Result<(Option<u64>, bool)>;

  /// 单趟查找或试探性插入（等价历史 `HashIndex::find_tag_or_insert_by_hash`）
  fn find_tag_or_insert_by_hash(&self, hash: u64, address: u64) -> Result<(Option<u64>, bool)>;

  /// 单趟探针定位（等价历史 `HashIndex::find_or_create_tag`）
  fn find_or_create_tag(&self, key: &[u8]) -> Result<HashEntryInfo<'_>>;

  /// 带截断线单趟探针定位（等价历史 `HashIndex::find_or_create_tag_with_min_addr`）
  fn find_or_create_tag_with_min_addr(
    &self,
    key: &[u8],
    min_valid_addr: u64,
  ) -> Result<HashEntryInfo<'_>>;
}

impl HashIndexTestOps for HashIndex {
  #[inline]
  fn insert_by_hash(&self, hash: u64, address: u64) -> Result<()> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    self.insert_to_bucket(self.bucket_index_for_hash(hash), tag, address)
  }

  #[inline]
  fn insert(&self, key: &[u8], address: u64) -> Result<()> {
    self.insert_by_hash(HashIndex::hash_key(key), address)
  }

  #[inline]
  fn lookup_vec(&self, key: &[u8]) -> Vec<u64> {
    self.lookup_candidates(key).to_vec()
  }

  fn find_tag_or_insert_by_hash(&self, hash: u64, address: u64) -> Result<(Option<u64>, bool)> {
    loop {
      let mut hei = self.find_or_create_tag_by_hash_with_min_addr(hash, 0)?;
      if hei.is_found() {
        return Ok((Some(hei.address()), false));
      }
      if hei.try_cas(address) {
        return Ok((None, true));
      }
      yield_now();
    }
  }

  #[inline]
  fn find_tag_or_insert(&self, key: &[u8], address: u64) -> Result<(Option<u64>, bool)> {
    self.find_tag_or_insert_by_hash(HashIndex::hash_key(key), address)
  }

  #[inline]
  fn find_or_create_tag(&self, key: &[u8]) -> Result<HashEntryInfo<'_>> {
    self.find_or_create_tag_with_min_addr(key, 0)
  }

  #[inline]
  fn find_or_create_tag_with_min_addr(
    &self,
    key: &[u8],
    min_valid_addr: u64,
  ) -> Result<HashEntryInfo<'_>> {
    self.find_or_create_tag_by_hash_with_min_addr(HashIndex::hash_key(key), min_valid_addr)
  }
}

/// 构造语义化测试键
pub fn make_key(prefix: &str, id: usize) -> Vec<u8> {
  format!("{}_{}", prefix, id).into_bytes()
}

/// 批量预生成语义化测试键，消除并发读写热路径中的重复堆分配
pub fn make_keys(prefix: &str, count: usize) -> Vec<Vec<u8>> {
  (0..count).map(|id| make_key(prefix, id)).collect()
}

/// 构造受 48 位地址掩码限制的测试地址
pub const fn make_address(high: u64, low: u64) -> u64 {
  ((high << 32) | (low & 0xFFFF_FFFF)) & HashBucketEntry::ADDRESS_MASK
}
