use std::sync::atomic::{AtomicU64, Ordering};

use hipstr::HipStr;
use quick_cache::sync::Cache;
use rapidhash::v3::rapidhash_v3;

/// 默认活跃租户元数据缓存容量（覆盖高频热点租户，冷租户 0 内存开销）
pub const DEFAULT_META_CACHE_CAPACITY: usize = 65_536;

/// 紧凑租户缓存项
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachedTenant {
  pub tenant_id: u64,
  pub db_id: u32,
  pub epoch: u64,
}

/// 极致节约内存的租户与鉴权令牌元数据缓存（基于 Clock-Pro 算法与 RapidHash）
#[derive(Debug)]
pub struct MetaCache {
  /// Token 哈希 -> 命名空间名称 (u64 -> HipStr)
  token_to_ns: Cache<u64, HipStr<'static>>,
  /// 命名空间名称哈希 -> Token (u64 -> HipStr)
  ns_to_token: Cache<u64, HipStr<'static>>,
  /// 全局元数据版本纪元号（单调递增，用于强一致性版本围栏）
  epoch: AtomicU64,
}

impl Default for MetaCache {
  #[inline]
  fn default() -> Self {
    Self::new(DEFAULT_META_CACHE_CAPACITY)
  }
}

impl MetaCache {
  /// 创建指定容量的元数据缓存实例
  pub fn new(capacity: usize) -> Self {
    Self {
      token_to_ns: Cache::new(capacity),
      ns_to_token: Cache::new(capacity),
      epoch: AtomicU64::new(1),
    }
  }

  /// 获取全局元数据当前版本纪元号
  #[inline]
  pub fn epoch(&self) -> u64 {
    self.epoch.load(Ordering::Acquire)
  }

  /// 递增全局元数据版本纪元号
  #[inline]
  pub fn increment_epoch(&self) -> u64 {
    self.epoch.fetch_add(1, Ordering::SeqCst) + 1
  }

  /// 根据 Token 快速查询命名空间名称（零分配纳秒级查询）
  #[inline]
  pub fn get_namespace_by_token(&self, token: &str) -> Option<HipStr<'static>> {
    let token_hash = rapidhash_v3(token.as_bytes());
    self.token_to_ns.get(&token_hash)
  }

  /// 根据命名空间名称快速查询 Token
  #[inline]
  pub fn get_token_by_namespace(&self, ns: &str) -> Option<HipStr<'static>> {
    let ns_hash = rapidhash_v3(ns.as_bytes());
    self.ns_to_token.get(&ns_hash)
  }

  /// 写入/更新命名空间与 Token 映射
  #[inline]
  pub fn put(&self, ns: &str, token: &str) {
    let token_hash = rapidhash_v3(token.as_bytes());
    let ns_hash = rapidhash_v3(ns.as_bytes());

    let ns_str = HipStr::from(ns);
    let token_str = HipStr::from(token);

    self.token_to_ns.insert(token_hash, ns_str);
    self.ns_to_token.insert(ns_hash, token_str);
    self.increment_epoch();
  }

  /// 失效指定的命名空间及旧 Token（用于状态机 Apply 钩子或命令处理）
  #[inline]
  pub fn invalidate(&self, ns: &str, old_token: Option<&str>) {
    let ns_hash = rapidhash_v3(ns.as_bytes());
    self.ns_to_token.remove(&ns_hash);

    if let Some(token) = old_token {
      let token_hash = rapidhash_v3(token.as_bytes());
      self.token_to_ns.remove(&token_hash);
    }
    self.increment_epoch();
  }

  /// 清空所有元数据缓存
  #[inline]
  pub fn clear(&self) {
    self.token_to_ns.clear();
    self.ns_to_token.clear();
    self.increment_epoch();
  }
}
