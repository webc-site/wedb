use whasher::fast_hash;

use crate::{
  Result, bucket::HashBucket, buckets::HashBuckets, error::Error, overflow_pool::OverflowPool,
};

/// 64 字节 Cacheline 对齐无锁哈希索引表
///
/// 仿写 Microsoft Garnet Tsavorite 的核心哈希索引：
/// - 每个主哈希桶大小严格为 64 字节，与 CPU 缓存行对齐
/// - 内部包含 7 个数据槽位与 1 个溢出桶/自旋锁管理槽位
/// - 使用 15 位 Tag 进行常数级哈希碰撞前置过滤
/// - 单次 CAS 无锁并发插入（0 -> 完整条目，无半成品窗口；C# 两阶段 Tentative
///   协议的查重职责由调用方候选地址择新承担，见 `insert_to_bucket` 注释）
/// - 支持 RCU 无锁 CAS 更新与原子置零删除
///
/// # 寻址不变量
///
/// `mask == buckets.len() - 1` 且 `buckets.len()` 恒为 2 的幂（构造时强制校验），
/// 三者一经构造终身绑定不可变：单表实例不可变，在线动态扩容通过 SplitIndex 状态机创建新表并分块迁移。
/// 全部 `get_unchecked` 裸寻址的安全前提均依赖该不变量（`x & mask < len` 恒成立）；
/// 公开字段仅供只读检查，外部改写将破坏该安全前提。
///
/// 各操作域以分部实现承载（对位 C# Tsavorite Implementation/ 目录一操作一件）：
/// 查找探针在 find.rs（find_tag、lookup_candidates 等），CAS 插入/更新/回收在
/// insert.rs（insert_to_bucket、find_or_create_tag_by_hash_with_min_addr、
/// update_address、delete），批量预取内核在 prefetch.rs（prefetch_batch_probes），
/// 单桶闩转发在 bucket.rs（try_lock_shared 等）。
pub struct HashIndex {
  pub buckets: HashBuckets,
  pub overflow_pool: OverflowPool,
  pub size: usize,
  pub mask: usize,
}

impl HashIndex {
  /// 创建指定容量的哈希索引表
  ///
  /// 要求 `num_buckets` 必须是 2 的幂且大于 0。
  pub fn new(num_buckets: usize) -> Result<Self> {
    if num_buckets == 0 || !num_buckets.is_power_of_two() {
      return Err(Error::InvalidBucketCount(num_buckets));
    }

    let buckets = HashBuckets::new(num_buckets)?;

    Ok(Self {
      buckets,
      overflow_pool: OverflowPool::new(),
      size: num_buckets,
      mask: num_buckets - 1,
    })
  }

  /// 清空哈希索引表（全部主桶置零，释放全部溢出桶）
  pub fn clear(&self) {
    self.buckets.clear();
    self.overflow_pool.clear();
  }

  /// 获取指定下标的主哈希桶引用（内部自动按 mask 截断，100% 内存安全且消除分支预测越界检查）
  #[inline(always)]
  pub fn get_bucket(&self, bucket_idx: usize) -> &HashBucket {
    let idx = bucket_idx & self.mask;
    unsafe { self.buckets.get_unchecked(idx) }
  }

  /// 使用 gxhash 高性能哈希函数计算键的 64 位哈希值
  #[inline]
  pub fn hash_key(key: &[u8]) -> u64 {
    fast_hash(key)
  }

  /// 获取指定下标的主桶引用
  #[inline]
  pub fn bucket(&self, bucket_idx: usize) -> &HashBucket {
    self.get_bucket(bucket_idx)
  }

  /// 计算哈希值对应的主桶索引下标
  #[inline]
  pub fn bucket_index_for_hash(&self, hash: u64) -> usize {
    (hash as usize) & self.mask
  }

  /// 计算键对应的主桶索引下标
  #[inline]
  pub fn bucket_index_for_key(&self, key: &[u8]) -> usize {
    let hash = Self::hash_key(key);
    (hash as usize) & self.mask
  }
}
