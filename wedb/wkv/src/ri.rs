//! 范围索引树操作（RiTreeOps，extension trait：BfTreeService 定义于 wbftree，
//! 孤儿规则下以单实现 trait 扩展其 ri_* 语义面；降固有方法块需 wbftree 反向
//! 依赖 wkv 的 CollectionError，依赖成环不可行）
//!
//! - Key: `sub_key: &[u8]`
//! - Val: `val: &[u8]`
//! - 接口：`ri_set`, `ri_set_batch`, `ri_get_callback`, `ri_del`, `ri_count_by_scan`, `ri_scan_with_field`, `ri_range_with_field`
//! - 直接将子键透传至底层独立 BfTree，零拼接开销，单树物理隔离

/// 在 garnet 中的相对路径:libs/server/Storage/Session/MainStore/RangeIndexOps.cs
use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField,
};

use crate::error::{CollectionError, CollectionResult};

/// RangeIndex 树操作 Trait
pub(crate) trait RiTreeOps {
  /// 插入或更新键值对 (新插入返回 Ok(true)，更新已有键返回 Ok(false))
  ///
  /// 存在性前查与落刷同在一次引擎借用内完成 (旧实现 contains_key + insert 各借一次)。
  fn ri_set(&self, sub_key: &[u8], val: &[u8]) -> CollectionResult<bool>;

  /// 批量插入或更新键值对，返回真实新增键数 (调用方据此单次维护 meta.size)
  ///
  /// rust 侧工程优化 (C# RangeIndexOps 无对应批量接口，按
  /// .agents/skills/transpile/SKILL.md 批量接口单次折叠机制实现)：整批一次引擎
  /// 借用，键按字节序栈上排序使批量集中命中相邻页压降页分裂，相邻重复键去重
  /// 保末值 (语义等价逐条 ri_set 后者胜)。排序/去重/存在性判定的单点实现见
  /// [`upsert`](wbftree::BfTreeService::upsert) 内核；存在性判定为
  /// O(1) 计数规约 (RI.COUNT 直读 meta.size) 的必要成本。
  fn ri_set_batch<K, V>(&self, entries: &[(K, V)]) -> CollectionResult<usize>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>;

  /// 零拷贝读取键对应的值 (零堆分配回调)
  fn ri_get_callback<R>(
    &self,
    sub_key: &[u8],
    f: impl FnOnce(Option<&[u8]>) -> R,
  ) -> CollectionResult<R>;

  /// 删除键 (若键存在且被删除返回 Ok(true)，不存在返回 Ok(false))
  fn ri_del(&self, sub_key: &[u8]) -> CollectionResult<bool>;

  /// 全树扫描计数（O(N) 复杂度，唯一消费者是迁移发布重建计数
  /// `range_index/migration.rs:publish_migrated_range_index`——换树后按树实况
  /// 重写 MetaValue.size；RESP 侧计数一律走 `range_index/ops.rs:range_index_count`
  /// 的 O(1) 直读，严禁调用本方法）
  fn ri_count_by_scan(&self) -> CollectionResult<usize>;

  /// 零拷贝流式扫描键值对 (支持指定投影字段)
  fn ri_scan_with_field<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_entry: F,
  ) -> CollectionResult<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool;

  /// 零拷贝闭区间 [start_key, end_key] 流式范围扫描 (支持指定投影字段)
  fn ri_range_with_field<F>(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    on_entry: F,
  ) -> CollectionResult<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool;
}

impl RiTreeOps for BfTreeService {
  fn ri_set(&self, sub_key: &[u8], val: &[u8]) -> CollectionResult<bool> {
    // 单条即 upsert 内核的一元素特例：新增键数为 1 即原不存在
    match self.upsert(&[(sub_key, val)]) {
      Ok(new) => Ok(new == 1),
      Err(BfTreeInsertResult::InvalidKV) => Err(CollectionError::KeyTooLong),
      Err(_) => Err(CollectionError::InvalidArgument("ri_set 插入失败")),
    }
  }

  fn ri_set_batch<K, V>(&self, entries: &[(K, V)]) -> CollectionResult<usize>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    match self.upsert(entries) {
      Ok(new) => Ok(new as usize),
      Err(BfTreeInsertResult::InvalidKV) => Err(CollectionError::KeyTooLong),
      Err(_) => Err(CollectionError::InvalidArgument("ri_set_batch 插入失败")),
    }
  }

  fn ri_get_callback<R>(
    &self,
    sub_key: &[u8],
    f: impl FnOnce(Option<&[u8]>) -> R,
  ) -> CollectionResult<R> {
    self.read_callback(sub_key, |res, bytes| match res {
      BfTreeReadResult::Found => Ok(f(Some(bytes))),
      BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => Ok(f(None)),
      _ => Err(CollectionError::InvalidArgument("ri_get 读取失败")),
    })
  }

  fn ri_del(&self, sub_key: &[u8]) -> CollectionResult<bool> {
    let exists = self.contains_key(sub_key);
    match self.delete(sub_key) {
      BfTreeDeleteResult::Success => Ok(exists),
      _ => Err(CollectionError::InvalidArgument("ri_del 删除失败")),
    }
  }

  fn ri_count_by_scan(&self) -> CollectionResult<usize> {
    // 全区间起点取 [0]：引擎层扫描要求 start_key 至少 1 字节，空键恒判
    // InvalidStartKey（非空键恒 ≥ [0]，起点即全域）
    self
      .scan_with_count_callback(&[0], usize::MAX, ScanReturnField::Key, |_, _| true)
      .map_err(Into::into)
  }

  fn ri_scan_with_field<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    mut on_entry: F,
  ) -> CollectionResult<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if count == 0 {
      return Ok(0);
    }
    self
      .scan_with_count_callback(start_key, count, return_field, |k, v| on_entry(k, v))
      .map_err(Into::into)
  }

  fn ri_range_with_field<F>(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    mut on_entry: F,
  ) -> CollectionResult<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if start_key > end_key {
      return Ok(0);
    }
    self
      .scan_with_end_key_callback(start_key, end_key, return_field, |k, v| on_entry(k, v))
      .map_err(Into::into)
  }
}
