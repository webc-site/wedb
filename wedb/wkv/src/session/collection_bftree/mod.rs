//! 千万级 BfTree 集合会话管理模块 (Hash / Set / ZSet / List)
//!
//! 基于集合库 `wcol` 与底层存储 `wbftree` / `RangeIndexManager` 构建，
//! 遵循零拷贝、极致性能、单次迭代、栈分配与严格删空生命周期规范。
//!
//! 支持集合操作：
//! - Hash: `bftree_hset`, `bftree_hget`, `bftree_hdel`, `bftree_hlen`, `bftree_hscan`, `bftree_hscan_stream`
//! - Set: `bftree_sadd`, `bftree_srem`, `bftree_sismember`, `bftree_scard`, `bftree_sscan`, `bftree_sscan_stream`
//! - ZSet: `bftree_zadd`, `bftree_zrem`, `bftree_zscore`, `bftree_zrange_by_score`, `bftree_zrange_by_score_stream`, `bftree_zcount`, `bftree_zcard`
//! - List: `bftree_lpush`, `bftree_rpush`, `bftree_lpop`, `bftree_rpop`, `bftree_lindex`, `bftree_lrange`, `bftree_lset`, `bftree_ltrim`, `bftree_llen`
//!
//! 生命周期与恢复：
//! - 删空生命周期：集合元素清零时写墓碑删除主存元记录、清理 TTL，并调用 `RangeIndexManager::delete_index` 排空在途写者并删除底层磁盘数据文件；
//! - CPR 检查点与故障恢复：无缝接入 `snapshot_all_trees_for_checkpoint` 与 `recover_range_indexes`。

pub mod hash;
pub mod list;
pub mod set;
pub mod zset;

use std::sync::{Arc, atomic::Ordering};

use wbftree::{
  RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub, StorageBackend, StorageBackendType,
  TreeTuning,
};
use wcol::{LIST_STUB_SIZE, ListStub};
use wdev::Device;
use wval::{CollectionType, META_VALUE_SIZE, MetaValue, StorageEncoding};

use crate::{
  error::Result,
  range_index::{RangeIndexError, TreeReadGuard, encode_meta_stub_record, range_index_blocking},
  session::StoreSession,
};

/// BfTree 集合默认页缓存大小 (16MiB)
pub(crate) const DEFAULT_BFTREE_CACHE_SIZE: usize = 16 * 1024 * 1024;
/// BfTree 集合最小记录大小 (4B，支持单字节成员/空值)
pub(crate) const DEFAULT_BFTREE_MIN_RECORD_SIZE: usize = 4;
/// BfTree 集合最大记录大小 (4096B)
pub(crate) const DEFAULT_BFTREE_MAX_RECORD_SIZE: usize = 4096;
/// BfTree 集合最大键长度 (1024B)
pub(crate) const DEFAULT_BFTREE_MAX_KEY_LEN: usize = 1024;

/// 内存直读存根解码结果
pub(crate) enum LoadedStub {
  NotFound,
  Inactive(u64, u64),
  Active(MetaValue, RangeIndexStub, Option<ListStub>),
}

impl<D: Device> StoreSession<D> {
  /// 创建新的 BfTree 索引实例并获取在线条带共享读锁
  pub(crate) async fn create_bftree_tree(
    &self,
    key: &[u8],
  ) -> Result<(TreeReadGuard<'_>, RangeIndexStub)> {
    let mut tuning = TreeTuning {
      cache_size: DEFAULT_BFTREE_CACHE_SIZE,
      min_record_size: DEFAULT_BFTREE_MIN_RECORD_SIZE,
      max_record_size: DEFAULT_BFTREE_MAX_RECORD_SIZE,
      max_key_len: DEFAULT_BFTREE_MAX_KEY_LEN,
      leaf_page_size: 0,
    };
    RangeIndexManager::resolve_tuning(&mut tuning);
    let mgr = Arc::clone(&self.store.range_index);
    let create_key = key.to_vec();
    let backend = StorageBackend::Std;
    let create_tuning = tuning;
    let tree = range_index_blocking(move || mgr.create_bftree(&create_key, backend, create_tuning))
      .await?
      .map_err(RangeIndexError::from)?;
    let stub = RangeIndexStub::from_tuning(tree.native_ptr(), &tuning, StorageBackendType::Disk);
    let guard = self.acquire_tree_read(key, &stub).await?;
    Ok((guard, stub))
  }

  /// 读取集合元数据及存根 (单帧解析 MetaValue + TTL 检查 + 存根解码，零堆分配)
  pub(crate) async fn load_bftree_meta_stub(
    &self,
    key: &[u8],
    expected_type: CollectionType,
  ) -> Result<Option<(MetaValue, RangeIndexStub, Option<ListStub>)>> {
    let meta_k = self.session_meta_key(key);
    let loaded = self
      .read_raw_with(&meta_k, |bytes| -> Result<LoadedStub> {
        if bytes.len() < META_VALUE_SIZE {
          return Ok(LoadedStub::NotFound);
        }
        let meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])?;
        if meta.size == 0 {
          return Ok(LoadedStub::Inactive(meta.key_id, meta.version));
        }
        if meta.collection_type != expected_type
          || meta.encoding() != StorageEncoding::FlattenedTree
        {
          return Err(RangeIndexError::WrongType.into());
        }
        if expected_type == CollectionType::List {
          if bytes.len() < META_VALUE_SIZE + LIST_STUB_SIZE {
            return Ok(LoadedStub::NotFound);
          }
          let list_stub =
            ListStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + LIST_STUB_SIZE])?;
          Ok(LoadedStub::Active(
            meta,
            list_stub.range_stub,
            Some(list_stub),
          ))
        } else {
          if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
            return Ok(LoadedStub::NotFound);
          }
          let stub = RangeIndexStub::decode(
            &bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE],
          )
          .map_err(RangeIndexError::from)?;
          Ok(LoadedStub::Active(meta, stub, None))
        }
      })
      .await?;

    match loaded {
      None => {
        if self.read(key).await?.is_some() {
          return Err(RangeIndexError::WrongType.into());
        }
        Ok(None)
      }
      Some(res) => match res? {
        LoadedStub::NotFound => Ok(None),
        LoadedStub::Inactive(key_id, version) => {
          self.store.update_key_id_meta(key_id, version, false);
          let _ = self.delete_raw(&meta_k).await;
          let _ = self.del_ttl(key).await;
          Ok(None)
        }
        LoadedStub::Active(meta, stub, list_stub) => {
          if self.has_ttl_tag(key)? && self.check_expired(key).await? {
            return Ok(None);
          }
          self
            .store
            .update_key_id_meta(meta.key_id, meta.version, true);
          Ok(Some((meta, stub, list_stub)))
        }
      },
    }
  }

  /// 栈分配持久化 MetaValue 与 RangeIndexStub (定长 67 字节，零堆分配)
  pub(crate) async fn save_bftree_meta_stub(
    &self,
    key: &[u8],
    meta: &MetaValue,
    stub: &RangeIndexStub,
  ) -> Result<()> {
    let meta_k = self.session_meta_key(key);
    let val = encode_meta_stub_record(meta, stub);
    self.upsert_raw(&meta_k, &val).await?;
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, true);
    Ok(())
  }

  /// 栈分配持久化 MetaValue 与 ListStub (定长 83 字节，零堆分配)
  pub(crate) async fn save_bftree_list_stub(
    &self,
    key: &[u8],
    meta: &MetaValue,
    list_stub: &ListStub,
  ) -> Result<()> {
    let meta_k = self.session_meta_key(key);
    let mut val = [0u8; META_VALUE_SIZE + LIST_STUB_SIZE];
    val[..META_VALUE_SIZE].copy_from_slice(&meta.to_bytes());
    val[META_VALUE_SIZE..].copy_from_slice(&list_stub.encode());
    self.upsert_raw(&meta_k, &val).await?;
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, true);
    Ok(())
  }

  /// 严格删空生命周期：写墓碑、清理 TTL，并排空在途写者、释放树实例及删除磁盘数据文件
  pub(crate) async fn handle_bftree_drain_and_delete(
    &self,
    key: &[u8],
    meta: &MetaValue,
  ) -> Result<()> {
    let mut meta_copy = *meta;
    self
      .drain_and_delete_collection_meta(key, &mut meta_copy)
      .await?;
    let mgr = Arc::clone(&self.store.range_index);
    let del_key = key.to_vec();
    let _ = range_index_blocking(move || mgr.delete_index(&del_key)).await??;
    if !self.store.aof_listeners_paused.load(Ordering::Relaxed)
      && let Some(listener) = self.store.range_drop_listener()
    {
      listener(key);
    }
    Ok(())
  }

  /// O(1) 直读 BfTree 集合元素总数 (直读主存元记录 MetaValue.size，严禁扫树)
  #[inline]
  pub(crate) async fn bftree_card(&self, key: &[u8], col_type: CollectionType) -> Result<usize> {
    let loaded = self.load_bftree_meta_stub(key, col_type).await?;
    let Some((meta, ..)) = loaded else {
      return Ok(0);
    };
    Ok(meta.size as usize)
  }

  /// 只读执行 BfTree 算子：
  /// - 若集合不存在或已过期，调用 `on_missing()` 返回默认值；
  /// - 若集合存在，加载存根、获取条带共享读锁 TreeReadGuard，调用 `op(&tree)`；
  /// - 零堆分配、零拷贝。
  pub(crate) async fn with_bftree_read<R, F>(
    &self,
    key: &[u8],
    expected_type: CollectionType,
    on_missing: impl FnOnce() -> R,
    op: F,
  ) -> Result<R>
  where
    F: FnOnce(&TreeReadGuard<'_>) -> Result<R>,
  {
    let loaded = self.load_bftree_meta_stub(key, expected_type).await?;
    let Some((_, stub, _)) = loaded else {
      return Ok(on_missing());
    };
    let tree = self.acquire_tree_read(key, &stub).await?;
    op(&tree)
  }

  /// 插入/更新 BfTree 集合算子：
  /// - 若集合已存在：获取条带共享读锁，在闭包中执行修改；闭包返回 `(delta_size, user_ret)`；
  ///   若 `delta_size > 0`，`meta.inc_size(delta_size)` 并更新元数据存根；
  /// - 若集合不存在：创建底层 BfTree 实例，在闭包中执行修改；
  ///   若 `delta_size == 0`，回滚清理释放树文件，返回 `user_ret`；
  ///   若 `delta_size > 0`，分配新 key_id，初始化 MetaValue 并持久化存根，失败则回滚清理树文件。
  pub(crate) async fn with_bftree_write_or_create<R, F>(
    &self,
    key: &[u8],
    col_type: CollectionType,
    op: F,
  ) -> Result<R>
  where
    F: FnOnce(&TreeReadGuard<'_>) -> Result<(u64, R)>,
  {
    let loaded = self.load_bftree_meta_stub(key, col_type).await?;
    if let Some((mut meta, stub, _)) = loaded {
      let tree = self.acquire_tree_read(key, &stub).await?;
      let (delta, ret) = op(&tree)?;
      drop(tree);
      if delta > 0 {
        meta.inc_size(delta);
        self.save_bftree_meta_stub(key, &meta, &stub).await?;
      }
      Ok(ret)
    } else {
      let (tree, stub) = self.create_bftree_tree(key).await?;
      let (delta, ret) = match op(&tree) {
        Ok(res) => res,
        Err(e) => {
          let _ = self.store.range_index.delete_index(key);
          return Err(e);
        }
      };
      drop(tree);
      if delta == 0 {
        let _ = self.store.range_index.delete_index(key);
        return Ok(ret);
      }
      let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
      let mut meta = MetaValue::new(key_id, col_type, 1, delta);
      meta.set_encoding(StorageEncoding::FlattenedTree);
      if let Err(e) = self.save_bftree_meta_stub(key, &meta, &stub).await {
        let _ = self.store.range_index.delete_index(key);
        return Err(e);
      }
      Ok(ret)
    }
  }

  /// 删除 BfTree 集合元素算子（带严格删空生命周期与原子墓碑自愈）：
  /// - 若集合不存在或已过期，返回 `on_missing()`；
  /// - 若集合存在，获取条带共享读锁执行删除；闭包返回 `(removed_count, user_ret)`；
  /// - 若 `removed_count > 0`：
  ///   - `meta.dec_size(removed_count)`；
  ///   - 若 `meta.size == 0`：严格调用 `handle_bftree_drain_and_delete`，原子写入元记录墓碑、清理随键 TTL、推进版本号、排空在途写者并 unlink 磁盘数据文件；
  ///   - 若 `meta.size > 0`：更新保存元数据存根；
  /// - 返回 `user_ret`。
  pub(crate) async fn with_bftree_remove<R, F>(
    &self,
    key: &[u8],
    col_type: CollectionType,
    on_missing: impl FnOnce() -> R,
    op: F,
  ) -> Result<R>
  where
    F: FnOnce(&TreeReadGuard<'_>) -> Result<(u64, R)>,
  {
    let loaded = self.load_bftree_meta_stub(key, col_type).await?;
    let Some((mut meta, stub, _)) = loaded else {
      return Ok(on_missing());
    };
    let tree = self.acquire_tree_read(key, &stub).await?;
    let (removed, ret) = op(&tree)?;
    drop(tree);
    if removed > 0 {
      meta.dec_size(removed);
      if meta.size == 0 {
        self.handle_bftree_drain_and_delete(key, &meta).await?;
      } else {
        self.save_bftree_meta_stub(key, &meta, &stub).await?;
      }
    }
    Ok(ret)
  }

  /// 只读执行 List 算子 (带 ListStub)：
  /// - 若集合不存在或已过期，调用 `on_missing()` 返回默认值；
  /// - 若集合存在，加载存根、获取条带共享读锁，调用 `op(&tree, &list_stub)`。
  pub(crate) async fn with_bftree_read_list<R, F>(
    &self,
    key: &[u8],
    on_missing: impl FnOnce() -> R,
    op: F,
  ) -> Result<R>
  where
    F: FnOnce(&TreeReadGuard<'_>, &ListStub) -> Result<R>,
  {
    let loaded = self
      .load_bftree_meta_stub(key, CollectionType::List)
      .await?;
    let Some((_, stub, Some(list_stub))) = loaded else {
      return Ok(on_missing());
    };
    let tree = self.acquire_tree_read(key, &stub).await?;
    op(&tree, &list_stub)
  }

  /// 推入 List 元素算子：
  /// - 若集合已存在：执行 `op(&tree, &mut list_stub)`，同步 `meta.size = new_len` 并持久化 `ListStub`；
  /// - 若集合不存在：创建底层树与 `ListStub(stub, 0, 0)`，执行 `op`，若失败清理树，成功则分配 key_id，初始化 MetaValue 并持久化 `ListStub`。
  pub(crate) async fn with_bftree_list_push<F>(&self, key: &[u8], op: F) -> Result<usize>
  where
    F: FnOnce(&TreeReadGuard<'_>, &mut ListStub) -> Result<usize>,
  {
    let loaded = self
      .load_bftree_meta_stub(key, CollectionType::List)
      .await?;
    if let Some((mut meta, stub, Some(mut list_stub))) = loaded {
      let tree = self.acquire_tree_read(key, &stub).await?;
      let new_len = op(&tree, &mut list_stub)?;
      drop(tree);
      meta.size = new_len as u64;
      self.save_bftree_list_stub(key, &meta, &list_stub).await?;
      Ok(new_len)
    } else {
      let (tree, stub) = self.create_bftree_tree(key).await?;
      let mut list_stub = ListStub::new(stub, 0, 0);
      let new_len = match op(&tree, &mut list_stub) {
        Ok(len) => len,
        Err(e) => {
          let _ = self.store.range_index.delete_index(key);
          return Err(e);
        }
      };
      drop(tree);
      let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
      let mut meta = MetaValue::new(key_id, CollectionType::List, 1, new_len as u64);
      meta.set_encoding(StorageEncoding::FlattenedTree);
      if let Err(e) = self.save_bftree_list_stub(key, &meta, &list_stub).await {
        let _ = self.store.range_index.delete_index(key);
        return Err(e);
      }
      Ok(new_len)
    }
  }

  /// 弹出/截断 List 元素算子（带严格删空生命周期与原子墓碑自愈）：
  /// - 若集合不存在或已过期，返回 `on_missing()`；
  /// - 若集合存在：执行 `op(&tree, &mut list_stub)`，返回 `(changed, ret)`；
  /// - 若 `changed` 为 true：
  ///   - `meta.size = list_stub.len() as u64`；
  ///   - 若 `meta.size == 0`：严格调用 `handle_bftree_drain_and_delete`；
  ///   - 若 `meta.size > 0`：持久化更新 `ListStub`；
  /// - 返回 `ret`。
  pub(crate) async fn with_bftree_list_pop_or_trim<R, F>(
    &self,
    key: &[u8],
    on_missing: impl FnOnce() -> R,
    op: F,
  ) -> Result<R>
  where
    F: FnOnce(&TreeReadGuard<'_>, &mut ListStub) -> Result<(bool, R)>,
  {
    let loaded = self
      .load_bftree_meta_stub(key, CollectionType::List)
      .await?;
    let Some((mut meta, stub, Some(mut list_stub))) = loaded else {
      return Ok(on_missing());
    };
    let tree = self.acquire_tree_read(key, &stub).await?;
    let (changed, ret) = op(&tree, &mut list_stub)?;
    drop(tree);
    if changed {
      let new_len = list_stub.len();
      meta.size = new_len as u64;
      if meta.size == 0 {
        self.handle_bftree_drain_and_delete(key, &meta).await?;
      } else {
        self.save_bftree_list_stub(key, &meta, &list_stub).await?;
      }
    }
    Ok(ret)
  }
}
