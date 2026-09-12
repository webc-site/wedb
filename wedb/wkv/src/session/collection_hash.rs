//! Hash 集合透明路由统一入口
//!
//! 规约参照 `.agents/skills/transpile/SKILL.md`：
//! - 初始创建：默认使用 CompactHash
//! - 若当前为 Compact：在紧凑载荷中操作，写后若项数 > 32768 或体积 > 1MB，自动调用 migrate_compact_to_flattened_hash 升级为 Flattened
//! - 若当前为 Flattened：调用 flattened_* 打平算子
//! - 删空语义：当 size 减至 0 时彻底清理元数据与 TTL

use std::sync::atomic::Ordering;

use wcol::HashTreeOps;
use wdev::Device;
use wval::{CollectionType, CompactHashCodec, KeyTag, MetaValue, StorageEncoding};

use crate::{
  error::Result,
  session::{
    StoreSession,
    collection_flattened::{COMPACT_HASH_ENTRY_MAX_OVERHEAD, should_upgrade_hash},
  },
};

impl<D: Device> StoreSession<D> {
  /// 设置哈希字段值（透明路由 Compact 与 Flattened 打平存储；新插入返回 Ok(true)，更新已有字段返回 Ok(false)）
  pub async fn hset(&self, user_key: &[u8], field: &[u8], value: &[u8]) -> Result<bool> {
    let _key_lock = self.store.index.acquire_keys_lock_exclusive(&[user_key])?;
    let (mut meta, mut payload) = match self
      .load_collection_raw_write(user_key, CollectionType::Hash)
      .await?
    {
      Some((meta, payload_opt)) => (meta, payload_opt.unwrap_or_default()),
      None => {
        let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
        let mut meta = MetaValue::new(key_id, CollectionType::Hash, 1, 0);
        meta.set_encoding(StorageEncoding::Compact);
        (meta, Vec::new())
      }
    };

    if meta.encoding() == StorageEncoding::FlattenedTree {
      // BfTree 树后端：转发树算子（bftree_hset 内部自管元数据与存根）
      return self.bftree_hset(user_key, field, value).await;
    }
    if meta.encoding() == StorageEncoding::Flattened {
      return self
        .flattened_hset_inner(user_key, &mut meta, field, value)
        .await;
    }

    let estimated_payload_len = payload
      .len()
      .saturating_add(field.len())
      .saturating_add(value.len())
      .saturating_add(COMPACT_HASH_ENTRY_MAX_OVERHEAD);

    let will_upgrade = field.len() > u16::MAX as usize
      || value.len() > u16::MAX as usize
      || should_upgrade_hash(
        (meta.size as usize).saturating_add(1),
        estimated_payload_len,
      );

    if will_upgrade {
      if !payload.is_empty() {
        self
          .migrate_compact_to_flattened_hash(user_key, &mut meta, &payload)
          .await?;
      } else {
        meta.set_encoding(StorageEncoding::Flattened);
      }
      self
        .flattened_hset_inner(user_key, &mut meta, field, value)
        .await
    } else {
      let is_new = CompactHashCodec::set_field(&mut payload, field, value, None)?;
      if is_new {
        meta.inc_size(1);
      }
      if should_upgrade_hash(meta.size as usize, payload.len()) {
        self
          .migrate_compact_to_flattened_hash(user_key, &mut meta, &payload)
          .await?;
      } else {
        self.save_compact_meta(user_key, &meta, &payload).await?;
      }
      Ok(is_new)
    }
  }

  /// 零拷贝读取哈希字段值（透明路由 Compact 与 Flattened 打平存储）
  pub async fn hget_with<R>(
    &self,
    user_key: &[u8],
    field: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    let Some(raw) = self
      .load_collection_raw_read(user_key, CollectionType::Hash)
      .await?
    else {
      return Ok(None);
    };
    if raw.meta.encoding() == StorageEncoding::FlattenedTree {
      self.bftree_hget_with(user_key, field, f).await
    } else if raw.meta.encoding() == StorageEncoding::Flattened {
      let sub_k = self.sub_key(KeyTag::Hash, raw.meta.key_id, raw.meta.version, field);
      self.read_raw_with(&sub_k, f).await
    } else if let Some(payload) = raw.compact_payload() {
      Ok(CompactHashCodec::find_field(payload, field).map(f))
    } else {
      Ok(None)
    }
  }

  /// 读取哈希字段值（透明路由 Compact 与 Flattened 打平存储）
  #[inline]
  pub async fn hget(&self, user_key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>> {
    self.hget_with(user_key, field, |v| v.to_vec()).await
  }

  /// 删除哈希字段（透明路由 Compact 与 Flattened 打平存储；严格删空自愈）
  pub async fn hdel(&self, user_key: &[u8], field: &[u8]) -> Result<bool> {
    let _key_lock = self.store.index.acquire_keys_lock_exclusive(&[user_key])?;
    let Some((mut meta, payload_opt)) = self
      .load_collection_raw_write(user_key, CollectionType::Hash)
      .await?
    else {
      return Ok(false);
    };
    if meta.encoding() == StorageEncoding::FlattenedTree {
      self.bftree_hdel(user_key, field).await
    } else if meta.encoding() == StorageEncoding::Flattened {
      self.flattened_hdel_inner(user_key, &mut meta, field).await
    } else {
      let mut payload = payload_opt.unwrap_or_default();
      let deleted = CompactHashCodec::delete_field(&mut payload, field)?;
      if deleted {
        meta.dec_size(1);
        if meta.size == 0 {
          self
            .drain_and_delete_collection_meta(user_key, &mut meta)
            .await?;
        } else {
          self.save_compact_meta(user_key, &meta, &payload).await?;
        }
      }
      Ok(deleted)
    }
  }

  /// 获取哈希表字段总数（O(1) 直读主存元数据 size，严禁扫全集）
  pub async fn hlen(&self, user_key: &[u8]) -> Result<usize> {
    let Some(meta) = self.load_meta(user_key).await? else {
      return Ok(0);
    };
    if meta.collection_type != CollectionType::Hash {
      return Ok(0);
    }
    Ok(meta.size as usize)
  }

  /// 判断哈希表中指定字段是否存在（透明路由 Compact 与 Flattened 打平存储）
  pub async fn hexists(&self, user_key: &[u8], field: &[u8]) -> Result<bool> {
    let Some(raw) = self
      .load_collection_raw_read(user_key, CollectionType::Hash)
      .await?
    else {
      return Ok(false);
    };
    if raw.meta.encoding() == StorageEncoding::FlattenedTree {
      self.bftree_hexists(user_key, field).await
    } else if raw.meta.encoding() == StorageEncoding::Flattened {
      let sub_k = self.sub_key(KeyTag::Hash, raw.meta.key_id, raw.meta.version, field);
      self.contains_key_raw(&sub_k).await
    } else if let Some(payload) = raw.compact_payload() {
      Ok(CompactHashCodec::find_field(payload, field).is_some())
    } else {
      Ok(false)
    }
  }

  /// 批量读取哈希字段值（透明路由 Compact 与 Flattened 打平存储）
  pub async fn hmget(&self, user_key: &[u8], fields: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
    let Some(raw) = self
      .load_collection_raw_read(user_key, CollectionType::Hash)
      .await?
    else {
      return Ok(vec![None; fields.len()]);
    };
    if raw.meta.encoding() == StorageEncoding::FlattenedTree {
      // BfTree 树后端：单次装载存根后逐字段零拷贝点查（免逐字段重复 load meta）
      self
        .with_bftree_read(
          user_key,
          CollectionType::Hash,
          || vec![None; fields.len()],
          |tree| {
            let mut results = Vec::with_capacity(fields.len());
            for field in fields {
              results.push(tree.hget_callback(field, |opt| opt.map(|v| v.to_vec()))?);
            }
            Ok(results)
          },
        )
        .await
    } else if raw.meta.encoding() == StorageEncoding::Flattened {
      self.hmget_flattened_inner(&raw.meta, fields).await
    } else {
      let payload = raw.compact_payload().unwrap_or(&[]);
      let mut results = Vec::with_capacity(fields.len());
      for &field in fields {
        results.push(CompactHashCodec::find_field(payload, field).map(|v| v.to_vec()));
      }
      Ok(results)
    }
  }
}
