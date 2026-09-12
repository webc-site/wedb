//! 大 Hash 打平存储（Flattened SubKey）纯算子与升级调度
//!
//! 规约参照 `.agents/skills/transpile/SKILL.md`：
//! - 离散点查型（Hash）：
//!   - 小集合（<=32768 项且 <=1MB）：Compact 紧凑二进制内联存入 whlog
//!   - 大集合（>32768 项或 >1MB）：采用 wkv whlog 打平存储（Flattened SubKey），点查保持 O(1)，版本号栅栏实现 O(1) 级联失效
//! - 定长刚性帧隔离公理：
//!   - 打平子键统一采用前 17B 定长帧：`[prefix][KeyTag::Hash][key_id: 8B be][version: 8B be][field]`
//! - 50% 迟滞防震荡门限：
//!   - 升级阈值：项数 > 32768 或体积 > 1MB，转换为大集合存储
//!   - 降级阈值：项数 <= 16384 且体积 <= 512KB，自动合并回 Compact

use std::sync::atomic::Ordering;

use wbase::time::now_ticks;
use wdev::Device;
use whasher::{GxBuildHasher, HashSet};
use wval::{CollectionType, CompactHashCodec, KeyTag, MetaValue, StorageEncoding};

use crate::{error::Result, session::StoreSession};

/// 单个紧凑哈希条目最大编码开销（长度前缀与过期标记等元数据）：
/// 2B (field len) + 2B (value len) + 1B (expire flag) + 8B (expire ticks) = 13B
pub const COMPACT_HASH_ENTRY_MAX_OVERHEAD: usize = 13;

/// 大哈希动态升级项数门限（> 32768 项触发升级）
pub const HASH_UPGRADE_ITEM_THRESHOLD: usize = 32768;
/// 大哈希动态升级体积门限（> 1MB 触发升级）
pub const HASH_UPGRADE_BYTE_THRESHOLD: usize = 1024 * 1024;
/// 大哈希动态降级项数门限（<= 16384 项且 <= 512KB 触发降级，50% 迟滞防抖动）
pub const HASH_DOWNGRADE_ITEM_THRESHOLD: usize = 16384;
/// 大哈希动态降级体积门限（<= 512KB 触发降级）
pub const HASH_DOWNGRADE_BYTE_THRESHOLD: usize = 512 * 1024;

/// 元数据 reserved[1] 次高位：降级受阻粘性标志（上次聚合判定失败：
/// 存在超 u16 上限字段或字节数超门限），置位后 hdel 零扫描直通；
/// 解锁路径（新增字段 / 超限字段被删 / 字节快照递减跨过门限 / 降级迁移，
/// 见 `maybe_downgrade_flattened_hash` 与 `flattened_hdel_inner`）
pub const META_DOWNGRADE_BLOCKED_MASK: u8 = 0x40;

/// 降级受阻字节快照在 reserved 内的起始偏移（reserved[2..7] 共 5 字节大端 u40）
const META_DOWNGRADE_BYTES_OFFSET: usize = 2;
/// 降级受阻字节快照饱和上限（u40 全 1：超限字段受阻场景无字节总量可记，置饱和
/// 表示"巨量"，删除递减永不触底，解锁仅靠超限字段本身被删）
const DOWNGRADE_BYTES_SATURATED: u64 = (1 << 40) - 1;

/// 读取降级受阻粘性标志（const fn）
#[inline(always)]
pub const fn get_meta_downgrade_blocked(reserved: &[u8; 7]) -> bool {
  reserved[1] & META_DOWNGRADE_BLOCKED_MASK != 0
}

/// 置位降级受阻粘性标志（const fn）
#[inline(always)]
pub const fn set_meta_downgrade_blocked(reserved: &mut [u8; 7]) {
  reserved[1] |= META_DOWNGRADE_BLOCKED_MASK;
}

/// 清除降级受阻粘性标志（const fn）
#[inline(always)]
pub const fn clear_meta_downgrade_blocked(reserved: &mut [u8; 7]) {
  reserved[1] &= !META_DOWNGRADE_BLOCKED_MASK;
}

/// 读取降级受阻字节快照（const fn，u40 大端；仅在受阻标志置位时有效）
#[inline(always)]
const fn get_meta_downgrade_bytes(reserved: &[u8; 7]) -> u64 {
  let o = META_DOWNGRADE_BYTES_OFFSET;
  ((reserved[o] as u64) << 32)
    | ((reserved[o + 1] as u64) << 24)
    | ((reserved[o + 2] as u64) << 16)
    | ((reserved[o + 3] as u64) << 8)
    | (reserved[o + 4] as u64)
}

/// 饱和写入降级受阻字节快照（const fn，u40 大端；超上限饱和记账）
#[inline(always)]
const fn set_meta_downgrade_bytes(reserved: &mut [u8; 7], bytes: u64) {
  let v = if bytes > DOWNGRADE_BYTES_SATURATED {
    DOWNGRADE_BYTES_SATURATED
  } else {
    bytes
  };
  let o = META_DOWNGRADE_BYTES_OFFSET;
  reserved[o] = (v >> 32) as u8;
  reserved[o + 1] = (v >> 24) as u8;
  reserved[o + 2] = (v >> 16) as u8;
  reserved[o + 3] = (v >> 8) as u8;
  reserved[o + 4] = v as u8;
}

/// 检查是否满足大哈希动态升级条件（项数 > 32768 或 体积 > 1MB）
#[inline(always)]
pub const fn should_upgrade_hash(item_count: usize, byte_count: usize) -> bool {
  item_count > HASH_UPGRADE_ITEM_THRESHOLD || byte_count > HASH_UPGRADE_BYTE_THRESHOLD
}

/// 检查是否满足大哈希动态降级条件（项数 <= 16384 且 体积 <= 512KB，50% 迟滞防震荡）
#[inline(always)]
pub const fn should_downgrade_hash(item_count: usize, byte_count: usize) -> bool {
  item_count <= HASH_DOWNGRADE_ITEM_THRESHOLD && byte_count <= HASH_DOWNGRADE_BYTE_THRESHOLD
}

impl<D: Device> StoreSession<D> {
  /// 将 Compact 紧凑哈希载荷全量打平迁移为 SubKey 离散存储（跳过已过期字段，精确同步元数据 size）
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/HashOps.cs
  pub async fn migrate_compact_to_flattened_hash(
    &self,
    user_key: &[u8],
    meta: &mut MetaValue,
    compact_payload: &[u8],
  ) -> Result<()> {
    let now = now_ticks();
    let prefix = self.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut valid_count = 0u64;

    for entry in CompactHashCodec::iter_fields(compact_payload) {
      if let Some(exp) = entry.expire_at_ticks
        && exp < now
      {
        continue;
      }
      let sub_k = Self::sub_key_with_prefix(
        prefix_slice,
        KeyTag::Hash,
        meta.key_id,
        meta.version,
        entry.field,
      );
      self.upsert_raw(&sub_k, entry.value).await?;
      valid_count += 1;
    }
    if valid_count == 0 {
      self.drain_and_delete_collection_meta(user_key, meta).await
    } else {
      meta.size = valid_count;
      meta.set_encoding(StorageEncoding::Flattened);
      // 升级迁移开启新版本生命周期：清除降级受阻负缓存残留
      clear_meta_downgrade_blocked(&mut meta.reserved);
      self.save_meta(user_key, meta).await
    }
  }

  /// 将 Flattened 打平哈希条目平滑降级迁移为 Compact 紧凑二进制格式（过滤已过期字段，精确同步元数据 size）
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/HashOps.cs
  ///
  /// 50% 迟滞防震荡降级门限：项数 <= 16384 且体积 <= 512KB
  /// 迁移时按 now_ticks() 过滤过期字段，精确同步 meta.size；
  /// 推进逻辑版本号（版本号栅栏使旧打平子键立即逻辑失效）；
  /// 将 StorageEncoding 切换为 Compact 并写入紧凑元数据记录。
  pub async fn migrate_flattened_to_compact_hash(
    &self,
    user_key: &[u8],
    meta: &mut MetaValue,
    entries: &[(&[u8], &[u8], Option<i64>)],
  ) -> Result<()> {
    let now = now_ticks();
    let mut payload = Vec::with_capacity(
      entries
        .iter()
        .map(|(f, v, _)| f.len() + v.len() + COMPACT_HASH_ENTRY_MAX_OVERHEAD)
        .sum(),
    );
    let mut valid_count = 0u64;

    for &(f, v, exp_ticks) in entries {
      if let Some(exp) = exp_ticks
        && exp < now
      {
        continue;
      }
      if f.len() > u16::MAX as usize {
        return Err(wval::Error::InvalidArgument("field 长度超出紧凑编码 u16 上限").into());
      }
      if v.len() > u16::MAX as usize {
        return Err(wval::Error::InvalidArgument("value 长度超出紧凑编码 u16 上限").into());
      }
      CompactHashCodec::set_field(&mut payload, f, v, exp_ticks)?;
      valid_count += 1;
    }

    if valid_count == 0 {
      self.drain_and_delete_collection_meta(user_key, meta).await
    } else {
      meta.version = meta.version.wrapping_add(1);
      meta.size = valid_count;
      meta.set_encoding(StorageEncoding::Compact);
      // 降级迁移清除受阻负缓存（Compact 态本不判定降级，防未来再升级残留）
      clear_meta_downgrade_blocked(&mut meta.reserved);
      self
        .store
        .update_key_id_meta(meta.key_id, meta.version, true);
      self.save_compact_meta(user_key, meta, &payload).await
    }
  }

  /// 打平存储：设置哈希字段（新插入返回 Ok(true)，更新已有字段返回 Ok(false)；若原为 Compact 自动迁移）
  ///
  /// FlattenedTree 树后端键转发树算子（防止 whlog 子键写入覆盖带存根的树元记录，
  /// 破坏 BfTree hash 完整性）
  pub async fn flattened_hset(&self, user_key: &[u8], field: &[u8], value: &[u8]) -> Result<bool> {
    let _key_lock = self.store.index.acquire_keys_lock_exclusive(&[user_key])?;
    if let Some((mut meta, payload_opt)) = self
      .load_collection_raw_write(user_key, CollectionType::Hash)
      .await?
    {
      if meta.encoding() == StorageEncoding::FlattenedTree {
        return self.bftree_hset(user_key, field, value).await;
      }
      if meta.encoding() == StorageEncoding::Compact
        && let Some(payload) = payload_opt
      {
        self
          .migrate_compact_to_flattened_hash(user_key, &mut meta, &payload)
          .await?;
      }
      self
        .flattened_hset_inner(user_key, &mut meta, field, value)
        .await
    } else {
      let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
      let mut meta = MetaValue::new(key_id, CollectionType::Hash, 1, 0);
      meta.set_encoding(StorageEncoding::Flattened);
      self
        .flattened_hset_inner(user_key, &mut meta, field, value)
        .await
    }
  }

  /// 打平存储：设置哈希字段内部实现（调用方已持有条带排他锁）
  pub(crate) async fn flattened_hset_inner(
    &self,
    user_key: &[u8],
    meta: &mut MetaValue,
    field: &[u8],
    value: &[u8],
  ) -> Result<bool> {
    let sub_k = self.sub_key(KeyTag::Hash, meta.key_id, meta.version, field);
    let is_new = !self.contains_key_raw(&sub_k).await?;
    self.upsert_raw(&sub_k, value).await?;
    if is_new {
      meta.inc_size(1);
      meta.set_encoding(StorageEncoding::Flattened);
      // 字段集变化解锁降级重试（负缓存仅在字段数不变时保持有效）
      clear_meta_downgrade_blocked(&mut meta.reserved);
      self.save_meta(user_key, meta).await?;
    }
    Ok(is_new)
  }

  /// 打平存储：零拷贝读取哈希字段值（点查 O(1) 物理直读，零堆分配）
  #[inline]
  pub async fn flattened_hget_with<R>(
    &self,
    user_key: &[u8],
    field: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    let Some(meta) = self.load_meta(user_key).await? else {
      return Ok(None);
    };
    if meta.collection_type != CollectionType::Hash || meta.encoding() != StorageEncoding::Flattened
    {
      return Ok(None);
    }
    let sub_k = self.sub_key(KeyTag::Hash, meta.key_id, meta.version, field);
    self.read_raw_with(&sub_k, f).await
  }

  /// 打平存储：读取哈希字段值（点查 O(1) 物理直读，<50ns）
  #[inline]
  pub async fn flattened_hget(&self, user_key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>> {
    self
      .flattened_hget_with(user_key, field, |v| v.to_vec())
      .await
  }

  /// 打平存储：删除哈希字段（字段存在且被删除返回 Ok(true)，否则返回 Ok(false)；严格删空自愈）
  ///
  /// FlattenedTree 树后端键转发树算子（同 [`Self::flattened_hset`] 防互毁）
  pub async fn flattened_hdel(&self, user_key: &[u8], field: &[u8]) -> Result<bool> {
    let _key_lock = self.store.index.acquire_keys_lock_exclusive(&[user_key])?;
    let Some(mut meta) = self.load_meta(user_key).await? else {
      return Ok(false);
    };
    if meta.collection_type != CollectionType::Hash || meta.encoding() != StorageEncoding::Flattened
    {
      if meta.collection_type == CollectionType::Hash
        && meta.encoding() == StorageEncoding::FlattenedTree
      {
        return self.bftree_hdel(user_key, field).await;
      }
      return Ok(false);
    }
    self.flattened_hdel_inner(user_key, &mut meta, field).await
  }

  /// 打平存储：删除哈希字段内部实现（调用方已持有条带排他锁）
  pub(crate) async fn flattened_hdel_inner(
    &self,
    user_key: &[u8],
    meta: &mut MetaValue,
    field: &[u8],
  ) -> Result<bool> {
    let sub_k = self.sub_key(KeyTag::Hash, meta.key_id, meta.version, field);
    // 降级受阻状态下预读被删值长度：若被删的正是超限字段则解锁降级重试
    //（仅受阻状态付此一次点查，远廉于每次 hdel 的全库聚合扫描）
    let old_val_len = if get_meta_downgrade_blocked(&meta.reserved) {
      self.read_raw_with(&sub_k, |v| v.len()).await?
    } else {
      None
    };
    let deleted = self.delete_raw(&sub_k).await?;
    if deleted {
      meta.dec_size(1);
      if meta.size == 0 {
        self
          .drain_and_delete_collection_meta(user_key, meta)
          .await?;
      } else {
        // 解锁路径一：被删字段本身超 u16 上限时清除受阻负缓存（超限是唯一阻碍，
        // 删除即消除），本次立即重试降级；
        // 解锁路径二：字节快照递减估算（删除字节量精确已知：field.len + 被删值长度）——
        // 受阻期间字段只减不增（新增字段即解锁，更新不记账仅致估算失真：高估提前
        // 重试由聚合判定兜底自愈，低估延迟解锁最终一致），估算跨过降级门限即清除
        // 受阻标志，本次随后的 maybe_downgrade 判定自动重试聚合
        if old_val_len.is_some_and(|l| field.len() > u16::MAX as usize || l > u16::MAX as usize) {
          clear_meta_downgrade_blocked(&mut meta.reserved);
        } else if let Some(l) = old_val_len {
          let est =
            get_meta_downgrade_bytes(&meta.reserved).saturating_sub(field.len() as u64 + l as u64);
          if est <= HASH_DOWNGRADE_BYTE_THRESHOLD as u64 {
            clear_meta_downgrade_blocked(&mut meta.reserved);
          } else {
            set_meta_downgrade_bytes(&mut meta.reserved, est);
          }
        }
        self.save_meta(user_key, meta).await?;
        // 动静分层"降"半边闭环：删减到迟滞门限以下时自动降级回 Compact
        //（项数门限先行快速判定，字节数经降级聚合精确判定）
        self.maybe_downgrade_flattened_hash(user_key, meta).await?;
      }
    }
    Ok(deleted)
  }

  /// Flattened 打平哈希降级判定与迁移入口（调用方已持本键条带排他锁）
  ///
  /// 50% 迟滞防震荡双门限（SKILL.md 动静分层规约）：项数 <= 16384 且
  /// 字节数 <= 512KB 才降级。32B MetaValue 无字节数域（对标 C# 不维护此统计，
  /// 属 wedb 扩展判定），字节数在降级候选时刻一次性扫描聚合——仅当项数
  /// 门限已满足才触发，不构成热路径开销。
  ///
  /// 降级受阻负缓存（META_DOWNGRADE_BLOCKED_MASK 粘性标志 + reserved[2..7]
  /// 字节快照）：聚合判定失败（存在超 u16 上限字段或字节数超门限）时置位并
  /// 记录受阻时刻字节快照（超限场景置饱和），后续 hdel 零扫描直通；解锁路径：
  /// 新增字段（flattened_hset_inner）、超限字段被删（flattened_hdel_inner）、
  /// 字节快照递减估算跨过门限（flattened_hdel_inner，纯删除跨过 512KB 门限
  /// 即重试）与降级迁移清除——更新已有超限字段不解锁（避免每笔字段更新写
  /// meta 的写放大），该极端场景下需字段数变化或删除记账触底后才重试降级。
  ///
  /// 返回 true 表示已执行降级迁移（meta 就地更新为 Compact 新态）
  pub(crate) async fn maybe_downgrade_flattened_hash(
    &self,
    user_key: &[u8],
    meta: &mut MetaValue,
  ) -> Result<bool> {
    // 项数门限快速先行（免扫描）：不满足直接返回，绝大多数 hdel 零额外开销
    if meta.size > HASH_DOWNGRADE_ITEM_THRESHOLD as u64 {
      return Ok(false);
    }
    // 降级受阻负缓存：上次聚合判定失败（超限/字节超），直通免扫描
    if get_meta_downgrade_blocked(&meta.reserved) {
      return Ok(false);
    }
    let Some(entries) = self.collect_flattened_hash_entries(meta).await? else {
      // 超限字段受阻：无字节总量可记，快照置饱和（解锁仅靠超限字段被删）
      return self
        .block_downgrade(user_key, meta, DOWNGRADE_BYTES_SATURATED)
        .await;
    };
    // 字节数门限精确判定（口径与现有迟滞测试一致：Σ(field.len + value.len)）
    let byte_total: usize = entries.iter().map(|(f, v)| f.len() + v.len()).sum();
    if !should_downgrade_hash(entries.len(), byte_total) {
      return self
        .block_downgrade(user_key, meta, byte_total as u64)
        .await;
    }
    let refs: Vec<(&[u8], &[u8], Option<i64>)> = entries
      .iter()
      .map(|(f, v)| (f.as_ref(), v.as_ref(), None))
      .collect();
    self
      .migrate_flattened_to_compact_hash(user_key, meta, &refs)
      .await?;
    Ok(true)
  }

  /// 置位降级受阻粘性标志并持久化元记录（负缓存写入仅发生在判定失败时刻）；
  /// 同时记录受阻时刻的字节总量快照（u40 饱和），供 hdel 受阻路径递减估算解锁
  async fn block_downgrade(
    &self,
    user_key: &[u8],
    meta: &mut MetaValue,
    byte_snapshot: u64,
  ) -> Result<bool> {
    set_meta_downgrade_blocked(&mut meta.reserved);
    set_meta_downgrade_bytes(&mut meta.reserved, byte_snapshot);
    self.save_meta(user_key, meta).await?;
    Ok(false)
  }

  /// 全窗口扫描聚合 Flattened 打平哈希的存活字段与值（降级专用，O(库) 一次）
  ///
  /// windex 哈希索引无按键前缀遍历 API，字段名清单只能从日志顺序扫描获得：
  /// 窗口 `[begin_address, tail)` 内过滤本集合前 17B 刚性帧 + 会话前缀的物理键，
  /// 收集字段名候选（同名多版本去重）；再逐字段经哈希索引点查取最新值——
  /// 点查天然免疫扫描窗口内的陈旧版本与并发写（调用方持键锁，同键写已串行化，
  /// 墓碑/不存在即字段已亡，不计入）。
  ///
  /// Compact 紧凑编码上限守卫：任一字段或值超 u16::MAX 时放弃降级返回 None
  ///（保持 Flattened，待超限字段被更新或删除后下次 hdel 再降）
  async fn collect_flattened_hash_entries(
    &self,
    meta: &MetaValue,
  ) -> Result<Option<Vec<(Box<[u8]>, Box<[u8]>)>>> {
    let prefix = self.session_prefix();
    let sub_prefix = Self::sub_key_with_prefix(
      prefix.as_slice(),
      KeyTag::Hash,
      meta.key_id,
      meta.version,
      b"",
    );
    let sub_prefix = sub_prefix.as_slice();
    // 字段名候选清单（日志内同名多版本去重）
    let mut fields: HashSet<Box<[u8]>> = HashSet::with_hasher(GxBuildHasher::default());
    self
      .store
      .hlog
      .scan(
        self.store.begin_address(),
        self.store.tail_address(),
        |_, rec| {
          if !rec.is_tombstone()
            && rec.key.starts_with(sub_prefix)
            && rec.key.len() > sub_prefix.len()
          {
            fields.insert(Box::from(&rec.key[sub_prefix.len()..]));
          }
          Ok(true)
        },
      )
      .await?;

    let prefix = self.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut entries = Vec::with_capacity(fields.len());
    for field in fields {
      // 索引点查取最新值（墓碑/不存在即已亡字段，跳过）
      let sub_k = Self::sub_key_with_prefix(
        prefix_slice,
        KeyTag::Hash,
        meta.key_id,
        meta.version,
        &field,
      );
      let Some(value) = self.read_raw(&sub_k).await? else {
        continue;
      };
      // Compact u16 长度上限守卫：超限字段使整集降级不可行
      if field.len() > u16::MAX as usize || value.len() > u16::MAX as usize {
        return Ok(None);
      }
      entries.push((field, value.into_boxed_slice()));
    }
    Ok(Some(entries))
  }

  /// 打平存储：判断哈希表中指定字段是否存在
  pub async fn flattened_hexists(&self, user_key: &[u8], field: &[u8]) -> Result<bool> {
    let Some(meta) = self.load_meta(user_key).await? else {
      return Ok(false);
    };
    if meta.collection_type != CollectionType::Hash || meta.encoding() != StorageEncoding::Flattened
    {
      return Ok(false);
    }
    let sub_k = self.sub_key(KeyTag::Hash, meta.key_id, meta.version, field);
    self.contains_key_raw(&sub_k).await
  }

  /// 打平存储：获取哈希表字段总数（O(1) 直读主存元数据 size，严禁扫全集）
  pub async fn flattened_hlen(&self, user_key: &[u8]) -> Result<usize> {
    let Some(meta) = self.load_meta(user_key).await? else {
      return Ok(0);
    };
    if meta.collection_type != CollectionType::Hash || meta.encoding() != StorageEncoding::Flattened
    {
      return Ok(0);
    }
    Ok(meta.size as usize)
  }

  /// 打平存储：批量读取哈希字段值
  pub async fn flattened_hmget(
    &self,
    user_key: &[u8],
    fields: &[&[u8]],
  ) -> Result<Vec<Option<Vec<u8>>>> {
    let Some(meta) = self.load_meta(user_key).await? else {
      return Ok(vec![None; fields.len()]);
    };
    if meta.collection_type != CollectionType::Hash || meta.encoding() != StorageEncoding::Flattened
    {
      return Ok(vec![None; fields.len()]);
    }
    self.hmget_flattened_inner(&meta, fields).await
  }

  /// 打平存储批量读公共体：循环前缀外提 + 逐字段子键直读（调用方已完成元数据校验）
  pub(crate) async fn hmget_flattened_inner(
    &self,
    meta: &MetaValue,
    fields: &[&[u8]],
  ) -> Result<Vec<Option<Vec<u8>>>> {
    let prefix = self.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut results = Vec::with_capacity(fields.len());
    for &field in fields {
      let sub_k =
        Self::sub_key_with_prefix(prefix_slice, KeyTag::Hash, meta.key_id, meta.version, field);
      results.push(self.read_raw(&sub_k).await?);
    }
    Ok(results)
  }
}
