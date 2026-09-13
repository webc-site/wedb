//! 键空间数组迭代函数（对标 libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs）
//!
//! C# 侧为 StorageSession partial + 静态迭代器类，基于 Tsavorite 扫描；Rust 侧
//! 统一落在 [`StorageSession`] 方法上，
//! 物理扫描走 wkv `scan_range_callback`（BfTree 闭区间范围扫描），会话隔离由
//! 会话前缀（ns/db 变长编码 + 1 字节标签）承担。

use std::{fmt, io};

use gxhash::{HashMap as GxHashMap, HashSet as GxHashSet};
use wbase::{
  glob::glob_match_nocase,
  hash_slot::hash_slot as cluster_slot,
  time::now_ticks,
};
use wdev::Device;
use wval::{GarnetObjectType, KeyTag, MetaValue};

use super::super::storage_session::StorageSession;

/// 物理键标签：普通字符串键
pub(crate) const TAG_STRING: u8 = KeyTag::String.as_u8();
/// 物理键标签：集合元数据记录
pub(crate) const TAG_META: u8 = KeyTag::Meta.as_u8();
/// 物理键标签：key 级 TTL 记录
pub(crate) const TAG_TTL: u8 = KeyTag::Ttl.as_u8();

/// SCAN TYPE 过滤三态（C# NetworkSCAN 的 matchType：`null` / `typeof(string)`
/// / 具体对象 Type；字符串类型不在 [`GarnetObjectType`] 域内，独立承载）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanTypeFilter {
  /// 仅字符串记录（C# matchType == typeof(string)）
  String,
  /// 集合对象类型精确匹配（C# matchType == 具体对象 Type）
  Object(GarnetObjectType),
}

/// whlog 错误 → wkv 错误统一包装（扫描属 IO 面）
pub(crate) fn scan_err(e: impl fmt::Display) -> wkv::Error {
  wkv::Error::Io(io::Error::other(e.to_string()))
}

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 当前会话的物理键前缀长度（ns/db 变长编码，不含标签字节）
  pub(crate) fn phys_prefix_len(&self) -> usize {
    self.batch.session_prefix().as_slice().len()
  }

  /// 地址游标增量扫描（SCAN 语义，对标 C# Tsavorite `ScanCursor` + Garnet `DbScan`）
  ///
  /// 游标为 hlog 逻辑地址（C# ScanCursor 同口径）：`0` 从 `begin_address` 起，
  /// 页满返回下一条起点地址，扫尽返回 `0`（终态）；无效低地址按 C#
  /// `cursor < BeginAddress → BeginAddress` 钳制重扫。同键多版本顺序扫描
  /// 后写先遇，页内首遇即最新版（C# `ConditionalScanPush` 的 seen 集等价），
  /// 墓碑 / TTL 到期 / 类型不匹配 / glob 不匹配均跳过。`type_filter` 为
  /// `Some(String)` 仅收字符串键，`Some(Object)` 收对应 `Meta` 键，
  /// `None` 收全部用户键（对标 C# `matchType == null`，未知 TYPE 值同此
  /// 口径——C# if 链掉出后 matchType 保持 null）；TYPE 过滤时的
  /// 单页无上限（C# `!typeObject.IsEmpty ? long.MaxValue : countValue`）
  /// 由 RESP 层调用方以 `usize::MAX` 传入。
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbScan
  pub async fn scan_cursor(
    &self,
    pattern: &[u8],
    all_keys: bool,
    cursor: u64,
    count: usize,
    type_filter: Option<ScanTypeFilter>,
  ) -> wkv::Result<(u64, Vec<Vec<u8>>)> {
    // count 下限钳 1 防首页即终（Redis 侧 COUNT<1 在 RESP 层拒绝）
    let limit = count.max(1);
    let prefix = self.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    let now = now_ticks();
    let mut seen: GxHashSet<Vec<u8>> = GxHashSet::default();
    let mut items = Vec::new();
    let hlog = self.batch.store.hlog();
    let from = cursor.max(self.batch.store.begin_address());
    let until = self.batch.store.tail_address();
    let mut it = hlog.scan_iter(from, until);
    // 闭包返回"本页是否收满"，Some(true) 早退并以当前游标续页
    let mut page_full = false;
    while let Some(full) = it
      .next_ref(|i| {
        let key = i.rec.key();
        let Some(rest) = key.strip_prefix(prefix_slice) else {
          return Ok(false);
        };
        // 用户键二选一：字符串记录或集合元记录；TTL/子键旁路记录不入键空间
        let (user_key, tag) = match rest.split_first() {
          Some((&t, k)) if t == TAG_STRING => (k, KeyTag::String),
          Some((&t, k)) if t == TAG_META => (k, KeyTag::Meta),
          _ => return Ok(false),
        };
        if !seen.insert(user_key.to_vec()) {
          return Ok(false); // 同键旧版本：首遇即最新，跳过
        }
        if matches!(self.batch.probe_ttl(user_key, now), wkv::TtlProbe::Due) {
          return Ok(false); // 已到期：C# CheckExpiry 跳过
        }
        if !i.rec.is_tombstone()
          && (all_keys || glob_match_nocase(pattern, user_key))
          && Self::type_matches(tag, i.rec.value(), type_filter)
        {
          items.push(user_key.to_vec());
        }
        Ok(items.len() >= limit)
      })
      .await?
    {
      if full {
        page_full = true;
        break;
      }
    }
    if page_full {
      return Ok((it.current_address(), items));
    }
    Ok((0, items))
  }

  /// SCAN TYPE 过滤判定（C# UnifiedStoreGetDBKeys.Reader 的 matchType 分支）
  ///
  /// 类型判定与对象信封域同口径（`obj_encode`/`obj_decode`）：紧凑对象为
  /// `String` 记录 + 值首字节类型标签，打平大集合为 `Meta` 记录 +
  /// `MetaValue.collection_type`；`Some(String)` 匹配无信封的普通字符串
  #[inline]
  fn type_matches(tag: KeyTag, value: &[u8], type_filter: Option<ScanTypeFilter>) -> bool {
    match type_filter {
      None => true,
      Some(ScanTypeFilter::String) => match tag {
        // 普通字符串：值首字节非对象类型标签（信封域 1..=5）
        KeyTag::String => !value.first().is_some_and(|t| {
          GarnetObjectType::from_u8(*t).is_some_and(|o| o != GarnetObjectType::Null)
        }),
        _ => false,
      },
      Some(ScanTypeFilter::Object(t)) => match tag {
        // 紧凑对象信封：值首字节类型标签精确匹配
        KeyTag::String => value.first() == Some(&t.as_u8()),
        // 打平集合：MetaValue.collection_type 精确匹配
        KeyTag::Meta => MetaValue::read_collection_type(value) == Ok(t),
        _ => false,
      },
    }
  }

  /// 判定本库是否存在命中给定集群槽位的键（早退）
  ///
  /// 对标 C# `HasKeysInSlotsScan`：用户面记录（String/Meta）槽位命中即
  /// true 并提前终止；刻意不过滤墓碑与到期键（保守方向——CLUSTER RESET
  /// 判定宁可误报不可漏报，与 C# 拉迭代语义一致）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:HasKeysInSlotsScan
  pub async fn has_keys_in_slots(&self, slots: &[u16]) -> wkv::Result<bool> {
    if slots.is_empty() {
      return Ok(false);
    }
    let prefix = self.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut found = false;
    self
      .batch
      .store
      .hlog()
      .scan(
        self.batch.store.begin_address(),
        self.batch.store.tail_address(),
        |_addr, rec| {
          let key = rec.key();
          let Some(rest) = key.strip_prefix(prefix_slice) else {
            return Ok(true);
          };
          match rest.split_first() {
            Some((&t, user_key)) if t == TAG_STRING || t == TAG_META => {
              if slots.contains(&cluster_slot(user_key)) {
                found = true;
                return Ok(false); // 早退
              }
              Ok(true)
            }
            _ => Ok(true),
          }
        },
      )
      .await
      .map_err(scan_err)?;
    Ok(found)
  }

  /// 全库记录迭代（回调拿到 (用户键, 值)，返回 false 提前终止）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:IterateStore
  pub async fn iterate_store(
    &self,
    mut on_record: impl FnMut(&[u8], &[u8]) -> bool,
  ) -> wkv::Result<usize> {
    let records = self.string_snapshot().await?;
    let ctx = self.consistent_read_context();
    let mut n = 0usize;
    for (key, value) in &records {
      let cont = if let Some(ctx) = ctx.as_ref() {
        ctx.with_consistent_read(key, || on_record(key, value))
      } else {
        on_record(key, value)
      };
      n += 1;
      if !cont {
        break;
      }
    }
    Ok(n)
  }

  /// 删除命中给定集群槽位的所有键，返回删除数
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DeleteSlotKeys
  pub async fn delete_slot_keys(&self, slots: &[u16]) -> wkv::Result<u64> {
    let keys = self.string_keys_snapshot().await?;
    let mut deleted = 0u64;
    for key in keys {
      if slots.contains(&cluster_slot(&key)) && self.delete_string(&key).await? {
        deleted += 1;
      }
    }
    Ok(deleted)
  }

  /// 统计命中给定集群槽位的用户键数（原始日志计数，不按版本去重）
  ///
  /// libs/cluster/Session/ClusterCommands.cs:CountKeysInSlot
  ///（C# ClusterKeyIterationFunctions.CountKeys 同为 Tsavorite 原始迭代计数）
  pub async fn count_keys_in_slot(&self, slot: u16) -> wkv::Result<usize> {
    let prefix = self.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut count = 0usize;
    self
      .batch
      .store
      .hlog()
      .scan(
        self.batch.store.begin_address(),
        self.batch.store.tail_address(),
        |_addr, rec| {
          let key = rec.key();
          if let Some(rest) = key.strip_prefix(prefix_slice)
            && matches!(
              rest.split_first(),
              Some((&t, user_key)) if (t == TAG_STRING || t == TAG_META) && cluster_slot(user_key) == slot
            )
          {
            count += 1;
          }
          Ok(true)
        },
      )
      .await
      .map_err(scan_err)?;
    Ok(count)
  }

  /// 列出命中给定集群槽位的用户键（至多 `key_count` 个，早停）
  ///
  /// libs/cluster/Session/ClusterCommands.cs:GetKeysInSlot
  pub async fn get_keys_in_slot(&self, slot: u16, key_count: usize) -> wkv::Result<Vec<Vec<u8>>> {
    let prefix = self.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut keys = Vec::new();
    self
      .batch
      .store
      .hlog()
      .scan(
        self.batch.store.begin_address(),
        self.batch.store.tail_address(),
        |_addr, rec| {
          let key = rec.key();
          if let Some(rest) = key.strip_prefix(prefix_slice)
            && let Some((&t, user_key)) = rest.split_first()
            && (t == TAG_STRING || t == TAG_META)
            && cluster_slot(user_key) == slot
          {
            keys.push(user_key.to_vec());
            if keys.len() >= key_count {
              return Ok(false);
            }
          }
          Ok(true)
        },
      )
      .await
      .map_err(scan_err)?;
    Ok(keys)
  }

  /// 列出当前库全部匹配键（KEYS 语义，无分页；匹配口径同 [`Self::db_scan`]：
  /// C# UnifiedStoreGetDBKeys 的 ignoreCase=true，读一致性会话下对标
  /// C# ConsistentUnifiedStoreGetDBKeys 逐键执行一致读协议）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DBKeys
  pub async fn db_keys(&self, pattern: &[u8]) -> wkv::Result<Vec<Vec<u8>>> {
    let keys = self.string_keys_snapshot().await?;
    let all_keys = pattern == b"*";
    let ctx = self.consistent_read_context();
    let mut items = Vec::new();
    for key in keys {
      let matches = if let Some(ctx) = ctx.as_ref() {
        ctx.with_consistent_read(&key, || all_keys || glob_match_nocase(pattern, &key))
      } else {
        all_keys || glob_match_nocase(pattern, &key)
      };
      if matches {
        items.push(key);
      }
    }
    Ok(items)
  }

  /// 当前库键数量（DBSIZE 语义，按会话前缀过滤统计）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbSize
  pub async fn db_size(&self) -> wkv::Result<usize> {
    let keys = self.string_keys_snapshot().await?;
    Ok(keys.len())
  }

  /// 键在内存中已到期则就地物理清除，返回是否确有删除
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DeleteIfExpiredInMemory
  pub async fn delete_if_expired_in_memory(&self, key: &[u8]) -> wkv::Result<bool> {
    // 判定基准与 TTL 记录同域：.NET Ticks（wkv::probe_ttl 契约）
    if matches!(self.batch.probe_ttl(key, now_ticks()), wkv::TtlProbe::Due) {
      let deleted = self.delete_string(key).await?;
      return Ok(deleted);
    }
    Ok(false)
  }

  /// 判断物理键是否为内部记录（TTL / 集合元数据等非用户字符串记录）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:IsInternalRecord
  pub fn is_internal_record(&self, physical_key: &[u8]) -> bool {
    let prefix_len = self.phys_prefix_len();
    physical_key.len() > prefix_len
      && (physical_key[prefix_len] == TAG_META || physical_key[prefix_len] == TAG_TTL)
  }

  /// 当前会话字符串键空间快照（hlog 区间扫描 + 会话前缀过滤 + 同键留最新）
  ///
  /// 键 -> 值（None = 已删除墓碑）。BfTree 扫描（scan_range_callback）只覆盖
  /// 范围索引记录，普通写仅入 hlog 与哈希索引，故全量迭代必须走 hlog 扫描；
  /// 逻辑地址升序遍历天然后写覆盖前写。整库物化为快照，生产大库 SCAN 应改
  /// 分页增量（对标 C# cursor 语义），此处以正确性优先。
  pub(crate) async fn collect_records(&self) -> wkv::Result<GxHashMap<Vec<u8>, Option<Vec<u8>>>> {
    let prefix = self.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut map = GxHashMap::default();
    self
      .batch
      .store
      .hlog()
      .scan(
        self.batch.store.begin_address(),
        self.batch.store.tail_address(),
        |_addr, rec| {
          let key = rec.key();
          let Some(rest) = key.strip_prefix(prefix_slice) else {
            return Ok(true);
          };
          let Some(user_key) = rest.strip_prefix(&[TAG_STRING][..]) else {
            return Ok(true);
          };
          if rec.is_tombstone() {
            map.insert(user_key.to_vec(), None);
          } else {
            map.insert(user_key.to_vec(), Some(rec.value().to_vec()));
          }
          Ok(true)
        },
      )
      .await
      .map_err(scan_err)?;
    Ok(map)
  }

  /// 存活字符串键值快照（按键字节序排序，零键克隆）
  pub(crate) async fn string_snapshot(&self) -> wkv::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut map = self.collect_records().await?;
    let now = now_ticks();
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = map
      .drain()
      .filter_map(|(k, v)| {
        if !matches!(self.batch.probe_ttl(&k, now), wkv::TtlProbe::Due) {
          v.map(|val| (k, val))
        } else {
          None
        }
      })
      .collect();
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    Ok(entries)
  }

  /// 存活字符串键名快照（仅收集键名，零值拷贝，极大节约内存与 CPU；键按字节序排序）
  pub(crate) async fn string_keys_snapshot(&self) -> wkv::Result<Vec<Vec<u8>>> {
    let prefix = self.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut map: GxHashMap<Vec<u8>, bool> = GxHashMap::default();
    self
      .batch
      .store
      .hlog()
      .scan(
        self.batch.store.begin_address(),
        self.batch.store.tail_address(),
        |_addr, rec| {
          let key = rec.key();
          let Some(rest) = key.strip_prefix(prefix_slice) else {
            return Ok(true);
          };
          let Some(user_key) = rest.strip_prefix(&[TAG_STRING][..]) else {
            return Ok(true);
          };
          map.insert(user_key.to_vec(), !rec.is_tombstone());
          Ok(true)
        },
      )
      .await
      .map_err(scan_err)?;

    let now = now_ticks();
    let mut keys: Vec<Vec<u8>> = map
      .into_iter()
      .filter(|(k, alive)| *alive && !matches!(self.batch.probe_ttl(k, now), wkv::TtlProbe::Due))
      .map(|(k, _)| k)
      .collect();
    keys.sort_unstable();
    Ok(keys)
  }
}
