//! 键空间数组迭代函数（对标 libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs）
//!
//! C# 侧为 StorageSession partial + 静态迭代器类，基于 Tsavorite 扫描；Rust 侧
//! 统一落在 [`StorageSession`] 方法上，
//! 物理扫描走 wkv `scan_range_callback`（BfTree 闭区间范围扫描），会话隔离由
//! 会话前缀（ns/db 变长编码 + 1 字节标签）承担。

use std::{fmt, io};

use gxhash::HashMap as GxHashMap;
use wbase::{glob::glob_match_nocase, time::now_ticks};
use wdev::Device;
use wval::KeyTag;

use super::super::storage_session::StorageSession;

/// 物理键标签：普通字符串键
pub(crate) const TAG_STRING: u8 = KeyTag::String.as_u8();
/// 物理键标签：集合元数据记录
pub(crate) const TAG_META: u8 = KeyTag::Meta.as_u8();
/// 物理键标签：key 级 TTL 记录
pub(crate) const TAG_TTL: u8 = KeyTag::Ttl.as_u8();

/// Redis 集群槽位数（libs/server/Cluster/ClusterSlotUtils.cs 语义常量）
pub const CLUSTER_SLOTS: u16 = 16384;

/// whlog 错误 → wkv 错误统一包装（扫描属 IO 面）
pub(crate) fn scan_err(e: impl fmt::Display) -> wkv::Error {
  wkv::Error::Io(io::Error::other(e.to_string()))
}

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 当前会话的物理键前缀长度（ns/db 变长编码，不含标签字节）
  pub(crate) fn phys_prefix_len(&self) -> usize {
    self.batch.session_prefix().as_slice().len()
  }

  /// 数据库键增量扫描（SCAN 语义）
  ///
  /// 以"上次返回的最后一个用户键"为游标续扫（wkv 无快照游标，等价 Redis
  /// 基准键演进方案）：跳过游标及之前的全部键，游标键在两页之间被删时
  /// 仍可从其后首个键无缝续扫；因 count 截断才报告新游标，自然收尽返回
  /// 空游标（终态）。`all_keys` 为真时忽略模式全量返回。
  ///
  /// 匹配器为 `wbase::glob_match_nocase`：C# UnifiedStoreGetDBKeys 调
  /// GlobUtils.Match 时 ignoreCase 固定传 true
  ///（libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:303-306），
  /// SCAN/KEYS 模式匹配为大小写不敏感口径。
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbScan
  pub async fn db_scan(
    &self,
    pattern: &[u8],
    all_keys: bool,
    cursor: &[u8],
    count: usize,
  ) -> wkv::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let keys = self.string_keys_snapshot().await?;
    // 键已按字节序升序：二分定位首个大于游标的键作为续扫起点
    let start = keys.partition_point(|k| !cursor.is_empty() && k.as_slice() <= cursor);
    // count 下限钳制为 1：count=0 会立即"满页截断"且无处定游标，首页即空终
    // （Redis 侧 COUNT<1 在 RESP 层拒绝）
    let count = count.max(1);
    let slice = &keys[start..];
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut last_idx = None;
    let mut truncated = false;
    let ctx = self.consistent_read_context();
    for (i, key) in slice.iter().enumerate() {
      if items.len() >= count {
        truncated = true;
        break;
      }
      let matches = if let Some(ctx) = ctx.as_ref() {
        ctx.with_consistent_read(key, || all_keys || glob_match_nocase(pattern, key))
      } else {
        all_keys || glob_match_nocase(pattern, key)
      };
      if matches {
        items.push(key.clone());
      }
      last_idx = Some(start + i);
    }
    let next = if truncated {
      last_idx.map(|idx| keys[idx].clone()).unwrap_or_default()
    } else {
      Vec::new()
    };
    Ok((next, items))
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

/// Redis 集群槽位计算：CRC16-XMODEM 取模 16384（含 `{...}` 哈希标签语义）
pub fn cluster_slot(key: &[u8]) -> u16 {
  let hashed = hash_tag_of(key);
  (crc16_xmodem(hashed) % u32::from(CLUSTER_SLOTS)) as u16
}

/// 提取 `{...}` 哈希标签；无配对花括号或标签为空时整键参与哈希
fn hash_tag_of(key: &[u8]) -> &[u8] {
  let Some(open) = key.iter().position(|&b| b == b'{') else {
    return key;
  };
  let Some(close_rel) = key[open + 1..].iter().position(|&b| b == b'}') else {
    return key;
  };
  let inner = &key[open + 1..open + 1 + close_rel];
  if inner.is_empty() {
    return key;
  }
  inner
}

/// CRC16-XMODEM 编译期查表（poly 0x1021）
const fn make_crc16_table() -> [u16; 256] {
  let mut table = [0u16; 256];
  let mut i = 0usize;
  while i < 256 {
    let mut crc = (i as u16) << 8;
    let mut j = 0;
    while j < 8 {
      crc = if crc & 0x8000 != 0 {
        (crc << 1) ^ 0x1021
      } else {
        crc << 1
      };
      j += 1;
    }
    table[i] = crc;
    i += 1;
  }
  table
}

const CRC16_TABLE: [u16; 256] = make_crc16_table();

/// CRC16-XMODEM（poly 0x1021，初值 0），基于编译期常量表单遍查表计算
pub fn crc16_xmodem(data: &[u8]) -> u32 {
  let mut crc = 0u16;
  for &b in data {
    let idx = ((crc >> 8) ^ u16::from(b)) as usize;
    crc = (crc << 8) ^ CRC16_TABLE[idx];
  }
  u32::from(crc)
}
