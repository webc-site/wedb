//! 键空间数组迭代函数（对标 libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs）
//!
//! C# 侧为 StorageSession partial + 静态迭代器类，基于 Tsavorite 扫描；Rust 侧
//! 统一落在 [`StorageSession`](super::super::storage_session::StorageSession) 方法上，
//! 物理扫描走 wkv `scan_range_callback`（BfTree 闭区间范围扫描），会话隔离由
//! 会话前缀（ns/db 变长编码 + 1 字节标签）承担。

use std::{fmt, io};

use gxhash::HashMap as GxHashMap;
use wdev::Device;

use super::super::storage_session::StorageSession;

/// 物理键标签：普通字符串键（对齐 wval::KeyTag::String）
pub(crate) const TAG_STRING: u8 = 0x00;
/// 物理键标签：集合元数据记录（对齐 wval::KeyTag::Meta）
pub(crate) const TAG_META: u8 = 0x01;
/// 物理键标签：key 级 TTL 记录（对齐 wval::KeyTag::Ttl）
pub(crate) const TAG_TTL: u8 = 0x09;

/// Redis 集群槽位数（libs/server/Cluster/ClusterSlotUtils.cs 语义常量）
pub const CLUSTER_SLOTS: u16 = 16384;

/// whlog 错误 → wkv 错误统一包装（扫描属 IO 面）
pub(crate) fn scan_err(e: impl fmt::Display) -> wkv::Error {
  wkv::Error::Io(io::Error::other(e.to_string()))
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// 当前会话的物理键前缀长度（ns/db 变长编码，不含标签字节）
  pub(crate) fn phys_prefix_len(&self) -> usize {
    self.batch.session_prefix().as_slice().len()
  }

  /// 数据库键增量扫描（SCAN 语义）
  ///
  /// 以"上次返回的最后一个用户键"为游标续扫（wkv 无快照游标，等价 Redis
  /// 基准键演进方案）；返回新游标与本页匹配键。`all_keys` 为真时忽略模式全量返回。
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbScan
  pub async fn db_scan(
    &self,
    pattern: &[u8],
    all_keys: bool,
    cursor: &[u8],
    count: usize,
  ) -> wkv::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let (_map, keys) = self.string_snapshot().await?;
    let start = if cursor.is_empty() {
      0
    } else {
      keys
        .iter()
        .position(|k| k.as_slice() == cursor)
        .map(|p| p + 1)
        .unwrap_or(keys.len())
    };
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut last: Option<Vec<u8>> = None;
    for key in keys.into_iter().skip(start) {
      if items.len() >= count {
        break;
      }
      if all_keys || glob_match(pattern, &key) {
        items.push(key.clone());
      }
      last = Some(key);
    }
    Ok((last.unwrap_or_default(), items))
  }

  /// 全库记录迭代（回调拿到 (用户键, 值)，返回 false 提前终止）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:IterateStore
  pub async fn iterate_store(
    &self,
    mut on_record: impl FnMut(&[u8], &[u8]) -> bool,
  ) -> wkv::Result<usize> {
    let (map, keys) = self.string_snapshot().await?;
    let mut n = 0usize;
    for key in keys {
      let value = map.get(&key).cloned().flatten().unwrap_or_default();
      n += 1;
      if !on_record(&key, &value) {
        break;
      }
    }
    Ok(n)
  }

  /// 删除命中给定集群槽位的所有键，返回删除数
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DeleteSlotKeys
  pub async fn delete_slot_keys(&self, slots: &[u16]) -> wkv::Result<u64> {
    let (_, keys) = self.string_snapshot().await?;
    let mut deleted = 0u64;
    for key in keys {
      if slots.contains(&cluster_slot(&key)) && self.delete_string(&key).await? {
        deleted += 1;
      }
    }
    Ok(deleted)
  }

  /// 列出当前库全部匹配键（KEYS 语义，无分页）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DBKeys
  pub async fn db_keys(&self, pattern: &[u8]) -> wkv::Result<Vec<Vec<u8>>> {
    let (_, keys) = self.string_snapshot().await?;
    Ok(
      keys
        .into_iter()
        .filter(|k| glob_match(pattern, k))
        .collect(),
    )
  }

  /// 当前库键数量（DBSIZE 语义，按会话前缀过滤统计）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbSize
  pub async fn db_size(&self) -> wkv::Result<usize> {
    let (_, keys) = self.string_snapshot().await?;
    Ok(keys.len())
  }

  /// 键在内存中已到期则就地物理清除，返回是否确有删除
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DeleteIfExpiredInMemory
  pub async fn delete_if_expired_in_memory(&self, key: &[u8]) -> wkv::Result<bool> {
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
    if matches!(self.batch.probe_ttl(key, now_ms), wkv::TtlProbe::Due) {
      let _ = self.delete_string(key).await?;
      return Ok(true);
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
    let prefix = self.batch.session_prefix().as_slice().to_vec();
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
          let Some(rest) = key.strip_prefix(prefix.as_slice()) else {
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

  /// 存活字符串键快照：剔除墓碑与已到期键，键按字节序排序
  pub(crate) async fn string_snapshot(
    &self,
  ) -> wkv::Result<(GxHashMap<Vec<u8>, Option<Vec<u8>>>, Vec<Vec<u8>>)> {
    let map = self.collect_records().await?;
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
    let mut keys: Vec<Vec<u8>> = map
      .iter()
      .filter(|(k, v)| {
        v.is_some() && !matches!(self.batch.probe_ttl(k, now_ms), wkv::TtlProbe::Due)
      })
      .map(|(k, _)| k.clone())
      .collect();
    keys.sort_unstable();
    Ok((map, keys))
  }
}

/// Redis 风格 glob 模式匹配（支持 `*` `?` `[...]`，含 `^`/`!` 取反与 `-` 区间）
///
/// 单 `*` 回溯点算法：O(pattern+text) 单遍扫描，无递归无堆分配。
pub(crate) fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
  let (mut p, mut t) = (0usize, 0usize);
  // 星号回溯点：`*` 在 pattern 中的位置与其匹配进行中的 text 偏移
  let (mut star_p, mut star_t) = (usize::MAX, 0usize);
  while t < text.len() {
    if p < pattern.len() && pattern[p] == b'*' {
      star_p = p;
      star_t = t;
      p += 1;
    } else if p < pattern.len() && pattern[p] == b'[' {
      let (hit, next) = match_bracket(pattern, p, text[t]);
      if hit {
        p = next;
        t += 1;
      } else if star_p != usize::MAX {
        star_t += 1;
        t = star_t;
        p = star_p + 1;
      } else {
        return false;
      }
    } else if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
      p += 1;
      t += 1;
    } else if star_p != usize::MAX {
      // 回溯：让上一个 `*` 多吞一个字符
      star_t += 1;
      t = star_t;
      p = star_p + 1;
    } else {
      return false;
    }
  }
  while p < pattern.len() && pattern[p] == b'*' {
    p += 1;
  }
  p == pattern.len()
}

/// 匹配 `[...]` 字符类：返回 (是否命中, 类结束后的下一个 pattern 下标)
fn match_bracket(pattern: &[u8], start: usize, c: u8) -> (bool, usize) {
  let mut i = start + 1;
  let mut negate = false;
  if i < pattern.len() && (pattern[i] == b'^' || pattern[i] == b'!') {
    negate = true;
    i += 1;
  }
  let mut hit = false;
  while i < pattern.len() && pattern[i] != b']' {
    if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
      if pattern[i] <= c && c <= pattern[i + 2] {
        hit = true;
      }
      i += 3;
    } else {
      if pattern[i] == c {
        hit = true;
      }
      i += 1;
    }
  }
  // 越过闭合 `]`（未闭合按字面量处理，命中判定已失效）
  if i < pattern.len() {
    i += 1;
  } else {
    hit = negate; // 未闭合类视为普通字符序列，不可能命中单字符
    return (hit, i);
  }
  (hit != negate, i)
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

/// CRC16-XMODEM（poly 0x1021，初值 0），逐字节位运算单遍计算
pub fn crc16_xmodem(data: &[u8]) -> u32 {
  let mut crc = 0u32;
  for &b in data {
    crc ^= u32::from(b) << 8;
    for _ in 0..8 {
      crc = if crc & 0x8000 != 0 {
        (crc << 1) ^ 0x1021
      } else {
        crc << 1
      };
      crc &= 0xFFFF;
    }
  }
  crc
}
