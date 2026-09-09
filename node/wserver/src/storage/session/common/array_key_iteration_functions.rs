//! 键空间数组迭代函数（对标 libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs）
//!
//! C# 侧为 StorageSession partial + 静态迭代器类，基于 Tsavorite 扫描；Rust 侧
//! 统一落在 [`StorageSession`](super::super::storage_session::StorageSession) 方法上，
//! 物理扫描走 wkv `scan_range_callback`（BfTree 闭区间范围扫描），会话隔离由
//! 会话前缀（ns/db 变长编码 + 1 字节标签）承担。

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

impl<'a, D: Device> StorageSession<'a, D> {
  /// 当前会话的物理键前缀长度（ns/db 变长编码，不含标签字节）
  pub(crate) fn phys_prefix_len(&self) -> usize {
    self.batch.session_prefix().as_slice().len()
  }

  /// 构造当前会话指定标签的物理键扫描下界（用户键为空）
  pub(crate) fn scan_lower_bound(&self, tag: u8) -> Vec<u8> {
    let mut buf = self.batch.session_prefix().as_slice().to_vec();
    buf.push(tag);
    buf
  }

  /// 构造当前会话指定标签的物理键扫描上界（下一标签下界，闭区间安全）
  pub(crate) fn scan_upper_bound(&self, tag: u8) -> Vec<u8> {
    let mut buf = self.scan_lower_bound(tag);
    let last = buf.len() - 1;
    buf[last] = tag + 1;
    buf
  }

  /// 数据库键增量扫描（SCAN 语义）
  ///
  /// 以"上次返回的最后一个用户键"为游标续扫（wkv 无快照游标，等价 Redis
  /// 基准键演进方案）；返回新游标与本页匹配键。`all_keys` 为真时忽略模式全量返回。
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbScan
  pub fn db_scan(
    &self,
    pattern: &[u8],
    all_keys: bool,
    cursor: &[u8],
    count: usize,
  ) -> wkv::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut keys = Vec::new();
    let mut last: Option<Vec<u8>> = None;
    let prefix_len = self.phys_prefix_len();
    let start = if cursor.is_empty() {
      self.scan_lower_bound(TAG_STRING)
    } else {
      self.batch.session_string_key(cursor).into_vec()
    };
    let end = self.scan_upper_bound(TAG_STRING);
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();

    self.batch.store.scan_range_callback(&start, &end, |k, _| {
      let user_key = &k[prefix_len + 1..];
      // 闭区间扫描需跳过游标自身
      if last.is_none() && !cursor.is_empty() && user_key == cursor {
        return true;
      }
      if keys.len() >= count {
        return false;
      }
      // TTL 已到期键视同不存在，交由 GC 物理回收
      if matches!(self.batch.probe_ttl(user_key, now_ms), wkv::TtlProbe::Due) {
        return true;
      }
      if all_keys || glob_match(pattern, user_key) {
        keys.push(user_key.to_vec());
      }
      last = Some(user_key.to_vec());
      true
    })?;

    Ok((last.unwrap_or_default(), keys))
  }

  /// 全库记录迭代（底层物理扫描回调，回调返回 false 提前终止）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:IterateStore
  pub fn iterate_store(
    &self,
    mut on_record: impl FnMut(&[u8], &[u8]) -> bool,
  ) -> wkv::Result<usize> {
    let start = self.scan_lower_bound(TAG_STRING);
    let end = self.scan_upper_bound(TAG_STRING);
    self
      .batch
      .store
      .scan_range_callback(&start, &end, |k, v| on_record(k, v))
  }

  /// 删除命中给定集群槽位的所有键，返回删除数
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DeleteSlotKeys
  pub async fn delete_slot_keys(&self, slots: &[u16]) -> wkv::Result<u64> {
    let mut deleted = 0u64;
    let prefix_len = self.phys_prefix_len();
    let start = self.scan_lower_bound(TAG_STRING);
    let end = self.scan_upper_bound(TAG_STRING);
    let mut victims: Vec<Vec<u8>> = Vec::new();
    self.batch.store.scan_range_callback(&start, &end, |k, _| {
      let user_key = &k[prefix_len + 1..];
      if slots.contains(&cluster_slot(user_key)) {
        victims.push(user_key.to_vec());
      }
      true
    })?;
    for key in victims {
      if self.delete_string(&key).await? {
        deleted += 1;
      }
    }
    Ok(deleted)
  }

  /// 列出当前库全部匹配键（KEYS 语义，无分页）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DBKeys
  pub fn db_keys(&self, pattern: &[u8]) -> wkv::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    let prefix_len = self.phys_prefix_len();
    let start = self.scan_lower_bound(TAG_STRING);
    let end = self.scan_upper_bound(TAG_STRING);
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
    self.batch.store.scan_range_callback(&start, &end, |k, _| {
      let user_key = &k[prefix_len + 1..];
      if matches!(self.batch.probe_ttl(user_key, now_ms), wkv::TtlProbe::Due) {
        return true;
      }
      if glob_match(pattern, user_key) {
        keys.push(user_key.to_vec());
      }
      true
    })?;
    Ok(keys)
  }

  /// 当前库键数量（DBSIZE 语义，按会话前缀过滤统计）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbSize
  pub fn db_size(&self) -> wkv::Result<usize> {
    let mut n = 0usize;
    let prefix_len = self.phys_prefix_len();
    let start = self.scan_lower_bound(TAG_STRING);
    let end = self.scan_upper_bound(TAG_STRING);
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
    self.batch.store.scan_range_callback(&start, &end, |k, _| {
      let user_key = &k[prefix_len + 1..];
      if !matches!(self.batch.probe_ttl(user_key, now_ms), wkv::TtlProbe::Due) {
        n += 1;
      }
      true
    })?;
    Ok(n)
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
