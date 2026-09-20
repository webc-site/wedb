//! 键空间数组迭代函数（对标 libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs）
//!
//! C# 侧为 StorageSession partial + 静态迭代器类，基于 Tsavorite 扫描；Rust 侧
//! 统一落在 [`StorageSession`] 方法上，
//! 物理扫描走 wkv `scan_range_callback`（BfTree 闭区间范围扫描），会话隔离由
//! 会话前缀（ns/db 变长编码 + 1 字节标签）承担。

use std::{fmt, io};

use wbase::{
  glob::glob_match_nocase, hash_slot::slot_of, map::HashMap as GxHashMap, time::now_ticks,
};
use wdev::Device;
use wkv::{Error, TtlGate};
use wval::{GarnetObjectType, KeyTag, MetaValue, NamespaceDbCodec};

use super::super::storage_session::StorageSession;

/// 会话用户键提取（String + 对象信封 + Meta；前缀比对 + tag 解析 + 可见性过滤由 wval 单点承担）
#[inline]
fn live_value_key<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
  NamespaceDbCodec::extract_live_user_key(key, prefix).map(|(_, user_key)| user_key)
}

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
  Error::Io(io::Error::other(e.to_string()))
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// 会话库级槽位（doc/zh/db.md 4.1 定槽单点 `wbase::hash_slot::slot_of`
  /// 的存储会话消费面）：`Slot = Mixer(namespace, active_db) & SLOT_MASK`，
  /// 键内容一律不参与定槽
  #[inline]
  fn session_slot(&self) -> u16 {
    slot_of(
      self.batch.session.namespace(),
      self.batch.session.active_db(),
    )
  }

  /// 扫描序「当前最新存活用户键」判定单点（SCAN / COUNTKEYSINSLOT / GETKEYSINSLOT 共用）
  ///
  /// 一条记录依次通过以下判定才放行并返回 (标签, 用户键)：
  /// 1. 物理键命中用户面域（TTL / 子键等内部旁路记录过滤由 wval 单点承担）
  /// 2. 索引链首地址校验：仅该键当前最新版本（链首即本记录）可进入过滤链，
  ///    旧版本 / 墓碑链 / elide 链 / tag 碰撞一律跳过（对标 C# AllocatorScan.cs
  ///    ConditionalScanPush 的「存在更高地址版本即不推送」与 ScanLookup 层
  ///    !Tombstone；碰撞占据链首时按「非链首」保守跳过，漏报方向，SCAN 本无
  ///    快照保证）
  /// 3. 双物理域互斥（String / ObjectEnvelope 同键两链）：C# 单键单链由链首校验
  ///    天然收敛为「最新版本胜」，Rust 信封分域后同键跨域覆写（resp 层 WRONGTYPE
  ///    拦截之外的旁路写）会双链并达，此处按日志地址新者胜裁决，杜绝重复输出与
  ///    类型过滤双报（对侧探针 = 单次哈希查询）
  /// 4. 非墓碑记录
  /// 5. TTL 未到期：probe_ttl Due 剔除（C# ClusterKeyIterationFunctions 两 Reader
  ///    的记录头 `!Expired(in srcLogRecord)` 判定在本仓的对应口径——TTL 为键级
  ///    旁路记录，probe_ttl 即同一判据）
  #[inline]
  fn live_key_at<'k>(
    &self,
    prefix: &[u8],
    now: i64,
    addr: u64,
    key: &'k [u8],
    is_tombstone: bool,
  ) -> Option<(KeyTag, &'k [u8])> {
    let (tag, user_key) = NamespaceDbCodec::extract_live_user_key(key, prefix)?;
    if self.batch.store.index.load().find_tag(key) != Some(addr) {
      return None;
    }
    if matches!(tag, KeyTag::String | KeyTag::ObjectEnvelope) {
      let other = self.batch.session_tag_key(
        if tag == KeyTag::String {
          KeyTag::ObjectEnvelope
        } else {
          KeyTag::String
        },
        user_key,
      );
      if self
        .batch
        .store
        .index
        .load()
        .find_tag(&other)
        .is_some_and(|a| a > addr)
      {
        return None;
      }
    }
    if is_tombstone || matches!(self.batch.probe_ttl(user_key, now), TtlGate::Due) {
      return None;
    }
    Some((tag, user_key))
  }

  /// 地址游标增量扫描（SCAN 语义，对标 C# Tsavorite `ScanCursor` + Garnet `DbScan`）
  ///
  /// 游标为 hlog 逻辑地址（C# ScanCursor 同口径）：`0` 从 `begin_address` 起，
  /// 页满返回下一条起点地址，扫尽返回 `0`（终态）；低于 `begin_address` 的
  /// 陈旧游标（截断后）按 C# `cursor < BeginAddress → BeginAddress` 钳制重扫。
  /// 其余非零游标先经 whlog `validate_cursor` 校验（对标 C# ScanLookup 的
  /// validateCursor + iter.SnapCursorToLogicalAddress：游标须落在记录起始
  /// 字节且未越尾），无效游标终结遍历回 `(0, 空)`——C# Snap 失败走
  /// IterationComplete（resetCursor 归 0 + 空列表）同口径；C# 的
  /// lastScanCursor 幂等豁免依赖连接态，本函数无状态故不做。同键多版本经
  /// 索引链首地址校验去重（见下），墓碑 / TTL 到期 / 类型不匹配 / glob 不匹配
  /// 均跳过。`type_filter` 为 `Some(String)` 仅收字符串键，`Some(Object)` 收
  /// 对应 `Meta` 键，`None` 收全部用户键（对标 C# `matchType == null`；未知
  /// TYPE 值由 RESP 层提前回空，不进入本函数——C# DbScan :82-84 同口径）；
  /// TYPE 过滤时的单页无上限（C# `!typeObject.IsEmpty ? long.MaxValue :
  /// countValue`）由 RESP 层调用方以 `usize::MAX` 传入。
  ///
  /// 同键多版本去重、双物理域新者胜、墓碑与 TTL 到期剔除统一走存活判定单点
  /// [`Self::live_key_at`]（对标 C# AllocatorScan.cs ConditionalScanPush 与
  /// ScanLookup 层 !Tombstone + CheckExpiry 过滤链，详见该函数文档）。
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
    let mut items = Vec::new();
    let hlog = self.batch.store.hlog();
    let begin = self.batch.store.begin_address();
    let from = cursor.max(begin);
    // 游标校验（对标 C# ScanLookup 的 validateCursor + iter.SnapCursorToLogicalAddress）：
    // 0 从头扫、begin 之下钳制重扫均已由上方分流；其余游标须落在记录起始字节
    // 且未越尾，无效直接终结遍历回 (0, 空)，杜绝中间地址撕裂解析与伪造游标
    if cursor != 0 && cursor >= begin && !hlog.validate_cursor(cursor).await? {
      return Ok((0, Vec::new()));
    }
    let until = self.batch.store.tail_address();
    let mut it = hlog.scan_iter(from, until);
    // 闭包返回"本页是否收满"，Some(true) 早退并以当前游标续页
    let mut page_full = false;
    while let Some(full) = it
      .next_ref(|i| {
        // 存活用户键判定（链首去重 + 双域裁决 + 墓碑 + TTL）走单点 [`Self::live_key_at`]；
        // 值内容仅 TYPE 过滤消费，故在判定放行后按标签分流
        let Some((tag, user_key)) =
          self.live_key_at(prefix_slice, now, i.addr, i.rec.key(), i.rec.is_tombstone())
        else {
          return Ok(false);
        };
        if (all_keys || glob_match_nocase(pattern, user_key))
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
  /// 类型判定按物理键标签带外分流（对齐 C# DataHeader.ValueIsObject 位）：
  /// `String` 记录即用户字符串（值内容任意，不做嗅探）；对象信封为
  /// `ObjectEnvelope` 记录 + 值首字节内层类型标签；打平大集合为 `Meta` 记录 +
  /// `MetaValue.collection_type`；`Some(String)` 仅匹配字符串记录
  #[inline]
  fn type_matches(tag: KeyTag, value: &[u8], type_filter: Option<ScanTypeFilter>) -> bool {
    match type_filter {
      None => true,
      Some(ScanTypeFilter::String) => tag == KeyTag::String,
      Some(ScanTypeFilter::Object(t)) => match tag {
        // 对象信封：值首字节即内层对象类型标签（载荷自身类型字段）
        KeyTag::ObjectEnvelope => value.first() == Some(&t.as_u8()),
        // 打平集合：MetaValue.collection_type 精确匹配
        KeyTag::Meta => MetaValue::read_collection_type(value) == Ok(t),
        _ => false,
      },
    }
  }

  /// 判定本库是否存在命中给定集群槽位的键（早退）
  ///
  /// 对标 C# `HasKeysInSlotsScan`。库级定槽（doc/zh/db.md 4.1）：槽位是
  /// `(namespace, active_db)` 的属性，键内容不参与定槽——会话库槽命中任一
  /// 待查槽位时本库全部键均在该槽，退化为「库非空」探测；未命中直接
  /// false（零扫描）。刻意不过滤墓碑与到期键（保守方向——CLUSTER RESET
  /// 判定宁可误报不可漏报，与 C# 拉迭代语义一致）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:HasKeysInSlotsScan
  pub async fn has_keys_in_slots(&self, slots: &[u16]) -> wkv::Result<bool> {
    if slots.is_empty() {
      return Ok(false);
    }
    let slot = self.session_slot();
    if !slots.contains(&slot) {
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
          // 用户面三域（String/Meta/ObjectEnvelope）过滤由 wval 单点承担；
          // 刻意保留墓碑与到期键（保守误报，见函数文档）
          if NamespaceDbCodec::extract_live_user_key(rec.key(), prefix_slice).is_some() {
            found = true;
            return Ok(false); // 早退
          }
          Ok(true)
        },
      )
      .await
      .map_err(scan_err)?;
    Ok(found)
  }

  /// 删除命中给定集群槽位的所有键，返回删除数
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DeleteSlotKeys
  ///
  /// 库级定槽（doc/zh/db.md 4.1）：会话库槽命中待删槽位时本库全部键均在该
  /// 槽，删除收敛为全库逐键删除；未命中直接 0（零扫描）
  pub async fn delete_slot_keys(&self, slots: &[u16]) -> wkv::Result<u64> {
    if !slots.contains(&self.session_slot()) {
      return Ok(0);
    }
    let keys = self.string_keys_snapshot().await?;
    let mut deleted = 0u64;
    for key in keys {
      if self.delete_string(&key).await? {
        deleted += 1;
      }
    }
    Ok(deleted)
  }

  /// 统计命中给定集群槽位的活用户键数（与 [`Self::get_keys_in_slot`] 共用
  /// [`Self::live_key_at`] 单点判定，恒有 COUNT == len(GET 全量)，杜绝
  /// COUNTKEYSINSLOT / GETKEYSINSLOT 两命令口径分叉）
  ///
  /// libs/cluster/Session/ClusterCommands.cs:CountKeysInSlot
  /// libs/cluster/Session/ClusterKeyIterationFunctions.cs:CountKeys.Reader
  ///
  /// 库级定槽（doc/zh/db.md 4.1）：会话库槽不匹配直接 0（零扫描），匹配
  /// 即全库活键计数，键内容不参与定槽。偏差声明：C# CountKeys.Reader 按
  /// 原始日志逐记录计（记录头 !Expired，旧版本与墓碑链一并计入），本仓
  /// wkv 紧缩前保留旧版本记录、TTL 为带外旁路记录，raw 计数与 GET 的活键
  /// 口径必然互斥（已 DEL 键旧版本计入、到期键仅 COUNT 侧计入），故取
  /// 链首去重 + 墓碑 + Due 剔除的活键判定（即 C# `!Expired` 在本仓的对应
  /// 判据，get 侧已示范同一写法）收成两命令共用单点
  pub async fn count_keys_in_slot(&self, slot: u16) -> wkv::Result<usize> {
    if slot != self.session_slot() {
      return Ok(0);
    }
    let prefix = self.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    let now = now_ticks();
    let mut count = 0usize;
    self
      .batch
      .store
      .hlog()
      .scan(
        self.batch.store.begin_address(),
        self.batch.store.tail_address(),
        |addr, rec| {
          if self
            .live_key_at(prefix_slice, now, addr, rec.key(), rec.is_tombstone())
            .is_some()
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

  /// 迭代器面扫描单点：逐个放行命中给定集群槽位的活用户键（至多 `key_count`
  /// 个）给 `emit` 消费，`emit` 返回 false 即满额早停（对标 C# 槽位迭代器
  /// GetKeysInSlot.Reader 逐记录 `keys.Add` + `return keys.Count < maxKeyCount`）；
  /// 活键判定与 [`Self::count_keys_in_slot`] 共用 [`Self::live_key_at`] 单点。
  /// 键以 `&[u8]` 切片视图零拷贝交付，供 RESP 流式编码等消费面消除中间
  /// `Vec<Vec<u8>>`；须持有键集的消费面（命令入口、迁移分批、复制快照）走
  /// 本函数的 collect 封装 [`Self::get_keys_in_slot`]
  ///
  /// libs/cluster/Session/ClusterKeyIterationFunctions.cs:GetKeysInSlot.Reader
  ///
  /// 库级定槽（doc/zh/db.md 4.1）：会话库槽不匹配直接空（零扫描），匹配
  /// 即全库活跃键列举，键内容不参与定槽
  pub async fn get_keys_in_slot_with(
    &self,
    slot: u16,
    key_count: usize,
    mut emit: impl FnMut(&[u8]) -> bool,
  ) -> wkv::Result<()> {
    if slot != self.session_slot() || key_count == 0 {
      return Ok(());
    }
    let prefix = self.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    let now = now_ticks();
    self
      .batch
      .store
      .hlog()
      .scan(
        self.batch.store.begin_address(),
        self.batch.store.tail_address(),
        |addr, rec| {
          if let Some((_, user_key)) =
            self.live_key_at(prefix_slice, now, addr, rec.key(), rec.is_tombstone())
            && !emit(user_key)
          {
            return Ok(false);
          }
          Ok(true)
        },
      )
      .await
      .map_err(scan_err)?;
    Ok(())
  }

  /// 命令入口面：列出命中给定集群槽位的活用户键（至多 `key_count` 个，扫描序
  /// 前 N 个），对标 C# 命令入口备 List 交迭代器填充后原样返回的形态；是扫描
  /// 单点 [`Self::get_keys_in_slot_with`] 的 collect 薄封装，迁移分批与复制
  /// 快照等须持有键集的消费面出口（RESP GETKEYSINSLOT 走流式编码，不经此口）
  ///
  /// libs/cluster/Session/ClusterCommands.cs:GetKeysInSlot
  pub async fn get_keys_in_slot(&self, slot: u16, key_count: usize) -> wkv::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    self
      .get_keys_in_slot_with(slot, key_count, |key| {
        keys.push(key.to_vec());
        keys.len() < key_count
      })
      .await?;
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
        // 逐键一致读预检按 String 记录物理键域取哈希（[`wkv::StoreSession::
        // consistent_read_hash`] 单点，与回放侧草图入账同键同哈希）；
        // string_keys_snapshot 已折叠标签维，快照口径为库内活键名
        ctx.with_consistent_read(&key, KeyTag::String, || {
          all_keys || glob_match_nocase(pattern, &key)
        })?
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
    if matches!(self.batch.probe_ttl(key, now_ticks()), TtlGate::Due) {
      let deleted = self.delete_string(key).await?;
      return Ok(deleted);
    }
    Ok(false)
  }

  /// 存活用户键名快照（仅收集键名，零值拷贝，极大节约内存与 CPU；键按字节序排序；
  /// 含 String、对象信封与 Meta 物理域，KEYS / DBSIZE / 槽位删除共用）
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
          let Some(user_key) = live_value_key(key, prefix_slice) else {
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
      .filter(|(k, alive)| *alive && !matches!(self.batch.probe_ttl(k, now), TtlGate::Due))
      .map(|(k, _)| k)
      .collect();
    keys.sort_unstable();
    Ok(keys)
  }
}
