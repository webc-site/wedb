//! 键空间数组迭代函数（对标 libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs）
//!
//! C# 侧为 StorageSession partial + 静态迭代器类，基于 Tsavorite 扫描；Rust 侧
//! 统一落在 [`StorageSession`] 方法上，
//! 物理扫描走 wkv `scan_range_callback`（BfTree 闭区间范围扫描），会话隔离由
//! 会话前缀（ns/db 变长编码 + 1 字节标签）承担。

use std::{
  collections::TryReserveError,
  fmt,
  io::{self, ErrorKind},
};

use wbase::{glob::glob_match_nocase, hash_slot::slot_of, time::now_ticks};
use wdev::Device;
use whlog::Error as HLogError;
use wkv::{Error, TtlGate};
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec};

use super::{super::storage_session::StorageSession, ttl_sync::meta_collection_type_of};

/// 扫描序活键判定三态结果（live_key_at 出口）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveKeyOutcome<'k> {
  /// 存活用户键（已通过链首去重、双域裁决、非墓碑、Meta 存活、TTL 未到期门控）
  Live(KeyTag, &'k [u8]),
  /// 剔除（非用户键 / 非链首 / 跨域旧版 / 墓碑 / Meta 死桩 / TTL 已到期）
  Dead,
  /// TTL 记录有磁盘候选，待降级异步复判
  Degrade(KeyTag, &'k [u8]),
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

/// 活键判定协同探针错误折叠（§35 口径，票 zcode-r135c-rehash 案一）：
/// [`wkv::StoreSession::find_tag_cooperative`] 上抛的迁移内核错误沿既有 whlog
/// Err 通道传出扫描闭包，由命令层折叠慢路径存储错误帧，严禁折成 Dead 假剔除
fn live_probe_err(e: Error) -> HLogError {
  HLogError::Io(io::Error::other(e.to_string()))
}

/// 扫描物化分配失败的平滑收口单点（票 zcode-r55-alloc）：`TryReserveError` 转
/// `ErrorKind::OutOfMemory`，沿既有 whlog/wkv 错误通道上抛，慢路径统一降级
/// `RESP_ERR_SLOW_PATH_STORAGE` 错误帧（对标 C# `OutOfMemoryException` 被会话
/// 主循环 `catch (Exception)` 兜住的单会话收场），杜绝 std 容器扩容触顶
/// `handle_alloc_error` abort 全进程；容量充足时 `try_reserve` fast-path 零系统调用
fn reserve_fail(e: TryReserveError) -> io::Error {
  io::Error::new(
    ErrorKind::OutOfMemory,
    format!("扫描应答物化内存预留失败: {e}"),
  )
}

/// 键内容物化的平滑拷贝（`to_vec` 的 `try_reserve` 前置等价形态）
fn try_key_vec(key: &[u8]) -> io::Result<Vec<u8>> {
  let mut k = Vec::new();
  k.try_reserve(key.len()).map_err(reserve_fail)?;
  k.extend_from_slice(key);
  Ok(k)
}

/// 键名物化的平滑追加（scan/keys 应答物化的逐键大额收口）：容器扩容与键内容
/// 双面 `try_reserve` 前置，失败经 [`reserve_fail`] 上抛。键面物化追加全域单源
/// （本模块两臂与向量登记表并页臂 `merge_vector_keys` 共用，杜绝第二套分配轨）
pub(crate) fn try_push_key(dst: &mut Vec<Vec<u8>>, key: &[u8]) -> io::Result<()> {
  dst.try_reserve(1).map_err(reserve_fail)?;
  dst.push(try_key_vec(key)?);
  Ok(())
}

/// SCAN 有界页入口预算门的预留额硬顶（票 wnode-scan-count-entry-reserve-unbounded）：
/// COUNT 系用户给定的 advisory 页上界，参数面可无界（C# NetworkSCAN/DbScan/
/// AllocatorScan 全程零按 count 分配，合法巨 COUNT 照常一轮扫尽正常应答），
/// 入口前置预留取 `limit.min(本顶)` 封顶——4096×24B=96KB 与测试面分配失败
/// 注入阈值同量级、覆盖常规默认页（COUNT 10）；超出部分由扫描循环内逐键
/// `try_reserve(1)` 平滑臂摊还增长兜底（唯一增长轨，对位 C#
/// `keys.Add` 的 List 摊还扩容），杜绝单条用户 COUNT 直驱无界预分配
pub(crate) const SCAN_PAGE_RESERVE_CAP: usize = 4096;

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

  /// 基础活跃用户键判定（仅前缀命中 + 链首地址校验 + 双域新者胜 + 非墓碑）
  ///
  /// 供 [`Self::live_key_at`] 与 [`Self::delete_slot_keys`] 共用底层链路。
  /// 两处链探针一律经 [`wkv::StoreSession::find_tag_cooperative`] 协同单点
  /// （先分裂协同后探针，同点读 `read_probe` / TTL `has_ttl_key_unprotected`
  /// 一套机制，对标 C# InternalRead.cs:70-73 入口铁律）：扩容进行期未迁分块
  /// 的桶在新表恒空，裸 find_tag 采得 None 会被整批活键误判剔除，SCAN 游标
  /// 固化成全程漏键、双域裁决侧误放行旧版；迁移内核错误沿 Result 上抛，
  /// 严禁折成 Dead 假剔除（票 zcode-r135c-rehash 案一）
  #[inline]
  fn active_user_key_at<'k>(
    &self,
    prefix: &[u8],
    addr: u64,
    key: &'k [u8],
    is_tombstone: bool,
  ) -> wkv::Result<Option<(KeyTag, &'k [u8])>> {
    let Some((tag, user_key)) = NamespaceDbCodec::extract_live_user_key(key, prefix) else {
      return Ok(None);
    };
    if self.batch.find_tag_cooperative(key)? != Some(addr) {
      return Ok(None);
    }
    if matches!(tag, KeyTag::String | KeyTag::ObjectEnvelope) {
      let other = wkv::StoreSession::<D>::session_tag_key_with_prefix(
        prefix,
        if tag == KeyTag::String {
          KeyTag::ObjectEnvelope
        } else {
          KeyTag::String
        },
        user_key,
      );
      if self
        .batch
        .find_tag_cooperative(&other)?
        .is_some_and(|a| a > addr)
      {
        return Ok(None);
      }
    }
    if is_tombstone {
      return Ok(None);
    }
    Ok(Some((tag, user_key)))
  }

  /// 扫描序「当前最新存活用户键」判定单点（SCAN / DBSIZE / KEYS / COUNTKEYSINSLOT / GETKEYSINSLOT 共用）
  ///
  /// 一条记录依次通过以下判定才放行并返回 [`LiveKeyOutcome`]：
  /// 1. 物理键命中用户面域、索引链首地址校验、双物理域新者胜、非墓碑（共用 [`Self::active_user_key_at`]）
  /// 2. 分层元记录存活校验（KeyTag::Meta）：复用 `meta_collection_type_of` 判活（RangeIndex 恒活或 size > 0），死元记录桩判死
  /// 3. TTL 未到期：probe_ttl 三态裁决——Pass 存活、Due 剔除（Dead）、Degrade（冷区磁盘候选）待调用方异步复判
  ///
  /// 探针协同错误经 1 的 Result 通道上抛，不折 Dead。
  #[inline]
  fn live_key_at<'k>(
    &self,
    prefix: &[u8],
    now: i64,
    addr: u64,
    key: &'k [u8],
    value: &[u8],
    is_tombstone: bool,
  ) -> wkv::Result<LiveKeyOutcome<'k>> {
    let Some((tag, user_key)) = self.active_user_key_at(prefix, addr, key, is_tombstone)? else {
      return Ok(LiveKeyOutcome::Dead);
    };
    if tag == KeyTag::Meta && meta_collection_type_of(value).is_none() {
      return Ok(LiveKeyOutcome::Dead);
    }
    Ok(match self.batch.probe_ttl(user_key, now) {
      TtlGate::Due => LiveKeyOutcome::Dead,
      TtlGate::Pass => LiveKeyOutcome::Live(tag, user_key),
      TtlGate::Degrade => LiveKeyOutcome::Degrade(tag, user_key),
    })
  }

  /// 异步复判用户键在指定 ticks 是否已过期（降级异步裸读，无 TTL 视同未过期）
  #[inline(always)]
  pub(crate) async fn is_ttl_expired(&self, user_key: &[u8], now: i64) -> wkv::Result<bool> {
    self.batch.is_expired_at(user_key, now).await
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
  /// 一致读会话下出帧键逐键 pre/post 包裹（对标 C# DbScan :90 装配
  /// `ConsistentUnifiedStoreGetDBKeys` 包装迭代器、Reader :271-277 逐记录
  /// pre → base → post）：经 wkv 附着态单源
  /// [`wkv::StoreSession::with_session_consistent_read_with_prefix`]（内部
  /// `single_key_around` 协议单点 + `consistent_read_hash_with_prefix` 前缀
  /// 外提哈希，与回放侧草图入账同键同标签同哈希）；仅包出帧键的保守口径
  /// 登记 doc/zh/deviations.md §155；未附着会话零开销直通、扫描轨道不变；
  /// pre 超时沿既有 wkv Err 通道上抛，RESP 层 err_frame，与点读族同口径。
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
    // 有界页入口预算门（可预估大额点 `try_reserve` 前置，票 zcode-r55-alloc）：
    // count 为用户给定的页上界，页容量可预估；但用户输入界须有硬顶兜底，
    // 预留额取 `limit.min(SCAN_PAGE_RESERVE_CAP)` 封顶（票
    // wnode-scan-count-entry-reserve-unbounded），limit 页满语义一字不动，
    // 仅封顶入口预留额，超额增长交循环内逐键平滑臂；TYPE 过滤页无上限
    //（`usize::MAX`，C# `long.MaxValue` 同口径）不可预估，跳过前置门，
    // 物化收口交循环内逐键平滑臂
    if limit < usize::MAX {
      items
        .try_reserve(limit.min(SCAN_PAGE_RESERVE_CAP))
        .map_err(scan_err)?;
    }
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
    let mut page_full = false;
    while items.len() < limit {
      let step = it
        .next_ref(|i| {
          let outcome = self
            .live_key_at(
              prefix_slice,
              now,
              i.addr,
              i.rec.key(),
              i.rec.value(),
              i.rec.is_tombstone(),
            )
            .map_err(live_probe_err)?;
          // Live/Degrade 双臂过滤与物化同源单轨（仅降级标记分流）
          let (is_degrade, tag, user_key) = match outcome {
            LiveKeyOutcome::Live(tag, user_key) => (false, tag, user_key),
            LiveKeyOutcome::Degrade(tag, user_key) => (true, tag, user_key),
            LiveKeyOutcome::Dead => return Ok(None),
          };
          if (!all_keys && !glob_match_nocase(pattern, user_key))
            || !Self::type_matches(tag, i.rec.value(), type_filter)
          {
            return Ok(None);
          }
          let k = try_key_vec(user_key).map_err(HLogError::from)?;
          Ok(Some((is_degrade, tag, k)))
        })
        .await?;

      let Some(cand) = step else {
        break;
      };

      if let Some((is_degrade, tag, user_key)) = cand {
        // items 追加前平滑扩容（无界 TYPE 页无入口预算门，逐键收口兜底；
        // 有界页 fast-path 零开销）
        items.try_reserve(1).map_err(scan_err)?;
        if !is_degrade || !self.is_ttl_expired(&user_key, now).await? {
          // 出帧键一致读 pre/post 包裹（同步臂，post 不抛）：tag 取存活判定
          // 三域标签（String/ObjectEnvelope/Meta，与 AOF 入账域一致），前缀
          // 经入参外提免逐键重读原子变量；push 在门禁放行后物化——帧整体
          // 待本调用返回方可被客户端观察，post 先行不撕裂会话前缀
          self.batch.with_session_consistent_read_with_prefix(
            prefix_slice,
            &user_key,
            tag,
            || (),
          )?;
          items.push(user_key);
        }
        if items.len() >= limit {
          page_full = true;
          break;
        }
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
        // 打平集合：MetaValue.collection_type 精确匹配且通过 is_live 判活
        KeyTag::Meta => meta_collection_type_of(value) == Some(t),
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
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DeleteSlotKeysScan
  ///
  /// 库级定槽（doc/zh/db.md 4.1）：会话库槽命中待删槽位时本库全部键均在该
  /// 槽，删除收敛为全库逐键删除；未命中直接 0（零扫描）。
  ///
  /// 偏差声明：对标 C# DeleteSlotKeysScan（libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:416-418）
  /// 注释明示的「every matched live key is deleted, including expired-but-not-yet-tombstoned records (no expiry filter)」，
  /// 本删除枚举使用不过滤到期键的用户面判定（仅前缀命中 + 链首去重 + 双域最新胜 + 非墓碑，见 [`Self::active_user_key_at`]），
  /// 到期未回收键（Due 与 Degrade 键）照常纳入列表交由 delete_string 统一执行删除与裁决（delete_string
  /// 本身无过期门，统一清除数据与随键 TTL/ETag 并推进 WATCH 版本），彻底对齐 C# 语义（见 doc/zh/deviations.md 第 48 条）。
  pub async fn delete_slot_keys(&self, slots: &[u16]) -> wkv::Result<u64> {
    let slot = self.session_slot();
    if !slots.contains(&slot) {
      return Ok(0);
    }
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
        |addr, rec| {
          if let Some((_, user_key)) = self
            .active_user_key_at(prefix_slice, addr, rec.key(), rec.is_tombstone())
            .map_err(live_probe_err)?
          {
            // 逐键物化经 reserve_fail 单机制平滑收口（票 zcode-r125c-clucount1），
            // 与 string_keys_snapshot 臂逐字同形，杜绝扩容触顶 abort 全进程
            try_push_key(&mut keys, user_key).map_err(HLogError::from)?;
          }
          Ok(true)
        },
      )
      .await
      .map_err(scan_err)?;

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
  /// 活键全窗扫描单点（count/db_size/snapshot 三消费面共用骨架）：
  /// `hlog().scan` 全窗逐记录过 [`Self::live_key_at`] 三态判定——Live 键即时
  /// 交 `on_live` 消费，Degrade 键经 [`try_push_key`] 平滑收口暂存并原样返回
  ///（到期复判交调用方异步段），Dead 跳过；探针错误统一 `live_probe_err`、
  /// 扫描错误统一 `scan_err`
  async fn scan_live_keys(
    &self,
    now: i64,
    mut on_live: impl FnMut(&[u8]) -> Result<(), HLogError>,
  ) -> wkv::Result<Vec<Vec<u8>>> {
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
        |addr, rec| {
          match self
            .live_key_at(
              prefix_slice,
              now,
              addr,
              rec.key(),
              rec.value(),
              rec.is_tombstone(),
            )
            .map_err(live_probe_err)?
          {
            LiveKeyOutcome::Live(_, user_key) => on_live(user_key)?,
            // 冷区 TTL 候选入列同走 reserve_fail 单机制（票 zcode-r125c-clucount1）
            LiveKeyOutcome::Degrade(_, user_key) => {
              try_push_key(&mut keys, user_key).map_err(HLogError::from)?;
            }
            LiveKeyOutcome::Dead => {}
          }
          Ok(true)
        },
      )
      .await
      .map_err(scan_err)?;
    Ok(keys)
  }

  pub async fn count_keys_in_slot(&self, slot: u16) -> wkv::Result<usize> {
    if slot != self.session_slot() {
      return Ok(0);
    }
    let now = now_ticks();
    let mut count = 0usize;
    let degrade_keys = self
      .scan_live_keys(now, |_| {
        count += 1;
        Ok(())
      })
      .await?;
    for key in degrade_keys {
      if !self.is_ttl_expired(&key, now).await? {
        count += 1;
      }
    }
    Ok(count)
  }

  /// 迭代器面扫描单点：逐个放行命中给定集群槽位的活用户键（至多 `key_count`
  /// 个）给 `emit` 消费，`emit` 返回 false 即满额早停（对标 C# 槽位迭代器
  /// GetKeysInSlot.Reader 逐记录 `keys.Add` + `return keys.Count < maxKeyCount`）；
  /// 活键判定与 [`Self::count_keys_in_slot`] 共用 [`Self::live_key_at`] 单点。
  /// 键以 `&[u8]` 切片视图交付；须持有键集的消费面（命令入口、迁移分批、复制快照）走
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
    let hlog = self.batch.store.hlog();
    let begin = self.batch.store.begin_address();
    let until = self.batch.store.tail_address();
    let mut it = hlog.scan_iter(begin, until);

    while let Some(cand) = it
      .next_ref(|i| {
        let outcome = self
          .live_key_at(
            prefix_slice,
            now,
            i.addr,
            i.rec.key(),
            i.rec.value(),
            i.rec.is_tombstone(),
          )
          .map_err(live_probe_err)?;
        match outcome {
          // 逐候选键物化经 reserve_fail 单机制平滑收口（票 zcode-r125c-clucount1，
          // 与 scan_cursor :267 同形）；仅键拷贝套壳，不改流式 emit 帧形与 out 缓冲面
          LiveKeyOutcome::Live(_, user_key) => Ok(Some((
            false,
            try_key_vec(user_key).map_err(HLogError::from)?,
          ))),
          LiveKeyOutcome::Degrade(_, user_key) => Ok(Some((
            true,
            try_key_vec(user_key).map_err(HLogError::from)?,
          ))),
          LiveKeyOutcome::Dead => Ok(None),
        }
      })
      .await?
    {
      if let Some((is_degrade, user_key)) = cand {
        if is_degrade {
          if !self.is_ttl_expired(&user_key, now).await? && !emit(&user_key) {
            break;
          }
        } else if !emit(&user_key) {
          break;
        }
      }
    }
    Ok(())
  }

  /// collect 臂单点收口：[`Self::get_keys_in_slot_with`] 的 emit 契约返回 bool
  /// 携不了错误，逐键物化失败先经 [`try_push_key`]（[`reserve_fail`] 单机制）
  /// 早停扫描并暂存 io::Error，待扫描返回后统一转 whlog 沿既有错误通道上抛，
  /// 由消费面（命令入口 / 迁移分批 / 复制快照）折叠慢路径存储错误帧——与流式
  /// 臂同轨，杜绝裸 push 扩容触顶 abort（票 zcode-r125c-clucount1）
  async fn collect_keys_in_slot(
    &self,
    slot: u16,
    key_count: usize,
    initial_capacity: usize,
    mut is_excluded: impl FnMut(&[u8]) -> bool,
  ) -> wkv::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::with_capacity(initial_capacity);
    let mut push_err: Option<io::Error> = None;
    self
      .get_keys_in_slot_with(slot, key_count, |key| {
        if push_err.is_some() {
          return false;
        }
        if is_excluded(key) {
          return true;
        }
        if let Err(e) = try_push_key(&mut keys, key) {
          push_err = Some(e);
          return false;
        }
        keys.len() < key_count
      })
      .await?;
    if let Some(e) = push_err {
      return Err(HLogError::from(e).into());
    }
    Ok(keys)
  }

  /// 命令入口面：列出命中给定集群槽位的活用户键（至多 `key_count` 个，扫描序
  /// 前 N 个），对标 C# 命令入口备 List 交迭代器填充后原样返回的形态；是扫描
  /// 单点 [`Self::get_keys_in_slot_with`] 的 collect 薄封装，迁移分批与复制
  /// 快照等须持有键集的消费面出口（RESP GETKEYSINSLOT 走流式编码，不经此口）
  ///
  /// libs/cluster/Session/ClusterCommands.cs:GetKeysInSlot
  pub async fn get_keys_in_slot(&self, slot: u16, key_count: usize) -> wkv::Result<Vec<Vec<u8>>> {
    // 零预付容量：key_count 参数面可无界（复制快照以 usize::MAX 全量收集），
    // 逐键 try_reserve(1) 摊还增长即唯一增长轨（对位 C# List 摊还扩容）
    self
      .collect_keys_in_slot(slot, key_count, 0, |_| false)
      .await
  }

  /// 列出命中给定集群槽位的活用户键（至多 `key_count` 个），跳过由 `is_excluded` 指定的键
  ///
  /// 排除键不计入 `key_count` 名额，扫描持续推进至取满 `key_count` 个未排除键或扫完全窗，
  /// 杜绝已处理/拉黑键持续占据槽头导致扫描提前截断
  pub async fn get_keys_in_slot_excluding(
    &self,
    slot: u16,
    key_count: usize,
    is_excluded: impl FnMut(&[u8]) -> bool,
  ) -> wkv::Result<Vec<Vec<u8>>> {
    // 预付容量以 key_count 为额：唯一消费面迁移分批以 MAX_MIGRATION_BATCH_COUNT
    // = 64 有界（slots.rs），零触顶面；push 臂与全族同走 collect_keys_in_slot
    // 平滑单轨（票执行注记①，杜绝族内双轨）
    self
      .collect_keys_in_slot(slot, key_count, key_count, is_excluded)
      .await
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
        items.try_reserve(1).map_err(scan_err)?;
        items.push(key);
      }
    }
    Ok(items)
  }

  /// 当前库键数量（DBSIZE 语义，按会话前缀过滤统计）
  ///
  /// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbSize
  /// libs/server/API/SessionApi.cs:DbSize
  ///（C# `RespServerSession.DbSize()` 即 `basicGarnetApi.GetDbSize()` 转发；
  /// rust 无该 API 包装层，DBSIZE 慢路径 `StoreGarnetApi::exec_slow` Dbsize 臂
  /// 直调本内核，向量登记表域内增量在该消费点合并）
  ///
  /// 偏差声明：C# UnifiedStoreGetDBSize.Reader 按原始日志逐记录计数（仅
  /// `!IsInternalRecord && !CheckExpiry`，不查链首不去重），RCU 更新留下的旧版本
  /// 一并计入导致 C# DBSIZE 与 len(KEYS) 同态不等；本仓采用链首去重、双物理域新者胜
  /// 以及统一活键判定（与 [`Self::db_keys`] / [`Self::string_keys_snapshot`] 同源同判据），
  /// 恒保证 DBSIZE == len(KEYS)，系上游缺陷修复型偏离（详见 doc/zh/deviations.md 第 48 条）。
  /// 流式计数（task/todo/zcode-r3-perf-dbsize-materialize.md）：复用存活判定单点逐键累加，
  /// 消除大库全量物化 `Vec<Vec<u8>>` 的堆内存与拷贝开销。
  pub async fn db_size(&self) -> wkv::Result<usize> {
    let now = now_ticks();
    let mut count = 0usize;
    let degrade_keys = self
      .scan_live_keys(now, |_| {
        count += 1;
        Ok(())
      })
      .await?;
    for key in degrade_keys {
      if !self.is_ttl_expired(&key, now).await? {
        count += 1;
      }
    }

    Ok(count)
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
  /// 含 String、对象信封与 Meta 物理域，KEYS / 复制快照等共用）
  ///
  /// 偏差声明：去重口径与 [`Self::db_size`] 一致采用链首地址去重与统一活键判定，
  /// 详见 doc/zh/deviations.md 第 48 条。
  pub(crate) async fn string_keys_snapshot(&self) -> wkv::Result<Vec<Vec<u8>>> {
    let now = now_ticks();
    let mut keys = Vec::new();
    let degrade_keys = self
      .scan_live_keys(now, |user_key| {
        try_push_key(&mut keys, user_key).map_err(HLogError::from)
      })
      .await?;
    for key in degrade_keys {
      if !self.is_ttl_expired(&key, now).await? {
        keys.try_reserve(1).map_err(scan_err)?;
        keys.push(key);
      }
    }

    keys.sort_unstable();
    Ok(keys)
  }
}
