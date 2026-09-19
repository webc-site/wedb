//! key 级 TTL 记录的同步快路径读写（批处理纪元上下文专用）
//!
//! 归属存储层（对标 libs/server/Storage/Functions/SessionFunctionsUtils.cs 的
//! 过期评估域：C# 的过期判定与 TTL 记录操作在存储会话函数层完成，RESP 命令
//! 层仅调用）。wkv 的 `expire_at`/`persist`/`pttl_ms` 全为异步入口（内含磁盘
//! I/O 与物理清除），而 RESP 命令层是同步函数、经 `Ok(false)` 向调用方发降级
//! 信号。本模块在 [`wkv::BatchStoreSession`] 纪元保护下，用 wkv 公开的 raw 同步内核
//! （`try_read_raw_in_memory`/`try_modify_raw_in_place_unprotected`/
//! `try_upsert_raw_sync`/`try_delete_raw_sync`）实现 TTL 记录的最深同步路径：
//! 可变区命中的读写删全部零 I/O 闭环；遇磁盘候选或环形页翻转时返回降级信号，
//! 由调用方按已实现命令的既有约定整体转异步路由。
//!
//! TTL 内存门裁决三态单点收敛于 `wkv::TtlGate` 与
//! [`wkv::StoreSession::ttl_gate_mem_at`]：本模块经 Deref 直调，不再本地重复
//! 定义枚举与判定逻辑（对标 C# LogRecordUtils.cs:CheckExpiry 单点判定）。
//!
//! TTL 记录值编码直接复用 `wval::I64Codec`：8 字节大端 i64 .NET Ticks
//! （100ns 单位，0001-01-01 纪元），与 C# Garnet RecordDataHeader 的 expiration
//! 同域；Unix 秒/毫秒 ↔ ticks 的 RESP 边界换算见 `wbase::convert`
//! （garnet/libs/common/ConvertUtils.cs 镜像），本模块零手抄镜像。

use std::result;

use wbase::time::now_ticks;
use wdev::Device;
use wkv::{BatchStoreSession, Result, StoreResult, TtlGate, is_expired};
use wval::{GarnetObjectType, I64_VAL_LEN, I64Codec, KeyTag, META_VALUE_SIZE, MetaValue};

use crate::resp::vector::vector_manager::VectorManager;

/// 读 key 的 TTL 记录（同步）
///
/// 三态对齐 [`StoreResult`] 约定：
/// - `Ok(StoreResult::Success(Some(exp)))`：内存命中，绝对过期 .NET Ticks；
/// - `Ok(StoreResult::Success(None))` / `NotFound`：无 TTL 记录（哈希探针未命中
///   即零 I/O 判定；墓碑与非法值长度视同无 TTL）；
/// - `Ok(StoreResult::RecordOnDisk)`：TTL 记录有磁盘候选，须降级异步裁决。
pub fn ttl_of_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<StoreResult<Option<i64>>> {
  let prefix = session.session_prefix();
  ttl_of_sync_with_prefix(session, prefix.as_slice(), key)
}

/// 显式前缀读 key 的 TTL 记录（循环前缀外提对位，语义与 [`ttl_of_sync`]
/// 一致；rust 工程优化无 c# 对应）
pub fn ttl_of_sync_with_prefix<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
) -> Result<StoreResult<Option<i64>>> {
  let ttl_k = <wkv::StoreSession<D>>::ttl_key_with_prefix(prefix, key);
  match session.try_read_raw_in_memory(&ttl_k, I64Codec::decode)? {
    // 内存闭环：非法值长度按无 TTL 容错
    StoreResult::Success(v) => Ok(StoreResult::Success(v)),
    // 墓碑 / 无候选视同无 TTL 记录
    StoreResult::NotFound => Ok(StoreResult::Success(None)),
    // 磁盘候选：降级异步裁决
    StoreResult::RecordOnDisk => Ok(StoreResult::RecordOnDisk),
  }
}

/// 写 key 的 TTL 记录（同步）：可变区原位改写优先，失败降级 RCU 盲插
///
/// 返回 `Ok(true)` 已闭环；`Ok(false)` 遭遇环形页翻转须降级异步。
/// 裸写内核：输入 ticks 的值域裁决（EXPIRE 族 4-bit coarse 粗化）已在上游
/// 命令边界 `network_expire` / 异步入口 wkv `expire_at` 完成（单点
/// `wbase::convert::coarse_expire_ticks`），本函数不判不掩，镜像
/// `wkv::StoreSession::put_ttl` 的取舍，见其注释；长度守卫比 wkv
/// 原版更严——记录值非定长 8B 时放弃原位改写转 RCU，杜绝
/// `copy_from_slice` 长度失配 panic
///
/// WATCH 版本推进收口（TTL 写单点）：闭环即推进——EXPIRE 族经 RMW 面落
/// TTL，对标 C# UnifiedStore RMWMethods.cs InPlaceUpdater/PostInitialUpdater
/// 无条件 IncrementVersion；降级臂未落写入不推进，由异步 `expire_at_ticks`
/// 慢路径补推
pub fn put_ttl_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  expire_at_ticks: i64,
) -> Result<bool> {
  let bytes = I64Codec::encode(expire_at_ticks);
  let ttl_k = session.ttl_key(key);
  let in_place = session
    .try_modify_raw_in_place_unprotected(&ttl_k, |slot| {
      if slot.len() == I64_VAL_LEN {
        slot.copy_from_slice(&bytes);
        Some(())
      } else {
        None
      }
    })?
    .is_some();
  if in_place {
    session.bump_watch_version(key);
    return Ok(true);
  }
  let upserted = session.try_upsert_raw_sync(&ttl_k, &bytes)?.is_ok();
  if upserted {
    session.bump_watch_version(key);
  }
  Ok(upserted)
}

/// 删 key 的 TTL 记录（同步）
///
/// 返回 `Ok(true)` 已闭环（含本就无记录：删除内核纯查找探针，未命中零写入
/// 亦不建槽）；`Ok(false)` 须降级异步
///
/// WATCH 版本推进收口（TTL 删单点）：真实删除（`deleted == true`）才推进，
/// 未命中零写入视为无状态变化不推进——对齐 C# InPlaceDeleter 的
/// `!logRecord.Info.Modified` 条件（MainStore DeleteMethods.cs:32，PERSIST
/// 经 RMW 面同向）；降级臂由异步 `persist_key` 慢路径补推
pub fn del_ttl_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<bool> {
  let ttl_k = session.ttl_key(key);
  match session.try_delete_raw_sync(&ttl_k)? {
    // 环形页翻转：降级异步
    Err(_) => Ok(false),
    // 内存闭环：真实删除才推进版本（未命中零写入视同无状态变化）
    Ok(deleted) => {
      if deleted {
        session.bump_watch_version(key);
      }
      Ok(true)
    }
  }
}

/// 裸数据存活探针（镜像 `contains_key_ignore_ttl`：剥离 TTL 门控，仅判数据记录）
///
/// 三态：`Ok(Some(true))` 数据在内存存活；`Ok(Some(false))` 内存确认不存在；
/// `Ok(None)` 数据有磁盘候选须降级。
pub fn data_alive_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: wval::KeyTag,
) -> Result<Option<bool>> {
  let prefix = session.session_prefix();
  data_alive_sync_with_prefix(session, prefix.as_slice(), key, tag)
}

/// 裸数据存活探针的显式前缀变体（循环前缀外提对位，语义与
/// [`data_alive_sync`] 一致；rust 工程优化无 c# 对应）
pub fn data_alive_sync_with_prefix<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
  tag: wval::KeyTag,
) -> Result<Option<bool>> {
  Ok(
    match session.try_read_tag_in_memory_unprotected_with_prefix(prefix, key, tag, |_| ())? {
      StoreResult::Success(()) => Some(true),
      StoreResult::NotFound => Some(false),
      StoreResult::RecordOnDisk => None,
    },
  )
}

/// 分层元记录存活判据单点：解析 MetaValue 并校验存活（RangeIndex 或 size > 0），
/// 返回集合类型；畸形记录 / 死记录返回 None
pub fn meta_collection_type_of(raw: &[u8]) -> Option<wval::GarnetObjectType> {
  raw
    .get(..META_VALUE_SIZE)
    .and_then(|raw| MetaValue::from_slice(raw).ok())
    .filter(|meta| meta.is_live())
    .map(|meta| meta.collection_type)
}

/// 分层元记录是否 RangeIndex 记录（写面门的单点判据，三态）
///
/// `KeyTag::Meta` 上 `MetaValue.collection_type == RangeIndex` 的物理域事实，
/// 对标 C# 主存记录 `RecordType == RangeIndexRecordType` 判别
/// （ReadMethods.cs:118 / RMWMethods.cs:397）。存活口径与
/// [`meta_collection_type_of`] 完全一致（过期 / 死记录视同非 RI，判据不另起
/// 第二套）；三态同 [`meta_alive_sync_with_prefix`]：内存命中 → `Some(bool)`，
/// 元记录有磁盘候选 → `None`（调用方须降级异步，不得盲写）。
pub fn meta_is_range_index<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<bool>> {
  let prefix = session.session_prefix();
  Ok(
    match session.try_read_tag_in_memory_unprotected_with_prefix(
      prefix.as_slice(),
      key,
      KeyTag::Meta,
      |raw| meta_collection_type_of(raw) == Some(GarnetObjectType::RangeIndex),
    )? {
      StoreResult::Success(found) => Some(found),
      StoreResult::NotFound => Some(false),
      StoreResult::RecordOnDisk => None,
    },
  )
}

/// 分层元记录存活探针（同步）
///
/// 升阶键（wcol should_promote 触发后）数据迁入 KeyTag::Meta 元记录 + wbftree
/// 树，String / ObjectEnvelope 双域皆缺。存活判据与 wkv `load_collection_stub`
/// 同口径：MetaValue 可解析且（RangeIndex 或 size > 0）。C# 无物理域分派
///（UnifiedStore ReadMethods.cs 的 Reader 单记录统一存活判定），本探针为 rust
/// 分域布局下的语义等价臂，[`probe_alive_domain_with_prefix`] 与
/// [`read_adjudicated_user_sync_with_prefix`] 共用
pub fn meta_alive_sync_with_prefix<D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
) -> Result<Option<bool>> {
  // 三态同 [`data_alive_sync_with_prefix`]：内层命中存活判据 → Some(true)，
  // 元记录缺失/死记录 → Some(false)，磁盘候选 → None
  Ok(
    match session.try_read_tag_in_memory_unprotected_with_prefix(
      prefix,
      key,
      KeyTag::Meta,
      |raw| meta_collection_type_of(raw).is_some(),
    )? {
      StoreResult::Success(found) => Some(found),
      StoreResult::NotFound => Some(false),
      StoreResult::RecordOnDisk => None,
    },
  )
}

/// 带 TTL 裁决的同步数据读（键值读不因 TTL 标签墓碑而降级）
///
/// 三态对齐 [`wkv::StoreResult`]，但门控更精细：数据在内存且
/// TTL 记录可内存裁决（无记录 / 有记录未过期 / 记录为墓碑）时直接闭环；键已
/// 过期时快路径直接回 NOTFOUND 语义（`StoreResult::NotFound`，CheckExpiry 判定同 wkv 读原语层，映射注释在该层
/// 内 CheckExpiry 失败即 NOTFOUND，物理清理留写路径惰性清退与后台 GC）；仅
/// 数据有磁盘候选或 TTL 值有磁盘候选时才降级异步
pub fn read_adjudicated_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> Result<StoreResult<R>> {
  read_adjudicated_tag_sync(session, key, KeyTag::String, f)
}

/// 带 TTL 裁决的同步数据读（指定物理标签域，[`read_adjudicated_sync`] 的带标签内核）
///
/// TTL 裁决前移到用户闭包执行之前（对齐 wkv 异步版 `read_tag_with` 的注释
/// 契约：闭包可能直写响应缓冲，过期键「先执行读闭包后应答」会造成同步段
/// 残留半成品应答）——先零副作用探测数据在否，裁决通过后二次内存读执行
/// 用户闭包（批处理纪元内同键稳定，二次读即最新态）；已过期键在闭包执行前
/// 直接闭环 NOTFOUND，用户闭包绝不触达过期数据
pub fn read_adjudicated_tag_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: wval::KeyTag,
  f: impl FnOnce(&[u8]) -> R,
) -> Result<StoreResult<R>> {
  let prefix = session.session_prefix();
  read_adjudicated_tag_sync_with_prefix(session, prefix.as_slice(), key, tag, f)
}

/// 显式前缀带 TTL 裁决的指定标签域同步读（循环前缀外提对位，语义与
/// [`read_adjudicated_tag_sync`] 一致；rust 工程优化无 c# 对应）
pub fn read_adjudicated_tag_sync_with_prefix<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
  tag: wval::KeyTag,
  f: impl FnOnce(&[u8]) -> R,
) -> Result<StoreResult<R>> {
  let present = session.try_read_tag_in_memory_unprotected_with_prefix(prefix, key, tag, |_| ())?;
  if matches!(present, StoreResult::Success(())) {
    match session.ttl_gate_mem_at_with_prefix(prefix, key, now_ticks())? {
      // 已过期：快路径 NOTFOUND（用户闭包不执行，物理清理留写路径/后台 GC）
      TtlGate::Due => return Ok(StoreResult::NotFound),
      TtlGate::Degrade => return Ok(StoreResult::RecordOnDisk),
      TtlGate::Pass => {}
    }
  }
  Ok(match present {
    // 裁决通过：二次读执行用户闭包（副作用仅发生在确定输出时）
    StoreResult::Success(()) => {
      session.try_read_tag_in_memory_unprotected_with_prefix(prefix, key, tag, f)?
    }
    // 数据内存确认缺失：键不存在
    StoreResult::NotFound => StoreResult::NotFound,
    // 数据有磁盘候选：整体降级异步裁决
    StoreResult::RecordOnDisk => StoreResult::RecordOnDisk,
  })
}

/// 带 TTL 裁决的记录物理尺寸同步读（指定物理标签域）
///
/// MEMORY USAGE 统计内核（[`read_adjudicated_tag_sync`] 的带尺寸对位）：命中
/// 时向闭包披露 (值切片, 记录物理分配尺寸)，尺寸口径见
/// [`wkv::RecordRead`]——记录头 + 键 + 值 + 对齐填充/显式松弛，对标 C#
/// `srcLogRecord.AllocatedSize`（ReadMethods.cs:HandleMemoryUsage）。
/// TTL 裁决前移语义与 [`read_adjudicated_tag_sync`] 一致
pub fn read_adjudicated_tag_with_size<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: wval::KeyTag,
  f: impl FnOnce(&[u8], usize) -> R,
) -> Result<StoreResult<R>> {
  let present = session.try_read_tag_in_memory_unprotected(key, tag, |_| ())?;
  if matches!(present, StoreResult::Success(())) {
    match session.ttl_gate_mem_at(key, now_ticks())? {
      TtlGate::Due => return Ok(StoreResult::NotFound),
      TtlGate::Degrade => return Ok(StoreResult::RecordOnDisk),
      TtlGate::Pass => {}
    }
  }
  Ok(match present {
    // 裁决通过：二次读（带尺寸）执行用户闭包
    StoreResult::Success(()) => session.try_read_tag_in_memory_with_size(key, tag, f)?,
    // 数据内存确认缺失：键不存在
    StoreResult::NotFound => StoreResult::NotFound,
    // 数据有磁盘候选：整体降级异步裁决
    StoreResult::RecordOnDisk => StoreResult::RecordOnDisk,
  })
}

/// RMW 重建前清退过期残留 TTL 记录（对象信封同步写入口前置单点）
///
/// 读侧把过期键判缺失（信封域裁决 `Due` → 缺失
/// 闭环）后，RMW 重建写回若保留残留 TTL，新值写完立即可判过期（写入即幽灵），
/// 故对象同步写回入口先经本门清退。三态：
/// - `Ok(true)`：已闭环（无 TTL 记录 / 未到期 / 已到期且残留记录已删）；
/// - `Ok(false)`：TTL 记录有磁盘候选，同步段无法裁决，调用方降级异步
///   （异步臂经 wkv `read_tag_with` 的 probe_alive 惰性清除自愈）；
/// - `Err`：存储层错误透传
///
/// 对标 C# libs/server/Storage/Functions/ObjectStore/RMWMethods.cs
/// InPlaceUpdaterWorker/CopyUpdater 的 CheckExpiry → RMWAction.ExpireAndResume
/// （中止更新后转 InitialUpdater，初始记录无 Expiration）
pub fn rmw_ttl_rebuild_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<bool> {
  match session.ttl_gate_mem_at(key, now_ticks())? {
    TtlGate::Due => del_ttl_sync(session, key),
    TtlGate::Pass => Ok(true),
    TtlGate::Degrade => Ok(false),
  }
}

/// 带 TTL 裁决的同步用户数据双域读（一处定义，跨型命令共用）
///
/// 对标 C# UnifiedStore ReadMethods:HandleObjectEncoding 的 ValueIsObject
/// 带外判定：先探 String 域——命中即用户数据（值内容任意，不做对象嗅探）；
/// 未命中再探 ObjectEnvelope 域——命中即集合对象键（`Success(Err(()))` 供调用
/// 方答 WRONGTYPE）；两域皆缺为键缺失（`NotFound`）。返回值外层语义与
/// [`read_adjudicated_sync`] 一致（`RecordOnDisk` = 须降级异步）
pub fn read_adjudicated_user_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> Result<StoreResult<result::Result<R, ()>>> {
  let prefix = session.session_prefix();
  read_adjudicated_user_sync_with_prefix(session, prefix.as_slice(), key, f)
}

/// 显式前缀带 TTL 裁决的用户数据双域同步读（循环前缀外提对位，语义与
/// [`read_adjudicated_user_sync`] 一致；rust 工程优化无 c# 对应）
pub fn read_adjudicated_user_sync_with_prefix<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> Result<StoreResult<result::Result<R, ()>>> {
  match read_adjudicated_tag_sync_with_prefix(session, prefix, key, KeyTag::String, f)? {
    // String 域磁盘候选：整体降级（信封域不再探，保守方向；String 域已过期键
    // 在 tag_sync 内提前闭环 NotFound，不会进入本层误探信封域）
    StoreResult::RecordOnDisk => Ok(StoreResult::RecordOnDisk),
    StoreResult::Success(v) => Ok(StoreResult::Success(Ok(v))),
    // String 域确认缺失：探对象信封域，再探分层元记录域（升阶键判对象键）
    StoreResult::NotFound => Ok(
      match read_adjudicated_tag_sync_with_prefix(
        session,
        prefix,
        key,
        KeyTag::ObjectEnvelope,
        |_| (),
      )? {
        StoreResult::RecordOnDisk => StoreResult::RecordOnDisk,
        StoreResult::Success(()) => StoreResult::Success(Err(())),
        StoreResult::NotFound => {
          match read_adjudicated_tag_sync_with_prefix(
            session,
            prefix,
            key,
            KeyTag::Meta,
            meta_collection_type_of,
          )? {
            // 升阶键：对象键 WRONGTYPE 口径（C# Reader 单记录统一判定，
            // 对象记录即 ValueIsObject）
            StoreResult::RecordOnDisk => StoreResult::RecordOnDisk,
            StoreResult::Success(Some(_)) => StoreResult::Success(Err(())),
            StoreResult::Success(None) | StoreResult::NotFound => StoreResult::NotFound,
          }
        }
      },
    ),
  }
}

/// 键存活探针（数据存活 + TTL 未过期才视为存活；String / ObjectEnvelope /
/// Meta 三域）
///
/// 返回 `Ok(None)` 须降级：数据或 TTL 记录有磁盘候选。键已过期返回
/// `Ok(Some(false))`（视同不存在，CheckExpiry 判定同 wkv 读原语层，映射注释在该层
/// 失败即 NOTFOUND 的同栈裁决；覆盖写入由 upsert 写面同步清退旧 TTL 记录自愈，
/// 物理清除留写路径惰性清退与后台 GC）。条件写（SET NX/XX）、RESTORE NX、
/// EXPIRE 族、EXISTS、TTL 族等依赖"键在否"判定的命令共用
pub fn probe_alive<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<bool>> {
  let prefix = session.session_prefix();
  probe_alive_with_prefix(session, prefix.as_slice(), key)
}

/// 键存活探针的显式前缀变体（循环前缀外提对位，语义与 [`probe_alive`]
/// 一致；rust 工程优化无 c# 对应：批量遍历单次外提 `session_prefix()`
/// 消除逐键重读 ns/db 原子变量与重算 Varint）
pub fn probe_alive_with_prefix<D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
) -> Result<Option<bool>> {
  Ok(probe_alive_domain_with_prefix(session, prefix, key)?.map(|found| found.is_some()))
}

/// 键存活探针的向量登记表第四态（只读观测面单点）
///
/// 对标 C# UnifiedStore ReadMethods.cs:31-47 的 Reader：主存里
/// RecordType == VectorManager.RecordType 的向量索引记录（VADD 落
/// MainStore/VarLenInputMethods.cs:192-195）与 String/集合记录同库同探针，
/// EXISTS / TTL / PTTL / EXPIRETIME / MEMORY USAGE 对向量键无类型门判存活。
/// rust 把该记录单列于 `VectorManager::key_index_registry`（不驻 wkv 值域），
/// 故 [`probe_alive_with_prefix`] 三域判缺失后，须再问登记表——命中即存活。
/// 判据入口与 EXISTS 原用 read_stored_index 同源，不新起第二套登记表判据、
/// 不给登记表加过期字段、不新增第二套 TTL 掩码（TTL 值仍由 [`ttl_of_sync_with_prefix`]
/// 单点裁决，本探针仅定「活/缺」）。`Ok(None)` 三域磁盘候选须降级异步。
///
/// 刻意只覆盖只读观测面：写面（EXPIRE/PERSIST/RENAME 迁移及 AOF/复制/迁移重放）
/// 暂不接本态——重放端 `wkv::expire_at` 拿不到登记表，主端单独开闸会造成两套
/// 终态，须与重放端配套改造后再统一放行（见 task/ing 残余）。
pub fn probe_alive_with_registry<D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
  vector: Option<&VectorManager>,
) -> Result<Option<bool>> {
  match probe_alive_with_prefix(session, prefix, key)? {
    // 三域命中存活：直接存活
    Some(true) => Ok(Some(true)),
    // 三域判缺失（含三域皆空与命中域记录已过期）：再问向量登记表第四态
    Some(false) => {
      Ok(Some(vector.is_some_and(|vm| {
        vm.read_stored_index(prefix, key).is_some()
      })))
    }
    // 三域磁盘候选：整体降级异步，不在此盲判登记表
    None => Ok(None),
  }
}

/// 键存活域探针（[`probe_alive`] 的带域对位）：存活键所在物理域
///
/// 返回 `Ok(Some(None))` 键不存在；`Ok(Some(Some(domain)))` 键存活且驻留
/// String / ObjectEnvelope 域；`Ok(None)` 须降级（语义同 [`probe_alive`]）。
/// 供 SET 条件写区分 object 键（对标 C# UnifiedStore WRONGTYPE 事务重试的
/// 域判定），其余调用方用 [`probe_alive`] 即可
pub fn probe_alive_domain<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<Option<KeyTag>>> {
  let prefix = session.session_prefix();
  probe_alive_domain_with_prefix(session, prefix.as_slice(), key)
}

/// 键存活域探针的显式前缀变体（循环前缀外提对位，语义与
/// [`probe_alive_domain`] 一致；rust 工程优化无 c# 对应）
pub fn probe_alive_domain_with_prefix<D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
) -> Result<Option<Option<KeyTag>>> {
  let domain = match data_alive_sync_with_prefix(session, prefix, key, KeyTag::String)? {
    None => return Ok(None),
    Some(true) => KeyTag::String,
    Some(false) => {
      // String 域缺失：探对象信封域（对象键 EXPIRE/EXISTS/条件写生效前提）
      match data_alive_sync_with_prefix(session, prefix, key, KeyTag::ObjectEnvelope)? {
        None => return Ok(None),
        Some(false) => {
          // 信封域缺失：探分层元记录域（升阶键 EXPIRE/EXISTS/条件写生效前提；
          // C# Reader 单记录统一存活判定，本臂为 rust 分域布局的语义等价）
          match meta_alive_sync_with_prefix(session, prefix, key)? {
            None => return Ok(None),
            Some(false) => return Ok(Some(None)),
            Some(true) => KeyTag::Meta,
          }
        }
        Some(true) => KeyTag::ObjectEnvelope,
      }
    }
  };
  match ttl_of_sync_with_prefix(session, prefix, key)? {
    StoreResult::RecordOnDisk => Ok(None),
    // 到期判定严格小于（读路径口径，对标 LogRecordUtils.cs:20，统一收敛至 is_expired）；
    // exp == now 视为存活，已过期键视同不存在（Some(None)，经 probe_alive
    // 映射为 Some(false)），不再排队异步重判；覆盖写入由 upsert 写面同步清退
    // 旧 TTL 记录自愈，物理清除留写路径惰性清退与后台 GC
    StoreResult::Success(v) if is_expired(v, now_ticks()) => Ok(Some(None)),
    // 无 TTL 记录（NotFound 臂）与未到期同判存活
    StoreResult::NotFound | StoreResult::Success(_) => Ok(Some(Some(domain))),
  }
}

#[cfg(test)]
mod tests {
  use super::I64Codec;

  /// TTL 记录编解码与落库值同域（.NET Ticks）：I64Codec 往返
  #[test]
  fn ttl_codec_roundtrip() {
    let ticks = 638_600_000_000_000_000_i64;
    let bytes = I64Codec::encode(ticks);
    assert_eq!(I64Codec::decode(&bytes), Some(ticks));
    // 非法长度按无 TTL 容错
    assert_eq!(I64Codec::decode(&[]), None);
    assert_eq!(I64Codec::decode(&[0, 0, 0]), None);
  }
}
