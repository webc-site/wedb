//! RMW 读改写调度（对齐 Garnet RMW 执行链路与 MainStore/ObjectStore RMWMethods）
//!
//! 值写回面挂在 [`RmwWindow`] 上：本键读改写窗口未取到即无写回入口，
//! 「先双域读算新值、再盲写绝对值」的两步式在类型面上不可表达
//!（对标 C# InternalRMW 的 ephemeral 桶闩跨读—算—写全程）。

use std::result::Result as StdResult;

use wbase::time::now_ticks;
use wdev::Device;
use wval::KeyTag;

use crate::{
  error::Result,
  session::{RmwWindow, StoreSession},
  ttl::TtlGate,
};

/// 原位增长写回三态（[`RmwWindow::try_grow_in_place`] 出口）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmwGrow {
  /// 已原位发布新值（携新逻辑值长度），调用方可直接应答
  InPlace(usize),
  /// 未命中原位（键缺失 / 只读区 / 密封在途 / 槽位富余不足 / 旧值已过期）：
  /// 调用方回落 [`RmwWindow::try_rmw_sync`] 整值尾部追加
  Fallback,
  /// TTL 记录有磁盘候选，同步段无法裁决：调用方降级异步闭环
  Degrade,
}

impl<'a, 'k, D: Device> RmwWindow<'a, 'k, D> {
  /// 纯同步快速路径 RMW 写回窗口键（对齐 Garnet RMW 执行链路）
  ///
  /// libs/server/Storage/Functions/MainStore/RMWMethods
  ///
  /// RMW 语义：未过期键保留既有 key 级 TTL 记录不触碰——对标 C#
  /// UnifiedStore/VarLenInputMethods 的 GetRMWModifiedFieldInfo
  /// （HasExpiration 随源记录保留）与 SETRANGE/APPEND 原位增长分支注释
  /// "not changing the presence of ETag or Expiration"；已过期键清退残留
  /// TTL 记录后重建（新值无 TTL）——对标 C# UnifiedStore/RMWMethods.cs
  /// CopyUpdater 的 CheckExpiry → RMWAction.ExpireAndResume（中止更新转
  /// InitialUpdater）与 MainStore/RMWMethods.cs InitialUpdater 的
  /// Debug.Assert（初始记录无 Expiration）；INCR/DECR/INCRBYFLOAT/
  /// APPEND/SETRANGE/SETBIT/BITFIELD 写子命令/PFADD/PFMERGE 写回共用
  ///
  /// 前置契约：调用方须已处于批处理纪元保护下（`BatchStoreSession` 形态，
  /// 与 `StoreSession::try_upsert_raw_sync_unprotected` 同纪约），且新值已由
  /// 带 TTL 裁决的双域读算出（读路径已闭环 NOTFOUND/WRONGTYPE）；读侧把过期键
  /// 判缺失后，重建写回若保留残留 TTL，新值写完立即可判过期（写入即幽灵），故
  /// 本入口带过期残留清退门：无 TTL 记录单次哈希探针零额外 I/O 放行。降级三态与
  /// [`StoreSession::try_upsert_sync`] 一致，另增 `Ok(Err(u64::MAX))`：TTL 记录有
  /// 磁盘候选或清退遭环形页翻转，同步段无法裁决，调用方降级 [`Self::upsert_rmw`]
  /// 异步完整闭环
  #[inline(always)]
  pub fn try_rmw_sync(&self, val: &[u8]) -> Result<StdResult<u64, u64>> {
    let session = self.session;
    let prefix = session.session_prefix();
    let prefix = prefix.as_slice();
    let user_key = self.user_key;
    // 过期残留清退门（对标 C# CheckExpiry → ExpireAndResume → InitialUpdater）：
    // Due 先删残留 TTL 记录再写数据（重建无 TTL）；TTL 记录磁盘候选或删除
    // 遭环形页翻转时交调用方降级异步（upsert_rmw 完整裁决闭环）
    match session.ttl_gate_mem_at_with_prefix(prefix, user_key, now_ticks())? {
      TtlGate::Due => {
        let ttl_k = StoreSession::<D>::ttl_key_with_prefix(prefix, user_key);
        if session.try_delete_raw_sync_unprotected(&ttl_k)?.is_err() {
          return Ok(Err(u64::MAX));
        }
      }
      TtlGate::Degrade => return Ok(Err(u64::MAX)),
      TtlGate::Pass => {}
    }
    let rec_k = StoreSession::<D>::session_tag_key_with_prefix(prefix, KeyTag::String, user_key);
    let written = session.try_upsert_raw_sync_unprotected(&rec_k, val)?;
    if written.is_ok() {
      session.bump_watch_version(user_key);
    }
    Ok(written)
  }

  /// 字符串记录原位增长写回（对标 libs/server/Storage/Functions/MainStore/
  /// RMWMethods.cs:InPlaceUpdater 的 APPEND 分支 :799-834 与 SETRANGE 分支
  /// :734-763——C# 有的「只拷新字节、原位改长」臂）
  ///
  /// 与本窗口 [`Self::try_rmw_sync`] 共用同一批处理纪元、同键桶排他闩与同一
  /// TTL/WATCH 收口，是同一 RMW 状态机的原位臂而非第二条写路径：
  /// - 闭包在持页写锁期内拿到「旧值 + 槽位松弛富余」的完整容量切片与旧值长，
  ///   只写新字节并回报新逻辑值长——旧数据零复制、零尾部追加、零中间 Vec；
  /// - TTL 门先裁决：`Due` 说明旧值逻辑上已不存在、绝不可原位续接，`Degrade`
  ///   须异步闭环——两态一律回退整值写回，过期残留 TTL 记录的清退仍由
  ///   `try_rmw_sync` 单点承担，本臂不重复清退（杜绝两套过期善后）；
  /// - 闭环即推进 WATCH 版本（与 `try_rmw_sync` 同一收口，对位 C#
  ///   watchVersionMap.IncrementVersion），AOF/写通知在持锁闭包内由 wkv 原位
  ///   内核透传**改长后的新全值**切片。
  ///
  /// 前置契约与 [`Self::try_rmw_sync`] 一致（调用方持本窗口、已处批处理纪元
  /// 保护下）；未命中即 [`RmwGrow::Fallback`]，调用方按现状整值读改写，两路
  /// 应答与 TTL 语义逐字节一致。
  #[inline(always)]
  pub fn try_grow_in_place(
    &self,
    grow: impl FnOnce(&mut [u8], usize) -> Option<usize>,
  ) -> Result<RmwGrow> {
    let session = self.session;
    let prefix = session.session_prefix();
    let prefix = prefix.as_slice();
    let user_key = self.user_key;
    // 过期门前置于原位写之前（对位 C# CheckExpiry 先于 InPlaceUpdater 的
    // 增长臂）：Due 即旧值不可续接，交整值写回臂重建
    match session.ttl_gate_mem_at_with_prefix(prefix, user_key, now_ticks())? {
      TtlGate::Due => return Ok(RmwGrow::Fallback),
      TtlGate::Degrade => return Ok(RmwGrow::Degrade),
      TtlGate::Pass => {}
    }
    let rec_k = StoreSession::<D>::session_tag_key_with_prefix(prefix, KeyTag::String, user_key);
    Ok(
      match session.try_grow_raw_in_place_unprotected(&rec_k, grow)? {
        Some(new_len) => {
          session.bump_watch_version(user_key);
          RmwGrow::InPlace(new_len)
        }
        None => RmwGrow::Fallback,
      },
    )
  }

  /// 写回窗口键（RMW 语义，异步闭环）
  ///
  /// libs/server/Storage/Functions/MainStore/RMWMethods
  ///
  /// 未过期键保留既有 key 级 TTL 记录不触碰（INCR/APPEND/SETRANGE 等读改写
  /// 回写面，对标 C# GetRMWModifiedFieldInfo 的 HasExpiration 保留）；已过期
  /// 键经 check_expired 完整裁决（含磁盘路径 purge_expired，先删 TTL 再删
  /// 数据）后重建，新值无 TTL——对标 C# ObjectStore/RMWMethods.cs
  /// InPlaceUpdaterWorker/CopyUpdater 的 CheckExpiry → ExpireAndResume 后转
  /// InitialUpdater；[`Self::try_rmw_sync`] 快路径降级臂由此统一闭环，WATCH
  /// 推进同收口一次
  #[inline(always)]
  pub async fn upsert_rmw(&self, val: &[u8]) -> Result<u64> {
    let session = self.session;
    let user_key = self.user_key;
    // 过期残留完整裁决门：has_ttl_tag 单次哈希探针初筛，无 TTL 记录零额外 I/O
    if session.has_ttl_tag(user_key)? {
      session.check_expired(user_key).await?;
    }
    let rec_k = session.session_tag_key(KeyTag::String, user_key);
    let addr = session.upsert_raw(&rec_k, val).await?;
    session.bump_watch_version(user_key);
    Ok(addr)
  }
}
