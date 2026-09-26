//! RMW 读改写调度（对齐 Garnet RMW 执行链路与 MainStore/ObjectStore RMWMethods）
//!
//! 值写回面挂在 [`RmwWindow`] 上：本键读改写窗口未取到即无写回入口，
//! 「先双域读算新值、再盲写绝对值」的两步式在类型面上不可表达
//!（对标 C# InternalRMW 的 ephemeral 桶闩跨读—算—写全程）。

use std::result::Result as StdResult;

use wbase::time::now_ticks;
use wdev::Device;
use wval::KeyTag;

use super::super::DEGRADE_ASYNC;
use crate::{
  error::Result,
  session::{RmwWindow, StoreSession},
  ttl::{TtlGate, is_expired},
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
  /// TTL/ETag 旁路记录后重建（新值无 TTL、etag 从头计）——对标 C#
  /// UnifiedStore/RMWMethods.cs CopyUpdater 的 CheckExpiry →
  /// RMWAction.ExpireAndResume（中止更新转 InitialUpdater，过期臂先
  /// RemoveETag，MainStore/RMWMethods.cs:441-446/:1043-1049）与 MainStore/
  /// RMWMethods.cs InitialUpdater 的 Debug.Assert（初始记录无
  /// Expiration/ETag）；INCR/DECR/INCRBYFLOAT/
  /// APPEND/SETRANGE/SETBIT/BITFIELD 写子命令/PFADD/PFMERGE 写回共用
  ///
  /// PFADD/PFMERGE 裁决声明（deviations.md 第 17 条，严禁回改）：对带
  /// TTL 键恒保留 key 级 TTL（Redis 语义、与 C# CopyUpdater 臂
  /// TryCopyOptionals 同口径），不对齐 C# InPlaceUpdater PFADD/PFMERGE 臂
  /// （RMWMethods.cs:666/:677/:707/:718）的 RemoveExpiration——该臂与
  /// Copy 臂自相矛盾、TTL 结局随记录可变性漂移且原位更新不变长无空间
  /// 腾挪动机，属上游缺陷
  ///
  /// SETBIT/BITFIELD 裁决声明（deviations.md 第 18 条，严禁回改）：同上
  /// 恒保留，不对齐 C# InPlaceUpdater SETBIT/BITFIELD 增长臂
  /// （RMWMethods.cs:581/:595/:621/:635）的 RemoveExpiration——该清退系
  /// inline 槽位腾挪的实现副作用，与 Copy 臂 TryCopyOptionals 及同文件
  /// SETRANGE/APPEND 增长臂 "not changing the presence of ETag or
  /// Expiration" 自相矛盾、TTL 结局随值大小漂移，属上游缺陷
  ///
  /// 前置契约：调用方须已处于批处理纪元保护下（`BatchStoreSession` 形态，
  /// 与 `StoreSession::try_upsert_raw_sync_unprotected` 同纪约），且新值已由
  /// 带 TTL 裁决的双域读算出（读路径已闭环 NOTFOUND/WRONGTYPE）；读侧把过期键
  /// 判缺失后，重建写回若保留残留 TTL，新值写完立即可判过期（写入即幽灵），故
  /// 本入口带过期残留清退门：无 TTL 记录单次哈希探针零额外 I/O 放行。降级三态与
  /// [`StoreSession::try_upsert_sync`] 一致，另增 `Ok(Err(DEGRADE_ASYNC))`：TTL 记录有
  /// 磁盘候选、Meta 分层存根在场或清退遭环形页翻转，同步段无法裁决，调用方降级
  /// [`Self::upsert_rmw`] 异步完整闭环  ///
  /// 键缺席/过期清退后的重建落笔即 C# 主存 InitialUpdater
  /// （初始记录无 Expiration/ETag 不变式）：libs/server/Storage/Functions/MainStore/RMWMethods.cs:InitialUpdater
  ///
  /// 重建臂旁域清退单源（票 zcode-r147c-incrovf 案一）：`Due`（已过期未清退）键的
  /// 重建写回一律改道 SET 同步内核
  /// [`StoreSession::try_upsert_tag_sync_unprotected_with_prefix`]（`KeyTag::String`），
  /// 与 SET 覆写共用同一套旁域卫生——Meta 在簿即降级异步树清退、数据落笔提交成功
  /// 后清退信封残留，杜绝「Due 臂删 TTL 证据 + 裸写 String 域」致幽灵信封/幽灵 Meta
  /// 失去唯一过期凭据而永生（GC 由 TTL 记录驱动，无据不清），与
  /// [`StoreSession::try_upsert_tag_sync_unprotected`] 头注「无反向并存窗」单源声明
  /// 同栈。**本臂及任何 RMW 重建臂禁再长出第二套裸清退或裸写**（唯一成文例外：
  /// Due 分支首、改道之前的 ETag 对偶探针——票 zcode-r157c-srethead 案一，SET
  /// 内核裁决保 etag 故不入内核，勿据此扩及 TTL/信封/Meta）；`Pass`（健在键）
  /// 臂仍走 String 域裸写以守 §17/§18 TTL 恒保留，其旁域在场已由读漏斗带 TTL
  /// 裁决先行收敛为 WRONGTYPE，无清退缺口。
  ///
  /// TTL 腿失败补偿（票 zcode-r34-writekernel 条目一）：Due 臂「先删残留 TTL
  /// 再写数据」为过期清退语义所需（旧值逻辑上已不存在，重建无 TTL），该先删后写
  /// 次序与补偿依据（删除前预读原过期刻度、落笔遇页翻转/硬错误即盲写回填再
  /// 降级/上抛）现由 SET 同步内核 TTL 腿单点承接（本臂改道后不再自持一份），
  /// 回填值即已过期刻度，读面立即判死（等价键不存在语义），杜绝「已过期旧值失
  /// TTL 判死依据而永生复活」；回填条目经写监听镜像入 AOF，副本/恢复重放同向
  #[inline(always)]
  pub fn try_rmw_sync(&self, val: &[u8]) -> Result<StdResult<u64, u64>> {
    let session = self.session;
    let prefix = session.session_prefix();
    let prefix = prefix.as_slice();
    let user_key = self.user_key;
    // 过期残留清退门（对标 C# CheckExpiry → ExpireAndResume → InitialUpdater）：
    // Due 先以 ETag 单次哈希探针成对清退残留旁路（重建语义全新、etag 域从头
    // 计，票 zcode-r157c-srethead 案一在册挂点），再改道 SET 同步内核单源承接
    // TTL 清退 + 信封清退 + Meta 在簿降级；旁路记录磁盘候选或删除遭环形页
    // 翻转时交调用方降级异步（upsert_rmw 完整裁决闭环）
    match session.ttl_gate_mem_at_with_prefix(prefix, user_key, now_ticks())? {
      TtlGate::Due => {
        // 过期重建 ETag 对偶清退（对齐 C# MainStore/RMWMethods.cs InPlaceUpdater
        // 过期臂 :441-446 与 CopyUpdater 过期臂 :1043-1049 的 RemoveETag +
        // ExpireAndResume——过期键转 InitialUpdater 语义全新，etag 域必然从头
        // 计）：仿删除内核 try_delete_sync_unprotected_with_prefix 同形，
        // has_etag_key_unprotected 单次哈希探针初筛（无 etag 键零额外写入），
        // 命中即清退、失败零副作用早退降级。置序于改道内核 TTL 腿之前杜绝
        // 「etag 删失后降级、异步臂 has_ttl_tag 已假不再 purge」半程形——异步臂
        // upsert_rmw 经 purge_expired → delete 对 TTL+ETag 级联幂等重做；旧
        // etag 对逻辑死键无保留价值，不做 TTL 刻度式回填补偿。SET 内核裁决
        // 保 etag（etag.rs「SET 保留」条）故本探针不入内核，系下方
        // 「禁第二套裸清退」单源声明的成文例外，勿据此扩及 TTL/信封/Meta
        let etag_k = StoreSession::<D>::etag_key_with_prefix(prefix, user_key);
        if session.has_etag_key_unprotected(&etag_k)?
          && session.try_delete_raw_sync_unprotected(&etag_k)?.is_err()
        {
          return Ok(Err(DEGRADE_ASYNC));
        }
        return session.try_upsert_tag_sync_unprotected_with_prefix(
          prefix,
          user_key,
          KeyTag::String,
          val,
        );
      }
      TtlGate::Degrade => return Ok(Err(DEGRADE_ASYNC)),
      TtlGate::Pass => {}
    }
    // 健在键 RMW 语义：既有 key 级 TTL 记录一概不触碰（§17/§18），只写 String 域
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
  ///
  /// 无存活 TTL 记录臂改道 SET 异步内核 [`StoreSession::upsert_tag`]（票
  /// zcode-r147c-incrovf 案一，与同步臂改道同栈）：本臂覆盖两种同语义形态——
  /// 键本就无 TTL（缺失键重建，C# InitialUpdater 无 Expiration 不变式）与同步
  /// 重建臂已清 TTL、数据已提交而信封清退遭环形页翻转降级之残余窗；两者终态
  /// 均为「String 域新值 + 旁域无残留」，与 SET 覆写终态同构，故旁域卫生
  ///（信封清退 + Meta 在簿树清退）一律交 SET 异步内核单源承接，禁在本臂另起
  /// 第二套清退；过期/未过期两臂维持原裁决序（purge_expired 全域级联保留
  /// AOF 单条化 TtlPurge 端口、未过期保留 TTL 裸写），逐臂终态与同步臂逐字节
  /// 同构
  #[inline(always)]
  pub async fn upsert_rmw(&self, val: &[u8]) -> Result<u64> {
    let session = self.session;
    let user_key = self.user_key;
    // 过期残留完整裁决门：has_ttl_tag 单次哈希探针初筛，无 TTL 记录零额外 I/O；
    // 窗口自身已持有本键桶排他闩，确认过期后直走 purge_expired 清除，免重复取闩自锁
    let live_ttl = if session.has_ttl_tag(user_key)? {
      session.ttl_of(user_key).await?
    } else {
      None
    };
    match live_ttl {
      Some(exp) if is_expired(exp, now_ticks()) => {
        session.purge_expired(user_key, exp).await?;
      }
      // 无存活 TTL 记录（本无 TTL / 墓碑 / 同步重建臂已清退）：重建语义即 SET
      // 语义，旁域卫生交 SET 异步内核单源
      None => return session.upsert_tag(user_key, KeyTag::String, val).await,
      // 未过期：RMW 语义保留既有 TTL，仅覆写 String 域
      Some(_) => {}
    }
    let rec_k = session.session_tag_key(KeyTag::String, user_key);
    let addr = session.upsert_raw(&rec_k, val).await?;
    session.bump_watch_version(user_key);
    Ok(addr)
  }
}
