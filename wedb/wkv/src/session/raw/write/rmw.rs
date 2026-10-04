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

/// RMW 同步臂共享前置门三态（[`RmwWindow::rmw_sync_gate`] 出口；TTL 三态 + etag
/// 剥除合并裁决，`try_rmw_sync`/`try_grow_in_place` 两臂同判据单点）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RmwGate {
  /// TTL 健在且无 etag 旁路残留：直进覆写/原位臂
  Pass,
  /// TTL 已过期（Due）：rmw 臂改道 SET 同步内核重建，原位臂回落整值写回
  Due,
  /// 同步段无法裁决（TTL 探针需异步闭环或 etag 剥除遭环形页翻转）：降级异步
  Degrade,
}

impl<'a, 'k, D: Device> RmwWindow<'a, 'k, D> {
  /// RMW 同步臂共享前置门单点（TTL 过期残留清退门 + etag 旁路剥除，
  /// `try_rmw_sync`/`try_grow_in_place` 两臂同判据，杜绝同旁路两套覆写语义）：
  ///
  /// - TTL 腿（对标 C# CheckExpiry 先于写臂）：`Due` 即旧值逻辑上已不存在；
  /// - etag 腿（定裁 (a)：对位 C# GetRMWModifiedFieldInfo 恒 HasETag=false，票
  ///   wkv-set-family-etag-bypass-residue-cas-baseline-fork）：String 域覆写
  ///   （含原位增长）落笔前单点清退 etag 旁路，与 SET 覆写同判据。置序于落笔
  ///   之前，维持「同步段降级即值未提交」不变式（异步重放对 APPEND/SETRANGE
  ///   重读旧值重算，绝不双增）；单次哈希探针初筛，无 etag 键零额外写入，命中
  ///   删除经写监听分发 EtagWrite(None)→AOF Setwithetag(0) 令副本/恢复同收敛；
  ///   删除遭环形页翻转即早退 Degrade（值未触碰），交调用方异步臂幂等重做。
  ///
  /// `Due` 臂语义两臂有意分叉（rmw 改道 SET 重建 / 原位回落整值写回），留调用
  /// 方映射；过期残留清退本身由 `try_rmw_sync` 的 Due 臂（改道 SET 同步内核）
  /// 单点承担，原位臂不重复清退（杜绝两套过期善后）。
  #[inline]
  fn rmw_sync_gate(&self) -> Result<RmwGate> {
    let session = self.session;
    let prefix = session.session_prefix();
    let prefix = prefix.as_slice();
    let user_key = self.user_key;
    match session.ttl_gate_mem_at_with_prefix(prefix, user_key, now_ticks())? {
      TtlGate::Due => return Ok(RmwGate::Due),
      TtlGate::Degrade => return Ok(RmwGate::Degrade),
      TtlGate::Pass => {}
    }
    let etag_k = StoreSession::<D>::etag_key_with_prefix(prefix, user_key);
    if session.has_etag_key_unprotected(&etag_k)?
      && session.try_delete_raw_sync_unprotected(&etag_k)?.is_err()
    {
      return Ok(RmwGate::Degrade);
    }
    Ok(RmwGate::Pass)
  }

  /// 纯同步快速路径 RMW 写回窗口键（对齐 Garnet RMW 执行链路）
  ///
  /// libs/server/Storage/Functions/MainStore/RMWMethods
  ///
  /// RMW 语义：未过期键保留既有 key 级 TTL 记录不触碰（§17/§18）——对标 C#
  /// UnifiedStore/VarLenInputMethods 的 GetRMWModifiedFieldInfo
  /// （HasExpiration 随源记录保留）；ETag 则一律清退——同函数 GetRMWModifiedFieldInfo
  /// 恒 HasETag=false，String 域覆写（INCR/APPEND/SETRANGE 写回）使 etag 消亡，
  /// 与 SET 覆写内核同判据单点收口（票 wkv-set-family-etag-bypass-residue-
  /// cas-baseline-fork 定裁 (a)，勿据 C# 原位增长分支 "not changing the presence
  /// of ETag" 反推 Rust 保留——该注释系 C# InPlaceUpdater 与 CopyUpdater/TryCopyFrom
  /// 清退臂自相矛盾的上游布局偶然，Rust 统一收敛于 FieldInfo 规范）；已过期键
  /// 清退残留 TTL/ETag 旁路记录后重建（新值无 TTL、etag 从头计）——对标 C#
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
  /// 后成对清退 TTL/etag 与信封残留，杜绝「Due 臂删 TTL 证据 + 裸写 String 域」致幽灵信封/幽灵 Meta
  /// 失去唯一过期凭据而永生（GC 由 TTL 记录驱动，无据不清），与
  /// [`StoreSession::try_upsert_tag_sync_unprotected`] 头注「无反向并存窗」单源声明
  /// 同栈。**本臂及任何 RMW 重建臂禁再长出第二套裸清退或裸写**：定裁 (a) 撤销
  /// 旧 zcode-r157c-srethead「SET 内核裁决保 etag 故本探针不入内核」成文例外——
  /// etag 清退现由 SET 内核旁路腿单源承接，Due 臂不再自持对偶探针；`Pass`（健在
  /// 键）臂对 String 域覆写按同一 HasETag=false 判据在落笔前单点清退 etag（守
  /// §17/§18 TTL 恒保留，其信封/Meta 在场已由读漏斗带 TTL 裁决先行收敛为
  /// WRONGTYPE，无清退缺口）。
  ///
  /// TTL 腿失败补偿（票 zcode-r34-writekernel 条目一）：Due 臂「先删残留 TTL
  /// 再写数据」为过期清退语义所需（旧值逻辑上已不存在，重建无 TTL），该先删后写
  /// 次序与补偿依据（删除前预读原过期刻度、落笔遇页翻转/硬错误即盲写回填再
  /// 降级/上抛）现由 SET 同步内核 TTL 腿单点承接（本臂改道后不再自持一份），
  /// 回填值即已过期刻度，读面立即判死（等价键不存在语义），杜绝「已过期旧值失
  /// TTL 判死依据而永生复活」；回填条目经写监听镜像入 AOF，副本/恢复重放同向
  /// C# ClientSession 五臂对位（与 Upsert/Delete 族同表形制，本会话面单点承载）：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:RMW
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ITsavoriteContext.cs:RMW
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:RMW
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalUnsafeContext.cs:RMW
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/UnsafeContext.cs:RMW
  #[inline(always)]
  pub fn try_rmw_sync(&self, val: &[u8]) -> Result<StdResult<u64, u64>> {
    let session = self.session;
    let prefix = session.session_prefix();
    let prefix = prefix.as_slice();
    let user_key = self.user_key;
    // 过期残留清退门 + etag 剥除前置（单点 [`Self::rmw_sync_gate`]；对标 C#
    // CheckExpiry → ExpireAndResume → InitialUpdater）：Due 改道 SET 同步内核
    // 单源承接 TTL 清退 + etag 清退 + 信封清退 + Meta 在簿降级（定裁 (a) 后内核
    // 旁路腿成对清退 etag，本臂不再自持对偶探针）；旁路记录磁盘候选或删除遭环形
    // 页翻转时交调用方降级异步（upsert_rmw 完整裁决闭环）
    match self.rmw_sync_gate()? {
      RmwGate::Due => {
        return session.try_upsert_tag_sync_unprotected_with_prefix(
          prefix,
          user_key,
          KeyTag::String,
          val,
        );
      }
      RmwGate::Degrade => return Ok(Err(DEGRADE_ASYNC)),
      RmwGate::Pass => {}
    }
    // 既有 key 级 TTL 记录一概不触碰（§17/§18），只覆写 String 域
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
    // 增长臂）：Due 即旧值不可续接，交整值写回臂重建；门单点见
    // [`Self::rmw_sync_gate`]
    match self.rmw_sync_gate()? {
      RmwGate::Due => return Ok(RmwGrow::Fallback),
      RmwGate::Degrade => return Ok(RmwGrow::Degrade),
      RmwGate::Pass => {}
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
  /// 键经 check_expired 完整裁决（含磁盘路径 purge_expired，记录墓碑先行、
  /// TTL 随后）后重建，新值无 TTL——对标 C# ObjectStore/RMWMethods.cs
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
      // 语义，旁域卫生交 SET 异步内核单源（含 etag 清退）
      None => return session.upsert_tag(user_key, KeyTag::String, val).await,
      // 未过期：RMW 语义保留既有 TTL，覆写 String 域
      Some(_) => {}
    }
    // String 域覆写按定裁 (a) 清退 etag 旁路（对位 C# GetRMWModifiedFieldInfo
    // HasETag=false，票 wkv-set-family-etag-bypass-residue-cas-baseline-fork），
    // 与同步 Pass/原位臂同判据；purge_expired 已过 etag 时本探针幂等零写，未过期
    // 臂经此剥除并经 EtagWrite(None)→Setwithetag(0) 令副本/恢复同收敛
    session.del_etag(user_key).await?;
    let rec_k = session.session_tag_key(KeyTag::String, user_key);
    let addr = session.upsert_raw(&rec_k, val).await?;
    session.bump_watch_version(user_key);
    Ok(addr)
  }
}
