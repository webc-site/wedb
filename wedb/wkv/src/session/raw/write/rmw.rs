//! RMW 读改写调度（对齐 Garnet RMW 执行链路与 MainStore/ObjectStore RMWMethods）

use std::result::Result as StdResult;

use wbase::time::now_ticks;
use wdev::Device;
use wval::KeyTag;

use crate::{error::Result, session::StoreSession, ttl::TtlGate};

impl<D: Device> StoreSession<D> {
  /// 纯同步快速路径 RMW 写回当前会话普通字符串键（对齐 Garnet RMW 执行链路）
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
  /// 前置契约：调用方须先经带 TTL 裁决的双域读确认键存活与域归属（读路径
  /// 已闭环 NOTFOUND/WRONGTYPE）；读侧把过期键判缺失后，重建写回若保留
  /// 残留 TTL，新值写完立即可判过期（写入即幽灵），故本入口带过期残留
  /// 清退门：无 TTL 记录单次哈希探针零额外 I/O 放行。降级三态与
  /// [`Self::try_upsert_sync`] 一致，另增 `Ok(Err(u64::MAX))`：TTL 记录有
  /// 磁盘候选或清退遭环形页翻转，同步段无法裁决，调用方降级
  /// [`Self::upsert_rmw`] 异步完整闭环
  #[inline(always)]
  pub fn try_rmw_sync(&self, user_key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    let _guard = self.enter_gated();
    self.try_rmw_sync_unprotected(user_key, val)
  }

  /// 纯同步快速路径 RMW 写回内核（调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// 语义与 [`Self::try_rmw_sync`] 完全一致且零 enter() 原子开销
  #[inline(always)]
  pub fn try_rmw_sync_unprotected(
    &self,
    user_key: &[u8],
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let prefix = self.session_prefix();
    self.try_rmw_sync_unprotected_with_prefix(prefix.as_slice(), user_key, val)
  }

  /// 显式前缀 RMW 写内核（循环前缀外提对位，语义与
  /// [`Self::try_rmw_sync_unprotected`] 完全一致；rust 工程优化无 c# 对应）
  #[inline(always)]
  pub fn try_rmw_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    // 过期残留清退门（对标 C# CheckExpiry → ExpireAndResume → InitialUpdater）：
    // Due 先删残留 TTL 记录再写数据（重建无 TTL）；TTL 记录磁盘候选或删除
    // 遭环形页翻转时交调用方降级异步（upsert_rmw 完整裁决闭环）
    match self.ttl_gate_mem_at_with_prefix(prefix, user_key, now_ticks())? {
      TtlGate::Due => {
        let ttl_k = Self::ttl_key_with_prefix(prefix, user_key);
        if self.try_delete_raw_sync_unprotected(&ttl_k)?.is_err() {
          return Ok(Err(u64::MAX));
        }
      }
      TtlGate::Degrade => return Ok(Err(u64::MAX)),
      TtlGate::Pass => {}
    }
    let rec_k = Self::session_tag_key_with_prefix(prefix, KeyTag::String, user_key);
    let written = self.try_upsert_raw_sync_unprotected(&rec_k, val)?;
    if written.is_ok() {
      self.bump_watch_version(user_key);
    }
    Ok(written)
  }

  /// 写回当前会话普通字符串键（RMW 语义，异步闭环）
  ///
  /// libs/server/Storage/Functions/MainStore/RMWMethods
  ///
  /// 未过期键保留既有 key 级 TTL 记录不触碰（INCR/APPEND/SETRANGE 等读改写
  /// 回写面，对标 C# GetRMWModifiedFieldInfo 的 HasExpiration 保留）；已过期
  /// 键经 check_expired 完整裁决（含磁盘路径 purge_expired，先删 TTL 再删
  /// 数据）后重建，新值无 TTL——对标 C# ObjectStore/RMWMethods.cs
  /// InPlaceUpdaterWorker/CopyUpdater 的 CheckExpiry → ExpireAndResume 后转
  /// InitialUpdater；[`Self::try_rmw_sync`] 快路径降级臂由此统一闭环，WATCH
  /// 推进同收口一次（与 modify.rs 闭包式 `rmw` 分名：本入口是直写值形态的
  /// RMW 语义 upsert）
  #[inline(always)]
  pub async fn upsert_rmw(&self, user_key: &[u8], val: &[u8]) -> Result<u64> {
    // 过期残留完整裁决门：has_ttl_tag 单次哈希探针初筛，无 TTL 记录零额外 I/O
    if self.has_ttl_tag(user_key)? {
      self.check_expired(user_key).await?;
    }
    let rec_k = self.session_tag_key(KeyTag::String, user_key);
    let addr = self.upsert_raw(&rec_k, val).await?;
    self.bump_watch_version(user_key);
    Ok(addr)
  }
}
