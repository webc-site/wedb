//! TTL / 键状态裁决会话操作面（对标 C# 统一存 EXPIRE/EXPIREAT/PERSIST/
//! EXISTS/PTTL 族：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs，
//! C# 为 StorageSession partial）
//!
//! [`StorageSession`] 会话面：TTL 读写清退（WATCH 版本推进收口）、三域存活
//! 异步裁决、EXISTS 存在性单点、RangeIndex 写门。批处理纪元同步工具与判据
//! 源（[`meta_collection_type_of`] 等）在 [`super::ttl_sync`] 一处定义，本面
//! 只做会话包装。

// TTL 清退故障注入（io/Ordering）仅 debug 装配，release 剔除防 unused imports
#[cfg(debug_assertions)]
use std::{io, sync::atomic::Ordering};

use wdev::Device;
use wkv::TtlOpt;
use wval::{GarnetObjectType, KeyTag};

#[cfg(debug_assertions)]
use super::super::storage_session::PERSIST_FAIL_INJECT;
use super::{
  super::storage_session::StorageSession,
  ttl_sync::{
    del_ttl_sync, meta_collection_type_of, probe_alive_with_registry,
    probe_alive_with_registry_async,
  },
};
use crate::{resp::vector::vector_manager::VectorManager, types::GarnetStatus};

impl<'a, D: Device> StorageSession<'a, D> {
  /// 三域存活异步裁决（返回存活键所在物理域；None = 键不存在或已过期）
  ///
  /// 同步探针的异步对偶：
  /// 同步探针的 `Deferred`（磁盘候选 / TTL 记录待裁决）在异步读内闭环——
  /// 过期键经 [`Self::read_tag_with`] 惰性清除后视同不存在。SET 条件写、
  /// RENAME / RESTORE / EXPIRE 族慢路径臂共用本面
  pub async fn probe_alive_domain(&self, key: &[u8]) -> wkv::Result<Option<KeyTag>> {
    let prefix = self.batch.session_prefix();
    self
      .probe_alive_domain_with_prefix(prefix.as_slice(), key)
      .await
  }

  /// 三域存活异步裁决的显式前缀变体（循环前缀外提对位，语义与
  /// [`Self::probe_alive_domain`] 逐臂一致；rust 工程优化无 c# 对应）
  ///
  /// 三域按序短路（String 命中即返回）与 Meta 域存活判据
  /// （[`meta_collection_type_of`]）均沿用本单点，异步读口的前缀由入参交出，
  /// 逐域不再重派生（EXISTS 等批量键命令的每键前缀成本降为零）
  pub async fn probe_alive_domain_with_prefix(
    &self,
    prefix: &[u8],
    key: &[u8],
  ) -> wkv::Result<Option<KeyTag>> {
    if self
      .read_tag_with_prefix(prefix, key, KeyTag::String, |_| ())
      .await?
      .is_some()
    {
      return Ok(Some(KeyTag::String));
    }
    if self
      .read_tag_with_prefix(prefix, key, KeyTag::ObjectEnvelope, |_| ())
      .await?
      .is_some()
    {
      return Ok(Some(KeyTag::ObjectEnvelope));
    }
    Ok(
      self
        .read_tag_with_prefix(prefix, key, KeyTag::Meta, meta_collection_type_of)
        .await?
        .flatten()
        .map(|_| KeyTag::Meta),
    )
  }

  /// 三域存活异步裁决的零入账对偶（[`Self::probe_alive_domain_with_prefix`]
  /// 的静默口，镜像 [`Self::read_user`] / [`Self::read_user_quiet`] 薄包装双口
  /// 先例，不起第二套判型机制）：逐域走 [`Self::read_tag_quiet_with_prefix`]
  /// 静默内核，出口不折叠命中/未命中——条件写族慢臂（SETNX / SET 条件写非 GET
  /// 形）「恰一帧」终态单点补账专用（票 wnode-string-bitmap-found-notfound-
  /// accounting-matrix：簿记档逐域入账使缺失键计 3、对象键计 2，与该族
  /// C# SET_Conditional 单帧口径失联；终态存在性由调用方经
  /// [`Self::record_read_outcome`] 折叠恰一条）。EXISTS 族等既有簿记消费者
  /// 不动，勿误取本口（防静默丢计）
  pub(crate) async fn probe_alive_domain_quiet_with_prefix(
    &self,
    prefix: &[u8],
    key: &[u8],
  ) -> wkv::Result<Option<KeyTag>> {
    if self
      .read_tag_quiet_with_prefix(prefix, key, KeyTag::String, |_| ())
      .await?
      .is_some()
    {
      return Ok(Some(KeyTag::String));
    }
    if self
      .read_tag_quiet_with_prefix(prefix, key, KeyTag::ObjectEnvelope, |_| ())
      .await?
      .is_some()
    {
      return Ok(Some(KeyTag::ObjectEnvelope));
    }
    Ok(
      self
        .read_tag_quiet_with_prefix(prefix, key, KeyTag::Meta, meta_collection_type_of)
        .await?
        .flatten()
        .map(|_| KeyTag::Meta),
    )
  }

  /// 字符串写入口的 RangeIndex 键门（异步对偶，`resp::basic_commands` 的
  /// `ri_write_gate` 同判据：存活 Meta 元记录 `collection_type == RangeIndex`
  /// 即拦截）
  ///
  /// 返回 true = 存活 RI 记录，字符串写一律拒（WRONGTYPE 由调用方出帧）；
  /// 判据源 [`meta_collection_type_of`] 单点，不另起第二套
  pub async fn ri_write_gate(&self, key: &[u8]) -> wkv::Result<bool> {
    Ok(
      self
        .read_tag_with(key, KeyTag::Meta, |raw| {
          meta_collection_type_of(raw) == Some(GarnetObjectType::RangeIndex)
        })
        .await?
        .unwrap_or(false),
    )
  }

  /// RI 写门的零入账对偶（[`Self::ri_write_gate`] 的静默口，镜像
  /// read_user / read_user_quiet 双口先例）：判据同源
  /// （[`meta_collection_type_of`]），逐域走 [`Self::read_tag_quiet`] 静默
  /// 内核、出口不折叠——条件写族 / BITOP 目的键门在其「恰一帧」终态单点
  /// 补账闭环内不得再入账（票 wnode-string-bitmap-found-notfound-
  /// accounting-matrix：簿记档 gate 读虚增 1 帧使慢臂与快臂 / C# 单帧口径
  /// 失联）。裸写族等既有簿记消费者走原口，勿误取本口（防静默丢计）
  pub(crate) async fn ri_write_gate_quiet(&self, key: &[u8]) -> wkv::Result<bool> {
    Ok(
      self
        .read_tag_quiet(key, KeyTag::Meta, |raw| {
          meta_collection_type_of(raw) == Some(GarnetObjectType::RangeIndex)
        })
        .await?
        .unwrap_or(false),
    )
  }

  /// 探测键是否存在（对标 C# libs/server/API/GarnetApiUnifiedCommands.cs:EXISTS）
  ///
  /// 本面对探针家族零判定体：同步档
  /// [`crate::storage::session::common::ttl_sync::probe_alive_with_registry`]
  /// 先行（批处理纪元内存直读三域 + 登记表第四态），仅当回降级态（三域有
  /// 磁盘候选）或同步段存储错误时才转异步档
  /// [`crate::storage::session::common::ttl_sync::probe_alive_with_registry_async`]
  /// 收尾（异步读口闭环 + 同一第四态判据源），两档同一条折叠式、同一判据源。
  ///
  /// 对标 C# Read_UnifiedStore（libs/server/Storage/Session/UnifiedStore/
  /// AdvancedOps.cs:12-21）：单次 `Read` 后仅 `status.IsPending` 才
  /// `CompletePendingForUnifiedStoreSession`，终态恒取同一 `status.Found`；
  /// 簇侧键可操作判定（libs/cluster/Session/SlotVerification/
  /// ClusterSlotVerify.cs:17）转调本口，与 EXISTS 命令共用同一存活单点，
  /// 故向量键（登记表第四态在场、三域无记录）在迁移门评下同判存活
  ///
  /// C# 统一存 EXISTS（组 EXISTS 命令 Input 走 Read 判定）的 rust 存活单点：
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:EXISTS
  pub async fn exists(
    &self,
    key: &[u8],
    vector: Option<&VectorManager>,
  ) -> wkv::Result<GarnetStatus> {
    let prefix = self.batch.session_prefix();
    let alive = match probe_alive_with_registry(&self.batch, prefix.as_slice(), key, vector) {
      Ok(Some(alive)) => alive,
      // 三域磁盘候选 / 同步段存储错误：均不得在同步段定终态，交异步档闭环
      _ => probe_alive_with_registry_async(self, prefix.as_slice(), key, vector).await?,
    };
    Ok(if alive {
      GarnetStatus::Ok
    } else {
      GarnetStatus::NotFound
    })
  }

  /// 清退键级 TTL 旁路记录（STORE 族 SET 语义的「随写清退」段抽核，一处定义）
  ///
  /// STORE 冷漏斗窗内信封写回臂（页翻转重试闭环成功后，见
  /// [`obj_save_pageswap_replay`](crate::resp::objects::rmw_helpers)）与升阶
  /// 成功臂（[`dest_cold_promote_arm`](crate::resp::objects::rmw_helpers) 换入
  /// bftree+Meta 后经本口清退——升阶迁移臂本身键存活不动 TTL 旁路，SET 语义
  /// 须显式落笔）共用同一清退判定。前置条件：调用方持本键 rmw 窗（窗与 TTL
  /// 闩同址，[`del_ttl_sync`] 契约）；清退经 [`del_ttl_sync`] 无闩变体（WATCH
  /// 推进单点内聚），页翻转降级 wkv 异步 `del_ttl`，AOF Persist 镜像由 TTL
  /// 物理键写监听 TtlWrite(None) 单点承接
  pub async fn clear_ttl(&self, key: &[u8]) -> wkv::Result<()> {
    #[cfg(debug_assertions)]
    if PERSIST_FAIL_INJECT.swap(false, Ordering::AcqRel) {
      return Err(wkv::Error::Io(io::Error::other(
        "TTL 清退故障注入（测试钩子）",
      )));
    }
    if !del_ttl_sync(&self.batch, key)? {
      self.batch.del_ttl(key).await?;
    }
    Ok(())
  }

  /// 设置键级绝对过期时间（.NET Ticks，对标 C# EXPIRE 族在 RESP 边界换算为
  /// ticks 后经 UnifiedInput 携带的同域语义；KeyAdminCommands.cs:421-427）
  ///
  /// C# 统一存 EXPIREAT（绝对时间戳 → ticks → RMW，timeoutSet 出参同 applied>0）
  /// 的 rust 会话级落点：
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:EXPIREAT
  pub async fn expire_at_ticks(&self, key: &[u8], expire_at_ticks: i64) -> wkv::Result<i32> {
    self
      .expire_at_ticks_opt(key, expire_at_ticks, TtlOpt::NONE)
      .await
  }

  /// [`Self::expire_at_ticks`] 的带 NX/XX/GT/LT 条件选项变体（EXPIRE 族
  /// 交互慢路径与 AOF 重放共用本包装的 applied > 0 推进尾巴，一处口径——
  /// 旁路裸写 `batch.expire_at` 缺席 WATCH 版本推进，禁再直调）
  ///
  /// C# 统一存 EXPIRE 核心重载（时长/时间戳四重载换算 ticks 后经
  /// ExpirationWithOption 进 RMW，timeoutSet = Found && 回包 1）的 rust 会话级
  /// 落点：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:EXPIRE
  pub async fn expire_at_ticks_opt(
    &self,
    key: &[u8],
    expire_at_ticks: i64,
    opt: TtlOpt,
  ) -> wkv::Result<i32> {
    // >0 = TTL 已写（1）或过期即删（2）：键状态实际变化才推进版本（C# EXPIRE
    // 经 RMW InPlaceUpdater/PostInitialUpdater，未命中不 Incremment 同向）
    let applied = self.batch.expire_at(key, expire_at_ticks, opt).await?;
    if applied > 0 {
      self.bump_watch_version(key);
    }
    Ok(applied)
  }

  /// 移除键级 TTL（对标 PERSIST）
  pub async fn persist_key(&self, key: &[u8]) -> wkv::Result<i32> {
    // 故障注入门（测试钩子，一次性）：模拟底层设备写故障，与真实
    // wdev::Error::OutOfBounds 同型上抛，杜绝测试绕过本统一清退入口
    #[cfg(debug_assertions)]
    if PERSIST_FAIL_INJECT.swap(false, Ordering::AcqRel) {
      return Err(wkv::Error::Io(io::Error::other(
        "TTL 清退故障注入（测试钩子）",
      )));
    }
    // 返回 1 = TTL 记录已删（键元数据变化，C# PERSIST 走 RMW 同向计版本）
    let applied = self.batch.persist(key).await?;
    if applied > 0 {
      self.bump_watch_version(key);
    }
    Ok(applied)
  }

  /// 查询键剩余生存毫秒（无 TTL 记录返回 -1，键不存在返回 -2；RESP 出参边界，
  /// 内部 .NET Ticks 经 wkv pttl_ms 换算，见 ConvertUtils.MillisecondsFromDiffUtcNowTicks）
  pub async fn pttl_ms(&self, key: &[u8]) -> wkv::Result<i64> {
    self.batch.pttl_ms(key).await
  }

  /// 查询键绝对过期 Unix 毫秒时间戳（语义同 pttl；内部 ticks 经
  /// `unix_time_in_milliseconds_from_ticks` 换算为出参毫秒）
  pub async fn expiretime_ms(&self, key: &[u8]) -> wkv::Result<i64> {
    self.batch.expiretime_ms(key).await
  }
}
