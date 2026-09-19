//! 存储会话（对标 C# `sealed partial class StorageSession`，libs/server/Storage/Session/StorageSession.cs）
//!
//! C# 侧 StorageSession 按 MainStore/ObjectStore/UnifiedStore 拆为多个 partial 文件；
//! Rust 侧以单一结构体 + 跨文件 `impl` 块承担同等职责。底层统一走 wkv
//! `BatchStoreSession`（纪元守卫持有者），同步快路径未闭环时降级 wkv 异步路径，
//! 对标 C# BasicContext 内部 CompletePending 语义。

use std::{future::Future, sync::Arc};

use wbase::time::{now_stopwatch_ticks, now_ticks};
use wcol::object_payload::obj_encode_into;
use wconf::DEFAULT_RESP_VERSION;
use wdev::Device;
use wkv::{
  BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, DeleteMissHook, StoreResult,
  TtlOpt, WatchHook,
};
use wmetric::{GarnetLatencyMetricsSession, LatencyMetricsType, SessionMetricsHandle};
use wresp::ext::RespVecExt;
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};
use wval::{GarnetObjectType, KeyTag};

use crate::{
  resp::vector::vector_manager::VectorManager,
  storage::session::common::{UserReadAsync, ttl_sync::meta_collection_type_of},
  types::GarnetStatus,
};

/// 存储会话：wserver 执行存储操作的内部层
pub struct StorageSession<'a, D: Device> {
  /// 底层批处理会话（对标 C# stringBasicContext/objectBasicContext 共用的底层会话）
  pub batch: BatchStoreSession<'a, D>,
  /// 会话指标共享句柄（对标 libs/server/Storage/Session/Metrics.cs:sessionMetrics
  /// 类引用：C# 由 RespServerSession 将同一 GarnetSessionMetrics 实例传入存储会话
  /// 直写计数；采样关闭（C# trackStats false）时为 None，写口经空条件跳过）
  session_metrics: Option<Arc<SessionMetricsHandle>>,
  /// 会话侧延迟表共享句柄（对标 C# StorageSession 构造传入的
  /// `readonly GarnetLatencyMetricsSession LatencyMetrics`
  /// （libs/server/Storage/Session/Metrics.cs:12），经
  /// [`Self::with_latency_metrics`] 单点注入：pending 闭环的 PENDING_LAT
  /// 计时唯一落点；非会话面（AOF/复制重放、事务过程视图、周期收集）构造不
  /// 出会话延迟表，None = 零取时零分配，与 C# 这些面 `latencyMetrics` 为
  /// null 同形）
  latency_metrics: Option<Arc<GarnetLatencyMetricsSession>>,
  /// RESP 协议版本：会话真实协议版本经 [`Self::with_resp_version`] 单点注入
  /// （对标 C# `storageSession.UpdateRespProtocolVersion` 的双写落点，rust 每命令
  /// 重构造存储会话，故改由慢路径入口传入而非会话内可变态）。缺省取
  /// [`DEFAULT_RESP_VERSION`] 作非会话面（AOF/复制重放、事务过程视图、周期收集）
  /// 下限，与 C# `CreateFunctionsState(respProtocolVersion = DEFAULT_RESP_VERSION)` 同形
  resp_version: u8,
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// 基于底层批处理会话创建存储会话
  ///
  /// 副本一致读会话状态不在此装配：由连接级 wkv `StoreSession` 附着态统一
  /// 承接（对标 C# readSessionState 挂各 SessionFunctions，
  /// libs/server/Storage/Session/StorageSession.cs:104-132），快慢路径自动共享
  pub fn new(batch: BatchStoreSession<'a, D>) -> Self {
    Self {
      batch,
      session_metrics: None,
      latency_metrics: None,
      resp_version: DEFAULT_RESP_VERSION,
    }
  }

  /// 构造只读扫描会话
  ///
  /// 适用于慢路径只读扫描（SCAN/KEYS/DBSIZE、CLUSTER RESET 的
  /// HasKeysInSlots 判定）；写入路径绝不可用
  pub fn new_readonly(batch: BatchStoreSession<'a, D>) -> Self {
    Self::new(batch)
  }

  /// 绑定会话指标共享句柄（对标 C# StorageSession 构造传入 sessionMetrics 的
  /// 共享装配；None 保持采样关闭形态）
  pub fn with_session_metrics(mut self, metrics: Option<Arc<SessionMetricsHandle>>) -> Self {
    self.session_metrics = metrics;
    self
  }

  /// 绑定会话侧延迟表共享句柄（对标 C# StorageSession 构造传入 LatencyMetrics
  /// 的共享装配，与会话指标并列为一条注入轨；None 保持不计时形态、零开销）
  pub fn with_latency_metrics(mut self, latency: Option<Arc<GarnetLatencyMetricsSession>>) -> Self {
    self.latency_metrics = latency;
    self
  }

  /// 注入会话真实 RESP 协议版本（版本源单点流入：慢路径入口取
  /// `RespServerSession::resp_protocol_version` 经此写入，对象族慢路径各消费点
  /// 经 [`Self::resp_protocol_version`] 读到真值，与 basic 面帧型一致）。
  /// 非会话面（重放/事务视图/周期收集）不调此装配，保持构造缺省下限
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/StorageSession.cs:UpdateRespProtocolVersion
  pub fn with_resp_version(mut self, resp_version: u8) -> Self {
    self.resp_version = resp_version;
    self
  }

  /// 是否为一致读会话（对标 C# StorageSession.IsConsistentReadSession；
  /// 派生自底层 wkv 会话附着态）
  #[inline]
  pub fn is_consistent_read_session(&self) -> bool {
    self.batch.session.is_consistent_read_session()
  }

  /// 获取一致读会话上下文（当且仅当底层会话附着一致读状态机时返回 Some，
  /// 对标 C# StorageSession.consistentReadContext）
  ///
  /// libs/server/Storage/Session/StorageSession.cs:consistentReadContext
  #[inline]
  pub fn consistent_read_context(
    &self,
  ) -> Option<ConsistentReadContext<'_, D, dyn ConsistentReadFunctions>> {
    let fns = self.batch.session.read_session_state()?;
    Some(self.batch.consistent_read(fns.as_ref()))
  }

  /// 当前 RESP 协议版本
  #[inline]
  pub fn resp_protocol_version(&self) -> u8 {
    self.resp_version
  }

  /// 推进键的 WATCH 版本（存储会话侧收口，对标 C# Tsavorite functions 面的
  /// functionsState.watchVersionMap.IncrementVersion：MainStore
  /// UpsertMethods.PostInitialWriter / DeleteMethods.InitialDeleter /
  /// RMWMethods.PostInitialUpdater+InPlaceUpdater 在完成实际写入后调用）
  ///
  /// 挂点分工（一处定义，杜绝快慢两套口径）：值写/删除的用户键入口已在
  /// wkv 引擎层统一收口（raw/write.rs 的 `try_upsert_tag_sync_unprotected`/
  /// `upsert_tag`/`try_delete_sync_unprotected`），本层仅在 wkv 未覆盖的
  /// 面补推——TTL 异步面（`expire_at_ticks`/`persist_key`，wkv ttl.rs 内部
  /// 异步入口无用户键收口）与删除降级臂（collection 层 async delete）。
  /// 推进统一转发引擎钩子（`batch.bump_watch_version`），保证 lua/事务/
  /// RESP 快慢路径与装配侧 `set_watch_hook` 注入的是同一张版本表
  #[inline]
  pub(crate) fn bump_watch_version(&self, key: &[u8]) {
    self.batch.bump_watch_version(key);
  }

  /// 在会话 pending（异步闭环）统计与延迟双指标守卫下执行异步操作
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/Metrics.cs:StartPendingMetrics
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/Metrics.cs:StopPendingMetrics
  ///
  /// C# StartPendingMetrics = incr_total_pending + latencyMetrics?.Start(PENDING_LAT)、
  /// StopPendingMetrics = latencyMetrics?.Stop(PENDING_LAT)（Metrics.cs 分部类），
  /// C# 在每次 CompletePending 前后内联成对起停表；rust 的全部异步闭环都收敛到
  /// 本漏斗，故计数与延迟两段语义同处承接：计数直写共享句柄，延迟以
  /// [`now_stopwatch_ticks`] 在 `f().await` 前后各取一次刻度起停 PENDING_LAT
  /// （与会话侧 NET_RS_LAT 同一计时源，不引第二套时钟）。表/句柄不在位（采样
  /// 关闭、延迟监视关闭、非会话面）即零取时零分配，语义等同 C# 对应 `?.` 短路
  #[inline]
  pub(crate) async fn with_pending_metrics<F, Fut, T>(&self, f: F) -> T
  where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
  {
    if let Some(metrics) = &self.session_metrics {
      metrics.incr_total_pending(1);
    }
    let Some(latency) = &self.latency_metrics else {
      return f().await;
    };
    latency.start(LatencyMetricsType::PendingLat, now_stopwatch_ticks());
    let ret = f().await;
    latency.stop(LatencyMetricsType::PendingLat, now_stopwatch_ticks());
    ret
  }

  /// 读指定标签物理键值（零拷贝闭包版：快路径内存直读，磁盘候选异步闭环）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:ReadWithUnsafeContext
  ///
  /// [`Self::read_string_with`] 的带标签内核（一处定义）：对象信封域
  /// （KeyTag::ObjectEnvelope）与字符串域共用本实现；TTL 门控按用户键裁决
  pub async fn read_tag_with<R>(
    &self,
    key: &[u8],
    tag: wval::KeyTag,
    f: impl Fn(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt = if let Some(ctx) = self.consistent_read_context() {
      match ctx.try_read_tag_sync_unprotected(key, tag, &f)? {
        StoreResult::RecordOnDisk => {
          self
            .with_pending_metrics(|| ctx.read_tag_with(key, tag, &f))
            .await?
        }
        res => res.value(),
      }
    } else {
      match self.batch.try_read_tag_sync(key, tag, &f)? {
        StoreResult::RecordOnDisk => {
          self
            .with_pending_metrics(|| self.batch.read_tag_with(key, tag, &f))
            .await?
        }
        res => res.value(),
      }
    };
    self.record_read_outcome(opt.is_some());
    Ok(opt)
  }

  /// 读指定标签物理键值并披露记录物理尺寸（MEMORY USAGE 慢路径专用）
  ///
  /// 在 garnet 中的相对路径:libs/server/API/GarnetApiUnifiedCommands.cs:MEMORYUSAGE
  ///
  /// [`Self::read_tag_with`] 的带尺寸对位（尺寸口径见 [`wkv::RecordRead`]，
  /// 对标 C# `srcLogRecord.AllocatedSize`）；附着一致读会话时协议口同步触发
  ///（pre 超时上抛中止，对标 C# 一致读会话切换后 MEMORYUSAGE 经一致读上下文）
  pub async fn read_tag_with_size<R>(
    &self,
    key: &[u8],
    tag: wval::KeyTag,
    f: impl Fn(&[u8], usize) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt = if let Some(ctx) = self.consistent_read_context() {
      match ctx.try_read_tag_sync_with_size(key, tag, &f)? {
        StoreResult::RecordOnDisk => {
          self
            .with_pending_metrics(|| ctx.read_tag_with_size(key, tag, &f))
            .await?
        }
        res => res.value(),
      }
    } else {
      match self.batch.try_read_tag_sync_with_size(key, tag, &f)? {
        StoreResult::RecordOnDisk => {
          self
            .with_pending_metrics(|| self.batch.read_tag_with_size(key, tag, &f))
            .await?
        }
        res => res.value(),
      }
    };
    Ok(opt)
  }

  /// 读字符串键值（零拷贝闭包版：快路径内存直读，磁盘候选异步闭环）
  ///
  /// 对标 C# StringBasicContext.Read 内部 CompletePending：`Ok(None)`（磁盘候选）
  /// 时降级 wkv 异步 `read_with`，对调用方呈现同步闭环语义。
  pub async fn read_string_with<R>(
    &self,
    key: &[u8],
    f: impl Fn(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    self.read_tag_with(key, KeyTag::String, f).await
  }

  /// 读字符串键值（拷贝版）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GET
  pub async fn read_string(&self, key: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    self.read_string_with(key, |v| v.to_vec()).await
  }

  /// 带 TTL 裁决的用户数据双域异步读（磁盘候选在 [`Self::read_tag_with`] 内
  /// 惰性清除闭环，无 Deferred 态）
  ///
  /// 域次序与判型对齐同步单点
  /// [`crate::storage::session::common::read_adjudicated_user_sync`]：String 域
  /// 命中即用户数据；未命中探 ObjectEnvelope 域（对象键 →
  /// [`UserRead::WrongType`]）；再未命中探 Meta 域（升阶 / RI 键同判对象键口径，
  /// C# Reader 单记录统一 ValueIsObject）。字符串 / 键管理 / Bitmap 族慢路径
  /// 分派臂共用本面，不另起第二套异步双域判型
  pub async fn read_user_async<R>(
    &self,
    key: &[u8],
    f: impl Fn(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    if let Some(v) = self.read_tag_with(key, KeyTag::String, &f).await? {
      return Ok(UserReadAsync::Hit(v));
    }
    if self
      .read_tag_with(key, KeyTag::ObjectEnvelope, |_| ())
      .await?
      .is_some()
    {
      return Ok(UserReadAsync::WrongType);
    }
    Ok(
      match self
        .read_tag_with(key, KeyTag::Meta, meta_collection_type_of)
        .await?
      {
        Some(Some(_)) => UserReadAsync::WrongType,
        _ => UserReadAsync::Missing,
      },
    )
  }

  /// 三域存活异步裁决（返回存活键所在物理域；None = 键不存在或已过期）
  ///
  /// [`crate::storage::session::common::probe_alive_domain`] 的异步对偶：
  /// 同步探针的 `Deferred`（磁盘候选 / TTL 记录待裁决）在异步读内闭环——
  /// 过期键经 [`Self::read_tag_with`] 惰性清除后视同不存在。SET 条件写、
  /// RENAME / RESTORE / EXPIRE 族慢路径臂共用本面
  pub async fn probe_alive_domain_async(&self, key: &[u8]) -> wkv::Result<Option<KeyTag>> {
    if self
      .read_tag_with(key, KeyTag::String, |_| ())
      .await?
      .is_some()
    {
      return Ok(Some(KeyTag::String));
    }
    if self
      .read_tag_with(key, KeyTag::ObjectEnvelope, |_| ())
      .await?
      .is_some()
    {
      return Ok(Some(KeyTag::ObjectEnvelope));
    }
    Ok(
      self
        .read_tag_with(key, KeyTag::Meta, meta_collection_type_of)
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
  pub async fn ri_write_gate_async(&self, key: &[u8]) -> wkv::Result<bool> {
    Ok(
      self
        .read_tag_with(key, KeyTag::Meta, |raw| {
          meta_collection_type_of(raw) == Some(GarnetObjectType::RangeIndex)
        })
        .await?
        .unwrap_or(false),
    )
  }

  /// 探测键是否存在（对标 C# GarnetApiUnifiedCommands.cs:EXISTS）
  pub async fn exists(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    if let Ok(Some(alive)) = super::common::ttl_sync::probe_alive(&self.batch, key) {
      return Ok(if alive {
        GarnetStatus::Ok
      } else {
        GarnetStatus::NotFound
      });
    }
    if self
      .read_tag_with(key, KeyTag::String, |_| ())
      .await?
      .is_some()
      || self
        .read_tag_with(key, KeyTag::ObjectEnvelope, |_| ())
        .await?
        .is_some()
      || self
        .read_tag_with(key, KeyTag::Meta, |_| ())
        .await?
        .is_some()
    {
      Ok(GarnetStatus::Ok)
    } else {
      Ok(GarnetStatus::NotFound)
    }
  }

  /// 批量读字符串键值并流式写出 RESP 应答（Scatter-Gather 批量冷读对标）
  ///
  /// 附着一致读会话时走 [`ConsistentReadContext::read_batch_with`] 折叠重试口
  ///（pre_batch/post_batch 协议，读后校验不过整批重试，对标 C#
  /// ConsistentReadContext.ReadWithPrefetch）；否则直读底层批量口。
  /// 逐键命中/未命中经 [`Self::record_read_outcome`] 共享句柄入账（对位 C#
  /// 批量 GET 循环内空条件累加 `sessionMetrics?.incr_total_found/notfound`）。
  /// 整批异步闭环复用 [`Self::with_pending_metrics`] 单点漏斗起停 PENDING_LAT
  ///（对位 C# MainStore/AdvancedOps.cs 的 GET_CompletePending 两个重载在
  /// `CompletePendingWithOutputs` 前后成对起停表）：C# 批量收割是一次
  /// CompletePending 调用，rust 批量口同样单次 await，故样本按批一条、pending
  /// 计数按批一条，条目命中计数仍只由 record_read_outcome 单点入账不重复
  pub async fn read_string_batch_into(
    &self,
    keys: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wkv::Result<()> {
    let mut emit = |_idx: usize, val_opt: Option<&[u8]>| {
      self.record_read_outcome(val_opt.is_some());
      match val_opt {
        Some(v) => output.write_resp_bulk_string(v),
        None => output.write_resp_null_ver(self.resp_protocol_version()),
      }
    };
    match self.consistent_read_context() {
      Some(ctx) => {
        self
          .with_pending_metrics(|| ctx.read_batch_with(keys, &mut emit))
          .await
      }
      None => {
        self
          .with_pending_metrics(|| self.batch.read_batch_with(keys, &mut emit))
          .await
      }
    }
  }

  /// 写指定标签物理键值（String 域 SET 语义清 TTL；ObjectEnvelope 域 RMW 语义
  /// 保留 TTL，同步快路径优先，环形缓冲翻转异步闭环）
  ///
  /// [`Self::upsert_string`] 的带标签内核（一处定义）；String 域写入附带
  /// 对象信封覆写清退（语义见 wkv `try_upsert_tag_sync_unprotected`）。
  /// 对象信封域写成功后同栈触发信封整值写通知（对标 C# WriteLogUpsert：
  /// libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs）——本方法是
  /// 异步段对象写回的唯一漏斗（自定义对象命令慢路径 / RENAME 经此），
  /// 同步快路径增量条目由 resp 层 ObjectStoreRMW 端口单独承接，不重复入账
  pub async fn upsert_tag(&self, key: &[u8], tag: wval::KeyTag, val: &[u8]) -> wkv::Result<()> {
    match self.batch.try_upsert_tag_sync(key, tag, val)? {
      Ok(_) => {}
      // 降级 wkv 异步闭环（BatchStoreSession 官方封装，等价于退出批处理纪元后重写）
      Err(_) => {
        self
          .with_pending_metrics(|| self.batch.upsert_tag(key, tag, val))
          .await
          .map(|_| ())?;
      }
    }
    // WATCH 版本推进由 wkv 用户键写入口统一收口（同步成功臂 /
    // 异步闭环臂各恰好一次，C# PostInitialWriter 对位），本层不再重复推进
    // 信封整值写通知（物理键随栈帧内联编码，零堆分配）
    if tag == KeyTag::ObjectEnvelope {
      let raw_key = self.batch.session_tag_key(tag, key);
      self
        .batch
        .store
        .notify_envelope_upsert(raw_key.as_slice(), val)?;
    }
    Ok(())
  }

  /// 写入对象键（覆盖既有信封；记录挂 KeyTag::ObjectEnvelope 物理域）
  #[inline]
  pub async fn obj_save(
    &self,
    key: &[u8],
    tag: GarnetObjectType,
    payload: &[u8],
  ) -> wkv::Result<()> {
    let mut val = Vec::with_capacity(payload.len() + 1);
    obj_encode_into(tag, payload, &mut val);
    self.upsert_tag(key, KeyTag::ObjectEnvelope, &val).await
  }

  /// 写字符串键值（SET 语义：同步快路径优先，环形缓冲翻转 / TTL 清除异步闭环）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SET
  pub async fn upsert_string(&self, key: &[u8], val: &[u8]) -> wkv::Result<()> {
    self.upsert_tag(key, KeyTag::String, val).await
  }

  /// RMW 写回字符串键值（未过期键保留既有 key 级 TTL，已过期键清退残留 TTL
  /// 后重建无 TTL：同步快路径优先，环形页翻转 / TTL 记录磁盘候选降级
  /// wkv `upsert_rmw` 异步完整闭环；WATCH 推进由 wkv 用户键写入口收口）
  ///
  /// libs/server/Storage/Functions/MainStore/RMWMethods
  ///
  /// INCR/DECR 族、INCRBYFLOAT、APPEND、SETRANGE、SETBIT、BITFIELD 写子命令、
  /// PFADD/PFMERGE 的读改写回写面（lua/事务/AOF 重放共用），对标 C#
  /// UnifiedStore/VarLenInputMethods 的 HasExpiration 保留语义与
  /// UnifiedStore/RMWMethods.cs CopyUpdater 的 CheckExpiry → ExpireAndResume
  /// （过期转 InitialUpdater 重建，初始记录无 Expiration）
  pub async fn rmw_string(&self, key: &[u8], val: &[u8]) -> wkv::Result<()> {
    match self.batch.try_rmw_sync(key, val)? {
      Ok(_) => {}
      // 降级 wkv 异步闭环（等价于退出批处理纪元后重写；upsert_rmw 内含
      // 过期残留完整裁决，先 purge 后重建）
      Err(_) => {
        self
          .with_pending_metrics(|| self.batch.upsert_rmw(key, val))
          .await
          .map(|_| ())?;
      }
    }
    Ok(())
  }

  /// 删除键（同步快路径优先，磁盘异步闭环）
  pub async fn delete_string(&self, key: &[u8]) -> wkv::Result<bool> {
    match self.batch.try_delete_sync(key)? {
      // 快路径闭环：版本推进由 wkv 用户键删除入口统一收口（对齐 C#
      // InitialDeleter 无条件 IncrementVersion，缺席键墓碑同向计入）
      Ok(deleted) => Ok(deleted),
      // 降级异步闭环（复合对象元数据 / 环形页翻转 / 冷数据确认）：完整
      // 异步路由经 collection 层 delete（内部物理键原语，无用户键收口），
      // 故降级臂在本层推进一次（C# InitialDeleter 对位）
      Err(_) => {
        let deleted = self.with_pending_metrics(|| self.batch.delete(key)).await?;
        self.bump_watch_version(key);
        Ok(deleted)
      }
    }
  }

  /// 删除指定标签物理键（同步快路径优先，磁盘异步闭环）
  ///
  /// [`Self::delete_string`] 的带标签对位（ACL 旁路标签 AOF 回放等以非
  /// String 域承载的整值记录删除面）
  pub async fn delete_tag(&self, key: &[u8], tag: wval::KeyTag) -> wkv::Result<bool> {
    match self.batch.try_delete_tag_sync(key, tag)? {
      Ok(deleted) => Ok(deleted),
      // 降级异步闭环（环形页翻转 / 冷数据确认）：物理键原语，无用户键
      // 版本收口，故本层推进一次
      Err(_) => {
        let rec_k = self.batch.session_tag_key(tag, key);
        let deleted = self
          .with_pending_metrics(|| self.batch.delete_raw(&rec_k))
          .await?;
        self.bump_watch_version(key);
        Ok(deleted)
      }
    }
  }

  /// 设置键级绝对过期时间（.NET Ticks，对标 C# EXPIRE 族在 RESP 边界换算为
  /// ticks 后经 UnifiedInput 携带的同域语义；KeyAdminCommands.cs:421-427）
  pub async fn expire_at_ticks(&self, key: &[u8], expire_at_ticks: i64) -> wkv::Result<i32> {
    // >0 = TTL 已写（1）或过期即删（2）：键状态实际变化才推进版本（C# EXPIRE
    // 经 RMW InPlaceUpdater/PostInitialUpdater，未命中不 Incremment 同向）
    let applied = self
      .batch
      .expire_at(key, expire_at_ticks, TtlOpt::NONE)
      .await?;
    if applied > 0 {
      self.bump_watch_version(key);
    }
    Ok(applied)
  }

  /// 以相对时长设置过期（TimeSpan 口径的 ticks；内部换算绝对 ticks）
  pub async fn expire_in_ticks(&self, key: &[u8], ttl_ticks: i64) -> wkv::Result<i32> {
    let expire_at = now_ticks().saturating_add(ttl_ticks);
    self.expire_at_ticks(key, expire_at).await
  }

  /// 移除键级 TTL（对标 PERSIST）
  pub async fn persist_key(&self, key: &[u8]) -> wkv::Result<i32> {
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

  /// 命中/未命中计数收纳点（对位 C# incr_session_found/incr_session_notfound
  /// 的转发语义：直写会话共享句柄，不设第二套累加器）
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/Metrics.cs:incr_session_found
  #[inline]
  pub(crate) fn record_read_outcome(&self, found: bool) {
    if let Some(metrics) = &self.session_metrics {
      if found {
        metrics.incr_total_found(1);
      } else {
        metrics.incr_total_notfound(1);
      }
    }
  }
}

/// 构造绑定版本表的引擎级 WATCH 推进钩子（装配期经
/// `WedbStore::set_watch_hook` 一次性注入，写面收口见 wkv raw/write.rs）
///
/// 键哈希与 WATCH 登记（`TxnWatchedKeysContainer.add_watch`）同一哈希面，
/// 对标 C# functionsState.watchVersionMap 与事务容器共享单点版本表的装配关系
pub fn version_map_watch_hook(map: Arc<WatchVersionMap>) -> wkv::WatchHook {
  fn on_key_write(map: &WatchVersionMap, key: &[u8]) {
    map.increment_version(TxnKeyEntryComparison::key_hash(key) as u64);
  }

  WatchHook::new(map, on_key_write)
}

/// 构造向量集登记表缺席删除观测钩子（对标 C# MainStore RemoveKey 回调 →
/// VectorManager.RequestDeletion，GarnetRecordTriggers.OnDispose 的 Deleted 臂）
pub fn vector_registry_delete_hook(vm: Arc<VectorManager>) -> wkv::DeleteMissHook {
  fn on_delete_miss(vm: &VectorManager, prefix: &[u8], key: &[u8]) -> bool {
    vm.delete_vector_set(prefix, key)
  }

  DeleteMissHook::new(vm, on_delete_miss)
}
