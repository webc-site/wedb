//! 存储会话（对标 C# `sealed partial class StorageSession`，libs/server/Storage/Session/StorageSession.cs）
//!
//! C# 侧 StorageSession 按 MainStore/ObjectStore/UnifiedStore 拆为多个 partial 文件；
//! Rust 侧以单一结构体 + 跨文件 `impl` 块承担同等职责。底层统一走 wkv
//! `BatchStoreSession`（纪元守卫持有者），同步快路径未闭环时降级 wkv 异步路径，
//! 对标 C# BasicContext 内部 CompletePending 语义。

use std::{
  future::Future,
  io,
  result::Result as StdResult,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use wbase::time::{now_stopwatch_ticks, now_ticks};
use wcol::object_payload::obj_encode_into;
use wconf::DEFAULT_RESP_VERSION;
use wdev::Device;
use wkv::{
  BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, DeleteMissHook, RmwWindow,
  StoreResult, TtlOpt, WatchHook,
};
use wmetric::{PendingLatencyMeter, SessionMetricsHandle};
use wresp::ext::RespVecExt;
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};
use wval::{GarnetObjectType, KeyTag, SessionPrefixBuf};

use crate::{
  resp::vector::vector_manager::VectorManager,
  service::{SharedStore, StoreSwapSlot},
  storage::session::common::{
    UserReadAsync,
    ttl_sync::{
      del_ttl_sync, meta_collection_type_of, probe_alive_with_registry,
      probe_alive_with_registry_async,
    },
  },
  types::GarnetStatus,
};

/// 删除故障注入钩子（仅测试用，一处定义，对标 wbftree::SCAN_FAIL_INJECT）：
/// 置真后**下一次** [`StorageSession::delete_string`] 即返回 Err（一次性，消费
/// 即自动复位，生产路径恒假零开销）。供宿主验证迁移 SLOTS 删除环「删除 Err
/// 必须登记 untouchable 留痕收敛，严禁静默吞错致槽头重扫死循环」
#[cfg(debug_assertions)]
pub static DELETE_FAIL_INJECT: AtomicBool = AtomicBool::new(false);

/// TTL 清退故障注入钩子（仅测试用，一处定义，对标 wbftree::SCAN_FAIL_INJECT）：
/// 置真后**下一次** [`StorageSession::persist_key`] 即返回 Err（一次性，消费即
/// 自动复位，生产路径恒假零开销）。供宿主验证迁移帧导入「旧 TTL 清退失败必须
/// 判错拒绝，严禁静默吞错致残留旧 TTL 误作用于迁移值」
#[cfg(debug_assertions)]
pub static PERSIST_FAIL_INJECT: AtomicBool = AtomicBool::new(false);

/// 存储会话：wserver 执行存储操作的内部层
pub struct StorageSession<'a, D: Device> {
  /// 底层批处理会话（对标 C# stringBasicContext/objectBasicContext 共用的底层会话）
  pub batch: BatchStoreSession<'a, D>,
  /// 会话指标共享句柄（对标 libs/server/Storage/Session/Metrics.cs:sessionMetrics
  /// 类引用：C# 由 RespServerSession 将同一 GarnetSessionMetrics 实例传入存储会话
  /// 直写计数；采样关闭（C# trackStats false）时为 None，写口经空条件跳过）
  pub session_metrics: Option<Arc<SessionMetricsHandle>>,
  /// PENDING_LAT 计量槽（对标 C# StorageSession 构造传入的
  /// `readonly GarnetLatencyMetricsSession LatencyMetrics`
  ///（libs/server/Storage/Session/Metrics.cs:12）在 pending 计时臂上的投影，经
  /// [`Self::with_pending_latency`] 单点注入：pending 闭环的 PENDING_LAT
  /// 计时唯一落点。C# 该字段是能直写全部类别的会话延迟表本体，rust 的表为
  /// 连接任务独占（执行域只有 `&self`），故此处只保留执行域真正需要的
  /// PENDING_LAT 一槽。非会话面（AOF/复制重放、事务过程视图、周期收集）构造
  /// 不出该槽，None = 零取时零分配，与 C# 这些面 `latencyMetrics` 为
  /// null 同形）
  pub pending_latency: Option<Arc<PendingLatencyMeter>>,
  /// RESP 协议版本：会话真实协议版本经 [`Self::with_resp_version`] 单点注入
  /// （对标 C# `storageSession.UpdateRespProtocolVersion` 的双写落点，rust 每命令
  /// 重构造存储会话，故改由慢路径入口传入而非会话内可变态）。缺省取
  /// [`DEFAULT_RESP_VERSION`] 作非会话面（AOF/复制重放、事务过程视图、周期收集）
  /// 下限，与 C# `CreateFunctionsState(respProtocolVersion = DEFAULT_RESP_VERSION)` 同形
  pub resp_version: u8,
}

/// 标签读「同步命中即回值 / 磁盘候选降级 PENDING 异步闭环」骨架单点：`read_tag_quiet`、
/// `read_tag_with_size` 的 ctx/batch 两分支共用同一 match（纯文本展开，`&mut f` 复用 /
/// `&f` 借值与返回口径逐字不变；`$this` 承 `self`，因宏卫生下 `self` 关键字不可由宏体直引）
macro_rules! tag_read_sync_then_pending {
  ($this:expr, $sync:expr, $asyncf:expr) => {
    match $sync? {
      StoreResult::RecordOnDisk => $this.with_pending_metrics($asyncf).await?,
      res => res.value(),
    }
  };
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
      pending_latency: None,
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

  /// 绑定 PENDING_LAT 计量槽（对标 C# StorageSession 构造传入 LatencyMetrics
  /// 的共享装配，与会话指标并列为一条注入轨；None 保持不计时形态、零开销）
  pub fn with_pending_latency(mut self, meter: Option<Arc<PendingLatencyMeter>>) -> Self {
    self.pending_latency = meter;
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

  /// 推进键的 WATCH 版本（存储会话侧收口，对标 C# Tsavorite functions 面的
  /// functionsState.watchVersionMap.IncrementVersion：MainStore
  /// UpsertMethods.PostInitialWriter / DeleteMethods.InitialDeleter /
  /// RMWMethods.PostInitialUpdater+InPlaceUpdater 在完成实际写入后调用）
  ///
  /// 挂点分工（一处定义，杜绝快慢两套口径）：值写/删除的用户键入口已在
  /// wkv 引擎层统一收口（raw/write.rs 的 `try_upsert_tag_sync_unprotected`/
  /// `upsert_tag`/`try_delete_sync_unprotected` 与 collection 层 async
  /// `delete` 含降级臂），本层仅在 wkv 未覆盖的面补推——TTL 异步面
  /// （`expire_at_ticks`/`persist_key`，wkv ttl.rs 内部异步入口无用户键收口）
  /// 与标签删除面（`delete_tag`，底层物理键原语无用户键收口，match 汇合后
  /// 单点推进）。
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
  /// [`now_stopwatch_ticks`] 在 `f().await` 前后各取一次刻度、把差值记入
  /// PENDING_LAT（与会话侧 NET_RS_LAT 同一计时源，不引第二套时钟）。
  /// 差值就地计算而非共享起始时间戳：C# 的 `startTimestamp` 是会话表里的
  /// 可写字段，rust 会话表为连接任务独占、执行域只有 `&self`，共享时间戳
  /// 会把零锁写口降级成锁；槽/句柄不在位（采样关闭、延迟监视关闭、非会话
  /// 面）即零取时零分配，语义等同 C# 对应 `?.` 短路
  #[inline]
  pub(crate) async fn with_pending_metrics<F, Fut, T>(&self, f: F) -> T
  where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
  {
    if let Some(metrics) = &self.session_metrics {
      metrics.incr_total_pending(1);
    }
    let Some(meter) = &self.pending_latency else {
      return f().await;
    };
    let start = now_stopwatch_ticks();
    let ret = f().await;
    meter.record(now_stopwatch_ticks().saturating_sub(start) as i64);
    ret
  }

  /// 读指定标签物理键值（零拷贝闭包版：快路径内存直读，磁盘候选异步闭环）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:ReadWithUnsafeContext
  ///
  /// [`Self::read_string_with`] 的带标签内核（一处定义）：对象信封域
  /// （KeyTag::ObjectEnvelope）与字符串域共用本实现；TTL 门控按用户键裁决
  ///
  /// C# 统一存 GET（Read → IsPending 才 CompletePending → Found 入账 found/
  /// notfound）的 rust 单点：libs/server/Storage/Session/UnifiedStore/
  /// UnifiedStoreOps.cs:GET （pending 段收口 [`Self::with_pending_metrics`]，
  /// RENAME/EXISTS 内部读亦经本口）。入账在本入口薄包装单点完成（不入
  /// [`Self::read_tag_quiet`] 内核），多域组合探针经静默内核出口折叠计数
  pub async fn read_tag_with<R>(
    &self,
    key: &[u8],
    tag: wval::KeyTag,
    f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt = self.read_tag_quiet(key, tag, f).await?;
    self.record_read_outcome(opt.is_some());
    Ok(opt)
  }

  /// 单域标签读静默内核（[`Self::read_tag_with`] 的不入账对偶）：供多域
  /// 组合漏斗（[`Self::read_user_with_prefix`]）逐探承接——域探各自入账会使
  /// 缺失键计 3、对象键计 2，与 C# GET 恒单条口径失联（票
  /// zcode-r131c-dumprest 案二）；组合漏斗出口按 `UserReadAsync::record_outcome`
  /// 折叠恰一条。另供零入账漏斗（[`Self::read_user_quiet`]）的域后二级续探
  /// （OBJECT 慢臂信封/Meta 两探，票 zcode-r157c-objenc 案一：该漏斗出口本不
  /// 入账，续探若走簿记入口会使分层驻态键虚报 2 条）。单域簿记消费者一律走
  /// [`Self::read_tag_with`]，勿直取本内核（防静默丢计）
  pub(crate) async fn read_tag_quiet<R>(
    &self,
    key: &[u8],
    tag: wval::KeyTag,
    mut f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    Ok(if let Some(ctx) = self.consistent_read_context() {
      tag_read_sync_then_pending!(
        self,
        ctx.try_read_tag_sync_unprotected(key, tag, &mut f),
        || ctx.read_tag_with(key, tag, &mut f)
      )
    } else {
      tag_read_sync_then_pending!(self, self.batch.try_read_tag_sync(key, tag, &mut f), || {
        self.batch.read_tag_with(key, tag, &mut f)
      })
    })
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
      tag_read_sync_then_pending!(self, ctx.try_read_tag_sync_with_size(key, tag, &f), || ctx
        .read_tag_with_size(key, tag, &f))
    } else {
      tag_read_sync_then_pending!(
        self,
        self.batch.try_read_tag_sync_with_size(key, tag, &f),
        || self.batch.read_tag_with_size(key, tag, &f)
      )
    };
    Ok(opt)
  }

  /// 读指定标签物理键值的显式前缀变体（循环前缀外提对位，语义与
  /// [`Self::read_tag_with`] 逐臂一致；rust 工程优化无 c# 对应：批量键命令
  /// （EXISTS 族）在循环外单次外提 `session_prefix()` 交本口，消除逐域重读
  /// ns/db 原子变量与重算 Varint）
  ///
  /// 同步快路径走 wkv 前缀内核 `try_read_tag_sync_unprotected_with_prefix`
  /// （TTL 门裁决与数据读取复用同一外提前缀），附着一致读会话的 pre/post 回合
  /// 与触发哈希同取自入参前缀（wkv `with_session_consistent_read_with_prefix`，
  /// 与 [`Self::read_tag_with`] 的 ctx 分支同一协议单点）；磁盘候选降级臂整体交
  /// 既有 [`Self::read_tag_with`] 闭环（wkv 异步内核自带前缀解析，冷路径成本
  /// 与本口接入前一致），入账与 [`Self::read_tag_with`] 同在本入口薄包装单点
  pub async fn read_tag_with_prefix<R>(
    &self,
    prefix: &[u8],
    key: &[u8],
    tag: wval::KeyTag,
    f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt = self.read_tag_quiet_with_prefix(prefix, key, tag, f).await?;
    self.record_read_outcome(opt.is_some());
    Ok(opt)
  }

  /// 带前缀标签读静默内核（[`Self::read_tag_with_prefix`] 的不入账对偶，
  /// 供 [`Self::read_user_with_prefix`] 多域组合漏斗逐探承接，出入账纪律与
  /// [`Self::read_tag_quiet`] 一致）；磁盘候选降级臂整体交
  /// [`Self::read_tag_quiet`]（wkv 异步内核自带前缀解析，冷路径成本与本口
  /// 接入前一致，记账统一在簿记入口薄包装完成，内核不重复记账）
  async fn read_tag_quiet_with_prefix<R>(
    &self,
    prefix: &[u8],
    key: &[u8],
    tag: wval::KeyTag,
    mut f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt =
      match self
        .batch
        .with_session_consistent_read_with_prefix(prefix, key, tag, || {
          self
            .batch
            .try_read_tag_sync_unprotected_with_prefix(prefix, key, tag, &mut f)
        })?? {
        // 磁盘候选：整体降级静默异步读口（记账在簿记入口承接）
        StoreResult::RecordOnDisk => self.read_tag_quiet(key, tag, &mut f).await?,
        res => res.value(),
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
  /// [`Self::read_user_with_prefix`] 的无前缀薄包装：本口取一次
  /// `session_prefix()` 后即交带前缀内核，全仓仅此一条异步读用户键机制
  pub async fn read_user<R>(
    &self,
    key: &[u8],
    f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    let prefix = self.batch.session_prefix();
    self.read_user_with_prefix(prefix.as_slice(), key, f).await
  }

  /// 用户数据双域异步读的零入账对偶（RMW 前置读慢臂与元数据读族慢臂探针
  /// 专用）：三域判型与 [`Self::read_user`] 同一静默内核，漏斗尾不折叠命中/
  /// 未命中入账——对位快臂 `read_user_sync(…, None, …)`「句柄 None＝采样关闭
  /// 或 RMW 前置读不入账」纪律（`user_read.rs`）的慢臂出口：GETDEL 族慢臂
  /// 探针走本口（C# GETDEL 全链零入账，MainStoreOps.cs:GETDEL 无
  /// incr_session_*）、OBJECT 族慢臂走本口（C# Read_UnifiedStore
  /// AdvancedOps.cs:12-21 恒零计，票 zcode-r157c-objenc 案一）；
  /// GET/GETEX 等读命令慢臂一律走 [`Self::read_user`] 簿记入口，勿误取本口
  ///
  /// [`Self::read_user_quiet_with_prefix`] 的无前缀薄包装（镜像
  /// [`Self::read_user`] 薄包装先例形，不起第二套判型机制）
  pub async fn read_user_quiet<R>(
    &self,
    key: &[u8],
    f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    let prefix = self.batch.session_prefix();
    self
      .read_user_quiet_with_prefix(prefix.as_slice(), key, f)
      .await
  }

  /// 带 TTL 裁决的用户数据双域异步读（簿记入口）：静默三域内核
  /// [`Self::read_user_quiet_with_prefix`] 的入账薄包装，漏斗出口按
  /// [`UserReadAsync::record_outcome`] 折叠恰一条（与同步漏斗
  /// [`crate::storage::session::common::UserRead::record_outcome`] 同一
  /// [`crate::storage::session::common::fold_outcome`] 规则单点）；入账在
  /// 本入口薄包装单点完成（不入静默内核），镜像 [`Self::read_tag_with`]
  /// 形态，读命令慢路径分派臂共用本面
  pub async fn read_user_with_prefix<R>(
    &self,
    prefix: &[u8],
    key: &[u8],
    mut f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    let read = self
      .read_user_quiet_with_prefix(prefix, key, &mut f)
      .await?;
    read.record_outcome(self.session_metrics.as_deref());
    Ok(read)
  }

  /// 带 TTL 裁决的用户数据双域异步读的显式前缀静默内核（循环前缀外提对位，
  /// 判型与 [`Self::read_user_with_prefix`] 逐臂一致；rust 工程优化无 c#
  /// 对应：BITOP 等批量键命令在循环外单次外提 `session_prefix()` 交簿记入口，
  /// 消除逐域重读 ns/db 原子变量与重算 Varint）
  ///
  /// 域次序与判型对齐同步单点
  /// [`crate::storage::session::common::read_adjudicated_user_sync`]：String 域
  /// 命中即用户数据；未命中探 ObjectEnvelope 域（对象键 →
  /// [`UserReadAsync::WrongType`]）；再未命中探 Meta 域（升阶 / RI 键同判对象键口径，
  /// C# Reader 单记录统一 ValueIsObject）。三域探针均走
  /// [`Self::read_tag_quiet_with_prefix`] 静默内核（逐域入账会使缺失键计 3、
  /// 对象键计 2，与 C# GET 单条口径失联）；本内核自身零入账（供
  /// [`Self::read_user_quiet`] RMW 前置读臂直取），读命令簿记口径的折叠入账
  /// 由 [`Self::read_user_with_prefix`] 薄包装入口承接
  ///
  /// C# 对象存 ISessionFunctions 读回调（ValueIsObject 门 + CheckExpiry +
  /// 对象输出三段职责）的 rust 异步漏斗：
  /// libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:Reader
  /// ——对象域命中即产出信封载荷（消费闭包内反序列化），过期在
  /// [`Self::read_tag_quiet`] 惰性清除闭环（C# ReadAction.Expire 同判），
  /// 自定义对象命令分派留在命令层单点
  async fn read_user_quiet_with_prefix<R>(
    &self,
    prefix: &[u8],
    key: &[u8],
    mut f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    let read = if let Some(v) = self
      .read_tag_quiet_with_prefix(prefix, key, KeyTag::String, &mut f)
      .await?
    {
      UserReadAsync::Hit(v)
    } else if self
      .read_tag_quiet_with_prefix(prefix, key, KeyTag::ObjectEnvelope, |_| ())
      .await?
      .is_some()
    {
      UserReadAsync::WrongType
    } else {
      match self
        .read_tag_quiet_with_prefix(prefix, key, KeyTag::Meta, meta_collection_type_of)
        .await?
      {
        Some(Some(_)) => UserReadAsync::WrongType,
        _ => UserReadAsync::Missing,
      }
    };
    Ok(read)
  }

  /// 三域存活异步裁决（返回存活键所在物理域；None = 键不存在或已过期）
  ///
  /// [`crate::storage::session::common::probe_alive_domain`] 的异步对偶：
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

  /// 批量读字符串键值并流式写出 RESP 应答（Scatter-Gather 批量冷读对标）
  ///
  /// 本函数是 C# MGET 异步批处理管线三件套在 rust 的合并承接：
  /// - libs/server/Resp/MGetReadArgBatch.cs:SetStatus（逐键 pending 态登记 +
  ///   ArrayPool 状态数组租用）：rust 无 Tsavorite pending IO 中间态，读经
  ///   单次 await 内联闭环，逐键状态数组不存在的，pending 记账按批一条经
  ///   [`Self::with_pending_metrics`] 承接；
  /// - libs/server/Resp/MGetReadArgBatch.cs:CompletePending（GET_CompletePending
  ///   强制收割 + 顺序补写应答）：rust 批量口单次 await 即收割完成，emit 闭包
  ///   顺序写等价承接；
  /// - libs/server/Resp/BasicCommands.cs:SetResult（输出数组惰性分配与倍增
  ///   累积）：rust 直接流式写 output，无中间累积数组。
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
  ///
  /// C# AdvancedOps 批量预读转发门（batch 一次性交 context 预取读）的对位：
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:ReadWithPrefetch
  ///
  /// # 输出缓冲次序契约
  /// 流式直写语义下 `Err` 中止时 output 内残留首个磁盘候选之前已交付的部分帧
  ///（透传底层 wkv `session/raw/batch.rs:read_batch_raw_with` 的整体丢弃契约），
  /// 调用方必须回滚或清空本批 output 后再成帧，严禁在其后追加错误帧
  ///（MGET 会成 `*N` + 部分元素 + 错误帧的畸形数组，SG GET 会缺帧错位）
  pub async fn read_string_batch_into(
    &self,
    keys: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wkv::Result<()> {
    let mut emit = |_idx: usize, val_opt: Option<&[u8]>| {
      self.record_read_outcome(val_opt.is_some());
      match val_opt {
        Some(v) => output.write_resp_bulk_string(v),
        None => output.write_resp_null_ver(self.resp_version),
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
  ///
  /// C# 统一存 SET 双重载（upsert 源记录 / RENAME 键覆写重投）的 rust 单轨
  /// 落点：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:SET
  /// （键覆写重载由 RENAME 慢路径经本口带新键重投承接）
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
      self.batch.notify_envelope_upsert(raw_key.as_slice(), val)?;
    }
    Ok(())
  }

  /// 写入对象键（覆盖既有信封；记录挂 KeyTag::ObjectEnvelope 物理域）
  ///
  /// C# 对象记录创建/更新双臂（对象存 InitialUpdater 建新记录、Simple 对象
  /// 会话函数 InitialUpdater/CopyUpdater/InPlaceUpdater 三写臂）在 rust 信封
  /// 单轨下的折叠落点——内存对象变更后整值写回一处承接：
  /// libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InitialUpdater
  /// libs/server/Storage/Functions/SimpleGarnetObjectSessionFunctions.cs:InitialUpdater
  /// libs/server/Storage/Functions/SimpleGarnetObjectSessionFunctions.cs:CopyUpdater
  /// libs/server/Storage/Functions/SimpleGarnetObjectSessionFunctions.cs:InPlaceUpdater
  ///
  /// 同步快路径走 wkv 单次成形直写（`try_upsert_envelope_sync_fill`）：信封
  /// `[1B 标签][payload]` 在记录槽位一次落笔、物理键单次编码，AOF 镜像与记录
  /// 共享同一已编码字节（写成功地址零拷贝借值，对齐 C# WriteLogUpsert 从
  /// srcLogRecord 取值入账）——全程无中间整值 `Vec`；环形页翻转降级臂才物化
  /// 整值走既有异步 `upsert_tag` 闭环（含 WATCH 推进与镜像收口，慢路径罕见）
  #[inline]
  pub async fn obj_save(
    &self,
    key: &[u8],
    tag: GarnetObjectType,
    payload: &[u8],
  ) -> wkv::Result<()> {
    let rec_k = self.batch.session_tag_key(KeyTag::ObjectEnvelope, key);
    match self
      .batch
      .try_upsert_envelope_sync_fill_with_prefix(key, &rec_k, tag as u8, payload)?
    {
      StdResult::Ok(addr) => {
        // 信封整值写通知：镜像值与记录共享同一字节；极端驻留缺口防御物化补投
        let mirrored = self.batch.with_record_value(addr, |val| {
          self.batch.notify_envelope_upsert(rec_k.as_slice(), val)
        });
        match mirrored {
          Some(r) => r?,
          None => {
            let mut val = Vec::with_capacity(payload.len() + 1);
            obj_encode_into(tag, payload, &mut val);
            self.batch.notify_envelope_upsert(rec_k.as_slice(), &val)?;
          }
        }
        Ok(())
      }
      // 环形页翻转：物化整值走既有异步闭环（upsert_tag 含 WATCH 推进与镜像收口）
      StdResult::Err(_) => {
        let mut val = Vec::with_capacity(payload.len() + 1);
        obj_encode_into(tag, payload, &mut val);
        self.upsert_tag(key, KeyTag::ObjectEnvelope, &val).await?;
        Ok(())
      }
    }
  }

  /// 写对象键并随写清退键级 TTL（STORE 族 SET 语义收尾单点，一处定义）
  ///
  /// SET 语义清既有 key 级 TTL：对标 C# STORE 族「Delete dst → ZADD」收尾——
  /// dest 键排他锁跨 GET → Delete → ZADD 全程、错误臂先于 Delete 返回
  ///（libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:129-133；
  /// ZUNIONSTORE 族 SetOps 同形），信封 upsert 走 RMW 语义保留 TTL，清退须
  /// 显式随写落笔。前置条件：调用方持目标键 rmw 窗（窗与 TTL 闩同址，见
  /// [`del_ttl_sync`] 契约）——清退与信封写同临界区后，对面 EXPIRE 的任何
  /// 落地时刻只剩窗前（随后被本清退抹除）与窗后（合法后序）两态，「清退与
  /// 写回之间落 TTL 借信封 upsert 保活」的交错在机制上不存在（窗外裸清
  /// `persist_key` 的次生缺陷，票 wnode-store-cold-window-ttl-clear-outsides-
  /// critical-section；错误臂经复验前置天然零清退）。清退经 [`del_ttl_sync`]
  /// 无闩变体（WATCH 推进单点内聚），页翻转降级 wkv 异步 `del_ttl`，AOF
  /// Persist 镜像由 TTL 物理键写监听 TtlWrite(None) 单点承接
  pub async fn obj_save_clear_ttl(
    &self,
    key: &[u8],
    tag: GarnetObjectType,
    payload: &[u8],
  ) -> wkv::Result<()> {
    self.obj_save(key, tag, payload).await?;
    self.clear_ttl(key).await
  }

  /// 清退键级 TTL 旁路记录（STORE 族 SET 语义的「随写清退」段抽核，一处定义）
  ///
  /// [`obj_save_clear_ttl`] 的信封写回臂与 STORE 冷漏斗窗内升阶成功臂
  /// （[`dest_cold_promote_arm`](crate::resp::objects::rmw_helpers) 换入
  /// bftree+Meta 后经本口清退——升阶迁移臂本身键存活不动 TTL 旁路，SET 语义
  /// 须显式落笔）共用同一清退判定。前置条件同 [`obj_save_clear_ttl`]：调用方
  /// 持本键 rmw 窗（窗与 TTL 闩同址，[`del_ttl_sync`] 契约）；清退经
  /// [`del_ttl_sync`] 无闩变体（WATCH 推进单点内聚），页翻转降级 wkv 异步
  /// `del_ttl`，AOF Persist 镜像由 TTL 物理键写监听 TtlWrite(None) 单点承接
  pub async fn clear_ttl(&self, key: &[u8]) -> wkv::Result<()> {
    if !del_ttl_sync(&self.batch, key)? {
      self.batch.del_ttl(key).await?;
    }
    Ok(())
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
  ///
  /// 写回目标键由 [`RmwWindow`] 承载：调用方须在装载旧值之前取窗（同步域
  /// `BatchStoreSession::try_rmw_window`、异步域 `rmw_window`），本入口只在窗口
  /// 内落笔，故「无锁读旧值 → 盲写绝对值」的两步式在类型面上不可表达
  pub async fn rmw_string<'k, 'w>(
    &self,
    window: &RmwWindow<'w, 'k, D>,
    val: &[u8],
  ) -> wkv::Result<()> {
    match window.try_rmw_sync(val)? {
      Ok(_) => {}
      // 降级 wkv 异步闭环（等价于退出批处理纪元后重写；upsert_rmw 内含
      // 过期残留完整裁决，先 purge 后重建）
      Err(_) => {
        self
          .with_pending_metrics(|| window.upsert_rmw(val))
          .await
          .map(|_| ())?;
      }
    }
    Ok(())
  }

  /// 删除键（同步快路径优先，磁盘异步闭环）
  ///
  /// C# 统一存 DELETE（unifiedContext.Delete → Found 判 OK/NOTFOUND）的
  /// rust 单点：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:DELETE
  pub async fn delete_string(&self, key: &[u8]) -> wkv::Result<bool> {
    // 故障注入门（测试钩子，一次性）：模拟底层设备写故障，与真实
    // wdev::Error::OutOfBounds 同型上抛，杜绝测试绕过本统一删除入口
    #[cfg(debug_assertions)]
    if DELETE_FAIL_INJECT.swap(false, Ordering::AcqRel) {
      return Err(wkv::Error::Io(io::Error::other("删除故障注入（测试钩子）")));
    }
    match self.batch.try_delete_sync(key)? {
      // 快路径闭环：版本推进由 wkv 用户键删除入口统一收口（对齐 C#
      // InitialDeleter 无条件 IncrementVersion，缺席键墓碑同向计入）
      Ok(deleted) => Ok(deleted),
      // 降级异步闭环（复合对象元数据 / 环形页翻转 / 冷数据确认）：WATCH
      // 版本推进已由 wkv collection 层 delete 无条件收口，本层零重复推进
      Err(_) => self.with_pending_metrics(|| self.batch.delete(key)).await,
    }
  }

  /// 取删字符串域键并回传被摘值（GETDEL 读删一体：应答值 = 实际摘除记录的值）
  ///
  /// [`Self::delete_string`] 的取值对位：快路径闭环答摘除值（捕获与摘除同一
  /// 临界区）；降级异步闭环由 wkv `take_string` 同级联收口（WATCH 版本推进
  /// 单点不重复）
  pub async fn take_string(&self, key: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    match self.batch.try_take_sync(key)? {
      Ok(taken) => Ok(taken),
      Err(_) => {
        self
          .with_pending_metrics(|| self.batch.take_string(key))
          .await
      }
    }
  }

  /// 删除指定标签物理键（同步快路径优先，磁盘异步闭环）
  ///
  /// [`Self::delete_string`] 的带标签对位（ACL 旁路标签 AOF 回放等以非
  /// String 域承载的整值记录删除面）
  ///
  /// WATCH 版本推进收口：底层 [`BatchStoreSession::try_delete_tag_sync`]
  /// 与降级臂 `delete_raw` 均为物理键原语（无用户键版本收口，与
  /// `try_delete_sync` 在 wkv 用户键入口收口不同），故统一在本层 match
  /// 汇合后单点推进 `bump_watch_version(key)`——含同步快路径未命中的
  /// `Ok(false)` 缺席观测，对标 C# MainStore
  /// DeleteMethods.InitialDeleter 无条件 IncrementVersion（缺席键墓碑
  /// 追加同向计入，MainStore DeleteMethods.cs:16）；降级臂物理删除失败
  /// （Err 上抛）未落任何写入不推进
  pub async fn delete_tag(&self, key: &[u8], tag: wval::KeyTag) -> wkv::Result<bool> {
    let deleted = match self.batch.try_delete_tag_sync(key, tag)? {
      // 快路径闭环：内存命中或明确未命中（Ok(false)），版本推进由下方单点承接
      Ok(deleted) => deleted,
      // 降级异步闭环（环形页翻转 / 冷数据确认）：物理键原语，无用户键
      // 版本收口，故本层推进一次
      Err(_) => {
        let rec_k = self.batch.session_tag_key(tag, key);
        self
          .with_pending_metrics(|| self.batch.delete_raw(&rec_k))
          .await?
      }
    };
    self.bump_watch_version(key);
    Ok(deleted)
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

  /// 以相对时长设置过期（TimeSpan 口径的 ticks；内部换算绝对 ticks）
  pub async fn expire_in_ticks(&self, key: &[u8], ttl_ticks: i64) -> wkv::Result<i32> {
    let expire_at = now_ticks().saturating_add(ttl_ticks);
    self.expire_at_ticks(key, expire_at).await
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
/// 版本轨=逻辑域：入参前缀由 wkv `StoreSession::bump_watch_version` 单点
/// 供给（`session_logical_prefix` 逻辑投影，不含换号虚拟代际），与 WATCH
/// 登记（`TxnWatchedKeysContainer::add_watch` 取 `watch_prefix`）同走
/// [`TxnKeyEntryComparison::scoped_key_hash`] 单点构槽：跨租户/跨库同名键
/// 落位正交保持，FLUSHDB/FLUSHNS/SWAPDB 换号前后同逻辑键写必落同槽必
/// abort，对位 C# 每库独持 watchVersionMap 与事务容器共享该库单表的装配
/// 关系（libs/server/GarnetDatabase.cs:156）。锁轨（事务锁桶）另取物理前缀，
/// 禁经本钩子（双轨分置两单点、禁共口互染）
pub fn version_map_watch_hook(map: Arc<WatchVersionMap>) -> wkv::WatchHook {
  fn on_key_write(map: &WatchVersionMap, prefix: &[u8], key: &[u8]) {
    map.increment_version(TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64);
  }

  WatchHook::new(map, on_key_write)
}

/// 向量登记表写面版本轨推进上下文（装配期初引擎 + 在线置换槽 + 版本表）
struct VectorVersionBump {
  store: SharedStore<wdev::SegmentedDevice>,
  store_swap: StoreSwapSlot,
  map: Arc<WatchVersionMap>,
}

/// 构造向量登记表写面的 WATCH 版本推进钩子（装配期经
/// `VectorManager::set_watch_bump` 一次性注入）
///
/// 向量写漏斗（try_add/try_remove/try_set_attribute/rename 新旧键与 AOF
/// 重放臂）以**物理**会话前缀寻址登记表（`registry_key` 复合键口径，随
/// 换号域迁移属既定架构），而版本轨=逻辑域：本装配钩子在构槽单点前将
/// 入参物理域经 `VirtualDbManager::version_domain_of` 换算为逻辑域再交
/// [`TxnKeyEntryComparison::scoped_key_hash`]，与主存储写面
/// [`version_map_watch_hook`] 落同版本轨槽（换号后向量改写对在途 WATCH
/// 同样必 abort，对位 C# 向量写经本库 VersionMap 单表推进）。当前引擎经
/// 置换槽现取、回落装配期初值（与 [`crate::service::build_txn_lock_table`]
/// 锁源同形态——向量钩子 OnceLock 一次注入跨置换存活，禁钉死旧引擎映射）
pub fn vector_version_watch_hook(
  store: SharedStore<wdev::SegmentedDevice>,
  store_swap: StoreSwapSlot,
  map: Arc<WatchVersionMap>,
) -> wkv::WatchHook {
  fn on_vector_write(ctx: &VectorVersionBump, prefix: &[u8], key: &[u8]) {
    // 入参前缀理论上恒为登记表寻址的 [NsVarint][DbVarint] 物理域；解码
    // 异常回根域 (0,0)——与未绑定会话口径一致，至多数值重合碰撞面、只多
    // abort 不少 abort，属安全侧
    let (pns, pdb) = SessionPrefixBuf::from_slice(prefix)
      .and_then(|buf| buf.decode())
      .unwrap_or((0, 0));
    let store = ctx
      .store_swap
      .get()
      .unwrap_or_else(|| Arc::clone(&ctx.store));
    let (lns, ldb) = store.vdb.version_domain_of(pns, pdb);
    ctx
      .map
      .increment_version(TxnKeyEntryComparison::scoped_key_hash(
        SessionPrefixBuf::new(lns, ldb).as_slice(),
        key,
      ) as u64);
  }

  WatchHook::new(
    Arc::new(VectorVersionBump {
      store,
      store_swap,
      map,
    }),
    on_vector_write,
  )
}

/// 构造向量集登记表缺席删除观测钩子（对标 C# MainStore RemoveKey 回调 →
/// VectorManager.RequestDeletion，GarnetRecordTriggers.OnDispose 的 Deleted 臂。
/// 观测臂为真异步：条带独占锁 + 登记写透 `.await` 闭环，无内联收割——
/// 同步快删路径遇钩子在场降级完整异步路由后归此收口，见 wkv DeleteMissHook）
pub fn vector_registry_delete_hook(vm: Arc<VectorManager>) -> wkv::DeleteMissHook {
  DeleteMissHook::new(move |prefix, key| {
    let vm = Arc::clone(&vm);
    Box::pin(async move { vm.delete_vector_set(prefix, key).await })
  })
}
