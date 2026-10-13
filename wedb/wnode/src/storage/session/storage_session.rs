//! 存储会话（对标 C# `sealed partial class StorageSession`，libs/server/Storage/Session/StorageSession.cs）
//!
//! C# 侧 StorageSession 按 MainStore/ObjectStore/UnifiedStore 拆为多个 partial 文件；
//! Rust 侧以单一结构体 + 跨文件 `impl` 块承担同等职责。底层统一走 wkv
//! `BatchStoreSession`（纪元守卫持有者），同步快路径未闭环时降级 wkv 异步路径，
//! 对标 C# BasicContext 内部 CompletePending 语义。
//!
//! 域分文件承接（对应 C# partial 分文件）：标签物理域读写
//! [`super::mainstore::tag_ops`]、主存字符串读写 [`super::mainstore::main_store_ops`]、
//! 向量写钩 [`super::mainstore::vector_store_ops`]、TTL/键状态裁决
//! [`super::common::ttl_ops`]、用户键读 [`super::common::user_read_ops`]。

// 故障注入钩子（AtomicBool）仅 debug 装配，release 剔除防 unused imports
#[cfg(debug_assertions)]
use std::sync::atomic::AtomicBool;
use std::{future::Future, sync::Arc};

use wbase::time::now_stopwatch_ticks;
use wconf::DEFAULT_RESP_VERSION;
use wdev::Device;
use wkv::{BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, WatchHook};
use wmetric::{PendingLatencyMeter, SessionMetricsHandle};
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};

// 向量钩子族归域 mainstore::vector_store_ops；装配侧 use 路径经此 re-export
// 保持 `storage_session::vector_*` 稳定（路径锚，非对外转发）
pub use super::mainstore::vector_store_ops::{
  vector_registry_delete_hook, vector_version_watch_hook,
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

/// 信封写页翻转故障注入钩子（仅测试用，一处定义，对标 DELETE_FAIL_INJECT）：
/// 置真后**下一次**信封同步快写（[`StorageSession::obj_save`] 异步档 /
/// `object_store_utils::obj_save_sync` / `obj_save_custom_notified` 同步档三
/// 收口点任一先到先消费）即直接判页翻转降级，零真实写入回降级信号（一次性，
/// 消费即自动复位，生产路径恒假零开销）。供宿主验证写回面「同步降级绝不转
/// 异步盲写」fail-closed 契约（票
/// wnode-collect-fallback-blind-write-after-recheck：降级闭环跨 await 无再裁决）
#[cfg(debug_assertions)]
pub static OBJ_SAVE_PAGESWAP_INJECT: AtomicBool = AtomicBool::new(false);

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
  /// 经 `resp_protocol_version` 读到真值，与 basic 面帧型一致）。
  /// 非会话面（重放/事务视图/周期收集）不调此装配，保持构造缺省下限
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/StorageSession.cs:UpdateRespProtocolVersion
  pub fn with_resp_version(mut self, resp_version: u8) -> Self {
    self.resp_version = resp_version;
    self
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
