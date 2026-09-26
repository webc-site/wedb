use std::{
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use wbftree::{RANGE_INDEX_STUB_SIZE, StorageBackendType, TreeTuning};
use wdev::Device;
use wval::GarnetObjectType;

use super::{AofListenerPauseGuard, WedbStore};
use crate::error::Result;

/// 对象 RMW 增量日志通知载荷
#[derive(Debug, Clone, Copy)]
pub struct ObjectRmwNotification<'a> {
  pub key: &'a [u8],
  pub obj_type: GarnetObjectType,
  pub op_code: u8,
  /// 事件时间戳：真 .NET Ticks（i64，100ns 单位，0001-01-01 纪元的
  /// DateTimeOffset.UtcNow.UtcTicks），非 Unix 毫秒——与 Garnet 对象 RMW
  /// 输入的时间戳同域，Unix 秒/毫秒 ↔ ticks 换算见 wbase::convert
  pub timestamp_ticks: i64,
  pub arg1: i32,
  pub arg2: i32,
  pub args: &'a [&'a [u8]],
}

/// 分层稳态写臂命令镜像通知载荷（[`StoreEvent::TieredCollectionWrite`] 的
/// 构造面，与 [`ObjectRmwNotification`] 同形对称）
///
/// 镜像的是「RESP 命令语义」（判别类型 + 族内操作码 + arg1/arg2 压缩字 +
/// 原始参数）而非最终值：副本/恢复端按主端同一判定单点路由分层臂逐条重放
/// 天然收敛（INCRBY 族 delta 与主端同序幂等一致），时间戳刻意不携——
/// 重放不消费时间，确定性由命令语义与同序保证
#[derive(Debug, Clone, Copy)]
pub struct TieredCollectionNotification<'a> {
  pub ns: u64,
  pub db: u64,
  pub key: &'a [u8],
  pub obj_type: GarnetObjectType,
  pub op_code: u8,
  pub arg1: i32,
  pub arg2: i32,
  pub args: &'a [&'a [u8]],
}

/// 存储事件枚举：统一收敛全部写、索引、对象、TTL、ETag 等事件
#[derive(Debug)]
pub enum StoreEvent<'a> {
  Write {
    key: &'a [u8],
    val: &'a [u8],
    tombstone: bool,
  },
  TtlWrite {
    ns: u64,
    db: u64,
    key: &'a [u8],
    expire_at: Option<i64>,
  },
  EtagWrite {
    ns: u64,
    db: u64,
    key: &'a [u8],
    etag: Option<i64>,
  },
  ObjectRmw(&'a ObjectRmwNotification<'a>),
  /// 分层稳态写臂命令镜像：树内原生写（HSET/HMSET/HSETNX/HINCRBY/HINCRBYFLOAT、
  /// SADD、ZADD/ZINCRBY、LPUSH/RPUSH/LPUSHX/RPUSHX）实际生效后的确定性命令
  /// 记录——升阶流只镜像升阶时点内容，本事件承接其后的稳态写面，杜绝副本树
  /// 永久缺字段与 meta.size 滞后。入账 `AofEntryType::ObjectStoreRMW` 条目、
  /// 物理键取 Meta 域物化键（与升阶流同域）；重放端
  /// `object_store_rmw` 先经 `load_collection_stub` 探分层态路由分层臂，
  /// 绝不落信封通道（信封空对象重建面）与 RangeIndexWrite（RI 判型门拒分层键）
  TieredCollectionWrite(&'a TieredCollectionNotification<'a>),
  EnvelopeUpsert {
    key: &'a [u8],
    val: &'a [u8],
  },
  /// RI 字段级写/删事件：携条目**物理域** `(vns, vdb)`（会话
  /// [`crate::session::StoreSession::virtual_domain`] 单点，与本键记录前缀
  /// 同源），入账物理键据此刚性前缀定位，杜绝回放/恢复端伪造 (0,0) 致索引落错库、
  /// 跨租界穿透（SKILL 单日志多库隔离）；回放端按该前缀直设虚拟域落回条目原域
  RangeIndexWrite {
    ns: u64,
    db: u64,
    key: &'a [u8],
    field: &'a [u8],
    val: &'a [u8],
    delete: bool,
  },
  RangeIndexCreate {
    ns: u64,
    db: u64,
    key: &'a [u8],
    backend: &'a StorageBackendType,
    tuning: TreeTuning,
  },
  RangeIndexDrop {
    ns: u64,
    db: u64,
    key: &'a [u8],
  },
  /// 集合就地升阶 / 分层重灌 / RENAME 的树数据通道事件：先把整棵树 CPR 快照至
  /// 迁移临时文件，携物化键真值（ns/db）、原集合判别类型（obj_type，副本据此
  /// 重建 MetaValue 而非硬编码 RangeIndex）、发布存根与成员 TTL 水位
  ///（next_expiry，副本据此重建元记录水位，杜绝 i64::MAX 假水位令副本到期成员
  /// 永不出账），交由复制面经既有 RangeIndexStreamChunk 通道分块灌入 AOF
  ///（复用 RI 迁移流通道，杜绝两套通道），副本重组末块后按 obj_type 发布为
  /// 树态元记录。
  /// `replace` 标记发布形态：首升阶 false（副本已存在同名索引属发散，拒发布）；
  /// 分层重灌与 RENAME（覆写目标键形态）true（副本必须以换入形态重放，否则
  /// 旧树在位的回放会被 IndexExists 拦截）。
  RangeIndexStream {
    ns: u64,
    db: u64,
    key: &'a [u8],
    obj_type: GarnetObjectType,
    stub: [u8; RANGE_INDEX_STUB_SIZE],
    file_path: &'a Path,
    replace: bool,
    /// 成员 TTL 水位（.NET Ticks；`i64::MAX` = 无成员挂 TTL）：升阶/重灌臂传
    /// 灌入批最早成员到期刻度，RENAME 臂透传旧元记录水位
    next_expiry: i64,
  },
  TtlPurge {
    ns: u64,
    db: u64,
    key: &'a [u8],
    expire_at: i64,
  },
}

/// 存储事件分发委托（事件体携借用切片，故以高阶形态 `for<'e>` 承接逐次调用的
/// 独立生命周期）
///
/// 对位 C# Tsavorite core 的单委托字段（Allocator/WorkQueueLIFO.cs:17
/// `readonly Action<T> work`、Allocator/AllocatorBase.cs:261
/// `Action<long, long> EvictCallback`）：`Arc<dyn Fn>` 即该委托形态的 rust 直译，
/// 与 [`crate::session::WatchHook`]、[`crate::session::DeleteMissHook`] 同一擦除
/// 形态，Send/Sync/Drop 皆由 std 保证，不自立虚表。
///
/// `aof_session_id` 为产生该写记录的存储会话 id（对标 C#
/// `upsertInfo.SessionID`/`rmwInfo.SessionID`/`deleteInfo.SessionID`，经
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:782-790 直传
/// `Log.Enqueue(..., sessionId, ...)`）：数据条目入 AOF 帧头携此值，与事务
/// 标记面（`TransactionManager.cs:516` `Session.ID`）同一取值源，重放归组口
/// （`AofReplayCoordinator.cs:142` `activeTxns.TryGetValue(header.sessionID)`）
/// 据此对组内数据与 TxnStart/TxnCommit 同键命中。非事务/后台会话取 0（无活动
/// 组，即到即放，与 C# 会话内联写非事务命令同语义）。
type EmitFn = Arc<dyn for<'e> Fn(i64, i32, StoreEvent<'e>) -> Result<()> + Send + Sync>;

/// 统一存储事件分发器（宿主上下文类型由 std 闭包对象擦除；sink 一经注入即被
/// `OnceLock` 独占持有、全程 `&self` 借用消费，故不携 Clone 语义）
///
/// 分发携版本参数：AOF 条目的 store_version 由写侧单点传入（hlog 追加域与
/// 记录头纪元位同源自分配成功点一次合字读，读点下移后经 append 返回值/
/// 池取复活臂传导；其余域取分发时点版本域），处理函数
/// 不再做第二点版本读。
pub struct StoreEventSink(EmitFn);

impl StoreEventSink {
  /// 由任意线程安全上下文及静态处理函数构造分发器（handler 返回
  /// `Result`：AOF 入队失败沿写路径上抛拒绝该命令，杜绝静默缺条目）
  pub fn new<T: Send + Sync + 'static>(
    ctx: Arc<T>,
    handler: fn(&T, i64, i32, StoreEvent<'_>) -> Result<()>,
  ) -> Self {
    Self(Arc::new(
      move |ver: i64, aof_session_id: i32, event: StoreEvent<'_>| {
        handler(&ctx, ver, aof_session_id, event)
      },
    ))
  }

  /// 统一分发存储事件（失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn emit(&self, ver: i64, aof_session_id: i32, event: StoreEvent<'_>) -> Result<()> {
    (self.0)(ver, aof_session_id, event)
  }
}

impl<D: Device> WedbStore<D> {
  /// 注入存储事件处理器（须在创建任何会话前调用；重复注入返回 false）
  pub fn set_event_sink(&self, sink: StoreEventSink) -> bool {
    self.event_sink.set(sink).is_ok()
  }

  /// 写入口窗口合字快照：单次原子读同取「本写是否携带纪元位」与「AOF 条目
  /// 版本戳」（对标 C# ExecutionContext 从 SystemState 单 Word 一次取得版本与
  /// InNewVersion——位与版本戳同源，恢复期「回滚 + 重放」恰一次的结构性保证，
  /// 见 [`whlog::HybridLog::version_shift`] 一致性契约）。
  ///
  /// 读点分工（读点下移后）：追加域的位/戳不再经本口——hlog 分配成功点
  /// （[`whlog::HybridLog::append`] 两处 CAS 胜点与池取复活口）自读并向上
  /// 传导；本口现服务原位生效臂（`notify_write_listener` 在生效点后单读）与
  /// 事件分发（[`Self::emit_event`] 分发时点读）。
  #[inline]
  pub fn write_window_snapshot(&self) -> (bool, i64) {
    let word = self.hlog.version_shift_word();
    (
      word & whlog::VERSION_SHIFT_OPEN_BIT != 0,
      (word & whlog::VERSION_MASK) as i64,
    )
  }

  /// 统一分发存储事件（非 hlog 追加域：版本戳取分发时点合字版本域单次读；
  /// 暂停闸置位期间跳过；失败沿写路径上抛）
  ///
  /// `aof_session_id` 见 [`EmitFn`]：产生本事件的存储会话 id，随条目入 AOF
  /// 帧头，供重放归组口按同键与事务标记配对（后台/无会话面传 0）。
  #[inline]
  pub(crate) fn emit_event(&self, aof_session_id: i32, event: StoreEvent<'_>) -> Result<()> {
    let (_, ver) = self.write_window_snapshot();
    self.emit_event_with_version(ver, aof_session_id, event)
  }

  /// 携版本分发存储事件（hlog 追加域专用：ver 与记录头纪元位同源自分配成功点
  /// 单读、经 append 返回值/池取复活臂传导；暂停闸置位期间跳过；失败沿写路径上抛）
  #[inline]
  pub(crate) fn emit_event_with_version(
    &self,
    ver: i64,
    aof_session_id: i32,
    event: StoreEvent<'_>,
  ) -> Result<()> {
    if self.aof_listeners_paused.load(Ordering::Acquire) {
      return Ok(());
    }
    if let Some(sink) = self.event_sink.get() {
      return sink.emit(ver, aof_session_id, event);
    }
    Ok(())
  }

  /// 当前存储版本（对标 C# TsavoriteKV.CurrentVersion；0 = 无 checkpoint 历史）
  ///
  /// 单源合字：读 hlog 版本推进窗口合字（[`whlog::HybridLog::version_shift`]）
  /// 的版本域——窗口位与版本同字，检查点开窗/收口与版本基线推进共用同一原子，
  /// 绝无第二套版本源。
  #[inline]
  pub fn current_version(&self) -> i64 {
    (self.current_version.load(Ordering::SeqCst) & whlog::VERSION_MASK) as i64
  }

  /// 上一次成功检查点的版本号（对标 C# TsavoriteKV.lastVersion；0 = 无成功检查点历史）
  ///
  /// 与 CurrentVersion 分列：检查点在途或失败轮中，CurrentVersion 已推进至新版本，
  /// 而 LastCheckpointedVersion 恒保持上一成功版本。
  /// 仅在检查点持久化成功发布后单点登记，或恢复时由 set_current_version 同点对齐。
  #[inline]
  pub fn last_checkpointed_version(&self) -> u64 {
    self.last_checkpointed_version.load(Ordering::SeqCst)
  }

  /// 获取当前存储版本合字的共享原子引用（供外部监听器无环捕获；读侧按
  /// [`whlog::VERSION_MASK`] 掩取版本域）
  #[inline]
  pub fn current_version_atomic(&self) -> &Arc<AtomicU64> {
    &self.current_version
  }

  /// 推进当前存储版本（checkpoint 拍摄成功 / 从 checkpoint 恢复时由库管理层调用；单调推进，勿回退）
  ///
  /// 恢复基线写入窗口关态合字（恢复出的全新实例窗口恒关闭）：检查点运行期的
  /// 「开窗 + 版本推进」由 [`Self::begin_version_shift`] 单原子承担，本口仅服务
  /// 恢复基线，与运行期推进不同点、无交错面。恢复轮将当前版本与上一检查点版本同点对齐。
  #[inline]
  pub fn set_current_version(&self, version: i64) {
    let ver_u64 = version as u64 & whlog::VERSION_MASK;
    self.current_version.store(ver_u64, Ordering::SeqCst);
    self
      .last_checkpointed_version
      .store(ver_u64, Ordering::SeqCst);
  }

  /// 触发对象 RMW 增量日志通知（便捷内联转发）
  #[inline]
  pub fn notify_object_rmw(
    &self,
    aof_session_id: i32,
    notif: &ObjectRmwNotification<'_>,
  ) -> Result<()> {
    self.emit_event(aof_session_id, StoreEvent::ObjectRmw(notif))
  }

  /// 触发分层稳态写命令镜像通知（便捷内联转发）
  #[inline]
  pub fn notify_tiered_collection_write(
    &self,
    aof_session_id: i32,
    notif: &TieredCollectionNotification<'_>,
  ) -> Result<()> {
    self.emit_event(aof_session_id, StoreEvent::TieredCollectionWrite(notif))
  }

  /// 触发对象信封整值写通知（便捷内联转发）
  #[inline]
  pub fn notify_envelope_upsert(&self, aof_session_id: i32, key: &[u8], val: &[u8]) -> Result<()> {
    self.emit_event(aof_session_id, StoreEvent::EnvelopeUpsert { key, val })
  }

  /// 暂停全部 AOF 监听端口，返回恢复守卫
  ///
  /// 对标 C# AofProcessor 的重放会话（new StoreWrapper(storeWrapper, recordToAof: false)）：
  /// 重放/恢复写入不得镜像回写本端 AOF，否则副本重放自激放大。守卫 drop 时自动恢复。
  pub fn pause_aof_listeners(self: &Arc<Self>) -> AofListenerPauseGuard<D> {
    self.aof_listeners_paused.store(true, Ordering::Release);
    AofListenerPauseGuard {
      store: Arc::clone(self),
    }
  }
}
