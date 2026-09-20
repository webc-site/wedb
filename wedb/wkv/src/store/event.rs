use std::{
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
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
/// 形态，Send/Sync/Drop 皆由 std 保证，不自立虚表
type EmitFn = Arc<dyn for<'e> Fn(StoreEvent<'e>) -> Result<()> + Send + Sync>;

/// 统一存储事件分发器（宿主上下文类型由 std 闭包对象擦除；sink 一经注入即被
/// `OnceLock` 独占持有、全程 `&self` 借用消费，故不携 Clone 语义）
pub struct StoreEventSink(EmitFn);

impl StoreEventSink {
  /// 由任意线程安全上下文及静态处理函数构造分发器（handler 返回
  /// `Result`：AOF 入队失败沿写路径上抛拒绝该命令，杜绝静默缺条目）
  pub fn new<T: Send + Sync + 'static>(
    ctx: Arc<T>,
    handler: fn(&T, StoreEvent<'_>) -> Result<()>,
  ) -> Self {
    Self(Arc::new(move |event: StoreEvent<'_>| handler(&ctx, event)))
  }

  /// 统一分发存储事件（失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn emit(&self, event: StoreEvent<'_>) -> Result<()> {
    (self.0)(event)
  }
}

impl<D: Device> WedbStore<D> {
  /// 注入存储事件处理器（须在创建任何会话前调用；重复注入返回 false）
  pub fn set_event_sink(&self, sink: StoreEventSink) -> bool {
    self.event_sink.set(sink).is_ok()
  }

  /// 统一分发存储事件（暂停闸置位期间跳过；失败沿写路径上抛）
  #[inline]
  pub(crate) fn emit_event(&self, event: StoreEvent<'_>) -> Result<()> {
    if self.aof_listeners_paused.load(Ordering::Acquire) {
      return Ok(());
    }
    if let Some(sink) = self.event_sink.get() {
      return sink.emit(event);
    }
    Ok(())
  }

  /// 当前存储版本（对标 C# TsavoriteKV.CurrentVersion；0 = 无 checkpoint 历史）
  #[inline]
  pub fn current_version(&self) -> i64 {
    self.current_version.load(Ordering::Acquire)
  }

  /// 获取当前存储版本的共享原子引用（供外部监听器无环捕获）
  #[inline]
  pub fn current_version_atomic(&self) -> &Arc<AtomicI64> {
    &self.current_version
  }

  /// 推进当前存储版本（checkpoint 拍摄成功 / 从 checkpoint 恢复时由库管理层调用；单调推进，勿回退）
  #[inline]
  pub fn set_current_version(&self, version: i64) {
    self.current_version.fetch_max(version, Ordering::Release);
  }

  /// 触发对象 RMW 增量日志通知（便捷内联转发）
  #[inline]
  pub fn notify_object_rmw(&self, notif: &ObjectRmwNotification<'_>) -> Result<()> {
    self.emit_event(StoreEvent::ObjectRmw(notif))
  }

  /// 触发分层稳态写命令镜像通知（便捷内联转发）
  #[inline]
  pub fn notify_tiered_collection_write(
    &self,
    notif: &TieredCollectionNotification<'_>,
  ) -> Result<()> {
    self.emit_event(StoreEvent::TieredCollectionWrite(notif))
  }

  /// 触发对象信封整值写通知（便捷内联转发）
  #[inline]
  pub fn notify_envelope_upsert(&self, key: &[u8], val: &[u8]) -> Result<()> {
    self.emit_event(StoreEvent::EnvelopeUpsert { key, val })
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
