use std::{
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
};

use wbftree::{RANGE_INDEX_STUB_SIZE, StorageBackendType, TreeTuning};
use wdev::Device;

use super::{AofListenerPauseGuard, WedbStore};
use crate::error::Result;

/// 对象 RMW 增量日志通知载荷
#[derive(Debug, Clone, Copy)]
pub struct ObjectRmwNotification<'a> {
  pub key: &'a [u8],
  pub obj_type: u8,
  pub op_code: u8,
  /// 事件时间戳：真 .NET Ticks（i64，100ns 单位，0001-01-01 纪元的
  /// DateTimeOffset.UtcNow.UtcTicks），非 Unix 毫秒——与 Garnet 对象 RMW
  /// 输入的时间戳同域，Unix 秒/毫秒 ↔ ticks 换算见 wbase::convert
  pub timestamp_ticks: i64,
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
  /// 集合就地升阶 / 分层重灌的树数据通道事件：promote 先经 wbftree 建树内核把
  /// 整棵 scratch 树 CPR 快照至迁移临时文件，携物化键真值（ns/db）、原集合判别
  /// 类型（obj_type，副本据此重建 MetaValue 而非硬编码 RangeIndex）与发布存根，
  /// 交由复制面经既有 RangeIndexStreamChunk 通道分块灌入 AOF（复用 RI 迁移流
  /// 通道，杜绝两套通道），副本重组末块后按 obj_type 发布为树态元记录。
  /// `replace` 标记发布形态：首升阶 false（副本已存在同名索引属发散，拒发布）；
  /// 分层重灌 true（先建后拆不换旧树，副本必须以换入形态重放，否则旧树在位的
  /// 回放会被 IndexExists 拦截）。
  RangeIndexStream {
    ns: u64,
    db: u64,
    key: &'a [u8],
    obj_type: u8,
    stub: [u8; RANGE_INDEX_STUB_SIZE],
    file_path: &'a Path,
    replace: bool,
  },
  TtlPurge {
    ns: u64,
    db: u64,
    key: &'a [u8],
    expire_at: i64,
  },
}

/// 统一存储事件分发器（经单态化函数指针擦除宿主上下文类型；
/// sink 一经注入即被 `OnceLock` 独占持有、全程 `&self` 借用消费，故不携
/// Clone 语义，raw 指针 lifetime 由唯一 Arc 强引用锚定）
pub struct StoreEventSink {
  raw: *const (),
  emit_fn: fn(*const (), StoreEvent<'_>) -> Result<()>,
  drop_fn: fn(*const ()),
}

unsafe impl Send for StoreEventSink {}
unsafe impl Sync for StoreEventSink {}
// SAFETY: raw 恒为 new() 经 Arc::into_raw 产出的 *const State<T>，emit_fn/drop_fn 与
// T 同源单态化配对；State<T> 的 T 受 new() 的 Send + Sync + 'static 约束，故句柄
// 跨线程传递与静态处理函数调用均安全；生命周期由 OnceLock 独占持有的单 Arc 强引用锚定

impl Drop for StoreEventSink {
  fn drop(&mut self) {
    (self.drop_fn)(self.raw);
  }
}

impl StoreEventSink {
  /// 由任意线程安全上下文及静态处理函数构造分发器（handler 返回
  /// `Result`：AOF 入队失败沿写路径上抛拒绝该命令，杜绝静默缺条目）
  pub fn new<T: Send + Sync + 'static>(
    ctx: Arc<T>,
    handler: fn(&T, StoreEvent<'_>) -> Result<()>,
  ) -> Self {
    struct State<T> {
      ctx: Arc<T>,
      handler: fn(&T, StoreEvent<'_>) -> Result<()>,
    }

    fn emit_impl<T>(ptr: *const (), event: StoreEvent<'_>) -> Result<()> {
      let state = unsafe { &*(ptr as *const State<T>) };
      (state.handler)(&state.ctx, event)
    }

    fn drop_impl<T>(ptr: *const ()) {
      unsafe {
        drop(Arc::from_raw(ptr as *const State<T>));
      }
    }

    let state = Arc::new(State { ctx, handler });
    let raw = Arc::into_raw(state) as *const ();

    Self {
      raw,
      emit_fn: emit_impl::<T>,
      drop_fn: drop_impl::<T>,
    }
  }

  /// 统一分发存储事件（失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn emit(&self, event: StoreEvent<'_>) -> Result<()> {
    (self.emit_fn)(self.raw, event)
  }
}

impl<D: Device> WedbStore<D> {
  /// 注入存储事件处理器（须在创建任何会话前调用；重复注入返回 false）
  pub fn set_event_sink(&self, sink: StoreEventSink) -> bool {
    self.event_sink.set(sink).is_ok()
  }

  /// 检查是否已注入存储事件处理器
  #[inline]
  pub fn has_event_sink(&self) -> bool {
    self.event_sink.get().is_some()
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
