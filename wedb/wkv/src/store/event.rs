use std::sync::{
  Arc,
  atomic::{AtomicI64, Ordering},
};

use wbftree::{StorageBackendType, TreeTuning};
use wdev::Device;

use super::{AofListenerPauseGuard, WedbStore};

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
  RangeIndexWrite {
    key: &'a [u8],
    field: &'a [u8],
    val: &'a [u8],
    delete: bool,
  },
  RangeIndexCreate {
    key: &'a [u8],
    backend: &'a StorageBackendType,
    tuning: TreeTuning,
  },
  RangeIndexDrop {
    key: &'a [u8],
  },
  TtlPurge {
    ns: u64,
    db: u64,
    key: &'a [u8],
    expire_at: i64,
  },
}

/// 统一存储事件处理器端口（全部写、索引、对象、TTL、ETag 事件的唯一分发面；
/// 宿主 AOF 追加适配器经 [`WedbStore::set_event_sink`] 注入）
///
/// 形态：引用计数闭包类型别名（C# delegate 的直译；`Arc` 的 `Clone` 即引用
/// 计数递增，闭包调用为一次虚表间接跳转——对推流入队等本身做 I/O 的路径
/// 开销可忽略，全库监听端口的唯一 `dyn` 收敛点）
///
/// 为什么不用事件环拉取（消 dyn 的唯一架构级替代，已评估否决）：
/// 事件产生点分散在写/GC/索引路径深处，无法上提到调用点组合；改为 wkv
/// 定义事件环 + 宿主拉取则需 (a) `StoreEvent` 的 key/val 借用切片 owned
/// 化——写热路径每条命令 2 次堆分配，违反零拷贝规范；(b) 「存储写成功的
/// 瞬间 = 已镜像」的线性化保证退化为消费窗口，SAVE 前镜像落盘、pause
/// 闸门等语义需整体迁移重验。消 1 个 dyn 的收益 << 上述成本，保留。
pub type StoreEventSink = Arc<dyn Fn(StoreEvent<'_>) + Send + Sync>;

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

  /// 统一分发存储事件（暂停闸置位期间跳过）
  #[inline]
  pub(crate) fn emit_event(&self, event: StoreEvent<'_>) {
    if self.aof_listeners_paused.load(Ordering::Relaxed) {
      return;
    }
    if let Some(sink) = self.event_sink.get() {
      sink(event);
    }
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
  pub fn notify_object_rmw(&self, notif: &ObjectRmwNotification<'_>) {
    self.emit_event(StoreEvent::ObjectRmw(notif));
  }

  /// 触发对象信封整值写通知（便捷内联转发）
  #[inline]
  pub fn notify_envelope_upsert(&self, key: &[u8], val: &[u8]) {
    self.emit_event(StoreEvent::EnvelopeUpsert { key, val });
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
