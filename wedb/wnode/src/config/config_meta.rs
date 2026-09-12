/// 动态配置更新的副作用入口（对标 C# ConfigMeta.UpdateAction 委托经由
/// `RuntimeServerConfig.owner`（StoreWrapper）触达的后台任务生命周期动作）。
///
/// C# 侧 StoreWrapper 以具体类型持有；此处以 trait 承接（servers 域落地后
/// 由 StoreWrapper 实现本 trait），语义逐项对齐：
/// - `reconcile_commit_task` ← StoreWrapper.ReconcilePrimaryTask(TaskType.CommitTask)
/// - `reconcile_object_collect_task` ← StoreWrapper.ReconcilePrimaryTask(TaskType.ObjectCollectTask)
/// - `reconcile_expired_key_deletion_task` ← StoreWrapper.ReconcilePrimaryTask(TaskType.ExpiredKeyDeletionTask)
/// - `apply_aof_sync_max_lag_bytes` ← StoreWrapper.ApplyAofSyncMaxLagBytes(long)
use std::sync::{
  Arc,
  atomic::{AtomicI64, AtomicUsize, Ordering},
};

use super::{
  config_kind::ConfigKind, config_time_unit::ConfigTimeUnit,
  log_compaction_type::LogCompactionType, runtime_server_config::RuntimeServerConfig,
  runtime_server_options::RuntimeServerOptions,
};

/// 对标 libs/server/StoreWrapper.cs 的配置更新回调契约
pub trait ConfigUpdateOwner: Send + Sync {
  /// 对标 libs/server/StoreWrapper.cs:ReconcilePrimaryTask（CommitTask）。
  fn reconcile_commit_task(&self);

  /// 对标 libs/server/StoreWrapper.cs:ReconcilePrimaryTask（ObjectCollectTask）。
  fn reconcile_object_collect_task(&self);

  /// 对标 libs/server/StoreWrapper.cs:ReconcilePrimaryTask（ExpiredKeyDeletionTask）。
  fn reconcile_expired_key_deletion_task(&self);

  /// 对标 libs/server/StoreWrapper.cs:ApplyAofSyncMaxLagBytes。
  fn apply_aof_sync_max_lag_bytes(&self, max_lag_bytes: i64);
}

/// 测试用配置持有者
#[derive(Debug, Default)]
pub struct TestOwner {
  pub commit: AtomicUsize,
  pub collect: AtomicUsize,
  pub expiry: AtomicUsize,
  pub lag: AtomicI64,
}

impl ConfigUpdateOwner for TestOwner {
  fn reconcile_commit_task(&self) {
    self.commit.fetch_add(1, Ordering::Relaxed);
  }
  fn reconcile_object_collect_task(&self) {
    self.collect.fetch_add(1, Ordering::Relaxed);
  }
  fn reconcile_expired_key_deletion_task(&self) {
    self.expiry.fetch_add(1, Ordering::Relaxed);
  }
  fn apply_aof_sync_max_lag_bytes(&self, max_lag_bytes: i64) {
    self.lag.store(max_lag_bytes, Ordering::Relaxed);
  }
}

/// 配置持有者具象分发枚举（消除虚表开销）
#[derive(Clone)]
pub enum ConfigOwner {
  Test(Arc<TestOwner>),
}

impl From<Arc<TestOwner>> for ConfigOwner {
  #[inline]
  fn from(t: Arc<TestOwner>) -> Self {
    Self::Test(t)
  }
}

impl ConfigUpdateOwner for ConfigOwner {
  #[inline]
  fn reconcile_commit_task(&self) {
    match self {
      Self::Test(t) => t.reconcile_commit_task(),
    }
  }

  #[inline]
  fn reconcile_object_collect_task(&self) {
    match self {
      Self::Test(t) => t.reconcile_object_collect_task(),
    }
  }

  #[inline]
  fn reconcile_expired_key_deletion_task(&self) {
    match self {
      Self::Test(t) => t.reconcile_expired_key_deletion_task(),
    }
  }

  #[inline]
  fn apply_aof_sync_max_lag_bytes(&self, max_lag_bytes: i64) {
    match self {
      Self::Test(t) => t.apply_aof_sync_max_lag_bytes(max_lag_bytes),
    }
  }
}

/// 更新动作：CONFIG SET 在校验通过、新值已写入槽位之后执行。
/// 返回 `Err`（含拒绝原因）则回滚槽位并拒绝本次更新。
/// 对标 libs/server/Config/ConfigMeta.cs:ConfigUpdateAction。
pub type ConfigUpdateAction =
  fn(&RuntimeServerConfig, old_value: i64, new_value: i64) -> Result<(), super::error::ConfigError>;

/// 单个运行时配置槽位的元数据：8 字节原始单元的解释方式、取值区间与读取途径
/// （对标 libs/server/Config/ConfigMeta.cs:ConfigMeta）。
#[derive(Clone)]
pub struct ConfigMeta {
  /// CONFIG 线上的规范参数名。
  pub name: &'static str,
  /// 槽位 storage 类别及全部可读视图。
  pub kind: ConfigKind,
  /// CONFIG SET 接受的闭区间下界。
  pub min: i64,
  /// CONFIG SET 接受的闭区间上界。
  pub max: i64,
  /// ENUM 选项声明的枚举类别；当前表内唯一枚举为 LogCompactionType
  ///（对齐 C# `Type EnumType`，以具体类型承载避免运行期反射）。
  pub enum_type: Option<EnumMeta>,
  /// 是否由 RuntimeServerConfig 槽位表承载（非 runtime 类型由 bespoke CONFIG 代码处理）。
  pub is_runtime: bool,
  /// 是否拒绝 CONFIG SET。
  pub read_only: bool,
  /// 时长类选项的存储单位。
  pub time_unit: ConfigTimeUnit,
  /// 只读选项的 CONFIG GET 取值函数：直接从启动选项计算（只读回落），不占用槽位。
  pub read_only_formatter: Option<fn(&RuntimeServerOptions) -> String>,
  /// 更新动作：见 `ConfigUpdateAction`；写入槽位后执行，失败即回滚。
  pub update_action: Option<ConfigUpdateAction>,
}

impl ConfigMeta {
  /// 未登记槽位的默认元数据（IsRuntime == false，由 bespoke CONFIG 代码处理）。
  pub const EMPTY: Self = Self {
    name: "",
    kind: super::config_kind::ConfigKind::NONE,
    min: 0,
    max: 0,
    enum_type: None,
    is_runtime: false,
    read_only: false,
    time_unit: ConfigTimeUnit::None,
    read_only_formatter: None,
    update_action: None,
  };
}

/// 表内枚举类别的静态描述（替代 C# `System.Type`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumMeta {
  /// libs/server/LogCompactionType.cs:LogCompactionType（compaction-type）。
  LogCompactionType,
}

impl EnumMeta {
  /// 按成员名（忽略大小写）或十进制数值解析为 64 位槽位表示。
  /// 对标 EnumExtensions.TryParseEnumToLong + Enum.IsDefined。
  pub fn try_parse_to_long(self, value: &str) -> Option<i64> {
    match self {
      Self::LogCompactionType => LogCompactionType::try_parse(value).map(|v| i64::from(v as u8)),
    }
  }

  /// 64 位槽位表示反查成员名（未命中返回 None）。
  /// 对标 RuntimeServerConfig.RespFormat 中的 Enum.GetName 路径。
  pub fn name_of(self, raw: i64) -> Option<&'static str> {
    match self {
      Self::LogCompactionType => LogCompactionType::from_raw(raw).map(LogCompactionType::as_name),
    }
  }
}
