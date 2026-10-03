//! CONFIG SET → 生产调停执行器桥集成测试
//!
//! 对标 C# CONFIG SET 路径最终回调 StoreWrapper.ReconcilePrimaryTask：
//! libs/server/StoreWrapper.cs:ReconcilePrimaryTask 经 TryStartExpiredKeyDeletionTask
//! 按 runtimeConfig 新值重启 ExpiredKeyDeletionTask。rust 侧等价链路：
//! `RuntimeServerConfig::try_set(expired-key-deletion-scan-freq)` 产出
//! `ConfigReconcile` 调停消息 → `apply_config_reconcile` 就地 match →
//! `WedbStore` 的内置 GC 扫描循环（启停/调频）。

use std::sync::Arc;

use aok::{OK, Void};
use wbase::cfg::LogCompactionType;
use wconf::{ConfigError, RuntimeServerConfig, RuntimeServerOptions, ServerConfigType};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  config_owner::apply_config_reconcile, resp::RespSessionConsumer, service::StorageSessionProvider,
};
use wtest_base::test_store_config;

/// 裸配置 + 执行器：CONFIG SET 落槽产出消息并就地执行（生产桥形态）
fn bridged_set(
  config: &RuntimeServerConfig,
  store: &Arc<WedbStore<SegmentedDevice>>,
  value: &str,
) -> Result<(), ConfigError> {
  if let Some(msg) = config.try_set(ServerConfigType::ExpiredKeyDeletionScanFreq, value)? {
    apply_config_reconcile(None, store, None, None, msg);
  }
  Ok(())
}

/// 生产装配节奏的 store 配置：显式点亮内置 GC 扫描、5s 常规节拍。
/// 启用位由配置声明而非装配越权默认（对标 C# 缺省 -1 禁用、
/// `--expired-key-deletion-scan-freq > 0` 才启扫描，
/// defaults.conf:524 / StoreWrapper.cs:994-999 TryStartExpiredKeyDeletionTask）。
fn gc_enabled_store_config() -> StoreConfig {
  let mut config = test_store_config();
  config.gc.enabled = true;
  config.gc.scan_interval_ms = 5_000;
  config
}

/// CONFIG SET 过期扫描频率 → Owner 收到 → 扫描任务实际启停/调频（主链路）
#[compio::test]
async fn config_set_expiry_freq_drives_gc_scan_task() -> Void {
  let dir = tempfile::tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("bridge.db"))?);
  let config = StoreConfig::new(1024, 4096, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  assert!(
    !store.gc_running(),
    "前置：默认 GcConfig 禁用，GC 未启动（对标 Garnet 默认 -1）"
  );
  let runtime_config = RuntimeServerConfig::new(RuntimeServerOptions::default());

  // SET freq=1 → 启用 + 间隔 1000ms + 循环拉起（对标 RegisterAndRun）
  bridged_set(&runtime_config, &store, "1")?;
  let gc = store.gc_config();
  assert!(gc.enabled, "SET freq>0 必须经 Owner 启用扫描开关");
  assert_eq!(gc.scan_interval_ms, 1000, "秒值必须换算为毫秒槽值");
  assert!(store.gc_running(), "SET freq>0 必须拉起后台扫描循环");

  // SET freq=3 → 调频，循环复用（每轮重读配置，下一轮生效）
  bridged_set(&runtime_config, &store, "3")?;
  assert_eq!(store.gc_config().scan_interval_ms, 3000);
  assert!(store.gc_running(), "调频不得停掉在跑的循环");

  // SET freq=-1 → 禁用 + 停循环（对标 CancelAsync）
  bridged_set(&runtime_config, &store, "-1")?;
  assert!(!store.gc_config().enabled, "SET freq<=0 必须禁用扫描开关");
  assert!(
    !store.gc_running(),
    "SET freq<=0 必须停止后台扫描循环（至多一个间隔内退出）"
  );

  // 禁用后再启用 → 循环重拉（对标 ReconcilePrimaryTask 停后按新间隔重启）
  bridged_set(&runtime_config, &store, "2")?;
  assert_eq!(store.gc_config().scan_interval_ms, 2000);
  assert!(store.gc_running(), "禁用后重新 SET 必须重拉循环");
  OK
}

/// CONFIG SET compaction-* → Owner 投影 GcConfig（对标 C# DoCompactionAsync 每轮
/// GetInt/GetEnum 现取 runtimeConfig：rust 以调停消息投影 + GcManager 每轮重读同效）
#[compio::test]
async fn config_set_compaction_knobs_project_to_gc_config() -> Void {
  let dir = tempfile::tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("cp_bridge.db"),
  )?);
  let store = Arc::new(WedbStore::open(
    StoreConfig::new(1024, 4096, 16, 0.5)?,
    device,
  )?);
  let runtime_config = RuntimeServerConfig::new(RuntimeServerOptions::default());
  let set = |type_: ServerConfigType,
             value: &str,
             store: &Arc<WedbStore<SegmentedDevice>>|
   -> Result<(), ConfigError> {
    if let Some(msg) = runtime_config.try_set(type_, value)? {
      apply_config_reconcile(None, store, None, None, msg);
    }
    Ok(())
  };

  // 档位：none → shift → lookup → scan（CONFIG SET 回显即引擎实效）
  set(ServerConfigType::CompactionType, "none", &store)?;
  assert_eq!(store.gc_config().compaction_type, LogCompactionType::None);
  set(ServerConfigType::CompactionType, "shift", &store)?;
  assert_eq!(store.gc_config().compaction_type, LogCompactionType::Shift);
  set(ServerConfigType::CompactionType, "lookup", &store)?;
  assert_eq!(store.gc_config().compaction_type, LogCompactionType::Lookup);
  set(ServerConfigType::CompactionType, "scan", &store)?;
  assert_eq!(store.gc_config().compaction_type, LogCompactionType::Scan);

  // 阈值段数
  set(ServerConfigType::CompactionMaxSegments, "5", &store)?;
  assert_eq!(store.gc_config().compaction_max_segments, 5);

  // 非法枚举值：槽位拒绝回滚，引擎实效不变
  assert!(matches!(
    set(ServerConfigType::CompactionType, "bogus", &store),
    Err(ConfigError::InvalidEnum { .. })
  ));
  assert_eq!(store.gc_config().compaction_type, LogCompactionType::Scan);
  OK
}

/// 无 Owner 时 CONFIG SET 仅存槽位（行为不变面）
#[test]
fn config_set_without_owner_stores_value_only() -> Void {
  let config = RuntimeServerConfig::new(RuntimeServerOptions::default());
  config.try_set(ServerConfigType::ExpiredKeyDeletionScanFreq, "7")?;
  assert_eq!(
    config.get_int(ServerConfigType::ExpiredKeyDeletionScanFreq),
    7
  );
  OK
}

/// 只读项与未知项行为不变：SET 被拒、未知参数不入表
#[test]
fn readonly_and_unknown_parameters_unchanged() -> Void {
  let config = RuntimeServerConfig::new(RuntimeServerOptions::default());
  assert!(matches!(
    config.try_set(ServerConfigType::Dir, "/tmp"),
    Err(ConfigError::ReadOnly { .. })
  ));
  assert_eq!(
    RuntimeServerConfig::try_get_type(b"no-such-parameter"),
    None
  );
  OK
}

/// 端到端：StorageSessionProvider 装配的 runtime_config 已挂桥
///（from_parts 接线验证：CONFIG SET 送达 provider.store() 引擎的 GC 域）
#[compio::test]
async fn provider_runtime_config_wired_to_store() -> Void {
  let dir = tempfile::tempdir()?;
  let provider =
    StorageSessionProvider::open_with_config(gc_enabled_store_config(), dir.path(), |_, api| {
      Some(RespSessionConsumer::new(
        0,
        Default::default(),
        Arc::new(api),
      ))
    })?;

  let store = provider.store();
  // 配置声明启用（enabled=true, 5s）→ 装配口 `open_node_with_config` →
  // `WedbStore::open_shared` 的 `start_gc` 即时拉起扫描循环，无须等首个会话
  assert!(
    store.gc_running(),
    "store 配置启用 GC 时装配必须拉起后台扫描循环"
  );

  // 紧缩旋钮槽位初值（32/None，对标 C# 选项默认）与引擎 GcConfig 实效
  // 零漂移（启用位不在此列：它经 CONFIG SET expired-key-deletion-scan-freq
  // 由唯一调停入口改写，见下方 1 / -1 往返）
  let gc = store.gc_config();
  assert_eq!(gc.compaction_max_segments, 32);
  assert_eq!(gc.compaction_type, LogCompactionType::None);

  bridged_set(&provider.runtime_config, &store, "1")?;
  assert_eq!(store.gc_config().scan_interval_ms, 1000);
  assert!(store.gc_running());

  bridged_set(&provider.runtime_config, &store, "-1")?;
  assert!(!store.gc_running(), "生产装配链路同样必须能停掉扫描循环");
  OK
}
