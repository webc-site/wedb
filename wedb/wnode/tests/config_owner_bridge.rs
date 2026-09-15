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
use compio::runtime::Runtime;
use wconf::{ConfigError, RuntimeServerConfig, RuntimeServerOptions, ServerConfigType};
use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  config_owner::apply_config_reconcile, resp::RespSessionConsumer, service::StorageSessionProvider,
};

/// 裸配置 + 执行器：CONFIG SET 落槽产出消息并就地执行（生产桥形态）
fn bridged_set(
  config: &RuntimeServerConfig,
  store: &Arc<WedbStore<SegmentedDevice>>,
  value: &str,
) -> Result<(), ConfigError> {
  if let Some(msg) = config.try_set(ServerConfigType::ExpiredKeyDeletionScanFreq, value)? {
    apply_config_reconcile(store, msg);
  }
  Ok(())
}

/// CONFIG SET 过期扫描频率 → Owner 收到 → 扫描任务实际启停/调频（主链路）
#[test]
fn config_set_expiry_freq_drives_gc_scan_task() -> Void {
  Runtime::new()?.block_on(async {
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

    aok::Result::<()>::Ok(())
  })?;
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
///（from_parts 接线验证：CONFIG SET 送达 provider.store 的 GC 域）
#[test]
fn provider_runtime_config_wired_to_store() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let provider =
      StorageSessionProvider::open_with_config(test_store_config(), dir.path(), |_, api| {
        Some(RespSessionConsumer::new(0, Default::default(), api))
      })?;

    // 生产装配默认常规节奏（enabled=true, 5s）——循环应已在跑
    assert!(
      provider.store.gc_running(),
      "生产 store_config 默认启用 GC，装配后循环必须在跑"
    );

    bridged_set(&provider.runtime_config, &provider.store, "1")?;
    assert_eq!(provider.store.gc_config().scan_interval_ms, 1000);
    assert!(provider.store.gc_running());

    bridged_set(&provider.runtime_config, &provider.store, "-1")?;
    assert!(
      !provider.store.gc_running(),
      "生产装配链路同样必须能停掉扫描循环"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}
