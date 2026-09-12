//! 运行时配置管理与更新行为集成测试

use std::{
  sync::{Arc, atomic::Ordering as AtomicOrdering},
  time::Duration,
};

use wnode::config::{
  config_kind::ConfigKind,
  config_meta::{ConfigOwner, TestOwner},
  config_name_comparer::ConfigNameComparer,
  error::ConfigError,
  log_compaction_type::LogCompactionType,
  runtime_server_config::RuntimeServerConfig,
  runtime_server_options::RuntimeServerOptions,
  server_config_type::ServerConfigType,
};

#[test]
fn table_size() {
  assert_eq!(RuntimeServerConfig::compute_table_size(), 38);
  assert_eq!(RuntimeServerConfig::meta().len(), 38);
}

#[test]
fn meta_static_validity() {
  // 每个登记条目均通过静态校验（对齐 C# 建表期的 EnsureValidKind/EnsureSupportedEnum）。
  for (i, meta) in RuntimeServerConfig::meta().iter().enumerate() {
    if !meta.is_runtime {
      assert_eq!(meta.name, "");
      continue;
    }
    assert!(
      RuntimeServerConfig::ensure_valid_kind(meta.kind, meta.time_unit).is_ok(),
      "槽位 {i} 元数据非法"
    );
    if (meta.kind & ConfigKind::ENUM) != ConfigKind::NONE {
      assert!(RuntimeServerConfig::ensure_supported_enum(meta.enum_type).is_ok());
    }
  }
}

#[test]
fn name_lookup_contains_alias_and_case_insensitive() {
  assert_eq!(
    RuntimeServerConfig::try_get_type(b"cluster-node-timeout"),
    Some(ServerConfigType::ClusterNodeTimeout)
  );
  assert_eq!(
    RuntimeServerConfig::try_get_type(b"CLUSTER-TIMEOUT"),
    Some(ServerConfigType::ClusterNodeTimeout)
  );
  assert_eq!(
    RuntimeServerConfig::try_get_type(b"slowlog-log-slower-than"),
    Some(ServerConfigType::SlowlogLogSlowerThan)
  );
  assert_eq!(RuntimeServerConfig::try_get_type(b"nonexistent"), None);
  assert_eq!(RuntimeServerConfig::name(ServerConfigType::SgGet), "sg-get");
}

#[test]
fn runtime_types_cover_settable_and_readonly() {
  let types = RuntimeServerConfig::runtime_types();
  assert!(!types.contains(&ServerConfigType::None));
  assert!(!types.contains(&ServerConfigType::SlaveReadOnly));
  assert!(types.contains(&ServerConfigType::ClusterNodeTimeout));
  assert!(types.contains(&ServerConfigType::Dir));
  assert!(types.contains(&ServerConfigType::AofNullDevice));
}

#[test]
fn init_seeds_slots_from_options() {
  let config = RuntimeServerConfig::with_defaults();
  assert_eq!(config.get_int(ServerConfigType::ClusterNodeTimeout), 60);
  assert_eq!(config.get_int(ServerConfigType::ReplicaSyncDelay), 5);
  assert_eq!(config.get_long(ServerConfigType::AofSyncMaxLagBytes), -1);
  assert_eq!(config.get_int(ServerConfigType::AofReplayMaxLagBytes), -1);
  assert!(!config.get_bool(ServerConfigType::CompactionForceDelete));
  assert!(config.get_bool(ServerConfigType::SgGet));
  assert_eq!(
    config.get_enum(ServerConfigType::CompactionType),
    Ok(LogCompactionType::None)
  );
  // ReplicaAttachTimeout 60s -> 60（秒）；负值/无限归 0。
  assert_eq!(config.get_int(ServerConfigType::ReplAttachTimeout), 60);
  assert_eq!(
    config.get_int(ServerConfigType::ExpiredKeyDeletionScanFreq),
    -1
  );
}

#[test]
fn duration_unit_conversions() {
  let config = RuntimeServerConfig::with_defaults();
  // slowlog-log-slower-than 存微秒（声明 MICROSECONDS 视图）。
  assert_eq!(
    config.get_microseconds(ServerConfigType::SlowlogLogSlowerThan),
    0
  );
  // replica-sync-delay 存毫秒（声明 MILLISECONDS/SECONDS 视图，无 MICROSECONDS）。
  assert_eq!(
    config.get_milliseconds(ServerConfigType::ReplicaSyncDelay),
    5
  );
  assert_eq!(config.get_seconds(ServerConfigType::ReplicaSyncDelay), 0);
  assert_eq!(config.get_seconds(ServerConfigType::ClusterNodeTimeout), 60);
  // aof-tail-witness-freq 存毫秒。
  assert_eq!(
    config.get_milliseconds(ServerConfigType::AofTailWitnessFreq),
    100
  );
}

#[test]
fn time_span_non_positive_means_infinite() {
  let config = RuntimeServerConfig::with_defaults();
  assert_eq!(
    config.get_time_span(ServerConfigType::ClusterNodeTimeout),
    Some(Duration::from_secs(60))
  );
  // 0 = 无限超时（下界 0，负值被 CONFIG SET 拒绝）。
  assert_eq!(
    config.try_set(ServerConfigType::ClusterNodeTimeout, "0"),
    Ok(())
  );
  assert_eq!(
    config.get_time_span(ServerConfigType::ClusterNodeTimeout),
    None
  );
}

#[test]
fn try_set_int_range_and_errors() {
  let config = RuntimeServerConfig::with_defaults();
  assert_eq!(
    config.try_set(ServerConfigType::ObjectScanCountLimit, "2000"),
    Ok(())
  );
  assert_eq!(config.get_int(ServerConfigType::ObjectScanCountLimit), 2000);

  assert_eq!(
    config.try_set(ServerConfigType::ObjectScanCountLimit, "abc"),
    Err(ConfigError::InvalidInteger {
      name: "object-scan-count-limit".into()
    })
  );
  assert_eq!(
    config.try_set(ServerConfigType::ObjectScanCountLimit, "-1"),
    Err(ConfigError::OutOfRange {
      name: "object-scan-count-limit".into(),
      min: 0,
      max: i64::from(i32::MAX)
    })
  );
  // 拒绝后槽位不变。
  assert_eq!(config.get_int(ServerConfigType::ObjectScanCountLimit), 2000);

  // Int32 负下界（-1 合法）。
  assert_eq!(
    config.try_set(ServerConfigType::AofReplayMaxLagBytes, "-1"),
    Ok(())
  );
  assert_eq!(
    config.try_set(ServerConfigType::AofReplayMaxLagBytes, "-2"),
    Err(ConfigError::OutOfRange {
      name: "aof-replay-max-lag-bytes".into(),
      min: -1,
      max: i64::from(i32::MAX)
    })
  );
}

#[test]
fn try_set_bool_forms() {
  let config = RuntimeServerConfig::with_defaults();
  for yes in ["yes", "YES", "true", "1"] {
    assert_eq!(config.try_set(ServerConfigType::SgGet, yes), Ok(()));
    assert!(config.get_bool(ServerConfigType::SgGet));
  }
  for no in ["no", "False", "0"] {
    assert_eq!(config.try_set(ServerConfigType::SgGet, no), Ok(()));
    assert!(!config.get_bool(ServerConfigType::SgGet));
  }
  assert!(matches!(
    config.try_set(ServerConfigType::SgGet, "maybe"),
    Err(ConfigError::InvalidBool { .. })
  ));
}

#[test]
fn try_set_enum_by_name_and_number() {
  let config = RuntimeServerConfig::with_defaults();
  assert_eq!(
    config.try_set(ServerConfigType::CompactionType, "lookup"),
    Ok(())
  );
  assert_eq!(
    config.get_enum(ServerConfigType::CompactionType),
    Ok(LogCompactionType::Lookup)
  );
  assert_eq!(
    config.try_set(ServerConfigType::CompactionType, "3"),
    Ok(())
  );
  assert_eq!(
    config.get_enum(ServerConfigType::CompactionType),
    Ok(LogCompactionType::Scan)
  );
  // 数值越界/未声明成员与未知名字均拒绝。
  assert!(matches!(
    config.try_set(ServerConfigType::CompactionType, "9"),
    Err(ConfigError::InvalidEnum { .. })
  ));
  assert!(matches!(
    config.try_set(ServerConfigType::CompactionType, "bogus"),
    Err(ConfigError::InvalidEnum { .. })
  ));
}

#[test]
fn read_only_rejects_set_and_falls_through_options() {
  let config = RuntimeServerConfig::with_defaults();
  assert_eq!(
    config.try_set(ServerConfigType::AppendOnly, "yes"),
    Err(ConfigError::ReadOnly {
      name: "appendonly".into()
    })
  );
  assert_eq!(config.resp_format(ServerConfigType::AppendOnly), "no");
  assert_eq!(config.resp_format(ServerConfigType::Timeout), "0");
  assert_eq!(config.resp_format(ServerConfigType::Save), "");
  assert_eq!(config.resp_format(ServerConfigType::Databases), "16");
  assert_eq!(config.resp_format(ServerConfigType::ClusterEnabled), "no");
  assert_eq!(config.resp_format(ServerConfigType::AofMemory), "128m");
  assert_eq!(
    config.resp_format(ServerConfigType::AofPhysicalSublogCount),
    "1"
  );
  assert_eq!(config.resp_format(ServerConfigType::UnixSocket), "");
}

#[test]
fn update_actions_invoke_owner_and_rollback_on_reject() {
  let owner = Arc::new(TestOwner::default());
  // 启动即周期提交（-1 手动基线），aof-commit-freq 才可运行期变更。
  let options = RuntimeServerOptions {
    commit_frequency_ms: -1,
    ..RuntimeServerOptions::default()
  };
  let config = RuntimeServerConfig::new(options, Some(ConfigOwner::Test(owner.clone())));

  // aof-commit-freq：-1 -> 5000，触发 commit 任务 reconcile。
  assert_eq!(
    config.try_set(ServerConfigType::AofCommitFreq, "5000"),
    Ok(())
  );
  assert_eq!(config.get_int(ServerConfigType::AofCommitFreq), 5000);
  assert_eq!(owner.commit.load(AtomicOrdering::Relaxed), 1);

  // 改 0 被拒绝且回滚。
  assert_eq!(
    config.try_set(ServerConfigType::AofCommitFreq, "0"),
    Err(ConfigError::CommitFreqZero)
  );
  assert_eq!(config.get_int(ServerConfigType::AofCommitFreq), 5000);
  assert_eq!(owner.commit.load(AtomicOrdering::Relaxed), 1);

  // aof-sync-max-lag-bytes 直推 owner 闸门。
  assert_eq!(
    config.try_set(ServerConfigType::AofSyncMaxLagBytes, "123456"),
    Ok(())
  );
  assert_eq!(owner.lag.load(AtomicOrdering::Relaxed), 123456);

  // 收集 / 扫描任务 reconcile。
  assert_eq!(
    config.try_set(ServerConfigType::ExpiredObjectCollectionFreq, "30"),
    Ok(())
  );
  assert_eq!(owner.collect.load(AtomicOrdering::Relaxed), 1);
  assert_eq!(
    config.try_set(ServerConfigType::ExpiredKeyDeletionScanFreq, "15"),
    Ok(())
  );
  assert_eq!(owner.expiry.load(AtomicOrdering::Relaxed), 1);

  // 无 owner 时更新动作仍成功（仅无生命周期副作用）。
  let bare_options = RuntimeServerOptions {
    commit_frequency_ms: -1,
    ..RuntimeServerOptions::default()
  };
  let bare = RuntimeServerConfig::new(bare_options, None);
  assert_eq!(bare.try_set(ServerConfigType::AofCommitFreq, "100"), Ok(()));
}

#[test]
fn commit_freq_rejected_when_started_auto_commit() {
  let options = RuntimeServerOptions {
    commit_frequency_ms: 0,
    ..RuntimeServerOptions::default()
  };
  let config = RuntimeServerConfig::new(options, None);
  assert_eq!(
    config.try_set(ServerConfigType::AofCommitFreq, "100"),
    Err(ConfigError::CommitFreqAutoCommitStart)
  );
}

#[test]
fn resp_format_of_runtime_slots() {
  let config = RuntimeServerConfig::with_defaults();
  assert_eq!(
    config.resp_format(ServerConfigType::ClusterNodeTimeout),
    "60"
  );
  assert_eq!(config.resp_format(ServerConfigType::SgGet), "yes");
  assert_eq!(config.resp_format(ServerConfigType::CompactionType), "None");
  assert_eq!(
    config.try_set(ServerConfigType::CompactionType, "Shift"),
    Ok(())
  );
  assert_eq!(
    config.resp_format(ServerConfigType::CompactionType),
    "Shift"
  );
  assert_eq!(
    config.resp_format(ServerConfigType::AofSyncMaxLagBytes),
    "-1"
  );
}

#[test]
fn seconds_from_time_span_non_positive_is_zero() {
  assert_eq!(RuntimeServerConfig::seconds_from_time_span(60), 60);
  assert_eq!(RuntimeServerConfig::seconds_from_time_span(0), 0);
  assert_eq!(RuntimeServerConfig::seconds_from_time_span(-1), 0);
}

#[test]
fn name_comparer_semantics() {
  assert!(ConfigNameComparer::equals(b"AppendOnly", b"appendonly"));
  assert!(!ConfigNameComparer::equals(b"appendonly", b"appendonlyx"));
  assert_eq!(ConfigNameComparer::to_upper_ascii(b'a'), b'A');
  assert_eq!(ConfigNameComparer::to_upper_ascii(b'0'), b'0');
}
