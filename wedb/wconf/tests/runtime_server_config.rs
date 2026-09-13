//! 运行时配置管理与更新行为集成测试（对标 C# RuntimeServerConfigTests）

use std::{
  sync::{Arc, atomic::Ordering as AtomicOrdering},
  time::Duration,
};

use wconf::{
  ConfigError, ConfigKind, ConfigNameComparer, ConfigOwner, LogCompactionType, RuntimeServerConfig,
  RuntimeServerOptions, ServerConfigType, TestOwner,
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
  assert_eq!(
    config.try_set(ServerConfigType::CompactionType, "invalid"),
    Err(ConfigError::InvalidEnum {
      name: "compaction-type".into(),
      value: "invalid".into()
    })
  );
  assert_eq!(
    config.try_set(ServerConfigType::CompactionType, "99"),
    Err(ConfigError::InvalidEnum {
      name: "compaction-type".into(),
      value: "99".into()
    })
  );
}

#[test]
fn try_set_readonly_rejected() {
  let config = RuntimeServerConfig::with_defaults();
  assert_eq!(
    config.try_set(ServerConfigType::Dir, "/tmp"),
    Err(ConfigError::ReadOnly { name: "dir".into() })
  );
  assert_eq!(
    config.try_set(ServerConfigType::AofMemory, "256m"),
    Err(ConfigError::ReadOnly {
      name: "aof-memory".into()
    })
  );
}

#[test]
fn commit_freq_lifecycle_and_invariants() {
  let opts = RuntimeServerOptions {
    commit_frequency_ms: 10,
    ..Default::default()
  };
  let owner = Arc::new(TestOwner::default());
  let config = RuntimeServerConfig::new(opts, Some(ConfigOwner::from(Arc::clone(&owner))));

  assert_eq!(config.get_int(ServerConfigType::AofCommitFreq), 10);
  assert_eq!(
    config.try_set(ServerConfigType::AofCommitFreq, "20"),
    Ok(())
  );
  assert_eq!(config.get_int(ServerConfigType::AofCommitFreq), 20);
  assert_eq!(owner.commit.load(AtomicOrdering::Relaxed), 1);

  assert_eq!(
    config.try_set(ServerConfigType::AofCommitFreq, "0"),
    Err(ConfigError::CommitFreqZero)
  );

  let auto_start = RuntimeServerConfig::new(RuntimeServerOptions::default(), None);
  assert_eq!(auto_start.get_int(ServerConfigType::AofCommitFreq), 0);
  assert_eq!(
    auto_start.try_set(ServerConfigType::AofCommitFreq, "10"),
    Err(ConfigError::CommitFreqAutoCommitStart)
  );
}

#[test]
fn other_lifecycle_actions_touch_owner() {
  let owner = Arc::new(TestOwner::default());
  let config = RuntimeServerConfig::new(
    RuntimeServerOptions::default(),
    Some(ConfigOwner::from(Arc::clone(&owner))),
  );

  assert_eq!(
    config.try_set(ServerConfigType::ExpiredObjectCollectionFreq, "10"),
    Ok(())
  );
  assert_eq!(owner.collect.load(AtomicOrdering::Relaxed), 1);

  assert_eq!(
    config.try_set(ServerConfigType::ExpiredKeyDeletionScanFreq, "30"),
    Ok(())
  );
  assert_eq!(owner.expiry.load(AtomicOrdering::Relaxed), 1);

  assert_eq!(
    config.try_set(ServerConfigType::AofSyncMaxLagBytes, "1024"),
    Ok(())
  );
  assert_eq!(owner.lag.load(AtomicOrdering::Relaxed), 1024);
}

#[test]
fn name_comparer_matches_csharp_ascii() {
  assert!(ConfigNameComparer::equals(
    b"cluster-timeout",
    b"CLUSTER-TIMEOUT"
  ));
  assert!(ConfigNameComparer::equals(b"dir", b"DIR"));
  assert!(!ConfigNameComparer::equals(b"dir", b"logdir"));
  assert_eq!(
    ConfigNameComparer::hash_code(b"cluster-timeout"),
    ConfigNameComparer::hash_code(b"CLUSTER-TIMEOUT")
  );
}

#[test]
fn readonly_parameters_resp_format_from_options() {
  let opts = RuntimeServerOptions {
    enable_aof: false,
    max_databases: 16,
    checkpoint_base_directory: "/tmp/wedb_test".to_string(),
    log_dir: Some("/tmp/wedb_test/log".to_string()),
    unix_socket_path: None,
    enable_cluster: false,
    aof_memory_size: Some("128m".to_string()),
    aof_physical_sublog_count: 1,
    ..Default::default()
  };
  let config = RuntimeServerConfig::new(opts, None);

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
  assert_eq!(config.resp_format(ServerConfigType::Dir), "/tmp/wedb_test");
  assert_eq!(
    config.resp_format(ServerConfigType::Logdir),
    "/tmp/wedb_test/log"
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

  config
    .try_set(ServerConfigType::CompactionType, "lookup")
    .unwrap();
  assert_eq!(
    config.resp_format(ServerConfigType::CompactionType),
    "Lookup"
  );

  assert_eq!(
    config.resp_format(ServerConfigType::AofSyncMaxLagBytes),
    "-1"
  );
}
