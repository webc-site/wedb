use wedb::cli::ConfFile;

#[test]
fn test_load_standard_wedb_nt_file() -> aok::Result<()> {
  let conf_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/wedb.nt");
  let conf = ConfFile::load_from_file(conf_path)?;

  // 1. IP 复用与端口自动组合
  assert_eq!(conf.get_ip().as_deref(), Some("127.0.0.1"));
  assert_eq!(conf.get_addr().as_deref(), Some("127.0.0.1:6379"));
  assert_eq!(conf.get_raft_addr().as_deref(), Some("127.0.0.1:4910"));

  // 2. 节点标识与拓扑
  assert_eq!(conf.get_node_id(), Some(1));
  assert_eq!(conf.get_weight(), Some(100));
  assert_eq!(conf.get_region().as_deref(), Some("cn-beijing"));
  assert_eq!(conf.get_zone().as_deref(), Some("zone-a"));
  assert_eq!(conf.get_rack().as_deref(), Some("rack-01"));
  assert_eq!(conf.get_host().as_deref(), Some("10.0.1.10"));

  // 3. 存储引擎与缓存
  assert_eq!(conf.get_data_dir().as_deref(), Some("./data"));
  assert_eq!(conf.get_compression().as_deref(), Some("lz4"));
  assert_eq!(conf.get_journal_compression().as_deref(), Some("none"));
  assert_eq!(conf.get_manual_journal_persist(), Some(false));
  assert_eq!(conf.get_cache_size(), Some(67108864));

  // 4. Raft 心跳与集群 Join (列表格式)
  assert_eq!(conf.get_heartbeat(), Some(50));
  assert_eq!(
    conf.get_join(),
    Some(vec![
      "127.0.0.1:4911".to_string(),
      "127.0.0.1:4912".to_string()
    ])
  );

  Ok(())
}

#[test]
fn test_load_hierarchical_wedb_nt_file() -> aok::Result<()> {
  let conf_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/wedb_hierarchical.nt");
  let conf = ConfFile::load_from_file(conf_path)?;

  // 1. 全局 IP 继承与端口解析
  assert_eq!(conf.get_ip().as_deref(), Some("0.0.0.0"));
  assert_eq!(conf.get_addr().as_deref(), Some("0.0.0.0:6380"));
  assert_eq!(conf.get_raft_addr().as_deref(), Some("0.0.0.0:4920"));

  // 2. 层级段落中的集群与拓扑
  assert_eq!(conf.get_node_id(), Some(2));
  assert_eq!(conf.get_weight(), Some(200));
  assert_eq!(conf.get_region().as_deref(), Some("us-west"));
  assert_eq!(conf.get_zone().as_deref(), Some("us-west-1a"));
  assert_eq!(conf.get_rack().as_deref(), Some("rack-99"));
  assert_eq!(conf.get_host().as_deref(), Some("192.168.1.2"));

  // 3. 存储与缓存
  assert_eq!(conf.get_data_dir().as_deref(), Some("/tmp/wedb_node2_data"));
  assert_eq!(conf.get_compression().as_deref(), Some("none"));
  assert_eq!(conf.get_journal_compression().as_deref(), Some("lz4"));
  assert_eq!(conf.get_manual_journal_persist(), Some(true));
  assert_eq!(conf.get_cache_size(), Some(134217728));

  // 4. Raft 心跳与 Join (列表格式)
  assert_eq!(conf.get_heartbeat(), Some(50));
  assert_eq!(conf.get_join(), Some(vec!["192.168.1.1:4920".to_string()]));

  Ok(())
}

#[test]
fn test_load_multi_join_nt_file() -> aok::Result<()> {
  let conf_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/wedb_multi_join.nt");
  let conf = ConfFile::load_from_file(conf_path)?;

  assert_eq!(conf.get_ip().as_deref(), Some("127.0.0.1"));
  assert_eq!(conf.get_addr().as_deref(), Some("127.0.0.1:4909"));
  assert_eq!(conf.get_raft_addr().as_deref(), Some("127.0.0.1:4910"));
  assert_eq!(conf.get_data_dir().as_deref(), Some("wedb"));
  assert_eq!(conf.get_heartbeat(), Some(100));
  assert_eq!(
    conf.get_join(),
    Some(vec![
      "10.0.1.1:4910".to_string(),
      "10.0.1.2:4910".to_string(),
      "10.0.1.3:4910".to_string(),
    ])
  );

  Ok(())
}

#[test]
fn test_load_minimal_nt_file() -> aok::Result<()> {
  let conf_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/wedb_minimal.nt");
  let conf = ConfFile::load_from_file(conf_path)?;

  assert_eq!(conf.get_addr().as_deref(), Some("127.0.0.1:4909"));
  assert_eq!(conf.get_raft_addr().as_deref(), Some("127.0.0.1:4910"));
  assert_eq!(conf.get_data_dir(), None); // 未指定时由 CLI 提供默认值 wedb
  assert_eq!(conf.get_join(), Some(vec!["127.0.0.1:4911".to_string()]));

  Ok(())
}
