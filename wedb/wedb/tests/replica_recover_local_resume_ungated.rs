//! 集群副本 --recover 重启恒本地续用无 ClusterReplicaResumeWithData 门控现状锁测
//!
//! 登记锚：deviations.md §162（工单 wedb-boot-replica-recover-local-resume-ungated）。
//! 对标 C# 真源：`garnet/libs/server/StoreWrapper.cs:RecoverAsync`（:377-399 集群
//! 分支角色门控委托）、`garnet/libs/cluster/Server/Replication/ReplicationManager.cs:
//! RecoverAsync`（:511-531 REPLICA 臂 ClusterReplicaResumeWithData 门控）、
//! `garnet/libs/server/Servers/GarnetServerOptions.cs:ClusterReplicaResumeWithData`
//!（:646 默认 false——C# 默认部署副本带 --recover 重启为空库、位点 0）。
//! rust 现状（本测锁形）：数据面恢复由 `boot.rs:run_cluster_server` 的
//! `StorageSessionProvider::open_from_args`（本测同款调用，(recover, aof) 四路分派）
//! 角色无关前置承接，副本 `--recover` 同目录重启即本地续用（等效 C# 置 true 恒开），
//! INFO replication 位点回填为本地重放尾；不带 `--recover` 冷重启才为空库待全量。
//! 装配接线逐段对标生产宿主 boot.rs 装配序（真 TCP 双节点，禁假 mock）。

use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};

use aok::{Result, Void};
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wconf::{ConfigFileArgs, NodeArgs};
use wconn::client::GarnetClient;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_provider::ClusterProvider,
  hash_slot::SlotState,
  replication::{StoreCommitFn, assembly::wire_replication_data_plane},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::cluster_decorate;
use wkv::WedbStore;
use wnode::{
  ClusterProvider as _, GarnetServer, RespSessionConsumer, resp::garnet_api::StoreGarnetApi,
  service::StorageSessionProvider, storage::session::storage_session::StorageSession,
};
use wtest_base::{test_store_config, wait_for};

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 键值集规模与同步等待上限（对标 cluster_replication.rs 基线）
const KV_COUNT: usize = 16;
const SYNC_TIMEOUT: Duration = Duration::from_secs(20);

type NodeBoot<D> = (
  GarnetServer<StorageSessionProvider<D>>,
  Arc<StorageSessionProvider<D>>,
  u16,
);

/// 宿主形态起服（与 boot.rs:run_cluster_server 同一装配口
/// `open_from_args_with_config`——(recover, aof) 四路分派仅凭命令行旗标、无角色
/// 门控，§162 登记的核心现状形态即由本口直锁）；集群资产接线对标 boot.rs 装配段
/// （复制域管理器先建、恢复态按 --recover 语义回放复制历史、装配尾段双角色一律
/// 回填 recovered_aof_tail 位点）
async fn boot_node<D>(
  node: &NodeArgs,
  data_path: &Path,
  provider: &Arc<ClusterProvider>,
  decorate: D,
) -> Result<NodeBoot<D>>
where
  D:
    Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync + 'static,
{
  let sp = StorageSessionProvider::open_from_args_with_config(
    test_store_config(),
    node,
    data_path,
    decorate,
  )
  .await?;
  // 复制域管理器（boot.rs 同序：先于挂 rm 资产的注入建立）
  provider.initialize_replication_manager(
    1,
    Some(&sp.checkpoint_dir.join("cluster")),
    node.recover,
  );
  provider.set_store(sp.store());
  provider.set_database_manager(Arc::clone(&sp.database_manager));
  provider.set_checkpoint_dir(sp.checkpoint_dir.clone());
  if let Some(rm) = provider.replication_manager() {
    rm.set_checkpoint_dir(sp.checkpoint_dir.clone());
  }
  if let Some(aof) = sp.aof() {
    provider.set_aof(Some(Arc::clone(aof)));
    let log = Arc::clone(aof.log());
    let commit: StoreCommitFn = Arc::new(move |op_type, version| {
      // 检查点版本切换标记入 AOF（C# EnqueueCommit 无返回码吞错同口径）
      let _ = log.enqueue_database_commit(op_type, version);
    });
    provider.set_commit_channel(Some(commit));
  }
  provider.set_store_swap_slot(sp.store_swap_slot());
  wire_replication_data_plane(provider, sp.wal().expect("AOF 门控点亮").clone());
  // boot.rs 装配尾段同形回填（:291-300）：双角色一律 set_current_replication_offset
  // (recovered_aof_tail)——角色无关，副本同样续用本地位点（§162）
  if node.recover
    && let Some(rm) = provider.replication_manager()
  {
    if let Some(tail) = sp.recovered_aof_tail() {
      rm.set_current_replication_offset(tail);
    }
    rm.recover_async(provider.is_primary()).await;
  }

  let sp = Arc::new(sp);
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 65536, 100, Arc::clone(&sp))?;
  server.start(NonZeroUsize::new(1))?;
  let port = server.local_addr()?.port();
  Ok((server, sp, port))
}

/// 本地位初始化（节点 id / 端点 / 纪元 / 角色）
fn init_local(
  config: &mut ClusterConfig,
  node_id: u128,
  port: u16,
  role: NodeRole,
  replica_of: Option<u128>,
) {
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port: port as i32,
    config_epoch: 1,
    role,
    replica_of_node_id: replica_of,
    hostname: None,
  });
}

/// 远程 worker 条目（互指拓扑）
fn remote_worker(node_id: u128, port: u16) -> Worker {
  Worker {
    nodeid: Some(node_id),
    address: "127.0.0.1".into(),
    port: port as i32,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  }
}

/// 主端全槽指派
fn assign_all_slots(config: &mut ClusterConfig) {
  let slots: Vec<usize> = (0..CLUSTER_SLOT_COUNT).collect();
  config.assign_slots(&slots, LOCAL_WORKER_ID as u16, SlotState::Stable);
}

/// RESP 客户端往返字符串应答（建连 + 单命令）
async fn client_roundtrip(endpoint: &str, command: &[&str]) -> Result<String> {
  let mut client =
    GarnetClient::new(endpoint.to_string(), None, None, Some("test".into()), 32, 0).unwrap();
  client.connect_async().await?;
  let resp = client.execute_for_string_result_async(command).await;
  resp.map_err(Into::into)
}

/// 确定性键值集
fn kv_batch(prefix: &str, count: usize) -> Vec<(String, String)> {
  (0..count)
    .map(|i| (format!("{prefix}:{i:04}"), format!("v-{prefix}-{i}")))
    .collect()
}

/// 引擎直读 string 键（副本数据校验数据面，对标 cluster_replication.rs 同口径）
async fn store_read(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().expect("会话可得");
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .read_string(key)
    .await
    .expect("读不可失败")
}

/// 逐键逐值校验（副本本地续用断言面）
async fn assert_store_has(store: &Arc<WedbStore<SegmentedDevice>>, kvs: &[(String, String)]) {
  for (key, value) in kvs {
    let got = store_read(store, key.as_bytes()).await;
    assert_eq!(got.as_deref(), Some(value.as_bytes()), "键 {key} 值须一致");
  }
}

/// 逐键缺席校验（空库待全量断言面）
async fn assert_store_lacks(store: &Arc<WedbStore<SegmentedDevice>>, kvs: &[(String, String)]) {
  for (key, _) in kvs {
    assert!(
      store_read(store, key.as_bytes()).await.is_none(),
      "冷重启空库形态键 {key} 不得在场"
    );
  }
}

/// INFO replication 装配面取字段（get_replication_info 为 INFO replication 段
/// 真身，消费面 wnode/resp/info_provider.rs）
fn repl_info_field(provider: &ClusterProvider, name: &str) -> String {
  provider
    .get_replication_info()
    .iter()
    .find(|i| i.name.as_ref() == name)
    .unwrap_or_else(|| panic!("INFO replication 缺少字段 {name}"))
    .value
    .clone()
}

/// 副本 --recover 同目录重启恒本地续用（attach 前）＋不带 --recover 空库对照
///
/// 一世代：双节点真 TCP 拓扑建立复制（REPLICAOF → 主端 SET → 副本重放追平）；
/// 副本纯刷盘停机（记停机前本地尾）。
/// 二世代（锁形主体）：副本同目录 `--recover` 重启——数据面恢复由
/// open_from_args 角色无关前置完成，attach 未发起态下即断言：本地键引擎直读
/// 可读、INFO replication `master_repl_offset` 等于本地重放尾（非零回填）、
/// role:slave 且复制流未建立（主端未收到任何 attach）。C# 默认部署
///（ClusterReplicaResumeWithData=false）此窗为空库、位点 0——本锁钉死 rust
/// 刻意改良形（等效置 true 恒开），严禁未重构 boot 装配序按 C# 门控回改。
/// 三世代（对照组）：同目录不带 `--recover` 冷重启 → 空库待全量、
/// recovered_aof_tail 不点亮。
#[compio::test]
async fn replica_recover_restart_resumes_locally_before_attach() -> Void {
  // ===== 一世代：双节点冷起服 + 互指拓扑 + REPLICAOF 建立复制
  let pdir = tempfile::tempdir()?;
  let rdir = tempfile::tempdir()?;
  let rdata = rdir.path().join("node.db");
  let cold = NodeArgs::from_args_iter(["wedb", "--aof"]).expect("冷启动旗标解析");
  assert!(!cold.recover && cold.aof, "一世代为 (false, true) 臂");

  let primary = ClusterProvider::new();
  let (pserver, psp, pport) = boot_node(
    &cold,
    &pdir.path().join("node.db"),
    &primary,
    cluster_decorate(Arc::clone(&primary)),
  )
  .await?;
  let replica = ClusterProvider::new();
  let (rserver, rsp, rport) = boot_node(
    &cold,
    &rdata,
    &replica,
    cluster_decorate(Arc::clone(&replica)),
  )
  .await?;
  {
    let cm = primary.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(&mut config, PRIMARY_ID, pport, NodeRole::Primary, None);
    assign_all_slots(&mut config);
    config.workers.push(remote_worker(REPLICA_ID, rport));
  }
  {
    let cm = replica.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(&mut config, REPLICA_ID, rport, NodeRole::Primary, None);
    config.workers.push(remote_worker(PRIMARY_ID, pport));
  }
  let p_endpoint = format!("127.0.0.1:{pport}");
  let r_endpoint = format!("127.0.0.1:{rport}");
  assert_eq!(
    client_roundtrip(&r_endpoint, &["REPLICAOF", "127.0.0.1", &pport.to_string()]).await?,
    "OK",
    "REPLICAOF 应成功"
  );
  let rm = replica.replication_manager().expect("rm ready");
  assert!(
    wait_for(|| rm.has_active_replication_stream(), SYNC_TIMEOUT).await,
    "复制流应在 REPLICAOF 后建立"
  );

  // ===== 主端写入 → 副本重放追平（副本键经复制流落盘，非本地写）
  let kvs = kv_batch("resume", KV_COUNT);
  for (key, value) in &kvs {
    assert_eq!(
      client_roundtrip(&p_endpoint, &["SET", key, value]).await?,
      "OK"
    );
  }
  psp.aof().expect("aof").log().commit_async().await;
  let tail1 = psp.wal().expect("wal").tail_address();
  assert!(
    wait_for(
      || rm.get_replication_offset(0) >= tail1 as i64,
      SYNC_TIMEOUT
    )
    .await,
    "初始数据位点应追平"
  );
  assert_store_has(&replica.try_store().expect("store 在位"), &kvs).await;

  // ===== 副本纯刷盘停机（副本 AOF 为主端流严格镜像，严禁本地 commit 帧）
  let shutdown_tail = {
    rsp
      .aof()
      .expect("aof")
      .log()
      .commit_flush_only_async()
      .await;
    let tail = rsp.wal().expect("wal").tail_address();
    assert_eq!(tail, tail1, "副本日志尾应与主端追平");
    tail
  };
  rserver.dispose();
  drop(rsp);
  drop(replica);
  drop(rm);

  // ===== 二世代：同目录 --recover 重启（open_from_args (true, true) 臂，
  // 角色此刻尚不可知——恢复先于 initialize_cluster_config，boot.rs 同序）
  let resume = NodeArgs::from_args_iter(["wedb", "--aof", "--recover"]).expect("恢复旗标解析");
  assert!(resume.recover && resume.aof, "二世代为 (true, true) 臂");
  let replica2 = ClusterProvider::new();
  let (rserver2, rsp2, rport2) = boot_node(
    &resume,
    &rdata,
    &replica2,
    cluster_decorate(Arc::clone(&replica2)),
  )
  .await?;
  {
    let cm = replica2.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(
      &mut config,
      REPLICA_ID,
      rport2,
      NodeRole::Replica,
      Some(PRIMARY_ID),
    );
    config.workers.push(remote_worker(PRIMARY_ID, pport));
  }

  // ===== attach 未发起态锁断言（不 REPLICAOF、无 ensure_replication 驱动）
  let rm2 = replica2.replication_manager().expect("rm2 ready");
  assert!(
    !rm2.has_active_replication_stream(),
    "断言窗口内 attach 必须尚未发起"
  );
  // a) 本地键 attach 前即引擎直读可读（C# 默认部署此窗为空库）
  assert_store_has(&replica2.try_store().expect("store2 在位"), &kvs).await;
  // b) 恢复尾回填锁：recovered_aof_tail 点亮且严格等于停机前本地重放尾，
  // INFO replication 位点（gossip 广播与 failover 判定基线）非零续用
  let recovered = rsp2
    .recovered_aof_tail()
    .expect("恢复臂点亮 recovered_aof_tail");
  let recovered_tail0 = recovered.get(0).expect("单槽地址");
  assert_eq!(
    recovered_tail0, shutdown_tail as i64,
    "恢复尾须严格等于停机前本地尾（角色无关前置恢复的直证）"
  );
  assert_eq!(
    rm2.get_current_replication_offset().get(0),
    Some(recovered_tail0),
  );
  assert_eq!(
    repl_info_field(&replica2, "master_repl_offset"),
    recovered.to_aof_string(),
    "INFO replication 位点应为本地重放尾（C# 默认为 0）"
  );
  assert_ne!(recovered_tail0, 0, "本地尾非零方能区分空库形");
  assert_eq!(repl_info_field(&replica2, "role"), "slave");

  // ===== 三世代前收口：二世代纯刷盘停机并释放同路径句柄
  rserver2.dispose();
  drop(rsp2);
  drop(replica2);
  drop(rm2);

  // ===== 对照组：同目录不带 --recover 冷重启 → 空库待全量（boot.rs 冷臂
  // 不执行检查点/AOF 恢复，recovered_aof_tail 不点亮，位点基线为 0）
  let replica3 = ClusterProvider::new();
  let (rserver3, rsp3, rport3) = boot_node(
    &cold,
    &rdata,
    &replica3,
    cluster_decorate(Arc::clone(&replica3)),
  )
  .await?;
  {
    let cm = replica3.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(
      &mut config,
      REPLICA_ID,
      rport3,
      NodeRole::Replica,
      Some(PRIMARY_ID),
    );
    config.workers.push(remote_worker(PRIMARY_ID, pport));
  }
  assert!(
    rsp3.recovered_aof_tail().is_none(),
    "冷臂不得点亮 recovered_aof_tail"
  );
  assert_store_lacks(&replica3.try_store().expect("store3 在位"), &kvs).await;

  pserver.dispose();
  rserver3.dispose();
  Ok(())
}
