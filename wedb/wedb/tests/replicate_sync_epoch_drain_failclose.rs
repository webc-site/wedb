//! CLUSTER REPLICATE / REPLICAOF 复制发起骨架纪元排空栅栏返值承判收口回归测试
//! （票 wedb-cluster-replicate-sync-epoch-drain-return-ignored）。
//!
//! 背景（对标 garnet C# 一手锚）：发起骨架
//! [`assembly::replicate_sync_async`](wedb::server::replication::assembly) 前置
//! 第 3 步 `bump_and_wait_for_epoch_transition_async` 原语在 C# 结构上恒真
//! （ClusterProvider.cs:366-389 while(true)+goto retry 无限自旋至全部
//! ActiveClusterSessions 追平），ReplicaDiskbasedSync.cs:52-55 /
//! ReplicaDisklessSync.cs:46-49「Wait for threads to agree」位置在 TryAddReplica
//! 与换代之后、attach 发起之前，且该 await 抛异常必经 catch(TryResetReplica)
//! → finally(EndRecovery) 收尾。rust 已以 cluster_node_timeout() 有界化
//! （cluster_provider/checkpoint.rs:54-66，追平判定 :90-111 只枚举
//! cluster_sessions 弱引用表、批外快照 0 放行），false = 静止未达成。
//!
//! 弃返值即令未静止 attach：滞留会话 bump 前的在途写批与破坏性段（attach 无
//! C# 清库臂，实序为检查点导入 + swap_online_store 引擎置换 + AOF 代际衔接）
//! 赛跑，已 ACK 写丢失或主从全序分叉。本票承判收口：判败一律经收尾族
//! `finish_replica_sync` 构造返回，触发其 catch 臂（allow_replica_reset_on_failure
//! 时 try_reset_replica + resume_primary_tasks）与 finally 臂（release_attach_recovery
//! 释放恢复锁）——裸 return Err 会恢复锁永持、已翻 REPLICA 不复位，后续恢复/
//! 重连被 ERR_RECOVERY_LOCK 拒死无自愈（本测试锁释放与角色复位两面直打此不变式）。
//!
//! 恒不追平夹具复用 diskless / failover_epoch_drain_failclose.rs 形态：注册会话
//! 先行批首纪元快照（acquire_current_epoch），其后原语自 bump 恒落后，
//! set_cluster_node_timeout_ms 极小值令超时即刻达。
//!
//! 反证基线（revert-proof）：
//! - 还原 :338 裸语句弃返值 → 骨架照常进第 4 步 attach，主端记录到出站帧
//!   （egress 断言转红）、try_replicate 返回 Ok（expect_err 转红）而非排空判败帧；
//! - 还原为裸 return Err（绕收尾族）→ 恢复锁不释放（NoRecovery / 可再取锁断言
//!   转红）、角色滞留 REPLICA（复位断言转红）。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  runtime::spawn,
};
use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    assembly::try_replicate_diskbased_sync_async, recovery_status::RecoveryStatus,
    replicate_sync_options::ReplicateSyncOptions,
  },
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::ClusterSessionFace;
use wtest_base::test_store_config;

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A2;

/// 排空未达成注入档：极小值令静止等待超时即刻达
const DRAIN_TIMEOUT_MS: u64 = 50;

/// 主端出站记录靶：接受连接并对任意帧回 +OK，一旦收到任何字节即置位 received。
/// 承判修复下本靶永不被连（attach 从未发起）；revert 弃返值下 attach 会连此端口
/// 发 CLUSTER INITIATE_REPLICA_SYNC，received 即真。
async fn bind_recording_primary(received: Arc<AtomicBool>) -> u16 {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = listener.local_addr().unwrap().port();
  spawn(async move {
    while let Ok((stream, _)) = listener.accept().await {
      let flag = Arc::clone(&received);
      spawn(async move {
        serve(stream, flag).await;
      })
      .detach();
    }
  })
  .detach();
  port
}

/// 单连接服务：读到任何字节即置 received，逐帧回 +OK（握手/发起帧一律应答）
async fn serve(mut stream: TcpStream, received: Arc<AtomicBool>) {
  let mut buf = vec![0u8; 4096];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let Ok(n) = res else { return };
    if n == 0 {
      return;
    }
    received.store(true, Ordering::Release);
    if stream.write_all(b"+OK\r\n").await.is_err() {
      return;
    }
  }
}

/// 纪元排空超时判败收口：恒不追平夹具令发起骨架第 3 步判败 →
/// - 经收尾族回「Failed to initiate replica sync: epoch drain not settled」；
/// - 主端零出站帧（attach 破坏性段从未发起，非跑到 attach 后才失败）；
/// - 恢复锁经 finish_replica_sync 正常释放（NoRecovery、可再取锁不被 ERR_RECOVERY_LOCK 拒）；
/// - allow_replica_reset_on_failure 臂角色复位回 Primary。
#[compio::test]
async fn replicate_sync_epoch_drain_failure_finishes_via_cleanup_family() {
  // ===== 副本装配（rm + store + wal 接线，角色 REPLICA 挂靠主端，主端 endpoint
  //     指向记录靶——attach 若被放行即会连该端口发帧）
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("drain.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let wal_device = Arc::new(SegmentedDevice::single_file(dir.path().join("drain.wal")).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());
  provider.set_store(Arc::clone(&store));
  provider.set_wal(Arc::clone(&wal));

  let received = Arc::new(AtomicBool::new(false));
  let primary_port = bind_recording_primary(Arc::clone(&received)).await;

  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: REPLICA_ID,
    address: "127.0.0.1",
    port: 7120,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(PRIMARY_ID),
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(PRIMARY_ID),
    address: "127.0.0.1".into(),
    port: primary_port as i32,
    config_epoch: 3,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(Arc::clone(&cm));

  // ===== 恢复锁握持前置（对标三个前台驱动点经 try_add_replica_async 成功后握
  //     ClusterReplicate；启动臂经 begin_recovery(InitializeRecover)——本面取
  //     ClusterReplicate 同锁形，opts.try_add_replica=false 让骨架不重复取锁）
  let rm = provider.replication_manager().expect("rm 在场");
  assert!(
    rm.begin_recovery(RecoveryStatus::ClusterReplicate, false),
    "起点须先握到 ClusterReplicate 恢复锁（NoRecovery 起点）"
  );

  // ===== 不追平夹具：注册会话先行批首纪元快照，其后原语自 bump 恒落后 →
  //     排空等待判定 entry_epoch < current_epoch 恒成立、超时即刻达
  provider.bump_current_epoch();
  let lag = provider.create_cluster_session();
  lag.acquire_current_epoch();
  assert_eq!(lag.local_current_epoch(), provider.current_epoch());
  provider.set_cluster_node_timeout_ms(DRAIN_TIMEOUT_MS);

  // ===== 直驱发起骨架（diskbased 支，attach 体 = recover_replication）
  //     opts: node_id / background / force / try_add_replica /
  //           allow_replica_reset_on_failure / upgrade_lock
  let opts = ReplicateSyncOptions::new(PRIMARY_ID, false, false, false, true, false);
  let err = try_replicate_diskbased_sync_async(&provider, opts)
    .await
    .expect_err("纪元排空未达成必须判败上抛，不得带病 attach");

  // ===== 断言 1：判败经收尾族回排空未达成文案（非 attach 的 to-primary 文案）
  let err = err.to_string();
  assert!(
    err.starts_with("Failed to initiate replica sync")
      && err.contains("epoch drain not settled within cluster-node-timeout"),
    "判败文案须指认排空未达成且系发起前置判败: {err}"
  );

  // ===== 断言 2：主端零出站帧——attach 破坏性段从未发起
  assert!(
    !received.load(Ordering::Acquire),
    "排空未达成即判败：不得连主发 CLUSTER INITIATE_REPLICA_SYNC（零出站帧）"
  );

  // ===== 断言 3：恢复锁经 finish_replica_sync 的 finally 臂释放到 NoRecovery，
  //     随后再发起取锁不被 ERR_RECOVERY_LOCK 拒（裸 return Err 即永持锁、此处取不到）
  assert_eq!(
    rm.recovery_status(),
    RecoveryStatus::NoRecovery,
    "收尾族 finally 臂须已释放恢复锁"
  );
  assert!(
    rm.begin_recovery(RecoveryStatus::ClusterReplicate, false),
    "判败后锁须已释放，重取 ClusterReplicate 不得被拒（ERR_RECOVERY_LOCK 路径）"
  );
  rm.end_recovery(RecoveryStatus::NoRecovery, false);

  // ===== 断言 4：allow_replica_reset_on_failure 臂角色经 try_reset_replica 复位回 Primary
  {
    let config = cm.current_config.read();
    assert_eq!(
      config.local_node_role(),
      NodeRole::Primary,
      "判败回滚臂须把已翻 REPLICA 的角色复位回 Primary"
    );
    assert_eq!(config.local_node_primary_id(), None, "复位后主指针须清空");
  }

  // 夹具会话落账释放：判败收口不残留注册面泄漏（死弱引用枚举时自清扫）
  drop(lag);
  provider.set_cluster_node_timeout_ms(2_000);
  assert!(
    provider.bump_and_wait_for_epoch_transition_async().await,
    "夹具释放后排空等待须恢复放行（注册面无残留锁死静止判定）"
  );
}
