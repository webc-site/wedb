//! CLUSTER RESET HARD 中断无盘 attach 集成测试
//!
//! 对标 C# test/cluster/Garnet.test.cluster.replication/ReplicationTests/
//! ClusterResetDuringReplicationTests.cs:ClusterResetHardDuringDisklessReplicationAttach
//! （diskbased 臂未落：rust 无盘同步为唯一同步路径，diskbased 支已删，行为
//! 等价由本 diskless 臂单独承担）。
//!
//! rust 侧机制形态差异（行为等价设计）：C# 经异常注入把主端停在
//! ReplicationManager.TryBeginDisklessSync 等待放行；rust 无进程内注入点，
//! 改以「假主端监听口」承接 attach——副本 attach 体连上假端并发出
//! CLUSTER ATTACH_SYNC 后停泊在应答窗（wait_async(repl_attach_timeout)），
//! attach 确定性在途。随后三段断言：
//! 1. RECOVER_STATUS = ClusterReplicate（对标 GetReplicationInfo 首断言）；
//! 2. CLUSTER RESET HARD（try_reset 单机基面，参考
//!    wedb/tests/cluster_management.rs:cluster_reset_test）：换新节点 id、
//!    epoch 归零、角色回 master、恢复态归 NoRecovery；
//! 3. 放行假主端回 -ERR：attach 会话被取消（错误上收），迟到的收尾不得
//!    复活恢复态与副本角色（end_recovery 状态矩阵自 NoRecovery 拒绝）；
//!    节点可立即重新入簇：恢复锁可重取 + 真主端全量同步一次通过并收敛。

use std::net::SocketAddr;

use compio::time::sleep;
use wedb_test::node_storage::{open_node, provider_with_role};

#[path = "common/replica_host.rs"]
mod replica_host;
use replica_host::replica_host;
use wedb_test::replica_attach::attach_replica_session;

#[path = "common/diskless_sync_kick.rs"]
mod diskless_sync_kick;
use std::{
  num::NonZeroUsize,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use aok::{OK, Void};
use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::spawn,
};
use diskless_sync_kick::try_full_sync;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    assembly::try_replicate_sync_async, recovery_status::RecoveryStatus,
    replicate_sync_options::ReplicateSyncOptions,
  },
  worker::{LOCAL_WORKER_ID, NodeRole, Worker},
};
use wedb_test::resp_frame_args::try_parse_frame_args;
use wnode::StorageSession;
use wtest_base::wait_for;

/// 测试节点身份（内部 u128）
const PRIMARY_ID: u128 = 0x0DE2_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE2_0000_0000_0000_0000_0000_0000_0002;

/// 重入簇阶段预置键值（全量同步收敛判据）
const REJOIN_KEY: &[u8] = b"rejoin:key";
const REJOIN_VAL: &[u8] = b"rejoin-val";

/// 解析监听地址的端口号
fn port_of(addr: SocketAddr) -> i32 {
  addr.port() as i32
}

/// 假主端：accept 一条连接，逐帧解析应答——握手帧（AUTH/CLIENT SETINFO，
/// wedb::client 门面建连即发）直答 +OK；CLUSTER ATTACH_SYNC 帧到达即置位
/// frame_seen 并停泊至 release 置位再回错误行——attach 由此确定性停泊在
/// ATTACH_SYNC 应答窗（握手帧早于 attach 帧，语义不可混判）
async fn serve_fake_primary(
  listener: TcpListener,
  frame_seen: Arc<AtomicBool>,
  release: Arc<AtomicBool>,
) {
  let (mut sock, _) = listener.accept().await.expect("假主端接受连接失败");
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; 4096];
  loop {
    let BufResult(res, next) = sock.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => return,
    };
    acc.extend_from_slice(&buf[..n]);
    while let Some((frame_len, args)) = try_parse_frame_args(&acc) {
      let is_attach = args.len() >= 2
        && args[0].eq_ignore_ascii_case(b"CLUSTER")
        && args[1].eq_ignore_ascii_case(b"ATTACH_SYNC");
      acc.drain(..frame_len);
      if is_attach {
        // 吞 ATTACH_SYNC 帧：attach 已抵达应答等待窗
        frame_seen.store(true, Ordering::Release);
        // 停泊至放行（async 轮询让渡，不冻线程）
        while !release.load(Ordering::Acquire) {
          sleep(Duration::from_millis(10)).await;
        }
        let BufResult(res, _) = sock
          .write_all(b"-ERR attach cancelled by cluster reset\r\n".to_vec())
          .await;
        debug_assert!(res.is_ok(), "假主端错误行应写达（对端在应答窗内）");
        return;
      }
      // 握手帧（AUTH / CLIENT SETINFO / SETNAME）：+OK 续泵
      let BufResult(res, _) = sock.write_all(b"+OK\r\n".to_vec()).await;
      if res.is_err() {
        return;
      }
    }
  }
}

/// 复制源地址簿装配：本地 worker 挂复制源（attach 体反查端点用）、初态
/// 翻回主角色（C# 节点初态 master；CLUSTER REPLICATE 前置门要求主角色）、
/// 推入主端 worker 条目（try_add_replica 目标主门 + 端点解析同源）
fn wire_replica_posture(provider: &Arc<ClusterProvider>, primary_port: i32) {
  let m = provider.cluster_manager().expect("集群管理器在位");
  let mut config = m.current_config.write();
  config.workers[LOCAL_WORKER_ID].role = NodeRole::Primary;
  config.workers.push(Worker {
    nodeid: Some(PRIMARY_ID),
    address: "127.0.0.1".into(),
    port: primary_port,
    config_epoch: 1,
    role: NodeRole::Primary,
    ..Worker::default()
  });
}

#[compio::test]
async fn cluster_reset_hard_cancels_in_flight_diskless_attach() -> Void {
  // ===== 副本端装配：无盘同步发起形态（攒批窗关窗），复制源指向假主端 =====
  let node_r = open_node("reset_attach_replica");
  let provider_r = provider_with_role(
    &node_r,
    REPLICA_ID,
    7001,
    NodeRole::Replica,
    PRIMARY_ID,
    false,
    Some(0),
  );
  // 无盘同步开关（对标 C# CreateInstances(enableDisklessSync: true)：选路单点
  // try_replicate_sync_async 据此走 diskless attach 支）
  provider_r.set_replica_diskless_sync(true);
  let fake_listener = TcpListener::bind("127.0.0.1:0").await?;
  let fake_port = port_of(fake_listener.local_addr()?);
  let frame_seen = Arc::new(AtomicBool::new(false));
  let release = Arc::new(AtomicBool::new(false));
  spawn(serve_fake_primary(
    fake_listener,
    Arc::clone(&frame_seen),
    Arc::clone(&release),
  ))
  .detach();
  wire_replica_posture(&provider_r, fake_port);
  attach_replica_session(&provider_r, &node_r.wal);
  let rm_r = provider_r
    .replication_manager()
    .expect("副本复制管理器在位");

  // ===== attach 进行中：前台发起，停泊在 ATTACH_SYNC 应答窗 =====
  // （对标 C# ClusterReplicate(async: true) + 注入停泊；前台臂取回 attach
  // 结果供取消断言，Background 臂只记日志不可断言）
  let attach_provider = Arc::clone(&provider_r);
  let attach = spawn(async move {
    try_replicate_sync_async(
      &attach_provider,
      ReplicateSyncOptions::new(PRIMARY_ID, false, false, true, true, false),
    )
    .await
  });
  assert!(
    wait_for(
      || frame_seen.load(Ordering::Acquire),
      Duration::from_secs(5)
    )
    .await,
    "attach 须已抵达 CLUSTER ATTACH_SYNC 发送点（假主端未收到帧）"
  );
  // 首帧已被假主端吞下：attach 任务此刻停泊在应答等待窗，确定性在途
  assert!(rm_r.is_recovering(), "attach 在途期间节点应处于恢复中状态");
  assert_eq!(
    rm_r.recovery_status(),
    RecoveryStatus::ClusterReplicate,
    "RECOVER_STATUS 应为 ClusterReplicate（对标 C# 首断言）"
  );
  assert_eq!(
    provider_r
      .cluster_manager()
      .unwrap()
      .current_config
      .read()
      .local_node_role(),
    NodeRole::Replica,
    "attach 在途期间角色应为 Replica"
  );

  // ===== CLUSTER RESET HARD（try_reset 单机基面，cluster_management.rs 同形）=====
  let m_r = provider_r.cluster_manager().unwrap();
  let old_id = m_r.current_config.read().local_node_id().unwrap();
  {
    let session = node_r.store.new_session().expect("复位只读会话");
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    m_r
      .try_reset(false, 60, &storage)
      .await
      .expect("空存储节点 CLUSTER RESET HARD 应成功");
  }
  {
    let config = m_r.current_config.read();
    let new_id = config.local_node_id().unwrap();
    assert_ne!(new_id, old_id, "HARD 复位必须换新节点 id");
    assert_eq!(
      config.local_node_config_epoch(),
      0,
      "HARD 复位后 config epoch 应归零"
    );
    assert_eq!(
      config.local_node_role(),
      NodeRole::Primary,
      "复位后角色应回 master（对标 C# RoleCommand 断言）"
    );
    assert_eq!(
      config.local_node_primary_id(),
      None,
      "复位后不得残留复制源指向"
    );
  }
  assert_eq!(
    rm_r.recovery_status(),
    RecoveryStatus::NoRecovery,
    "复位后 RECOVER_STATUS 应为 NoRecovery（对标 C# 二断言）"
  );

  // ===== 放行假主端：attach 会话被取消，迟到收尾不得复活副本态 =====
  release.store(true, Ordering::Release);
  let result = attach.await.expect("attach 任务不应 panic");
  let Err(attach_err) = result else {
    panic!("被 CLUSTER RESET HARD 取消的 attach 必须以失败收场，实际: {result:?}");
  };
  let attach_err_text = attach_err.to_string();
  assert!(
    attach_err_text.contains("attach cancelled"),
    "attach 失败因由应为假主端的取消错误行，实际: {attach_err_text}"
  );
  assert_eq!(
    rm_r.recovery_status(),
    RecoveryStatus::NoRecovery,
    "迟到的 attach 收尾不得复活恢复态（end_recovery 自 NoRecovery 被状态矩阵拒绝）"
  );
  {
    let config = m_r.current_config.read();
    assert_eq!(
      config.local_node_role(),
      NodeRole::Primary,
      "attach 失败回滚臂不得把已复位节点翻回副本态"
    );
    assert_eq!(config.local_node_primary_id(), None, "复制源指向不得复活");
  }

  // ===== 节点可立即重新入簇：恢复锁可重取 =====
  assert!(
    rm_r.begin_recovery(RecoveryStatus::ClusterReplicate, false),
    "复位后恢复锁必须立即可重取（无残留持锁）"
  );
  rm_r.reset_recovery();

  // ===== 真主端全量同步重入簇：一次通过并收敛 =====
  // 生产路径前置（对标 CLUSTER MEET + CLUSTER REPLICATE）：重推主端地址簿
  // （复位已截去远端 worker 条目）并经 try_add_replica_async 翻回副本态——
  // 复制承接面（APPENDLOG / ATTACH_SYNC 应答臂）以配置副本角色为门
  {
    let m = provider_r.cluster_manager().unwrap();
    let mut config = m.current_config.write();
    config.workers.push(Worker {
      nodeid: Some(PRIMARY_ID),
      address: "127.0.0.1".into(),
      port: 7100,
      config_epoch: 1,
      role: NodeRole::Primary,
      ..Worker::default()
    });
  }
  provider_r
    .cluster_manager()
    .unwrap()
    .try_add_replica_async(PRIMARY_ID, false, false)
    .await
    .expect("复位后的节点必须能立即重新登记为副本");
  assert_eq!(
    rm_r.recovery_status(),
    RecoveryStatus::ClusterReplicate,
    "重新登记后恢复点应转 ClusterReplicate（attach 在途常态）"
  );
  let source = open_node("reset_attach_primary");
  let provider_p = provider_with_role(
    &source,
    PRIMARY_ID,
    7100,
    NodeRole::Primary,
    PRIMARY_ID,
    true,
    Some(0),
  );
  {
    let session = source.store.new_session().expect("预置键会话");
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    storage
      .upsert_string(REJOIN_KEY, REJOIN_VAL)
      .await
      .expect("预置键写入");
  }
  let (_server, replica_addr) = replica_host(&provider_r, Some(NonZeroUsize::MIN));
  let (granted, assets) = try_full_sync(
    &provider_p,
    &source,
    &replica_addr,
    PRIMARY_ID,
    REPLICA_ID,
    &rm_r,
    Some("复位后的节点必须能立即重新入簇（全量同步发起成功）"),
  )
  .await;
  assert!(
    granted.get(0).is_some(),
    "复位后的节点必须拿到主端授予位点（全量同步协商在位）"
  );
  // 全量快照面：预置键必须落上复位后的节点
  {
    let session = provider_r
      .try_store()
      .expect("复位节点存储在位")
      .new_session()
      .expect("重入簇读会话");
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    assert_eq!(
      storage.read_string(REJOIN_KEY).await.unwrap().as_deref(),
      Some(REJOIN_VAL),
      "重入簇后预置键必须经全量同步落在复位后的节点上"
    );
  }
  // 增量衔接面：位点追平主端日志尾
  source.wal.commit().await.expect("主端日志提交");
  let _ = assets.pump.sync_backlog(&source.wal).await;
  let new_tail = source.wal.tail_address() as i64;
  assert!(
    wait_for(
      || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
      Duration::from_secs(10),
    )
    .await,
    "重入簇后副本复制位点必须追平主端日志尾"
  );
  assert_eq!(
    rm_r.recovery_status(),
    RecoveryStatus::CheckpointRecoveredAtReplica,
    "重入簇完成后恢复点应转 CheckpointRecoveredAtReplica（C# attach 收尾同态）"
  );
  _server.dispose();
  OK
}
