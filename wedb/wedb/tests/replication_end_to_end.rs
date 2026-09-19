//! 复制网络传输面端到端集成测试（主端写入 → 网络会话 → 副本落盘 → 重放通知）
//!
//! 对标 C# 全链路会话面：
//! - 主端发送：AofSyncTask.Consume → GarnetClientSession.ExecuteClusterAppendLog
//! - 副本接收：ClusterSession.NetworkClusterAppendLog → ReplicaReplaySession.
//!   ProcessPrimaryStream（UnsafeEnqueueRaw 保真落盘 + 异步重放通知）
//!
//! 两条通路：
//! 1. 内存通道（CallbackWire 帧回调直投副本会话，逐帧消费断言）；
//! 2. 真 socket（wnode GarnetServer 会话泵 + wconn TcpSessionWire，含 CLIENT
//!    握手 + APPENDLOG init 握手往返）。

use std::{
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  time::Duration,
};

use aok::{Result, Void};
use compio::runtime::Runtime;
use parking_lot::Mutex;
use tempfile::{TempDir, tempdir};
use waof::{AofAddress, WalConfig, WalLog, WalScanIterator};
use wdev::{Device, SegmentedDevice};
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    ReplicaReplayHook,
    aof_replication_pump::AofReplicationPump,
    aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
    cluster_replication_session::ClusterReplicationSession,
    replica_wire::{AofSyncWire, CallbackWire, FrameSink, TcpSessionWire},
  },
  worker::{LocalWorkerSpec, NodeRole},
};
use wnode::{GarnetServer, SessionProviderFace, WireFormat};
use wtest_base::wait_for;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 头区占位记录负载（8B 头 + 56B 负载 = 64B，复制域 [0,64) 头区基线）
const HEAD_PAD_PAYLOAD: usize = 56;
/// 首条业务记录地址
const FIRST_RECORD_ADDR: u64 = 64;

type NodeParts = (TempDir, Arc<WalLog<SegmentedDevice>>, Arc<AtomicI64>);

/// 装配单节点 WAL 日志与重放钩子
fn open_wal_node(tag: &str) -> Result<NodeParts> {
  let dir = tempdir()?;
  let wal_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.wal")),
  )?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  wal.enqueue(&[0u8; HEAD_PAD_PAYLOAD])?;
  let task = Arc::new(AtomicI64::new(FIRST_RECORD_ADDR as i64));
  Ok((dir, wal, task))
}

/// 装配副本角色 provider（REPLICA of primary_1）
fn replica_provider(node_id: u128, primary_id: u128) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  let cm = provider.cluster_manager().expect("cm ready");
  cm.try_initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(primary_id),
    hostname: None,
  });
  provider
}

/// 逐记录比对两份日志的记录帧序列（地址 + 字节级保真）
async fn assert_wal_records_identical<D: Device>(
  primary: &WalLog<D>,
  replica: &WalLog<D>,
  expect: usize,
) {
  let collect = |wal: &WalLog<D>| {
    let mut iter: WalScanIterator<D> = wal.scan(FIRST_RECORD_ADDR, wal.tail_address());
    async move {
      let mut records = Vec::new();
      while let Some(r) = iter.next().await.expect("日志扫描不可失败") {
        records.push((r.address, r.reconstruct_frame()));
      }
      records
    }
  };
  let pri = collect(primary).await;
  let rep = collect(replica).await;
  assert_eq!(pri.len(), expect, "主端业务记录数");
  assert_eq!(rep, pri, "副本记录帧必须与主端逐条字节一致（保真落盘）");
}

/// 主端 → 副本发送面装配（驱动入库 + 通道接线 + 推流泵挂载）
fn assemble_primary_send(
  pwal: &Arc<WalLog<SegmentedDevice>>,
  wire: impl Into<AofSyncWire>,
) -> (
  Arc<AofSyncDriverStore>,
  Arc<AofSyncDriver>,
  Arc<AofReplicationPump>,
) {
  let store = Arc::new(AofSyncDriverStore::new(1));
  let driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  driver.attach_wire(wire);
  assert!(store.try_add_replication_driver(driver.clone(), false));
  let pump = Arc::new(AofReplicationPump::new(Arc::clone(&store)));
  assert!(pump.attach_wake(pwal), "推流唤醒信号一次性挂载");
  (store, driver, pump)
}

/// 内存通道闭环：主端写入 → CallbackWire 帧投递 → 副本落盘 → 重放通知推进
#[test]
fn memory_wire_primary_to_replica_replay_consistency() -> Void {
  Runtime::new().unwrap().block_on(async {
    // ===== 副本装配：接收会话 + 重放钩子
    let (_rdir, rwal, replay_task) = open_wal_node("replica")?;
    let provider = replica_provider(REPLICA_ID, PRIMARY_ID);
    let replica_session = ClusterReplicationSession::new(
      Arc::clone(&provider),
      Arc::clone(&rwal),
      Some(ReplicaReplayHook::OffsetWatermark(replay_task.clone())),
    );

    // ===== 主端装配：attach 前积压 2 条业务记录
    let (_pdir, pwal, _pnode) = open_wal_node("primary")?;
    pwal.enqueue(b"payload_user_1001_alice")?;
    pwal.enqueue(b"payload_user_1002_bob")?;
    pwal.commit().await?;
    let backlog_tail = pwal.tail_address();
    assert!(backlog_tail > FIRST_RECORD_ADDR);

    // ===== 内存发送通道：帧回调直投副本会话
    let frames_seen = Arc::new(Mutex::new(0usize));
    let wire = Arc::new(CallbackWire::new(FrameSink::Session {
      session: Arc::new(Mutex::new(replica_session.clone())),
      seen: Some(Arc::clone(&frames_seen)),
    }));

    let (_store, driver, pump) = assemble_primary_send(&pwal, wire);

    // ===== 补扫存量积压
    let (forwarded, skipped) = pump.sync_backlog(&pwal).await?;
    // +1 = commit 元数据帧（推流面保真传输全部帧，从侧恢复据此收敛）
    assert_eq!(
      (forwarded, skipped),
      (2 + 1, 0),
      "积压 2 条与 commit 帧全部补扫转发"
    );
    assert_eq!(driver.get_previous_address(0) as u64, backlog_tail);

    // ===== 实时推流 1 条
    pwal.enqueue(b"payload_user_1003_carol")?;
    pwal.commit().await?;
    assert_eq!(
      driver.get_previous_address(0) as u64,
      pwal.tail_address(),
      "实时推流后主端已发位点追平日志尾"
    );
    // 3 补扫（2 数据 + 1 帧）+ 2 实时（1 数据 + 1 帧）全部送达
    assert_eq!(*frames_seen.lock(), 5, "补扫与实时帧全部送达");

    // ===== 副本落盘保真：位点与字节双一致
    assert_eq!(
      rwal.tail_address(),
      pwal.tail_address(),
      "副本日志尾必须与主端一致"
    );
    // 5 = 3 数据 + 2 commit 元数据帧（推流面保真传输全部帧）
    assert_wal_records_identical(&pwal, &rwal, 5).await;

    // ===== 重放通知追平
    let tail = pwal.tail_address() as i64;
    assert!(
      wait_for(
        || replay_task.load(Ordering::Acquire) >= tail,
        Duration::from_secs(5),
      )
      .await,
      "重放位点必须追平日志尾"
    );
    Ok(())
  })
}

/// 真 socket 闭环：wnode GarnetServer 会话泵 + TcpSessionWire（CLIENT 握手 +
/// APPENDLOG init 往返 + 逐记录帧传输），副本落盘后数据一致；断链后发送通道健康面转断连
#[test]
fn tcp_wire_end_to_end_over_real_socket() -> Void {
  Runtime::new().unwrap().block_on(async {
    // ===== 副本端：wnode 服务器挂接收会话
    let (_rdir, rwal, replay_task) = open_wal_node("replica")?;
    let provider = replica_provider(REPLICA_ID, PRIMARY_ID);
    let replica_session = ClusterReplicationSession::new(
      Arc::clone(&provider),
      Arc::clone(&rwal),
      Some(ReplicaReplayHook::OffsetWatermark(replay_task.clone())),
    );

    struct SessionProvider(ClusterReplicationSession<SegmentedDevice>);
    impl SessionProviderFace for SessionProvider {
      type Consumer = ClusterReplicationSession<SegmentedDevice>;
      fn get_session(
        &self,
        // 满足 SessionProviderFace trait 签名契约；测试场景无需区分线格式与网络发送端 ID
        _wire_format: WireFormat,
        _network_sender_id: u64,
      ) -> Option<ClusterReplicationSession<SegmentedDevice>> {
        Some(self.0.clone())
      }
    }
    let server = GarnetServer::new(
      &["127.0.0.1:0".to_string()],
      65536,
      100,
      Arc::new(SessionProvider(replica_session.clone())),
    )?;
    server.start(None)?;
    let addr = server.local_addr()?.to_string();

    // ===== 主端：attach 前积压 1 条
    let (_pdir, pwal, _pnode) = open_wal_node("primary")?;
    pwal.enqueue(b"payload_user_2001_dave")?;
    pwal.commit().await?;

    // ===== TCP 发送通道建连
    let wire = TcpSessionWire::connect(
      &addr,
      PRIMARY_ID,
      0,
      None,
      None,
      #[cfg(feature = "tls")]
      None,
    )
    .await?;
    assert!(wire.is_connected(), "建连后发送通道健康");

    let (_store, driver, pump) = assemble_primary_send(&pwal, wire);

    // ===== 补扫 + 实时推流
    let (forwarded, skipped) = pump.sync_backlog(&pwal).await?;
    // +1 = commit 元数据帧（推流面保真传输全部帧）
    assert_eq!((forwarded, skipped), (1 + 1, 0));
    pwal.enqueue(b"payload_user_2002_eve")?;
    pwal.commit().await?;

    // ===== 等 TCP 传输 + 副本落盘追平
    let tail = pwal.tail_address() as i64;
    assert!(
      wait_for(
        || rwal.tail_address() as i64 >= tail,
        Duration::from_secs(5)
      )
      .await,
      "副本日志尾必须经真 socket 追平主端"
    );
    assert_eq!(rwal.tail_address(), pwal.tail_address());
    // 4 = 2 数据 + 2 commit 元数据帧（推流面保真传输全部帧）
    assert_wal_records_identical(&pwal, &rwal, 4).await;

    // ===== 重放通知追平
    assert!(
      wait_for(
        || replay_task.load(Ordering::Acquire) >= tail,
        Duration::from_secs(5),
      )
      .await,
      "重放位点必须追平日志尾"
    );

    // ===== 断链感知：服务器停机后发送通道健康面转断连
    server.dispose();
    assert!(
      wait_for(|| !driver.is_connected(), Duration::from_secs(5)).await,
      "对端停机后发送通道必须感知断连（网络泵退出）"
    );
    Ok(())
  })
}
