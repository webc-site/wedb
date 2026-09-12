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
use compio::{runtime::Runtime, time::sleep};
use parking_lot::Mutex;
use tempfile::{TempDir, tempdir};
use waof::{AofAddress, WalConfig, WalLog, WalScanIterator};
use wdev::{Device, SegmentedDevice};
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    ReplicaReplayHook,
    aof_replication_pump::AofReplicationPump,
    aof_sync_driver::AofSyncDriver,
    aof_sync_driver_store::AofSyncDriverStore,
    cluster_replication_session::ClusterReplicationSession,
    replica_wire::{AofSyncWire, CallbackWire, TcpSessionWire},
  },
  worker::{LocalWorkerSpec, NodeRole},
};
use wnode::{GarnetServer, MessageConsumerFace, SessionProviderFace, WireFormat};

/// 头区占位记录负载（8B 头 + 56B 负载 = 64B，复制域 [0,64) 头区基线）
const HEAD_PAD_PAYLOAD: usize = 56;
/// 首条业务记录地址
const FIRST_RECORD_ADDR: u64 = 64;

struct MockReplayTask {
  replayed_offset: Arc<AtomicI64>,
}

impl ReplicaReplayHook for MockReplayTask {
  fn on_records_persisted(
    &self,
    _physical_sublog_idx: usize,
    _current_address: i64,
    next_address: i64,
  ) {
    self.replayed_offset.store(next_address, Ordering::Release);
  }
}

type NodeParts = (TempDir, Arc<WalLog<SegmentedDevice>>, Arc<MockReplayTask>);

/// 装配单节点 WAL 日志与重放钩子
fn open_wal_node(tag: &str) -> Result<NodeParts> {
  let dir = tempdir()?;
  let wal_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.wal")),
  )?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  wal.enqueue(&[0u8; HEAD_PAD_PAYLOAD])?;
  let task = Arc::new(MockReplayTask {
    replayed_offset: Arc::new(AtomicI64::new(FIRST_RECORD_ADDR as i64)),
  });
  Ok((dir, wal, task))
}

/// 装配副本角色 provider（REPLICA of primary_1）
fn replica_provider(node_id: &str, primary_id: &str) -> Arc<ClusterProvider> {
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

/// 轮询等待条件成立（10ms 步进；超时返回 false）
async fn wait_for(cond: impl Fn() -> bool, timeout: Duration) -> bool {
  let start = coarsetime::Instant::now();
  while !cond() {
    if start.elapsed() > timeout.into() {
      return false;
    }
    sleep(Duration::from_millis(10)).await;
  }
  true
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
      iter
        .collect_all()
        .await
        .expect("日志扫描不可失败")
        .into_iter()
        .map(|r| (r.address, r.reconstruct_frame()))
        .collect::<Vec<_>>()
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
  wire: Arc<dyn AofSyncWire>,
) -> (
  Arc<AofSyncDriverStore>,
  Arc<AofSyncDriver>,
  Arc<AofReplicationPump>,
) {
  let store = Arc::new(AofSyncDriverStore::new(1));
  let driver = Arc::new(AofSyncDriver::new(
    "primary_1".to_string(),
    "replica_1".to_string(),
    &AofAddress::create(1, 64),
  ));
  driver.attach_wire(wire);
  assert!(store.try_add_replication_driver(driver.clone(), false));
  let pump = Arc::new(AofReplicationPump::new(Arc::clone(&store)));
  assert!(pump.attach_sink(pwal), "推流端口一次性挂载");
  (store, driver, pump)
}

/// 内存通道闭环：主端写入 → CallbackWire 帧投递 → 副本落盘 → 重放通知推进
#[test]
fn memory_wire_primary_to_replica_replay_consistency() -> Void {
  Runtime::new().unwrap().block_on(async {
    // ===== 副本装配：接收会话 + 重放钩子
    let (_rdir, rwal, replay_task) = open_wal_node("replica")?;
    let provider = replica_provider("replica_1", "primary_1");
    let replica_session = ClusterReplicationSession::new(
      Arc::clone(&provider),
      Arc::clone(&rwal),
      Some(replay_task.clone()),
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
    let seen = Arc::clone(&frames_seen);
    let session_for_wire = Arc::clone(&replica_session);
    let wire = Arc::new(CallbackWire::new(move |frame: &[u8]| {
      let (consumed, resp) = session_for_wire.try_consume_messages(frame);
      assert_eq!(consumed, frame.len(), "帧必须被完整消费");
      assert!(resp.is_empty(), "记录帧不回写应答（发出即忘）");
      *seen.lock() += 1;
      true
    }));

    let (_store, driver, pump) = assemble_primary_send(&pwal, wire);

    // ===== 补扫存量积压
    let (forwarded, skipped) = pump.sync_backlog(&pwal).await?;
    assert_eq!((forwarded, skipped), (2, 0), "积压 2 条全部补扫转发");
    assert_eq!(driver.get_previous_address(0) as u64, backlog_tail);

    // ===== 实时推流 1 条
    pwal.enqueue(b"payload_user_1003_carol")?;
    pwal.commit().await?;
    assert_eq!(
      driver.get_previous_address(0) as u64,
      pwal.tail_address(),
      "实时推流后主端已发位点追平日志尾"
    );
    assert_eq!(*frames_seen.lock(), 3, "2 补扫 + 1 实时帧全部送达");

    // ===== 副本落盘保真：位点与字节双一致
    assert_eq!(
      rwal.tail_address(),
      pwal.tail_address(),
      "副本日志尾必须与主端一致"
    );
    assert_wal_records_identical(&pwal, &rwal, 3).await;

    // ===== 重放通知追平
    let tail = pwal.tail_address() as i64;
    assert!(
      wait_for(
        || replay_task.replayed_offset.load(Ordering::Acquire) >= tail,
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
    let provider = replica_provider("replica_1", "primary_1");
    let replica_session = ClusterReplicationSession::new(
      Arc::clone(&provider),
      Arc::clone(&rwal),
      Some(replay_task.clone()),
    );

    struct SessionProvider(Mutex<Arc<ClusterReplicationSession>>);
    impl SessionProviderFace for SessionProvider {
      type Consumer = ClusterReplicationSession;
      fn get_session(
        &self,
        // 满足 SessionProviderFace trait 签名契约；测试场景无需区分线格式与网络发送端 ID
        _wire_format: WireFormat,
        _network_sender_id: u64,
      ) -> Option<Arc<ClusterReplicationSession>> {
        Some(Arc::clone(&self.0.lock()))
      }
    }
    let server = GarnetServer::new(
      &["127.0.0.1:0".to_string()],
      65536,
      100,
      Arc::new(SessionProvider(Mutex::new(
        Arc::clone(&replica_session),
      ))),
    );
    server.start(None)?;
    let addr = server.local_addr()?.to_string();

    // ===== 主端：attach 前积压 1 条
    let (_pdir, pwal, _pnode) = open_wal_node("primary")?;
    pwal.enqueue(b"payload_user_2001_dave")?;
    pwal.commit().await?;

    // ===== TCP 发送通道建连
    let wire = TcpSessionWire::connect(&addr, "primary_1", 0, None, None).await?;
    assert!(wire.is_connected(), "建连后发送通道健康");

    let (_store, driver, pump) = assemble_primary_send(&pwal, wire);

    // ===== 补扫 + 实时推流
    let (forwarded, skipped) = pump.sync_backlog(&pwal).await?;
    assert_eq!((forwarded, skipped), (1, 0));
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
    assert_wal_records_identical(&pwal, &rwal, 2).await;

    // ===== 重放通知追平
    assert!(
      wait_for(
        || replay_task.replayed_offset.load(Ordering::Acquire) >= tail,
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
