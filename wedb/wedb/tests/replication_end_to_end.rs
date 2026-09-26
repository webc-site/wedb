//! 复制网络传输面端到端集成测试（主端写入 → 网络会话 → 副本落盘 → 重放通知）
//!
//! 对标 C# 全链路会话面：
//! - 主端发送：AofSyncTask.Consume → GarnetClientSession.ExecuteClusterAppendLog
//! - 副本接收：ClusterSession.NetworkClusterAppendLog → ReplicaReplaySession.
//!   ProcessPrimaryStream（UnsafeEnqueueRaw 保真落盘 + 异步重放通知）
//!
//! 真 socket 单一通路（wnode GarnetServer 会话泵 + wconn TcpSessionWire，
//! 含 CLIENT 握手 + APPENDLOG init 握手往返）；三条用例分别覆盖
//! 「attach 前积压补扫 + 实时推流」「断链感知」与
//! 「副本 flush-only 停机 → 重启 → 增量协商位点严格对齐」三个断言面。

use std::{
  num::NonZeroUsize,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  time::Duration,
};

use aok::{Result, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{AofAddress, WalConfig, WalLog, WalScanIterator};
use wbase::pool::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL, LimitedFixedBufferPool};
use wconf::RuntimeServerOptions;
use wdev::{Device, SegmentedDevice};
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    ReplicaReplayHook,
    aof_replication_pump::AofReplicationPump,
    aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
    cluster_replication_session::ClusterReplicationSession,
    replica_wire::{AofSyncWire, TcpSessionWire},
  },
  worker::{LocalWorkerSpec, NodeRole},
};
use wnode::{
  GarnetServer, SessionProviderFace, WireFormat, aof::waof_sublog::single_log_aof,
  primary_tasks::PrimaryTasks,
};
use wtest_base::wait_for;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 头区占位记录负载（8B 头 + 56B 负载 = 64B，复制域 [0,64) 头区基线）
const HEAD_PAD_PAYLOAD: usize = 56;
/// 首条业务记录地址
const FIRST_RECORD_ADDR: u64 = 64;

type NodeParts = (TempDir, Arc<WalLog<SegmentedDevice>>, Arc<AtomicI64>);

/// 单文件真段设备 WAL 打开口（不做任何写入；头区基线由调用方按需预置，
/// 副本重启重开臂走此口后以 recover 收敛位点）
fn wal_at(path: &Path) -> Result<Arc<WalLog<SegmentedDevice>>> {
  let wal_device = Arc::new(SegmentedDevice::single_file(path.to_path_buf())?);
  Ok(Arc::new(WalLog::new(wal_device, WalConfig::default())?))
}

/// 装配单节点 WAL 日志与重放钩子
fn open_wal_node(tag: &str) -> Result<NodeParts> {
  let dir = tempdir()?;
  let wal = wal_at(&dir.path().join(format!("{tag}.wal")))?;
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

/// 副本端会话供应器：GarnetServer 会话泵按消费面取会话
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

/// 装配真 socket 副本端服务器（GarnetServer 会话泵 + 接收会话），返回监听地址
fn replica_server(
  session: ClusterReplicationSession<SegmentedDevice>,
) -> aok::Result<GarnetServer<SessionProvider>> {
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    65536,
    100,
    Arc::new(SessionProvider(session)),
  )?;
  server.start(NonZeroUsize::new(1))?;
  Ok(server)
}

/// 主端 → 副本发送面装配（驱动入库 + 通道接线 + 推流泵挂载；
/// `replica_start` 为本轮协商的副本接续位点；`mount_wake` = 本轮是否首挂
/// 推流唤醒信号（对标生产：wake 随主端泵装配一次，副本重启重注册不重挂））
fn assemble_primary_send(
  pwal: &Arc<WalLog<SegmentedDevice>>,
  wire: impl Into<AofSyncWire>,
  replica_start: &AofAddress,
  mount_wake: bool,
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
    replica_start,
    None,
  ));
  driver.attach_wire(wire);
  assert!(store.try_add_replication_driver(driver.clone(), false));
  let pump = Arc::new(AofReplicationPump::new(Arc::clone(&store)));
  assert_eq!(pump.attach_wake(pwal), mount_wake, "推流唤醒信号一次性挂载");
  (store, driver, pump)
}

/// 积压补扫 + 实时推流闭环（真 socket）：主端写入 → TcpSessionWire →
/// 副本落盘 → 重放通知推进
#[compio::test]
async fn backlog_realtime_stream_over_real_socket() -> Void {
  // ===== 副本装配：真 socket 服务器挂接收会话 + 重放钩子
  let (_rdir, rwal, replay_task) = open_wal_node("replica")?;
  let provider = replica_provider(REPLICA_ID, PRIMARY_ID);
  let replica_session = ClusterReplicationSession::new(
    Arc::clone(&provider),
    Arc::clone(&rwal),
    Some(ReplicaReplayHook::OffsetWatermark(replay_task.clone())),
  );
  let server = replica_server(replica_session.clone())?;
  let addr = server.local_addr()?.to_string();

  // ===== 主端装配：attach 前积压 2 条业务记录
  let (_pdir, pwal, _pnode) = open_wal_node("primary")?;
  pwal.enqueue(b"payload_user_1001_alice")?;
  pwal.enqueue(b"payload_user_1002_bob")?;
  pwal.commit().await?;
  let backlog_tail = pwal.tail_address();
  assert!(backlog_tail > FIRST_RECORD_ADDR);

  // ===== TCP 发送通道建连（含 CLIENT 握手 + APPENDLOG init 往返）
  let wire = TcpSessionWire::connect(
    &addr,
    PRIMARY_ID,
    0,
    None,
    None,
    LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL),
    #[cfg(feature = "tls")]
    None,
  )
  .await?;
  assert!(wire.is_connected(), "建连后发送通道健康");

  let (_store, driver, pump) = assemble_primary_send(
    &pwal,
    wire,
    &AofAddress::create(1, FIRST_RECORD_ADDR as i64),
    true,
  );

  // ===== 补扫存量积压
  let (forwarded, skipped) = pump.sync_backlog(&pwal).await?;
  // +1 = commit 元数据帧（推流面保真传输全部帧，从侧恢复据此收敛）
  assert_eq!(
    (forwarded, skipped),
    (2 + 1, 0),
    "积压 2 条与 commit 帧全部补扫转发"
  );
  assert!(
    wait_for(
      || driver.get_previous_address(0) as u64 >= backlog_tail,
      Duration::from_secs(5),
    )
    .await,
    "补扫后主端已发位点必须追平积压尾"
  );

  // ===== 实时推流 1 条（唤醒循环异步拉取，等主端已发位点追平日志尾）
  pwal.enqueue(b"payload_user_1003_carol")?;
  pwal.commit().await?;
  assert!(
    wait_for(
      || driver.get_previous_address(0) as u64 >= pwal.tail_address(),
      Duration::from_secs(5),
    )
    .await,
    "实时推流后主端已发位点追平日志尾"
  );

  // ===== 副本落盘保真：3 补扫（2 数据 + 1 commit）+ 2 实时（1 数据 +
  // 1 commit）帧全部送达 → 位点与字节双一致（5 条记录逐条比对）
  assert!(
    wait_for(
      || rwal.tail_address() >= pwal.tail_address(),
      Duration::from_secs(5),
    )
    .await,
    "副本日志尾必须经真 socket 追平主端"
  );
  assert_eq!(
    rwal.tail_address(),
    pwal.tail_address(),
    "副本日志尾必须与主端一致"
  );
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
}

/// 真 socket 闭环：wnode GarnetServer 会话泵 + TcpSessionWire（CLIENT 握手 +
/// APPENDLOG init 往返 + 逐记录帧传输），副本落盘后数据一致；断链后发送通道健康面转断连
#[compio::test]
async fn tcp_wire_end_to_end_over_real_socket() -> Void {
  // ===== 副本端：wnode 服务器挂接收会话
  let (_rdir, rwal, replay_task) = open_wal_node("replica")?;
  let provider = replica_provider(REPLICA_ID, PRIMARY_ID);
  let replica_session = ClusterReplicationSession::new(
    Arc::clone(&provider),
    Arc::clone(&rwal),
    Some(ReplicaReplayHook::OffsetWatermark(replay_task.clone())),
  );

  let server = replica_server(replica_session.clone())?;
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
    LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL),
    #[cfg(feature = "tls")]
    None,
  )
  .await?;
  assert!(wire.is_connected(), "建连后发送通道健康");

  let (_store, driver, pump) = assemble_primary_send(
    &pwal,
    wire,
    &AofAddress::create(1, FIRST_RECORD_ADDR as i64),
    true,
  );

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
}

/// 副本停机重启增量协商全链路（无 checkpoint 场景，真 socket）：
/// 副本接收主端流保真落盘 → 正常停机只走 flush-only 入口（副本提交落盘
/// 唯一合法入口，对标 `wait_for_shutdown` → `dispose_async` 副本臂直达
/// [`WalLog::commit_flush_only`] 内核）→ 重启同路径重开 recover 收敛位点
/// 与主端停机尾严格对齐、AOF 流零本地帧 → 副本以恢复位点协商增量接续，
/// 主端自该位点扫描严格衔接记录边界（forwarded > 0 即无 Invalid 终止、
/// 无死锁），双端逐帧字节一致续行
#[test]
fn replica_flush_shutdown_restart_resyncs_incrementally() -> Void {
  Runtime::new().unwrap().block_on(async {
    // ===== 副本一世代：真 socket 服务器挂接收会话（段文件路径固定，供重启重开）
    let rdir = tempdir()?;
    let rpath = rdir.path().join("replica_resync.wal");
    let rwal = wal_at(&rpath)?;
    rwal.enqueue(&[0u8; HEAD_PAD_PAYLOAD])?;
    let provider = replica_provider(REPLICA_ID, PRIMARY_ID);
    let replay_task = Arc::new(AtomicI64::new(FIRST_RECORD_ADDR as i64));
    let replica_session = ClusterReplicationSession::new(
      Arc::clone(&provider),
      Arc::clone(&rwal),
      Some(ReplicaReplayHook::OffsetWatermark(replay_task.clone())),
    );
    let server = replica_server(replica_session)?;
    let addr = server.local_addr()?.to_string();

    // ===== 主端：停机基线 2 条业务记录 + commit（帧边界即协商基线）
    let (_pdir, pwal, _pnode) = open_wal_node("primary_resync")?;
    pwal.enqueue(b"payload_resync_3001_pre_shutdown_a")?;
    pwal.enqueue(b"payload_resync_3002_pre_shutdown_b")?;
    pwal.commit().await?;
    let shutdown_base = pwal.tail_address();

    let wire = TcpSessionWire::connect(
      &addr,
      PRIMARY_ID,
      0,
      None,
      None,
      LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL),
      #[cfg(feature = "tls")]
      None,
    )
    .await?;
    let (_store, driver, pump) = assemble_primary_send(
      &pwal,
      wire,
      &AofAddress::create(1, FIRST_RECORD_ADDR as i64),
      true,
    );

    // ===== 一代流：存量全部转发，副本尾与主端逐帧一致
    let (forwarded, skipped) = pump.sync_backlog(&pwal).await?;
    assert_eq!(
      (forwarded, skipped),
      (2 + 1, 0),
      "停机前存量 2 记录 + 1 commit 帧全部补扫转发"
    );
    assert!(
      wait_for(|| rwal.tail_address() == shutdown_base, Duration::from_secs(5)).await,
      "副本日志尾必须追平主端停机基线尾"
    );
    assert_wal_records_identical(&pwal, &rwal, 3).await;

    // ===== 副本正常停机（生产收口链实弹）：同一物理日志上 single_log_aof
    // 工厂装配权威 AOF 门面 → 装配期注入副本角色位（suspend = replica）→
    // 先驱动异步提交入口（commit_aof/检查点/周期任务同源生产面，本票副本
    // 角色闸管辖）→ dispose_async 副本臂收口。两入口一律 flush-only：
    // 尾位点不得因本地 commit 帧 +32 字节漂移
    let replica_aof = single_log_aof(Arc::clone(&rwal), &RuntimeServerOptions::default())?;
    let tasks = Arc::new(PrimaryTasks::default());
    tasks.suspend();
    replica_aof.attach_primary_tasks(tasks);
    replica_aof.log().commit_async().await;
    assert_eq!(
      rwal.tail_address(),
      shutdown_base,
      "副本提交面角色闸：commit_async 纯刷盘不得写本地 commit 帧"
    );
    replica_aof.dispose_async().await;
    assert_eq!(
      rwal.tail_address(),
      shutdown_base,
      "副本 dispose 副本臂纯刷盘，尾位点不动（本地帧即 32 字节漂移源）"
    );
    assert!(
      rwal.flushed_until_address() >= shutdown_base,
      "停机 flush-only 落盘须覆盖全部已收记录"
    );
    let replica_shutdown_tail = rwal.tail_address();
    drop(replica_aof);

    // ===== 重启：断开一代链路、释放会话与 WAL 句柄，同路径重开 recover
    server.dispose();
    assert!(
      wait_for(|| !driver.is_connected(), Duration::from_secs(5)).await,
      "一代链路须随副本停机断开"
    );
    drop(server);
    drop(rwal);
    drop(provider);
    let rwal2 = wal_at(&rpath)?;
    let recovered = rwal2.recover().await?;
    assert_eq!(
      recovered, replica_shutdown_tail,
      "重启恢复位点须严格等于副本停机位点（= 主端帧边界），漂出即本地帧污染镜像"
    );

    // ===== 主端停机后继续写入，重启副本以恢复位点协商增量接续
    pwal.enqueue(b"payload_resync_3003_after_restart")?;
    pwal.commit().await?;
    let provider2 = replica_provider(REPLICA_ID, PRIMARY_ID);
    let replay_task2 = Arc::new(AtomicI64::new(FIRST_RECORD_ADDR as i64));
    let session2 = ClusterReplicationSession::new(
      Arc::clone(&provider2),
      Arc::clone(&rwal2),
      Some(ReplicaReplayHook::OffsetWatermark(replay_task2.clone())),
    );
    let server2 = replica_server(session2)?;
    let addr2 = server2.local_addr()?.to_string();
    let wire2 = TcpSessionWire::connect(
      &addr2,
      PRIMARY_ID,
      0,
      None,
      None,
      LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL),
      #[cfg(feature = "tls")]
      None,
    )
    .await?;
    let (_store2, _driver2, pump2) = assemble_primary_send(
      &pwal,
      wire2,
      &AofAddress::create(1, recovered as i64),
      false,
    );
    let (forwarded2, skipped2) = pump2.sync_backlog(&pwal).await?;
    assert_eq!(
      (forwarded2, skipped2),
      (1 + 1, 0),
      "主端自协商位点扫描严格衔接记录边界：重启后 1 记录 + 1 commit 帧全部转发，无 Invalid 终止（forwarded>0 即增量流不死锁）"
    );

    // ===== 续行保真：重启副本尾再追平主端新尾，双端逐帧字节一致（5 条
    // 记录含两世代 commit 帧，副本侧零本地帧），重放位点追平
    assert!(
      wait_for(
        || rwal2.tail_address() == pwal.tail_address(),
        Duration::from_secs(5)
      )
      .await,
      "重启副本尾必须再追平主端新尾"
    );
    assert_wal_records_identical(&pwal, &rwal2, 5).await;
    let new_tail = pwal.tail_address() as i64;
    assert!(
      wait_for(
        || replay_task2.load(Ordering::Acquire) >= new_tail,
        Duration::from_secs(5)
      )
      .await,
      "重启副本重放位点必须追平新尾"
    );
    Ok(())
  })
}
