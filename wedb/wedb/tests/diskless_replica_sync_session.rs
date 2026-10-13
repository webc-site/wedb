#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::sync::Arc;

use compio::time::sleep;
use waof::AofAddress;
use wedb::server::{
  replication::{
    aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
    diskless_replication::{DisklessSyncSession, SyncStatus},
    error::ReplicationError,
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};
use wedb_test::fake_frame_pump::pump_frames;

/// 会话最小装配（仅状态机与驱动持有面）
fn session(node_id: u128, store: Arc<AofSyncDriverStore>) -> DisklessSyncSession {
  DisklessSyncSession::new(
    "127.0.0.1:0".to_string(),
    SyncMetadata {
      full_sync: true,
      origin_node_role: NodeRole::Replica,
      origin_node_id: node_id,
      current_primary_repl_id: "replid".to_string(),
      current_store_version: 0,
      current_aof_begin_address: AofAddress::create(2, 0),
      current_aof_tail_address: AofAddress::create(2, 100),
      checkpoint_entry: None,
    },
    true,
    2,
    store,
  )
}

fn driver(remote_node_id: u128, start: i64) -> Arc<AofSyncDriver> {
  Arc::new(AofSyncDriver::new(
    0x10CA1,
    remote_node_id,
    2,
    &AofAddress::create(2, start),
    None,
  ))
}

/// 会话判败必连带摘除回挂的推流驱动（对标
/// libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/
/// ReplicaSyncSession.cs:129 SetStatus(FAILED) 内
/// AofSyncDriverStore.TryRemove(AofSyncDriver)）。旧行为 set_status 只写状态
/// 与广播、不动驱动册：begin_aof_sync 于 data_loss_check 判败即早返，扇出前
/// 预锁的钉线驱动留册，永久拉回 AOF 截断线致无界增长
#[test]
fn test_set_status_failed_detaches_session_driver() {
  let store = Arc::new(AofSyncDriverStore::new(2));
  let s = session(0x21, Arc::clone(&store));

  // prepare 段建连即败（尚未回挂驱动）：判败无册可摘，仅置终态
  s.set_status(SyncStatus::Failed, Some("connect failed".to_string()));
  assert_eq!(store.count(), 0);
  assert_eq!(s.status_info().sync_status, SyncStatus::Failed);

  // 预锁段回挂 + 入库后判败：驱动当场出册
  let d = driver(0x21, 100);
  assert!(store.try_add_replication_driver(Arc::clone(&d), false));
  assert_eq!(store.count(), 1);
  s.add_aof_sync_task(d);
  s.set_status(SyncStatus::Failed, Some("aof truncated".to_string()));
  assert_eq!(store.count(), 0, "判败必摘本会话驱动");
  // 首错保留（C# ssInfo.error ??= error）
  assert_eq!(s.status_info().error.as_deref(), Some("connect failed"));
}

/// SUCCESS 留册不摘（终态广播后 AOF 增量推流继续）；FAILED 只按实例匹配退场，
/// 同节点已被二次 try_add 置换时不误删新驱动（C# TryRemove(AofSyncDriver) 的
/// 引用匹配语义，rust 不另立按节点 id 的第二注销通道）
#[test]
fn test_set_status_detach_matches_driver_instance_only() {
  let store = Arc::new(AofSyncDriverStore::new(2));
  let s = session(0x22, Arc::clone(&store));
  let old = driver(0x22, 100);
  assert!(store.try_add_replication_driver(Arc::clone(&old), false));
  s.add_aof_sync_task(old);

  s.set_status(SyncStatus::Success, None);
  assert_eq!(store.count(), 1, "成功会话驱动留册");

  // 恢复位点二次 try_add 原地置换后再判败：会话仍持旧实例，摘除不得波及新驱动
  let fresh = driver(0x22, 200);
  assert!(store.try_add_replication_driver(Arc::clone(&fresh), false));
  s.set_status(SyncStatus::Failed, Some("late failure".to_string()));
  let kept = store.drivers();
  assert_eq!(kept.len(), 1);
  assert!(
    Arc::ptr_eq(&kept[0], &fresh),
    "只摘本会话实例，同键重挂的新驱动留册"
  );
}

/// 清库复位帧超时跟随活旋钮（对标 C# ReplicaSyncSession.cs:143 WaitAsync(ReplicaSyncTimeout)）：
/// 调小至 1s 后，对端不应答清库帧须按 1s 旋钮超时收场，而非硬编码 30s
#[compio::test]
async fn test_issue_flush_all_async_timeout_knob() {
  use std::time::{Duration, Instant};

  use compio::{net::TcpListener, runtime::spawn};
  use wedb::{client::GarnetClient, server::cluster_provider::ClusterProvider};

  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  spawn(async move {
    if let Ok((mut stream, _)) = listener.accept().await {
      // 读循环骨架见 `wedb_test::fake_frame_pump`
      pump_frames(&mut stream, 4096, async |_: &[u8], args: &[&[u8]]| {
        let is_flushall = args.len() >= 2
          && args[0].eq_ignore_ascii_case(b"CLUSTER")
          && args[1].eq_ignore_ascii_case(b"FLUSHALL");
        if is_flushall {
          // 静默不回复，保持连接挂起，模拟副本卡死
          loop {
            sleep(Duration::from_secs(3600)).await;
          }
        }
        Some(b"+OK\r\n".to_vec())
      })
      .await;
    }
  })
  .detach();

  let client = Arc::new(GarnetClient::with_config(addr, None, None, 0, None));
  client.connect_async().await.unwrap();
  assert!(client.is_connected());

  let store = Arc::new(AofSyncDriverStore::new(2));
  let s = session(0x23, store);
  let provider = Arc::new(ClusterProvider::new());

  provider.set_replica_sync_timeout_secs(1);

  let started = Instant::now();
  let res = s.issue_flush_all_async(&provider, &client).await;
  let elapsed = started.elapsed();

  assert!(
    matches!(res, Err(ReplicationError::Timeout(_))),
    "静默副本应触发超时错误，实际结果: {res:?}"
  );
  assert!(
    elapsed < Duration::from_secs(10),
    "超时时间 ({elapsed:?}) 应贴近 1s 旋钮而非硬编码 30s"
  );
  assert!(
    elapsed >= Duration::from_millis(900),
    "超时时间 ({elapsed:?}) 不应过早发生"
  );
}

/// 副本正常返回 +OK 清库成功
#[compio::test]
async fn test_issue_flush_all_async_success() {
  use compio::{net::TcpListener, runtime::spawn};
  use wedb::{client::GarnetClient, server::cluster_provider::ClusterProvider};

  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  spawn(async move {
    if let Ok((mut stream, _)) = listener.accept().await {
      // 读循环骨架见 `wedb_test::fake_frame_pump`；FLUSHALL 应答后即收口
      pump_frames(&mut stream, 4096, async |_: &[u8], _: &[&[u8]]| {
        // FLUSHALL 应答后即无后续交互（连接随用例收口）
        Some(b"+OK\r\n".to_vec())
      })
      .await;
    }
  })
  .detach();

  let client = Arc::new(GarnetClient::with_config(addr, None, None, 0, None));
  client.connect_async().await.unwrap();
  assert!(client.is_connected());

  let store = Arc::new(AofSyncDriverStore::new(2));
  let s = session(0x24, store);
  let provider = Arc::new(ClusterProvider::new());
  provider.set_replica_sync_timeout_secs(5);

  let res = s.issue_flush_all_async(&provider, &client).await;
  assert!(res.is_ok(), "正常应答 +OK 应成功，实际: {res:?}");
}
