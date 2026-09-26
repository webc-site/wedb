//! AOF 同步驱动容器与安全截断集成测试
use std::sync::Arc;

use compio::runtime::Runtime;
use waof::{AofAddress, AofEntryType};
use wconf::RuntimeServerOptions;
use wedb::server::replication::aof_sync_driver::{AofSyncDriver, AofSyncDriverStore};
use wnode::{GarnetLog, RecordShape, aof::aof_backpressure::AofBackpressure};

/// 测试副本身份（内部 u128）
const R1: u128 = 0x21;

/// 内存双子日志拓扑（物理截断接线测试用）
fn mem_log_of(sublogs: usize) -> GarnetLog {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: sublogs as i32,
    ..RuntimeServerOptions::default()
  };
  let (_dirs, backends) = wnode_test::test_sublogs("driver_store", sublogs);
  GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")
}

#[test]
fn test_driver_store_lifecycle_and_safe_truncate() {
  // 未 attach 日志句柄：safe_truncate_aof 退化为纯记账（无物理删段），await 仅收敛接口
  Runtime::new().unwrap().block_on(async {
    let store = AofSyncDriverStore::new(2);
    assert_eq!(store.count(), 0);

    let d1 = Arc::new(AofSyncDriver::new(
      0x10CA1,
      0x21,
      2,
      &AofAddress::create(2, 100),
      None,
    ));
    assert!(store.try_add_replication_driver(d1, false));
    assert_eq!(store.count(), 1);

    let d2 = Arc::new(AofSyncDriver::new(
      0x10CA1,
      0x22,
      2,
      &AofAddress::create(2, 200),
      None,
    ));
    assert!(store.try_add_replication_driver(d2, false));
    assert_eq!(store.count(), 2);

    let min_addr = store.min_aof_address_from_active_sync_tasks();
    assert_eq!(min_addr.get(0), Some(100));

    let prune_addr = AofAddress::create(2, 50);
    assert_eq!(store.safe_truncate_aof(&prune_addr).await.get(0), Some(50));

    let truncate_beyond = AofAddress::create(2, 150);
    assert_eq!(
      store.safe_truncate_aof(&truncate_beyond).await.get(0),
      Some(100)
    );

    assert!(store.try_remove(R1));
    assert_eq!(store.count(), 1);
    let min_addr2 = store.min_aof_address_from_active_sync_tasks();
    assert_eq!(min_addr2.get(0), Some(200));

    store.reset();
    assert_eq!(store.count(), 0);
  });
}

/// 物理截断接线闭环（对标 C# SafeTruncateAof 尾部
/// `Log.TruncateUntil(TruncatedUntil); Log.Commit();`）：
/// 记账水位（truncated_until）必须同步驱动物理日志 begin 地址推进，
/// 且推进值受在册副本最小位点下界约束
#[compio::test]
async fn safe_truncate_aof_drives_physical_begin_shift() {
  let log = Arc::new(mem_log_of(2));
  let store = AofSyncDriverStore::new(2);
  store.attach_log(Some(Arc::clone(&log)));

  // 双子日志各写一条大记录推进物理尾地址（真实段设备 begin 初始 0；
  // 按目标物理子日志反选键，规避哈希路由集中；大载荷令 committed 越过截断位点）
  for i in 0..2 {
    let key: [u8; 2] = (0u8..u8::MAX)
      .map(|n| [b'k', n])
      .find(|k| log.get_physical_sublog_idx(GarnetLog::hash(k)) == i)
      .unwrap_or(*b"kk");
    let record = RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: &key,
      value: &[0u8; 200],
      input: &[],
      database_id: 0,
    };
    let _ = log.enqueue(&record);
  }
  // 提交刷盘推进 committed：物理截断受 min(committed) 钳制，须先落盘
  log.commit_async().await;
  for i in 0..2 {
    assert!(log.get_tail_address(i) > 150, "子日志 {i} 尾地址应充足");
    assert!(log.committed_until_address().get(i).unwrap_or(0) > 150);
    assert_eq!(log.get_begin_address(i), 0);
  }

  // 无副本：检查点覆盖地址 50 → 物理 begin 直达 50
  let safe = store.safe_truncate_aof(&AofAddress::create(2, 50)).await;
  assert_eq!(safe.get(0), Some(50));
  assert_eq!(log.get_begin_address(0), 50, "物理 begin 应推进至截断水位");

  // 有副本 start=100：截 150 受副本下界收敛到 100
  let d1 = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x21,
    2,
    &AofAddress::create(2, 100),
    None,
  ));
  assert!(store.try_add_replication_driver(d1, false));
  let safe = store.safe_truncate_aof(&AofAddress::create(2, 150)).await;
  assert_eq!(safe.get(0), Some(100));
  assert_eq!(
    log.get_begin_address(0),
    100,
    "物理 begin 不得超过在册副本最小位点"
  );

  // 未注入日志句柄时退化为纯记账（对标 C# appendOnlyFile == null 判空直通）
  let bare = AofSyncDriverStore::new(1);
  let safe = bare.safe_truncate_aof(&AofAddress::create(1, 32)).await;
  assert_eq!(safe.get(0), Some(32));
  assert_eq!(bare.get_truncated_until().get(0), Some(32));
}

/// 背压门接线闭环（对标 C# PublishShippedAddress/PublishShippedAddresses 的
/// 闸门写入语义：attach 收紧水位、推送刷新水位、detach/reset 释放门控）
#[test]
fn test_backpressure_gate_wiring() {
  let bp = Arc::new(AofBackpressure::new(2, 4096));
  let store = AofSyncDriverStore::new(2);
  store.attach_backpressure(Some(Arc::clone(&bp)));

  // 无副本：attach 后发布应把闸门水位写为 MAX（门控放行）
  store.publish_shipped_addresses();
  assert_eq!(bp.get_shipped_watermark(0), i64::MAX, "无副本时门控应放行");

  // attach 副本（start=0）：TryAdd 尾部重报应立即收紧水位到 0
  let d1 = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x21,
    2,
    &AofAddress::create(2, 0),
    None,
  ));
  assert!(store.try_add_replication_driver(Arc::clone(&d1), false));
  assert_eq!(bp.get_shipped_watermark(0), 0, "副本 attach 后水位收紧");

  // 推送进展：两个子日志各自 consume 到 9500
  d1.task_ref(0)
    .unwrap()
    .consume(b"payload", 64, 9_500)
    .unwrap();
  d1.task_ref(1)
    .unwrap()
    .consume(b"payload", 64, 9_500)
    .unwrap();
  assert!(store.throttle_replica(R1), "增量足够应触发水位重报");
  assert_eq!(bp.get_shipped_watermark(0), 9_500, "水位推进到 9500");

  // detach：TryRemove 尾部重报应写 MAX 释放门控
  assert!(store.try_remove(R1));
  assert_eq!(
    bp.get_shipped_watermark(0),
    i64::MAX,
    "副本移除后门控应放行"
  );

  // reset：无驱动场景同样释放
  let d2 = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x22,
    2,
    &AofAddress::create(2, 0),
    None,
  ));
  assert!(store.try_add_replication_driver(d2, false));
  store.reset();
  assert_eq!(bp.get_shipped_watermark(0), i64::MAX, "reset 后门控应放行");
}

/// 批量添加副本驱动测试（对标 C# TryAddReplicationDrivers）：
/// 批量注册多副本，已截断边界校验与背压门控重报
#[test]
fn test_try_add_replication_drivers_batch() {
  let store = AofSyncDriverStore::new(2);
  let bp = Arc::new(AofBackpressure::new(2, 4096));
  store.attach_backpressure(Some(Arc::clone(&bp)));

  let d1 = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x21,
    2,
    &AofAddress::create(2, 200),
    None,
  ));
  let d2 = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x22,
    2,
    &AofAddress::create(2, 300),
    None,
  ));

  assert!(store.try_add_replication_drivers(&[d1, d2], false));
  assert_eq!(store.count(), 2);
  assert_eq!(bp.get_shipped_watermark(0), 200, "最小水位应收紧至 200");

  // 已截断至 250 时，新添加 start=100 的驱动若不允许丢数据应被拒绝
  store.update_truncated_until(&AofAddress::create(2, 250));
  let d_outdated = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x25,
    2,
    &AofAddress::create(2, 100),
    None,
  ));
  assert!(!store.try_add_replication_drivers(&[d_outdated], false));

  // dispose 释放所有驱动并重置门控
  store.dispose();
  assert_eq!(store.count(), 0);
  assert_eq!(bp.get_shipped_watermark(0), i64::MAX);
}

/// 发现一测试验证：带损放行语义（对标 C# AofSyncDriverStore.cs:365 !clusterProvider.AllowDataLoss 判据）
/// 当 start < truncated_until 时，allow_data_loss=false 确定性拒绝，allow_data_loss=true 带损放行
#[test]
fn test_try_add_and_replace_driver_allow_data_loss_semantics() {
  let store = AofSyncDriverStore::new(1);
  store.update_truncated_until(&AofAddress::create(1, 500));

  let outdated = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x21,
    1,
    &AofAddress::create(1, 300),
    None,
  ));

  // 不允许丢数据：确定性拒绝
  assert!(
    !store.try_add_replication_driver(Arc::clone(&outdated), false),
    "不允许丢数据时落后截断线的驱动应被拒绝"
  );
  assert_eq!(store.count(), 0);

  // 允许丢数据：带损放行逃生门成功收敛
  assert!(
    store.try_add_replication_driver(Arc::clone(&outdated), true),
    "允许丢数据时落后截断线的驱动应被放行"
  );
  assert_eq!(store.count(), 1);

  // 原地置换口同理：对落后位点驱动，allow_data_loss 控制放行与否
  let more_outdated = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x21,
    1,
    &AofAddress::create(1, 200),
    None,
  ));
  assert!(
    !store.try_replace_replication_driver(Arc::clone(&more_outdated), false),
    "置换口不允许丢数据时应拒绝落后位点"
  );
  assert!(
    store.try_replace_replication_driver(Arc::clone(&more_outdated), true),
    "置换口允许丢数据时应带损放行并置换"
  );
  assert_eq!(store.count(), 1);
}

/// 发现三测试验证：原子原地置换与旧驱动 dispose（对标 C# syncDrivers[i] = aofSyncDriver; syncDriver.Dispose()）
/// 原地更新同 node_id 驱动，无先摘后挂窗口，旧驱动被安全 dispose，新驱动无缝在册
#[test]
fn test_in_place_atomic_driver_replacement_and_disposal() {
  let store = AofSyncDriverStore::new(1);
  let old_driver = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x31,
    1,
    &AofAddress::create(1, 100),
    None,
  ));
  assert!(store.try_add_replication_driver(Arc::clone(&old_driver), false));
  assert_eq!(store.count(), 1);
  assert!(old_driver.is_connected());

  // 校验最小位点下界为 100
  let min_addr = store.min_aof_address_from_active_sync_tasks();
  assert_eq!(min_addr.get(0), Some(100));

  // 原子原地置换为新驱动（授予位点 200）
  let fresh_driver = Arc::new(AofSyncDriver::new(
    0x10CA1,
    0x31,
    1,
    &AofAddress::create(1, 200),
    None,
  ));
  assert!(store.try_replace_replication_driver(Arc::clone(&fresh_driver), false));
  assert_eq!(store.count(), 1, "置换后总数恒保持为 1");

  // 旧驱动已被处置关闭，新驱动正常连接
  assert!(!old_driver.is_connected(), "被置换的旧驱动必须被 dispose");
  assert!(fresh_driver.is_connected(), "新驱动保持连接健康");

  // 截断下界立即反映新驱动位点 200
  let min_addr_after = store.min_aof_address_from_active_sync_tasks();
  assert_eq!(min_addr_after.get(0), Some(200));

  // 旧驱动退场移除不误伤新驱动
  assert!(!store.try_remove_current(&old_driver));
  assert_eq!(store.count(), 1);
}

/// 发现三并发验证：safe_truncate_aof 与驱动原地置换严格互斥
/// 在并发截断下，置换过程中绝对不会出现"驱动已被摘除"的空档导致截断线越位物理删段
#[test]
fn test_concurrent_safe_truncate_and_atomic_driver_replacement() {
  use std::thread;

  Runtime::new().unwrap().block_on(async {
    let store = Arc::new(AofSyncDriverStore::new(1));

    // 初始预锁钉线在 500
    let initial_driver = Arc::new(AofSyncDriver::new(
      0x10CA1,
      0x42,
      1,
      &AofAddress::create(1, 500),
      None,
    ));
    assert!(store.try_add_replication_driver(initial_driver, false));

    // 并发线程 1：持续发起大位点 safe_truncate_aof（目标 5000）
    let store_c1 = Arc::clone(&store);
    let h1 = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      rt.block_on(async {
        for _ in 0..100 {
          let clamped = store_c1
            .safe_truncate_aof(&AofAddress::create(1, 5000))
            .await;
          let clamped_val = clamped.get(0).unwrap();
          // 在册驱动位点始终在 [500, 800] 之间，截断线绝不可能在置换空档跃升至 5000
          assert!(
            clamped_val <= 800,
            "截断位点 {clamped_val} 越过了活跃驱动钉线！"
          );
        }
      });
    });

    // 并发线程 2：原子就地置换驱动从 500 推进到 800
    let store_c2 = Arc::clone(&store);
    let h2 = thread::spawn(move || {
      for step in 501..=800 {
        let driver = Arc::new(AofSyncDriver::new(
          0x10CA1,
          0x42,
          1,
          &AofAddress::create(1, step),
          None,
        ));
        assert!(
          store_c2.try_replace_replication_driver(driver, false),
          "原地置换驱动绝不能因竞态被拒"
        );
      }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    // 置换完毕后，最终截断线推进至 800，且受最新 800 驱动钳制
    let final_clamped = store.safe_truncate_aof(&AofAddress::create(1, 5000)).await;
    assert_eq!(
      final_clamped.get(0),
      Some(800),
      "最终截断位点应精确收敛于最新驱动位点 800"
    );
  });
}
