//! 高并发原位复活写与槽位搬迁交织：数据无撕裂、无丢失
//!
//! 对标 C# MigrationDriver.cs:139 BeginAsyncMigrationTaskAsync 首动作
//! `storeWrapper.store.PauseRevivification(_timeout, _cts.Token)`（Tsavorite.cs:121-143
//! 挂起计数 → BumpCurrentEpoch → 事件等待在途写者退出）与 finally 块 ResumeRevivification
//! （MigrationDriver.cs:224；rust 侧收口为 wkv/src/store/reviv_host.rs
//! `pause_revivification` 强同步门面 + 守卫 RAII）。
//!
//! SLOTS 全链迁移（假目标端逐帧 +OK）与热键复活压测写者同运行交织，断言依据：
//! 1. 首帧到达假目标端瞬间采样：挂起计数已置停且存储纪元已越过迁移启动基线——
//!    迁移扫描编排严格 happens-after 强同步暂停的纪元推进（C# pause → bump 序，
//!    Tsavorite.cs:129-133）；
//! 2. 写者每轮读回值必为合法写入图案（长度 + 填充字节双因子）——原位复活覆写与
//!    搬迁扫描并发下无半写入撕裂（同 wkv reviv 密封压测图案判据）；
//! 3. 迁移前落盘、写者全程不触碰的静态键在驱动返 `Ok` 后源端全数收口删除，且
//!    `Ok` 计数 ≥ 静态键数——「扫描可见 → 传输 → ACK → 删除」环无丢失、无残留
//!    （驱动扫描环重扫至槽空方退出，slots.rs 驱动循环留键即不收敛）；
//! 4. 热键末态只可能为「已收口删除」或「驱动末扫之后的合法新写图案」——协议允许
//!    并发改写键保留源端（slots.rs stuck_keys），但绝不可为半新半旧混合字节。

use std::{
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::spawn,
  time::sleep,
};
use parking_lot::Mutex;
use wbase::{
  hash_slot::{CLUSTER_SLOT_COUNT, slot_of},
  map::HashSet,
};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  migration::{
    migrate_driver::{run_slots_migration_task, try_add_slots_migration_task},
    migrate_session::MigrateTaskSpec,
    transfer_option::TransferOption,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::storage::session::storage_session::StorageSession;
use wtest_base::test_store_config;

/// 默认会话 (0,0) 库槽位（库级定槽：键内容不参与定槽，库内键恒共会话槽位）
const SLOT0: u16 = slot_of(0, 0);
const _: () = assert!(SLOT0 == 0, "会话库键恒落槽 0");
/// 远端节点承载的槽位（与 SLOT0 异槽）
const REMOTE_SLOT: u16 = SLOT0 ^ 1;

/// 初始热键数（驱动 Ok 计数基线；写者持续覆写故末态不受驱动约束）
const KEYS: usize = 8;
/// 静态键数：迁移前落盘、写者全程不触碰，收口删除判据的确定基线
const STATIC_KEYS: usize = 8;
/// 写者任务数与每轮覆写轮数（有限轮次保证驱动在写者静默后收敛收口）
const WRITERS: usize = 2;
const ROUNDS: usize = 25;

/// 合法写入图案集（长度与填充字节一一对应互异）：任一新旧混合字节必落图案之外（撕裂判据）
const FILLERS: [u8; 4] = *b"ABCD";
const SIZES: [usize; 4] = [64, 96, 192, 300];
/// 静态键图案下标：写者轮转仅用 0..3 三图案，静态图案独立可辨识
const STATIC_PATTERN: usize = 3;

fn valid_pattern(v: &[u8]) -> bool {
  SIZES
    .iter()
    .zip(FILLERS)
    .any(|(&len, f)| v.len() == len && v.iter().all(|&b| b == f))
}

fn hot_key(i: usize) -> String {
  format!("rpmi_{i}")
}

fn static_key(i: usize) -> String {
  format!("rpmi_s{i}")
}

/// 复活启用态迁移测试店铺（对标 cluster_migration.rs open_migrate_store(reviv=true)
/// 口径：is_enabled 暂停/恢复谓词仅在 --reviv 开时可二态区分）
fn migrate_store_reviv(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  let config = test_store_config().with_revivification(true);
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// 装配双主节点拓扑：node_1（本地）持 0..8192，node_2@7001 持 8192..16384
fn two_primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    let remote_worker_id = config.workers.len() as u16;
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE12),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for slot in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    config.slot_map[REMOTE_SLOT as usize] = HashSlot {
      worker_id: remote_worker_id,
      state: SlotState::Stable,
    };
  }
  cp.set_cluster_node_timeout_ms(100);
  cp
}

/// 解析缓冲中首个完整 RESP2 数组帧（复刻 cluster_migration.rs 同法：帧总字节数 + 参数切片）
fn try_parse_frame_args(buf: &[u8]) -> Option<(usize, Vec<&[u8]>)> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  let mut args = Vec::with_capacity(argc);
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    let end = len_line_end + len;
    if end + 2 > buf.len() {
      return None;
    }
    args.push(&buf[len_line_end..end]);
    pos = end + 2;
  }
  Some((pos, args))
}

/// 首帧到达瞬间的强同步暂停态采样（在帧处理线程同步读取，采样零时移）
#[derive(Default)]
struct FirstFrameSample {
  taken: AtomicBool,
  reviv_paused: AtomicBool,
  epoch_advanced: AtomicBool,
}

/// 逐帧 +OK 假迁移目标端：解析每个完整 RESP2 数组帧并回 +OK（IMPORTING/批次/
/// 哨兵/NODE/gossip 汇聚全承接，无脚本耗尽静默，迁移批迭代浮动不影响收敛）；
/// 首帧到达时对共享存储做暂停态与纪元基线采样（断言 1 的观测点）
async fn always_ok_migrate_target(
  store: Arc<WedbStore<SegmentedDevice>>,
  epoch_before: u64,
  sample: Arc<FirstFrameSample>,
  seen: Arc<Mutex<Vec<String>>>,
) -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  spawn(async move {
    while let Ok((mut stream, _)) = listener.accept().await {
      let store = Arc::clone(&store);
      let sample = Arc::clone(&sample);
      let seen = Arc::clone(&seen);
      spawn(async move {
        let mut acc: Vec<u8> = Vec::new();
        let mut buf = vec![0u8; 65536];
        loop {
          let BufResult(res, next) = stream.read(buf).await;
          buf = next;
          let n = match res {
            Ok(n) if n > 0 => n,
            _ => break,
          };
          acc.extend_from_slice(&buf[..n]);
          while let Some((frame_len, args)) = try_parse_frame_args(&acc) {
            if !sample.taken.swap(true, Ordering::SeqCst) {
              // 首帧 = begin_migration_phase 的握手/IMPORTING 帧，必在强同步暂停
              // 返回之后（slots.rs：pause await → connect → begin）；此刻守卫在册
              sample
                .reviv_paused
                .store(!store.reviv_pool.is_enabled(), Ordering::SeqCst);
              sample
                .epoch_advanced
                .store(store.epoch.current_epoch() > epoch_before, Ordering::SeqCst);
            }
            seen.lock().push(
              args
                .iter()
                .take(3)
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect::<Vec<_>>()
                .join(" "),
            );
            acc.drain(..frame_len);
            if stream.write_all(b"+OK\r\n").await.is_err() {
              return;
            }
          }
        }
      })
      .detach();
    }
  })
  .detach();
  addr
}

fn migrate_spec(port: i32, timeout_ms: i32) -> MigrateTaskSpec {
  MigrateTaskSpec {
    source_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
    target_address: "127.0.0.1".to_string(),
    target_port: port,
    target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE12,
    username: String::new(),
    passwd: String::new(),
    copy_option: false,
    replace_option: false,
    timeout: timeout_ms,
    transfer_option: TransferOption::Slots,
  }
}

fn port_of(addr: &str) -> i32 {
  addr.rsplit(':').next().unwrap().parse().unwrap()
}

async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.read_string(key).await.unwrap()
}

#[compio::test]
async fn slots_migration_interleaved_reviv_writes_no_tear_no_loss() {
  let cp = two_primary_provider();
  let store = migrate_store_reviv("rpmi.db");
  cp.set_store(Arc::clone(&store));

  // 1. 初始键集：热键落 (64,'A') 图案、静态键落写者永不再写的 (300,'D') 图案
  {
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    for i in 0..KEYS {
      storage
        .upsert_string(hot_key(i).as_bytes(), &vec![FILLERS[0]; SIZES[0]])
        .await
        .unwrap();
    }
    for i in 0..STATIC_KEYS {
      storage
        .upsert_string(
          static_key(i).as_bytes(),
          &vec![FILLERS[STATIC_PATTERN]; SIZES[STATIC_PATTERN]],
        )
        .await
        .unwrap();
    }
  }

  let seen = Arc::new(Mutex::new(Vec::new()));
  let sample = Arc::new(FirstFrameSample::default());
  let epoch_before = store.epoch.current_epoch();
  let addr = always_ok_migrate_target(
    Arc::clone(&store),
    epoch_before,
    Arc::clone(&sample),
    Arc::clone(&seen),
  )
  .await;
  let spec = migrate_spec(port_of(&addr), 5000);
  let slots: HashSet<i32> = [SLOT0 as i32].into_iter().collect();
  let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();

  // 2. 复活压测写者先起跑（尺寸轮换覆写击穿上原位容量门 → 追加 + 脱钩归池；
  //    删空重建直驱池 put/take——原位复活覆写与扫描读取同窗交织）。图案下标轮转
  //    严格避开静态图案（idx < STATIC_PATTERN），静态键收口判据方不被写者污染
  let mut writers = Vec::with_capacity(WRITERS);
  for w in 0..WRITERS {
    let store_w = Arc::clone(&store);
    writers.push(spawn(async move {
      let session = store_w.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new(batch);
      for round in 0..ROUNDS {
        for i in 0..KEYS {
          let key = hot_key(i);
          let idx = (round + i + w * 2) % STATIC_PATTERN;
          if (round + i) % 5 == 0 {
            storage.delete_string(key.as_bytes()).await.unwrap();
          }
          storage
            .upsert_string(key.as_bytes(), &vec![FILLERS[idx]; SIZES[idx]])
            .await
            .unwrap();
          if let Some(got) = storage.read_string(key.as_bytes()).await.unwrap() {
            assert!(
              valid_pattern(&got),
              "交织窗口 {w}/{round} 读到撕裂/越界内容: {:?}..",
              &got[..got.len().min(8)]
            );
          }
          // 让位假目标 I/O 与驱动批循环，保证真实交织而非写者独占了线程
          sleep(Duration::from_micros(200)).await;
        }
      }
    }));
  }

  // 3. SLOTS 全链驱动与写者同窗并发（驱动首动作即强同步暂停 + 纪元排空，写者
  //    钉住的在途复活临界区在 compio 同线程契约下由屏障解除自钉后快速达成）。
  //    驱动收口 await 排在写者静默之后：扫描环重扫至槽空方退出，故收口判据成立
  let driver = {
    let store_m = Arc::clone(&store);
    spawn(async move { run_slots_migration_task(store_m, spec, session).await })
  };

  for wd in writers {
    wd.await.expect("写者任务不得 panic/取消");
  }
  let migrated = driver
    .await
    .expect("驱动任务不得 panic/取消")
    .expect("逐帧 +OK 目标下 SLOTS 全链应成功");

  // 断言 1：首帧到达瞬间强同步暂停在册且纪元已推进（pause → bump → 编排扫描序）
  assert!(
    sample.taken.load(Ordering::SeqCst),
    "假目标端应至少收到一帧迁移编排命令"
  );
  assert!(
    sample.reviv_paused.load(Ordering::SeqCst),
    "首帧到达时复活挂起计数须处于暂停态（守卫存续窗覆盖全部扫描编排，对标 C# MigrationDriver.cs:139 先行 PauseRevivification）"
  );
  assert!(
    sample.epoch_advanced.load(Ordering::SeqCst),
    "首帧到达时存储纪元须已越过迁移启动基线 {epoch_before}（纪元排空先于任何扫描读取）"
  );

  // 断言 3：驱动收口成功——静态键（迁移前落盘、写者全程不触碰）必被扫描、
  // 传输、确认并收口删除，无丢失即无残留；驱动 `Ok` 计数至少覆盖静态键集
  assert!(
    migrated >= STATIC_KEYS,
    "每静态键至少被传输确认一次: migrated={migrated} < {STATIC_KEYS}"
  );
  for i in 0..STATIC_KEYS {
    assert_eq!(
      read_str(&store, static_key(i).as_bytes()).await,
      None,
      "静态键 {i} 应已被驱动扫描、传输并收口删除（无丢失即无残留）"
    );
  }

  // 断言 4：热键末态只可能为「已收口删除」或「驱动末扫之后的合法新写」——
  // 并发改写键按协议保留源端（slots.rs stuck_keys），但内容绝不可为混合字节
  for i in 0..KEYS {
    if let Some(got) = read_str(&store, hot_key(i).as_bytes()).await {
      assert!(
        valid_pattern(&got),
        "热键 {i} 末态非合法图案（原位复活覆写与扫描并发撕裂）: {:?}..",
        &got[..got.len().min(8)]
      );
    }
  }

  // 帧序见证全链：IMPORTING → MIGRATE(批/哨兵) → NODE；任务 finally 移除
  let frames = seen.lock();
  assert!(
    frames.iter().any(|f| f.contains("IMPORTING")),
    "应见 IMPORTING 编排帧: {frames:?}"
  );
  assert!(
    frames.iter().filter(|f| f.contains("MIGRATE")).count() >= 2,
    "至少一批数据帧 + 一哨兵帧: {frames:?}"
  );
  assert!(
    frames.iter().any(|f| f.contains("NODE")),
    "SLOTS 链必须交权 NODE: {frames:?}"
  );
  drop(frames);
  assert!(
    store.reviv_pool.is_enabled(),
    "迁移结束守卫 Drop 后复活分配恢复（对标 C# finally ResumeRevivification, MigrationDriver.cs:224）"
  );
  assert_eq!(
    cp.migration_manager().unwrap().get_migration_task_count(),
    0,
    "迁移任务结束必须移除"
  );
}
