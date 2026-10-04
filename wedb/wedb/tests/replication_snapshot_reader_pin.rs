//! 快照下发在途读钉（reader_pin）端到端集成测试
//! 票 wedb-repl-snapshot-live-hlog-segment-truncated-under-inflight-reader
//!
//! 对标 C# libs/server/Cluster/CheckpointStore.cs:196-210（活日志截断钳制到
//! 最老活跃 reader 引用条目）在 rust 端口的补齐验证：STORE_HLOG 全量下发
//! 直读主端活设备，检查点发布 step 10 的 release_history_until 若无条件抬
//! 地板删段，会在另一副本传输中途 unlink 其待读段 → SegmentNotFound →
//! 「IOERR device read at {offset}」整会话失败。本册用真设备（32KB 段
//! SegmentedDevice）、真 ReplicationManager、真 ReplicaSyncSession 与真
//! socket 假副本端点复现事故时序并验证钳制链：
//! a) 慢链路夹具：chunk1 应答闸住（读已发生、后续块未读），期间发布新检查点
//!    （新 begin 越过在途条目 begin）——历史段存活、无 SegmentNotFound、
//!    传输整会话成功；
//! b) 释放后滞后补删：注销读钉后经同一 release_history_until 通道补删，
//!    地板真实推进至新条目 begin、历史段回收、水位回 MAX；
//! c) 回归反证（在途票面执行，不落册）：临时回退 whlog 封顶/取小两处行为
//!    位，本册 a 用例即红于「IOERR device read at 196608」；
//! d) 多副本并发传输：B 会话释放（滞后补删）不得覆盖 A 会话在册读钉，
//!    A 依赖段在 B 全程与退场后仍存活，A 自行释放后才整体回收。
//!
//! 几何单源（设备段 32768 / 扇区 4096，E1 段流 [65536, end1]，end1 ∈
//! (196608, 229376] ⇒ chunk1 = [65536,196608)（段 2..5）、chunk2 =
//! [196608,end1)（恰落受害者段 6）；E2 begin = 229376 ⇒ 越窗删段目标
//! 覆盖段 id < 7（含段 6）——闸点因此严格夹住「删段必毁、钉必保」的读窗。

use std::{
  fs,
  path::PathBuf,
  result::Result,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use aok::Void;
use compio::{net::TcpListener, runtime::spawn, time::sleep};
use tempfile::{TempDir, tempdir};
use waof::{AofAddress, WalConfig, WalLog};
use wcpr::{CheckpointMeta, CheckpointType};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  replication::{
    aof_replication_pump::AofReplicationPump,
    checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    error::ReplicationError,
    replica_sync_session::ReplicaSyncSession,
    replication_manager::ReplicationManager,
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};
use wedb_test::fake_frame_pump::pump_frames;
use wkv::{StoreConfig, WedbStore};
use wnode::{database::checkpoint_version, storage::session::storage_session::StorageSession};
use wtest_base::wait_for;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_A_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;
const REPLICA_B_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0003;

/// 设备几何：段 32768 / 扇区 4096（段 id = addr >> 15）
const SEG: u64 = 32768;
const SECTOR: u64 = 4096;
/// E1 begin：段 2 起点（扇区/段双对齐——注册地板复核零误伤的基线）
const B1: u64 = 65536;
/// chunk1/chunk2 边界 = 受害者段 6 起点（chunk 大小 1<<17 自 start=65536 起算）
const MID_CHUNK: u64 = 196608;
/// E2 begin：段 7 起点（越窗删段目标 ⇒ buggy 下删除段 id < 7）
const B2: u64 = 229376;

/// 主端夹具：自定义几何真设备 + 检查点目录 + 真 provider/rm/推流资产
/// （装配面与公共核 provider_with_role 同源字段，无机制替身）
struct Fixture {
  _dir: TempDir,
  store: Arc<WedbStore<SegmentedDevice>>,
  checkpoint_dir: PathBuf,
  provider: Arc<ClusterProvider>,
  rm: Arc<ReplicationManager>,
  assets: Arc<PrimaryReplicationAssets>,
}

/// 开一套自定义几何主端（32KB 段设备——真删段/真 unlink 观测面；
/// gc 关闭，禁后台紧缩扰动水位）
fn open_fixture(tag: &str) -> Fixture {
  let dir = tempdir().unwrap();
  let device = Arc::new(
    SegmentedDevice::new(dir.path().join(format!("{tag}.db")), SEG, SECTOR as usize).unwrap(),
  );
  let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let wal_device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());
  let checkpoint_dir = dir.path().join("checkpoints");
  fs::create_dir_all(&checkpoint_dir).unwrap();
  let provider = ClusterProvider::new();
  provider.set_store(Arc::clone(&store));
  provider.set_wal(Arc::clone(&wal));
  provider.set_checkpoint_dir(checkpoint_dir.clone());
  let rm = provider.replication_manager().unwrap();
  let assets = Arc::new(PrimaryReplicationAssets {
    wal,
    pump: Arc::new(AofReplicationPump::new(Arc::clone(
      &rm.aof_sync_driver_store,
    ))),
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm))),
  });
  Fixture {
    _dir: dir,
    store,
    checkpoint_dir,
    provider,
    rm,
    assets,
  }
}

/// 写 string 键（对标 checkpoint_import 同款真会话写面）
async fn put_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8], value: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  storage.upsert_string(key, value).await.unwrap();
}

/// 追加写至 hlog 尾地址越过 `target`（1KB 值 ≈ 1.1KB 记录步长）
async fn write_until(fx: &Fixture, prefix: &str, target: u64) -> u64 {
  let mut i = 0u32;
  while fx.store.hlog().tail_address() < target {
    put_str(
      &fx.store,
      format!("{prefix}{i:05}").as_bytes(),
      &[b'v'; 1024],
    )
    .await;
    i += 1;
  }
  fx.store.hlog().tail_address()
}

/// 拍检查点并登记复制域条目（对标 take_primary_checkpoint 同款两步组合），
/// 回 (token, 落盘 meta 复读)
async fn publish_checkpoint(fx: &Fixture) -> (u128, CheckpointMeta) {
  let created = wcpr::create_checkpoint(
    fx.store.as_ref(),
    &fx.store.ckpt_gate,
    &fx.checkpoint_dir,
    CheckpointType::FoldOver,
  )
  .await
  .unwrap();
  let token = created.token;
  let mut metadata = CheckpointMetadata::new(1);
  metadata.store_version = checkpoint_version(token);
  metadata.store_hlog_token = token;
  metadata.store_index_token = token;
  metadata.store_checkpoint_covered_aof_address = AofAddress::create(1, 0);
  metadata.store_primary_repl_id = Some(fx.rm.primary_repl_id());
  fx.rm
    .add_checkpoint_entry(CheckpointEntry::new(metadata), true);
  let meta =
    CheckpointMeta::decode(&fs::read(fx.checkpoint_dir.join(wcpr::meta_filename(token))).unwrap())
      .unwrap();
  (token, meta)
}

/// 副本协商元数据（对标 checkpoint_import 套接字 e2e 同款形态：phantom
/// 条目 store_version -1 ⇒ FullResync 快照下发臂）
fn sync_metadata(rm: &Arc<ReplicationManager>, replica_id: u128) -> SyncMetadata {
  SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: replica_id,
    current_primary_repl_id: rm.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    checkpoint_entry: Some(CheckpointEntry::new(CheckpointMetadata::new(1))),
  }
}

/// 假副本端点观测位与闸口
struct StubReplica {
  addr: String,
  /// 首个非空 STORE_HLOG 数据帧已到达（= chunk1 设备读已完成、应答被闸）
  first_hlog_seen: Arc<AtomicBool>,
  /// BEGIN_REPLICA_RECOVER 帧已到达（快照流全帧送达实证）
  begin_recover_seen: Arc<AtomicBool>,
  /// 已见 STORE_HLOG 非空数据帧数（块切分观测）
  hlog_data_frames: Arc<AtomicUsize>,
}

/// 起假副本：逐帧解析 RESP2 数组；`gate = Some` 时对首个非空 STORE_HLOG
/// 数据帧闸应答（模拟慢链路——读已发生、后续块未读；闸开时限远小于
/// RuntimeServerOptions.replica_sync_timeout_secs 活值折出的帧级限时）；
/// SNAPSHOT_DATA → +OK、
/// BEGIN_REPLICA_RECOVER → bulk "0"（syncFromAofAddress=0）、其余 → +OK
async fn spawn_stub_replica(gate: Option<Arc<AtomicBool>>) -> StubReplica {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let first_hlog_seen = Arc::new(AtomicBool::new(false));
  let begin_recover_seen = Arc::new(AtomicBool::new(false));
  let hlog_data_frames = Arc::new(AtomicUsize::new(0));
  {
    let (first, recovered, frames) = (
      Arc::clone(&first_hlog_seen),
      Arc::clone(&begin_recover_seen),
      Arc::clone(&hlog_data_frames),
    );
    spawn(async move {
      while let Ok((mut stream, _)) = listener.accept().await {
        let (first, recovered, frames, gate) = (
          Arc::clone(&first),
          Arc::clone(&recovered),
          Arc::clone(&frames),
          gate.clone(),
        );
        spawn(async move {
          // 读循环骨架见 `wedb_test::fake_frame_pump`
          pump_frames(&mut stream, 4096, async |_: &[u8], args: &[&[u8]]| {
            match args.get(1).copied() {
              Some(b"SNAPSHOT_DATA") => {
                let is_hlog_data =
                  args.get(3).copied() == Some(b"1") && args.get(5).is_some_and(|p| !p.is_empty());
                if is_hlog_data {
                  frames.fetch_add(1, Ordering::SeqCst);
                  if !first.swap(true, Ordering::SeqCst)
                    && let Some(gate) = &gate
                  {
                    // 慢链路闸：1ms 步进让渡等待开闸（远小于 5s 帧限时）
                    while !gate.load(Ordering::Relaxed) {
                      sleep(Duration::from_millis(1)).await;
                    }
                  }
                }
                Some(b"+OK\r\n".to_vec())
              }
              Some(b"BEGIN_REPLICA_RECOVER") => {
                recovered.store(true, Ordering::SeqCst);
                Some(b"$1\r\n0\r\n".to_vec())
              }
              _ => Some(b"+OK\r\n".to_vec()),
            }
          })
          .await;
        })
        .detach();
      }
    })
    .detach();
  }
  StubReplica {
    addr,
    first_hlog_seen,
    begin_recover_seen,
    hlog_data_frames,
  }
}

/// 慢链路场景全过程观测采样（中途=闸内采样、终局=会话退场后采样）
struct Observations {
  // —— 中途（E2 发布后、A 闸仍闭时即时采样）——
  mid_pin: u64,
  mid_floor: u64,
  mid_reader_count: usize,
  mid_seg2: u64,
  mid_seg6: u64,
  // —— 终局 ——
  result: Result<AofAddress, ReplicationError>,
  hlog_frames: usize,
  begin_recover_seen: bool,
  final_pin: u64,
  final_floor: u64,
  final_count: usize,
  final_begin: u64,
  final_start_segment: u32,
  final_seg_sizes: [u64; 8],
}

/// 场景核：写满 E1 窗 → 移位 + 发布 E1 → 写越过 B2 + 移位 → 起闸假副本 →
/// 真会话发起全量同步 → 闸内发布 E2 + 采样 → 开闸 → 会话退场 → 终局采样。
/// 断言留给各用例（反证运行时红点即事故文本本身）。
async fn run_slow_link_scenario(case_tag: &str) -> (Fixture, Observations) {
  let fx = open_fixture(case_tag);
  let store = Arc::clone(&fx.store);
  let hlog = Arc::clone(store.hlog());

  // ===== E1 窗铺设：写到尾地址越过 chunk2 起点（保证 E1 段流 ≥ 2 块且
  // 受害者段 6 被读），且不超过 B2（chunk2 恰落段 6 内）
  let t1 = write_until(&fx, "e1k", MID_CHUNK + SECTOR).await;
  assert!(
    t1 > MID_CHUNK + SECTOR && t1 <= B2,
    "几何护栏：尾地址 {t1} 须落在 ({MID_CHUNK}+扇区, {B2}] 窗内"
  );
  store.shift_begin_address(B1).await.unwrap();
  let (token1, meta1) = publish_checkpoint(&fx).await;
  assert_eq!(meta1.hlog_meta.begin_address, B1, "E1 begin 段/扇区双对齐");
  let end1 = meta1
    .hlog_meta
    .flushed_until_address
    .max(meta1.hlog_meta.tail_address)
    / SECTOR
    * SECTOR;
  assert!(
    end1 > MID_CHUNK && end1 <= B2,
    "E1 发送终点 {end1} 须越过 chunk1 尾 {MID_CHUNK}（两块读）且 ≤ {B2}（受害者段 6 内）"
  );
  assert_eq!(hlog.delete_floor(), B1, "E1 发布后地板 = begin");
  assert_eq!(store.device.get_file_size(0).unwrap(), 0, "段 0 已回收");
  assert_eq!(store.device.get_file_size(1).unwrap(), 0, "段 1 已回收");
  assert!(store.device.get_file_size(2).unwrap() > 0, "段 2 在册");

  // ===== B2 移位：写越过段 7 起点后手动移位（compaction 代理；
  // create_checkpoint 自身不推 begin）
  let t2 = write_until(&fx, "e2k", B2 + 4 * SECTOR).await;
  assert!(t2 >= B2, "移位前提：尾地址 {t2} ≥ {B2}");
  store.shift_begin_address(B2).await.unwrap();
  assert_eq!(store.hlog().begin_address(), B2);

  // ===== 慢链路假副本 + 真会话并发发起
  let gate = Arc::new(AtomicBool::new(false));
  let stub = spawn_stub_replica(Some(Arc::clone(&gate))).await;
  let provider_t = Arc::clone(&fx.provider);
  let assets_t = Arc::clone(&fx.assets);
  let addr = stub.addr.clone();
  let meta = sync_metadata(&fx.rm, REPLICA_A_ID);
  let session = spawn(async move {
    assets_t
      .sync_session
      .initiate_replica_sync(&provider_t, &assets_t, PRIMARY_ID, &addr, &meta)
      .await
  });

  // ===== 闸点收敛：chunk1 设备读已完成（帧达）且读钉在册（入场即钉）
  let rm = Arc::clone(&fx.rm);
  let arrived = wait_for(
    || {
      stub.first_hlog_seen.load(Ordering::SeqCst)
        && rm.snapshot_reader_count() == 1
        && hlog.reader_pin() == B1
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(arrived, "取条目即钉：chunk1 帧达 + 在册 1 + 钉 = E1 begin");
  assert_eq!(
    stub.hlog_data_frames.load(Ordering::SeqCst),
    1,
    "闸点即第 1 个 hlog 数据帧（chunk1 [65536,{MID_CHUNK}) 读毕待应）"
  );

  // ===== 传输中途发布 E2（新 begin {B2} 越过在途条目 begin {B1}）——事故时序
  let (token2, meta2) = publish_checkpoint(&fx).await;
  assert_ne!(token2, token1);
  assert_eq!(
    meta2.hlog_meta.begin_address, B2,
    "E2 begin = 移位后活 begin"
  );

  // ===== 闸内即时采样（不先断言——反证运行红点落至终局会话结果）
  let obs_mid = (
    hlog.reader_pin(),
    hlog.delete_floor(),
    fx.rm.snapshot_reader_count(),
    store.device.get_file_size(2).unwrap(),
    store.device.get_file_size(6).unwrap(),
  );

  // ===== 开闸：chunk2 起读受害者段 6 → 后续 index/meta 帧 → 恢复往返 →
  // 退钉补删 → AOF 通道建连，会话退场
  gate.store(true, Ordering::Relaxed);
  let result = session.await.expect("会话任务必须收敛");

  let mut final_seg_sizes = [0u64; 8];
  for s in 0..8u32 {
    final_seg_sizes[s as usize] = store.device.get_file_size(s).unwrap_or(u64::MAX);
  }
  let obs = Observations {
    mid_pin: obs_mid.0,
    mid_floor: obs_mid.1,
    mid_reader_count: obs_mid.2,
    mid_seg2: obs_mid.3,
    mid_seg6: obs_mid.4,
    result,
    hlog_frames: stub.hlog_data_frames.load(Ordering::SeqCst),
    begin_recover_seen: stub.begin_recover_seen.load(Ordering::SeqCst),
    final_pin: hlog.reader_pin(),
    final_floor: hlog.delete_floor(),
    final_count: fx.rm.snapshot_reader_count(),
    final_begin: store.hlog().begin_address(),
    final_start_segment: store.device.start_segment(),
    final_seg_sizes,
  };
  (fx, obs)
}

/// 用例 a：慢链路传输跨越一次新检查点发布——历史段存活、无
/// SegmentNotFound、整会话成功
#[compio::test]
async fn slow_link_transfer_survives_concurrent_checkpoint_publish() -> Void {
  let (_fx, obs) = run_slow_link_scenario("rp_a").await;
  // 反证位（whlog 封顶/取小回退后，本断言即红于事故文本）：
  // buggy 下 E2 发布删段 id<7 → chunk2 读受害者段 6 → Err
  assert!(
    obs.result.is_ok(),
    "在途读钉保护下传输必须整会话成功，实际 {:?}",
    obs.result
  );
  assert_eq!(obs.result.unwrap().get(0), Some(0), "授予位点 = 快照覆盖点");
  assert!(obs.begin_recover_seen, "快照流全帧送达（含恢复往返）");
  assert_eq!(
    obs.hlog_frames, 2,
    "E1 段流恰两块（chunk1 + 受害者段 6 的 chunk2）"
  );
  // 闸内采样：钉在册、地板被钉封顶、受害段与依赖段全存活
  assert_eq!(obs.mid_pin, B1, "中途钉仍 = 在途读者 begin");
  assert_eq!(obs.mid_floor, B1, "E2 发布抬地板须被在途读者封顶");
  assert_eq!(obs.mid_reader_count, 1, "中途在册读钉恰一条目");
  assert!(obs.mid_seg2 > 0 && obs.mid_seg6 > 0, "中途依赖段全存活");
  Ok(())
}

/// 用例 b：释放后滞后补删沿同一 release_history_until 通道补收——
/// 地板真实推进、历史段回收、水位回 MAX
#[compio::test]
async fn deferred_replay_reclaims_history_after_release() -> Void {
  let (_fx, obs) = run_slow_link_scenario("rp_b").await;
  assert!(
    obs.result.is_ok(),
    "前置（用例 a 同链路）：{:?}",
    obs.result
  );
  assert_eq!(obs.final_count, 0, "全部注销后在册归零");
  assert_eq!(obs.final_pin, u64::MAX, "水位回无读者哨兵");
  assert_eq!(
    obs.final_floor, B2,
    "滞后补删把被钉钳制的地板推进至最新条目 begin"
  );
  for seg in 2..=6u32 {
    assert_eq!(
      obs.final_seg_sizes[seg as usize], 0,
      "越窗历史段 {seg} 必须已物理回收"
    );
  }
  assert!(obs.final_seg_sizes[7] > 0, "E2 窗下界段 7 受保");
  assert!(obs.final_start_segment >= 7, "设备起段随补删推进");
  assert_eq!(obs.final_begin, B2, "逻辑 begin 不因延后删段而回退");
  Ok(())
}

/// 用例 d：多副本并发传输——B 会话全程在册共享聚合水位，B 单会话释放
/// （含其滞后补删）不得覆盖 A 会话的钉；A 自行释放后才整体回收
#[compio::test]
async fn concurrent_sessions_release_keeps_other_pin() -> Void {
  let fx = open_fixture("rp_d");
  let store = Arc::clone(&fx.store);
  let hlog = Arc::clone(store.hlog());

  // ===== 与用例 a 同铺 E1/E2 几何（略中间断言主体，红线在并发段）
  let t1 = write_until(&fx, "e1k", MID_CHUNK + SECTOR).await;
  assert!(t1 > MID_CHUNK + SECTOR && t1 <= B2);
  store.shift_begin_address(B1).await.unwrap();
  publish_checkpoint(&fx).await;
  write_until(&fx, "e2k", B2 + 4 * SECTOR).await;
  store.shift_begin_address(B2).await.unwrap();

  // ===== A 先入场并被闸（钉 E1 begin）
  let gate = Arc::new(AtomicBool::new(false));
  let stub_a = spawn_stub_replica(Some(Arc::clone(&gate))).await;
  let provider_a = Arc::clone(&fx.provider);
  let assets_a = Arc::clone(&fx.assets);
  let addr_a = stub_a.addr.clone();
  let meta_a = sync_metadata(&fx.rm, REPLICA_A_ID);
  let session_a = spawn(async move {
    assets_a
      .sync_session
      .initiate_replica_sync(&provider_a, &assets_a, PRIMARY_ID, &addr_a, &meta_a)
      .await
  });
  let rm = Arc::clone(&fx.rm);
  assert!(
    wait_for(
      || {
        stub_a.first_hlog_seen.load(Ordering::SeqCst)
          && rm.snapshot_reader_count() == 1
          && hlog.reader_pin() == B1
      },
      Duration::from_secs(10),
    )
    .await,
    "A 入场即钉"
  );
  publish_checkpoint(&fx).await; // E2：新 begin 越过 A 在途条目 begin

  // ===== B 后入场（不等速假副本直连即通）：取 E2、聚合钉仍 = min = B1
  let stub_b = spawn_stub_replica(None).await;
  let provider_b = Arc::clone(&fx.provider);
  let assets_b = Arc::clone(&fx.assets);
  let addr_b = stub_b.addr.clone();
  let meta_b = sync_metadata(&fx.rm, REPLICA_B_ID);
  let session_b = spawn(async move {
    assets_b
      .sync_session
      .initiate_replica_sync(&provider_b, &assets_b, PRIMARY_ID, &addr_b, &meta_b)
      .await
  });
  let result_b = session_b.await.expect("B 会话任务必须收敛");
  assert!(
    result_b.is_ok(),
    "B 会话（传输 E2 窗）必须成功，实际 {result_b:?}"
  );
  assert!(
    stub_b.begin_recover_seen.load(Ordering::SeqCst),
    "B 全帧送达"
  );

  // ===== B 已释放（含滞后补删已跑过一轮 release_history_until(B2)）
  // 而 A 仍闸中：聚合钉仍是 A 的 B1，地板保持封顶，A 依赖段全存活
  assert_eq!(rm.snapshot_reader_count(), 1, "B 注销后 A 钉仍在册");
  assert_eq!(
    hlog.reader_pin(),
    B1,
    "B 单会话释放不得覆盖 A 会话的钉（聚合=min）"
  );
  assert_eq!(
    hlog.delete_floor(),
    B1,
    "B 的滞后补删抬地板被 A 钉封顶——地板原地"
  );
  assert!(
    store.device.get_file_size(6).unwrap() > 0,
    "A 的受害者段 6 必须活到 A 自己释放"
  );
  assert!(
    store.device.get_file_size(2).unwrap() > 0,
    "A 依赖段 2 存活"
  );

  // ===== 开闸收 A：A 退钉后聚合回 MAX，其滞后补删完成整体回收
  gate.store(true, Ordering::Relaxed);
  let result_a = session_a.await.expect("A 会话任务必须收敛");
  assert!(
    result_a.is_ok(),
    "B 全程未毁 A 的依赖段——A 必须整会话成功，实际 {:?}",
    result_a
  );
  assert_eq!(rm.snapshot_reader_count(), 0);
  assert_eq!(hlog.reader_pin(), u64::MAX);
  assert_eq!(
    hlog.delete_floor(),
    B2,
    "最后一名读者释放后地板放行至 E2 begin"
  );
  for seg in 2..=6u32 {
    assert_eq!(
      store.device.get_file_size(seg).unwrap(),
      0,
      "段 {seg} 待全部读者退场后方可回收"
    );
  }
  assert!(store.device.get_file_size(7).unwrap() > 0, "段 7 受保");
  Ok(())
}
