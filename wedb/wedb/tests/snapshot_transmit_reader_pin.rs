#![recursion_limit = "256"]
//! 复制快照在传读者钳活体 hlog 删段下界（reader-pin 水位）集成回归
//! 票：wedb-repl-snapshot-live-hlog-segment-truncated-under-inflight-reader
//!
//! 契约对标 C# CheckpointStore.cs:196-210（DeleteOutdatedCheckpoints 尾部按
//! 最旧仍被活跃读者引用的条目 ShiftBeginAddress 钳活体日志删段）。真引擎
//! 小段几何 + 真检查点发布链 + 真 SNAPSHOT_DATA 帧级下发（脚本化假端点应答，
//! checkpoint_import.rs 同款范式），下发在途时主端发布新一轮检查点：
//! - 在传读者钉在册：地板抬升被钳在读者 begin，在传段全程存活、read_range
//!   无 SegmentNotFound、下发完整成功；注销后沿同一 release_history_until
//!   通道滞后补删，按新地板回收历史段；
//! - 判据真锁缺陷（摘钉必红）：断言取钳制结果（floor 钉在读者 begin、在传
//!   段存活），无封顶实现下发布轮即 unlink 在传段，各断言必红。

use std::{
  fs::create_dir_all,
  io,
  path::Path,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::Duration,
};

use compio::{
  buf::BufResult,
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  runtime::{Runtime, spawn},
  time::timeout,
};
use crossfire::oneshot::{RxOneshot, TxOneshot, oneshot};
use waof::AofAddress;
use wcpr::{CheckpointMeta, CheckpointType, create_checkpoint};
use wdev::SegmentedDevice;
use wedb::{
  client::GarnetClient,
  server::{
    cluster_provider::ClusterProvider,
    replication::{
      checkpoint_entry::{CheckpointEntry, CheckpointFileType, CheckpointMetadata},
      replication_manager::ReplicationManager,
      snapshot_transmission::{SnapshotTransmitSources, send_store_checkpoint},
    },
  },
};
use wkv::WedbStore;
use wnode::{database::checkpoint_version, storage::session::storage_session::StorageSession};
use wtest_base::test_store_config;

/// 段几何：1MB 段 × 4096 扇区——3MB 数据跨 3 段，删段取证面为读者 begin
/// 之上的在传段 1/2（段 0 先随 E1 发布轮按既有契约回收）
const SEGMENT_SIZE: u64 = 1024 * 1024;
const SECTOR: u64 = 4096;

/// 单值负载 48KB × 64 条 ≈ 3MB（对象信封 1MB 内联阈值之下，全量入 hlog）
const VALUE_LEN: usize = 48 * 1024;
const KEY_COUNT: u64 = 64;

/// 握手预算（假端点异常退出时主流程不挂死）
const HANDSHAKE_BUDGET: Duration = Duration::from_secs(30);

/// 打开小段几何真库（真引擎 + 真分段设备；目录随句柄存活）
fn open_small_segment_store(dir: &tempfile::TempDir) -> Arc<WedbStore<SegmentedDevice>> {
  let device = Arc::new(
    SegmentedDevice::new(dir.path().join("pinrace.db"), SEGMENT_SIZE, SECTOR as usize)
      .expect("小段设备建立"),
  );
  let mut config = test_store_config();
  config.gc.enabled = false;
  Arc::new(WedbStore::open(config, device).expect("真库打开"))
}

/// 写 string 键（checkpoint_import.rs 同款真会话写路径）
async fn put_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8], value: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  storage.upsert_string(key, value).await.unwrap();
}

/// 主端拍检查点并登记复制域条目（对标 add_new_checkpoint_entry 登记组合）
async fn take_checkpoint(
  store: &Arc<WedbStore<SegmentedDevice>>,
  cp_dir: &Path,
  rm: &ReplicationManager,
) -> CheckpointMeta {
  let meta = create_checkpoint(
    store.as_ref(),
    &store.ckpt_gate,
    cp_dir,
    CheckpointType::FoldOver,
  )
  .await
  .expect("真检查点发布");
  let mut metadata = CheckpointMetadata::new(1);
  metadata.store_version = checkpoint_version(meta.token);
  metadata.store_hlog_token = meta.token;
  metadata.store_index_token = meta.token;
  metadata.store_checkpoint_covered_aof_address = AofAddress::create(1, 0);
  rm.add_checkpoint_entry(CheckpointEntry::new(metadata), true);
  meta
}

/// 下发面 hlog 段幅面（snapshot_transmission 同源公式：起点扇区下对齐，
/// 终点 = max(文件长, flushed) 原样——尾零头 pad 消解后文件尾精确停写尾
/// 不圆整，tail 含未刷盘内存字节不参与终点；起点 0 且段 0 从未创建的
/// 纯空库恒空流）
fn expected_hlog_span(store: &WedbStore<SegmentedDevice>, meta: &CheckpointMeta) -> u64 {
  let start = meta.hlog_meta.begin_address / SECTOR * SECTOR;
  let file_len = store.device.get_file_size(0).unwrap();
  if start == 0 && file_len == 0 {
    return 0;
  }
  file_len.max(meta.hlog_meta.flushed_until_address) - start
}

/// 读单行（不含 CRLF）
async fn read_line(sock: &mut TcpStream) -> io::Result<Vec<u8>> {
  let mut line = Vec::new();
  loop {
    let BufResult(res, buf) = sock.read_exact(vec![0u8; 1]).await;
    res?;
    if buf[0] == b'\n' {
      line.pop();
      return Ok(line);
    }
    line.push(buf[0]);
  }
}

/// 读单条 RESP 批量串（$len\r\n + 定长载荷 + CRLF）
async fn read_bulk(sock: &mut TcpStream) -> io::Result<Vec<u8>> {
  let head = read_line(sock).await?;
  assert_eq!(head[0], b'$', "非批量串帧");
  let len: usize = from_utf8(&head[1..]).unwrap().parse().unwrap();
  let BufResult(res, mut body) = sock.read_exact(vec![0u8; len + 2]).await;
  res?;
  body.truncate(len);
  Ok(body)
}

/// 读单条 RESP 数组帧
async fn read_array(sock: &mut TcpStream) -> io::Result<Vec<Vec<u8>>> {
  let head = read_line(sock).await?;
  assert_eq!(head[0], b'*', "非数组帧");
  let count: usize = from_utf8(&head[1..]).unwrap().parse().unwrap();
  let mut frames = Vec::with_capacity(count);
  for _ in 0..count {
    frames.push(read_bulk(sock).await?);
  }
  Ok(frames)
}

/// 脚本化假端点：逐帧应答 +OK；首个在传 token 的 hlog 数据帧上报到位并扣停
/// 应答（竞态窗口：下发读块已入装备、下一块未起念），放行后续帧照常应答
async fn scripted_snapshot_listener(
  listener: TcpListener,
  token: u128,
  reached_tx: TxOneshot<()>,
  gate_rx: RxOneshot<()>,
  hlog_bytes: Arc<AtomicU64>,
  first_hlog_sent: Arc<AtomicBool>,
) -> io::Result<()> {
  let (mut sock, _) = listener.accept().await?;
  let mut reached_tx = Some(reached_tx);
  let mut gate_rx = Some(gate_rx);
  loop {
    let frames = match read_array(&mut sock).await {
      Ok(frames) => frames,
      Err(_) => return Ok(()),
    };
    let is_hlog_data = frames.len() >= 6
      && frames[1] == b"SNAPSHOT_DATA".as_slice()
      && frames[2].as_slice() == token.to_le_bytes().as_slice()
      && frames[3]
        == (CheckpointFileType::StoreHlog as i64)
          .to_string()
          .as_bytes()
      && !frames[5].is_empty();
    if is_hlog_data {
      hlog_bytes.fetch_add(frames[5].len() as u64, Ordering::Relaxed);
      if !first_hlog_sent.swap(true, Ordering::SeqCst) {
        if let Some(tx) = reached_tx.take() {
          tx.send(());
        }
        if let Some(rx) = gate_rx.take() {
          let _ = rx.await;
        }
      }
    }
    let BufResult(res, _) = sock.write_all(b"+OK\r\n".to_vec()).await;
    res?;
  }
}

/// 主场景：下发在途跨越一轮检查点发布——读钉钳制令在传段全程存活、下发
/// 完整成功；注销后滞后补删按新地板回收
#[test]
fn snapshot_transmit_survives_checkpoint_publish_over_inflight_reader_pin() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let store = open_small_segment_store(&dir);
    let cp_dir = dir.path().join("checkpoints");
    create_dir_all(&cp_dir).unwrap();

    // 跨段数据落盘（真会话写路径），begin 挪线至段 1 起点作 E1 对齐锚
    let value = vec![b'v'; VALUE_LEN];
    for i in 0..KEY_COUNT {
      put_str(&store, format!("pinrace-key-{i:04}").as_bytes(), &value).await;
    }
    store
      .hlog()
      .shift_begin_address(SEGMENT_SIZE)
      .await
      .unwrap();

    let provider = ClusterProvider::new();
    provider.set_store(Arc::clone(&store));
    provider.set_checkpoint_dir(cp_dir.clone());
    let rm = provider.replication_manager().unwrap();

    // E1 发布（真检查点链，含发布步 release_history_until）
    let meta1 = take_checkpoint(&store, &cp_dir, &rm).await;
    let pin = meta1.hlog_meta.begin_address / SECTOR * SECTOR;
    assert_eq!(pin, SEGMENT_SIZE, "E1 begin 应钉在段 1 起点锚");
    let expected_span = expected_hlog_span(&store, &meta1);
    assert!(expected_span > 2 * SEGMENT_SIZE, "在传区间须跨 3 段以上");

    // E1 条目入读者 + 读钉登记（生产三件套：读者计数 / rm 聚合 / 前向写水位）
    let reader = rm
      .checkpoint_store
      .read()
      .try_get_latest_checkpoint_entry_from_memory()
      .expect("E1 条目应已在册");
    rm.register_snapshot_reader(meta1.token, pin, |agg| store.hlog().set_reader_pin(agg));
    assert_eq!(store.hlog().reader_pin(), pin, "前向写入聚合水位");
    assert_eq!(reader.reader_count(), 1, "条目读者在册");

    // 脚本化假端点 + 真客户端 + 真下发
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let (reached_tx, reached_rx) = oneshot::<()>();
    let (gate_tx, gate_rx) = oneshot::<()>();
    let hlog_bytes = Arc::new(AtomicU64::new(0));
    let listener_task = spawn(scripted_snapshot_listener(
      listener,
      meta1.token,
      reached_tx,
      gate_rx,
      Arc::clone(&hlog_bytes),
      Arc::new(AtomicBool::new(false)),
    ));

    let sources = SnapshotTransmitSources {
      device: Arc::clone(&store.device),
      checkpoint_dir: Arc::from(cp_dir.as_path()),
    };
    let client = GarnetClient::with_endpoint(endpoint);
    client.connect_async().await.expect("假端点建连");
    let reader_send = Arc::clone(&reader);
    let send_task =
      spawn(async move { send_store_checkpoint(&client, &sources, &reader_send, None).await });

    // 三段握手：首段帧应答被扣停 → 竞态窗口内主端发布新一轮检查点
    // （挪线 + E2 真发布链；无钉封顶时第 10 步即 unlink 段 0/1）
    timeout(HANDSHAKE_BUDGET, reached_rx)
      .await
      .expect("首段帧未到位")
      .expect("上报通道断裂");
    store
      .hlog()
      .shift_begin_address(2 * SEGMENT_SIZE)
      .await
      .unwrap();
    let meta2 = take_checkpoint(&store, &cp_dir, &rm).await;
    let floor2 = meta2.hlog_meta.begin_address / SECTOR * SECTOR;
    assert_eq!(floor2, 2 * SEGMENT_SIZE, "E2 begin 应钉在段 2 起点锚");

    // 在传读者钉钳制断言：地板未越过读者 begin、在传段全程存活
    // （在传面 = 读者 begin 段 1 起点之上的段 1/2；段 0 已随 E1 发布轮按
    // 「拍检查点才真正删文件」既有契约先行回收，不属在传面；无钉封顶时
    // E2 发布轮 truncate 直达段 2 起点即 unlink 段 1）
    assert_eq!(
      store.hlog().delete_floor(),
      pin,
      "地板抬升须被在传读者钉钳制在读者 begin"
    );
    assert_eq!(store.hlog().reader_pin(), pin);
    assert!(
      store.device.get_file_size(1).unwrap() > 0 && store.device.get_file_size(2).unwrap() > 0,
      "在传段 1/2 必须存活"
    );

    // 放行应答：整单下发完整成功（无 IOERR / SegmentNotFound）
    gate_tx.send(());
    let send_res = send_task.await.expect("下发任务中断");
    send_res.unwrap_or_else(|e| panic!("下发完整成功判据破: {e}"));
    assert_eq!(
      hlog_bytes.load(Ordering::Relaxed),
      expected_span,
      "hlog 段流字节数与发送面幅面不符"
    );

    // 注销（上层回抬水位）+ 同一 release_history_until 单通道滞后补删
    let agg = rm.unregister_snapshot_reader(meta1.token, |agg| store.hlog().set_reader_pin(agg));
    assert_eq!(agg, u64::MAX, "全部注销回无读者哨兵");
    reader.remove_reader();
    store.hlog().release_history_until(floor2).await.unwrap();
    assert_eq!(store.hlog().delete_floor(), floor2, "滞后补删按新地板抬升");
    assert!(
      store.device.start_segment() >= 2 && store.device.get_file_size(0).unwrap() == 0,
      "滞后补删须回收读者 begin 之上的段 1（段 0 已随 E1 发布轮按既有契约回收）"
    );

    drop(listener_task);
  });
}
