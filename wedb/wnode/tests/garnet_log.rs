//! GarnetLog 路由 / 入队 / 提交 / 分块 / 背压集成测试
//! （自 wnode/src/aof/garnet_log.rs 内嵌测试迁出；对标
//! libs/server/AOF/GarnetLog.cs 的消费面行为）。

use std::{ops::Deref, sync::Arc, thread, time::Duration};

use compio::runtime::Runtime;
use waof::{
  AofAddress, AofChunkHeader, AofEntryType, AofHeader, AofHeaderType, NO_COOKIE,
  SequenceNumberGenerator, WalConfig, WalRecord, is_commit_frame,
};
use wconf::RuntimeServerOptions;
use wnode::aof::{
  aof_chunked_record_reader::AofChunkedRecordReader,
  garnet_log::{GarnetLog, MIN_PARTIAL_ALLOC_SIZE, RecordShape},
  waof_sublog::WaofSublog,
};

struct TestLog {
  _dirs: Vec<tempfile::TempDir>,
  log: GarnetLog,
}

impl Deref for TestLog {
  type Target = GarnetLog;
  fn deref(&self) -> &Self::Target {
    &self.log
  }
}

fn log_with(sublogs: usize, replay_tasks: i32) -> TestLog {
  log_with_buffer(sublogs, replay_tasks, 64 * 1024 * 1024)
}

/// 大缓冲变体：分块并发测试的写入总量（多线程 × 多轮 × 1MB 分块记录）
/// 中途无 commit 释放环形窗口，窗口须覆盖总预留量
fn log_with_buffer(sublogs: usize, replay_tasks: i32, buffer_size: usize) -> TestLog {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: sublogs as i32,
    aof_replay_task_count: replay_tasks,
    ..RuntimeServerOptions::default()
  };
  let config = WalConfig {
    buffer_size,
    ..WalConfig::default()
  };
  let (_dirs, backends) = wnode_test::test_sublogs_with_config("glog", sublogs.max(1), config);
  let seq_num_gen = (sublogs > 1).then(|| Arc::new(SequenceNumberGenerator::new(0)));
  let log = GarnetLog::new(&options, backends, seq_num_gen).expect("构造 GarnetLog");
  TestLog { _dirs, log }
}

#[test]
fn sharding_routes_deterministically() {
  let log = log_with(4, 2);
  let hash = GarnetLog::hash(b"key");
  let physical = log.get_physical_sublog_idx(hash);
  assert!(physical < 4);
  assert!(log.get_replay_task_idx(hash) < 2);
  assert_eq!(
    log.get_virtual_sublog_idx(hash),
    physical * 2 + log.get_replay_task_idx(hash)
  );
}

#[test]
fn enqueue_scan_roundtrip() {
  let log = log_with(1, 1);
  let address = log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 7,
      key: b"key1",
      value: b"value1",
      input: &[],
      database_id: 0,
    })
    .unwrap();
  assert!(address >= 0);

  let records = scan_collect(&log, 0, 0);
  assert_eq!(records.len(), 1);
  let payload = &records[0].payload;
  assert_eq!(&payload[16..20], &4u32.to_le_bytes());
  assert_eq!(&payload[20..24], b"key1");
  assert_eq!(&payload[24..28], &6u32.to_le_bytes());
  assert_eq!(&payload[28..34], b"value1");
}

#[test]
fn commit_and_bitmask() {
  let log = log_with(2, 1);
  assert_eq!(log.all_logs_bitmask(), 0b11);
  let address = log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: b"k",
      value: b"v",
      input: &[],
      database_id: 0,
    })
    .unwrap();
  let physical = log.get_physical_sublog_idx(GarnetLog::hash(b"k"));
  log.commit();
  let tail = log.get_tail_address(physical);
  assert!(tail > address);
  log.wait_for_commit(physical, tail);
  let begins = log.begin_address();
  assert_eq!(begins.length(), 2);
}

#[test]
fn broadcast_writes_all_sublogs_with_txn_headers() {
  let log = log_with(2, 1);
  let _ = log.enqueue_database_commit(AofEntryType::FlushAll, 7);
  for i in 0..2 {
    let records = scan_collect(&log, i, 0);
    assert_eq!(records.len(), 1, "子日志 {i} 须有广播条目");
    let header = AofHeader::parse(&records[0].payload).unwrap();
    assert_eq!(header.session_id, -1);
    assert_eq!(
      header.header_type(),
      Some(AofHeaderType::ShardedLogTransactionHeader)
    );
  }
}

#[test]
fn recover_until_converges_from_commit_cookies() {
  let log = log_with(2, 1);
  assert_eq!(log.recover_latest_sequence_number(), None);
  let _ = log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 1,
    session_id: 1,
    key: b"k",
    value: b"v",
    input: &[],
    database_id: 0,
  });
  log.commit();
  assert!(log.recover_latest_sequence_number().is_some());
}

#[test]
fn chunked_write_reassembles() {
  let log = log_with(1, 1);
  // 大值（超最小分配尺寸）经统一入队口自动分块（对象 upsert 恒写 value）
  let value = vec![b'x'; MIN_PARTIAL_ALLOC_SIZE as usize + 8];
  let address = log
    .enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreUpsert,
      version: 3,
      session_id: 9,
      key: b"big",
      value: &value,
      input: &[],
      database_id: 0,
    })
    .unwrap();
  assert!(address >= 0);
  let records = scan_collect(&log, 0, 0);
  assert!(records.len() >= 2);
  let chunk = AofChunkHeader::parse(&records[0].payload[16..]).unwrap();
  assert_eq!(chunk.overflow_value_length, value.len() as u32);
  assert_eq!(chunk.key_hash, GarnetLog::hash(b"big"));
  let data: Vec<u8> = records[1..]
    .iter()
    .flat_map(|r| r.payload.clone())
    .collect();
  assert_eq!(data, value);
}

#[test]
fn sequence_number_from_cookie() {
  let cookie = 123456789i64.to_le_bytes();
  assert_eq!(
    GarnetLog::get_sequence_number_from_cookie(&cookie),
    123456789
  );
}

#[test]
fn lock_bitmap_and_truncate() {
  let log = log_with(2, 1);
  log.lock_sublogs(0b11);
  log.unlock_sublogs(0b11);

  // 物理截断受 min(committed) 钳制：先广播提交标记令两子日志尾越过截断位点
  let _ = log.enqueue_database_commit(AofEntryType::FlushAll, 7);
  Runtime::new().unwrap().block_on(async {
    log.commit_async().await;
    let until = AofAddress::create(2, 5);
    log.truncate_until_async(&until).await;
    assert_eq!(log.begin_address().get(0), Some(5));
    assert_eq!(log.begin_address().get(1), Some(5));
  });
}

/// CommittedBeginAddress 独立快照语义（TsavoriteLog.cs:120）：
/// 初值 FirstValidAddress → commit 采样 begin（:2696）→ safe_initialize
/// 恢复（:528/:596）→ reset 归 FirstValidAddress（:244-246）。
#[test]
fn committed_begin_snapshot_lifecycle() {
  let (_dir, sublog_arc) = wnode_test::test_sublog("glog_cb");
  let sublog: &WaofSublog<wdev::SegmentedDevice> = &sublog_arc;
  // 初值 = FirstValidAddress（TsavoriteLog.cs:244 构造语义；真实段设备首地址 0）
  assert_eq!(sublog.committed_begin_address(), 0);

  Runtime::new().unwrap().block_on(async {
    // 提交后 committed_begin = 提交时刻 begin 快照
    //（128B 载荷令 committed 越过截断位点 64，见下）
    let _ = sublog.enqueue(&[0u8; 128]);
    sublog.commit_flush_async(NO_COOKIE).await;
    assert_eq!(sublog.committed_begin_address(), 0);

    // begin 前移（物理截断，对标 C# UnsafeShiftBeginAddress truncateLog:true）
    // 后 commit 重新采样：快照跟随新 begin，与实时 begin 在无恢复干扰时同值
    // 但路径独立；物理截断受 min(committed) 钳制，须先落盘令 committed 越过 64
    sublog.truncate_until_async(64).await;
    assert_eq!(sublog.begin_address(), 64);
    sublog.commit_flush_async(NO_COOKIE).await;
    assert_eq!(sublog.committed_begin_address(), 64);
  });

  // 恢复链：safe_initialize 以 begin 参数恢复 CommittedBeginAddress
  //（TsavoriteLog.cs:528/:596 Initialize = beginAddress）
  sublog.safe_initialize(128, 256, 0);
  assert_eq!(sublog.committed_begin_address(), 128);

  // reset 归 FirstValidAddress（TsavoriteLog.cs:244-246；真实段设备首地址 0）
  Runtime::new().unwrap().block_on(sublog.reset_async());
  assert_eq!(sublog.committed_begin_address(), 0);
}

/// 容量/占用双方法口径（TsavoriteLog.cs:196/:201）：内存后端无页预算，
/// max 与当前占用同值，占用 = 记录 payload 字节和。
#[test]
fn memory_size_capacity_and_usage() {
  let (_dir, sublog) = wnode_test::test_sublog("glog_mem");
  assert_eq!(sublog.memory_size_bytes(), 0);
  let _ = sublog.enqueue(b"0123456789");
  let _ = sublog.enqueue(b"0123");
  // 占用 = tail - begin（帧头 8B + payload），真实设备口径
  assert_eq!(
    sublog.memory_size_bytes(),
    (waof::RECORD_HEADER_LEN + 10 + waof::RECORD_HEADER_LEN + 4) as i64
  );
  // 容量上限 = 环形窗口字节数（WalConfig::default().buffer_size）
  assert_eq!(
    sublog.max_memory_size_bytes(),
    WalConfig::default().buffer_size as i64
  );
}

#[test]
fn test_garnet_log_advanced_methods() {
  let log = log_with(2, 1);

  let safe_addr = AofAddress::create(2, 10);
  log.initialize_if(&safe_addr);
  assert!(
    log
      .enqueue_safe_flush_aof(AofEntryType::CheckpointStartCommit, false, 100, 0)
      .unwrap()
      >= 0
  );

  // 单口径物理回收真身：unsafe_shift_begin_address 已转 async（对标 C#
  // UnsafeShiftBeginAddress truncateLog:true），截断受 min(committed) 钳制
  Runtime::new().unwrap().block_on(async {
    log.unsafe_shift_begin_address(0, 100).await;
  });
}

#[test]
fn test_wait_for_commit_async() {
  let log = Arc::new(log_with(2, 1));
  let log_clone = Arc::clone(&log);

  Runtime::new().unwrap().block_on(async move {
    let addr = log_clone.enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: b"test_key",
      value: b"test_val",
      input: &[],
      database_id: 0,
    });
    let physical = log_clone.get_physical_sublog_idx(GarnetLog::hash(b"test_key"));
    let tail = log_clone.get_tail_address(physical);
    let addr = addr.unwrap();
    assert!(tail > addr);

    let log_bg = Arc::clone(&log_clone);
    let bg_handle = thread::spawn(move || {
      thread::sleep(Duration::from_millis(10));
      log_bg.commit();
    });

    log_clone.wait_for_commit_async(physical, tail).await.unwrap();
    assert!(log_clone.get_sub_log(physical).committed_until_address() >= tail);
    bg_handle.join().unwrap();

    log_clone.wait_for_commit_all_async(0).await.unwrap();
  });
}
/// 闭包收集扫描（等价旧 scan_single Vec 面，测试专用）
///
/// commit 帧不入条目检视面：提交元数据帧由常驻提交协程随批尾异步写入同一日志流
///（waof/src/wal/commit.rs，对标 C# TsavoriteLog.cs:TryEnqueueCommitRecord），它与主
/// 线程后续入队的先后无保证，混入即令按位索引断言的扫描流构成漂移。数据检视面一律
/// 以单点判据 [`is_commit_frame`] 滤除（对标 C#
/// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogScanIterator.cs
/// :TryGetNext「Continue looping until we find a record that is not a commit
/// record」；本仓同判据的产品消费点见 aof_processor.rs）。
fn scan_collect(log: &GarnetLog, sublog_idx: usize, begin: i64) -> Vec<WalRecord> {
  let mut records = Vec::new();
  log.scan_single_with(sublog_idx, begin, i64::MAX, |rec| {
    if !is_commit_frame(&rec.payload) {
      records.push(rec.clone());
    }
    true
  });
  records
}

/// 分块写入并发原子性（条 1 回归）：多会话同时分块大记录与普通小记录，
/// 全部帧按地址序重组后内容必须逐字节无损
///
/// 修前各片独立 CAS 预占地址，普通记录可插入首片与续片之间（插花），
/// 重组器按地址序把外来负载并入进行中记录——value 被污染或记录被弃置；
/// 修后全部片经 enqueue_frames 单次预留连续落盘
///（单在途预留操作）
#[test]
fn chunked_write_atomic_under_concurrent_enqueue() {
  let log = Arc::new(log_with(1, 1));
  let chunk_threads = 2;
  let chunk_rounds = 8;
  let plain_rounds = 64;
  // 超最小分配尺寸即由统一入队口自动分块（对象 upsert 恒写 value）
  let value_len = MIN_PARTIAL_ALLOC_SIZE as usize + 64;

  let mut handles = Vec::new();
  for t in 0..chunk_threads {
    let log = Arc::clone(&log);
    handles.push(thread::spawn(move || {
      // value 以线程号参与的字节模式填充，重组后逐字节核验（插花即污染）
      let value: Vec<u8> = (0..value_len)
        .map(|i| ((t * 61 + i) & 0xFF) as u8)
        .collect();
      for _ in 0..chunk_rounds {
        log
          .enqueue(&RecordShape {
            op_type: AofEntryType::ObjectStoreUpsert,
            version: 1,
            session_id: t as i32,
            key: b"chunk-key",
            value: &value,
            input: &[],
            database_id: 0,
          })
          .expect("分块入队不应失败");
      }
    }));
  }
  for _ in 0..2 {
    let log = Arc::clone(&log);
    handles.push(thread::spawn(move || {
      for _ in 0..plain_rounds {
        log
          .enqueue(&RecordShape {
            op_type: AofEntryType::StoreUpsert,
            version: 1,
            session_id: 0,
            key: b"plain-key",
            value: b"plain-value",
            input: &[],
            database_id: 0,
          })
          .expect("普通入队不应失败");
      }
    }));
  }
  for h in handles {
    h.join().expect("写线程不应 panic");
  }

  // 全部帧按地址序喂重组器：普通帧被解析面安全跳过（无分块头）
  let records = scan_collect(&log, 0, 0);
  let plain_count = records
    .iter()
    .filter(|r| r.payload.windows(9).any(|w| w == b"plain-key"))
    .count();
  assert_eq!(plain_count, 2 * plain_rounds, "普通记录须完整无损");

  let mut reader = AofChunkedRecordReader::new();
  let mut completed = Vec::new();
  for rec in &records {
    if let Some(acc) = reader.read_chunk(&rec.payload) {
      completed.push(acc);
    }
  }
  assert_eq!(
    completed.len(),
    chunk_threads * chunk_rounds,
    "全部分块记录须完整重组（弃置即插花损坏）"
  );
  for acc in &completed {
    let t = acc.session_id as usize;
    let value = acc.object_value_bytes().into_owned();
    assert_eq!(
      value.len(),
      value_len,
      "key={} value 长度不符",
      String::from_utf8_lossy(acc.key_span())
    );
    for (i, b) in value.iter().enumerate() {
      assert_eq!(
        *b,
        ((t * 61 + i) & 0xFF) as u8,
        "session {t} value 第 {i} 字节被污染（插花损坏面）"
      );
    }
  }
}

/// 插花流防御性核验（文档性测试）：续片为纯组件数据无帧头，读取器按地址序
/// 并入唯一进行中记录——首片与续片之间插入外来记录帧即污染重组。
/// 这是写端必须经 enqueue_frames 单次预留连续落盘的本质原因；C# 无此约束
///（AofChunkedRecordReader.cs:ChunkedAccumulator 按 objectId 聚合且每段
/// 复现帧头，插花下重组仍正确），为刻意架构差异
///
/// 前置：插花流的帧位序取自 [`scan_collect`]，commit 元数据帧已按检视面契约滤除
///——否则外来帧不再居首，插花退化为顺序流，本用例断言失真（曾为偶发竞态红）。
#[test]
fn chunked_interleaved_foreign_frame_poisons_reassembly() {
  let log = log_with(1, 1);
  // 超最小分配尺寸：统一入队口自动分块为多帧（首帧带分块头）
  let value: Vec<u8> = (0..MIN_PARTIAL_ALLOC_SIZE as usize + 8)
    .map(|i| (i as u8).wrapping_mul(7))
    .collect();

  // 外来普通记录（先入队、地址最低，扫描首帧）
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 0,
      key: b"foreign",
      value: b"F",
      input: &[],
      database_id: 0,
    })
    .unwrap();
  let foreign = scan_collect(&log, 0, 0)
    .first()
    .expect("外来记录帧")
    .payload
    .clone();

  // 分块记录（原子产出 N 帧）
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreUpsert,
      version: 3,
      session_id: 9,
      key: b"big",
      value: &value,
      input: &[],
      database_id: 0,
    })
    .unwrap();
  let mut frames: Vec<Vec<u8>> = scan_collect(&log, 0, 0)
    .into_iter()
    .map(|r| r.payload)
    .collect();
  frames.remove(0); // 首帧为外来记录帧（先入队、地址最低）
  assert!(frames.len() >= 2, "分块记录须拆出多帧");

  // 受控插花流：首帧 → 外来帧 → 其余帧（旧实现的天然交错形态）
  let mut interleaved = vec![frames[0].clone(), foreign];
  interleaved.extend(frames[1..].iter().cloned());

  let mut reader = AofChunkedRecordReader::new();
  let mut poisoned = None;
  for frame in &interleaved {
    if let Some(acc) = reader.read_chunk(frame) {
      poisoned = Some(acc);
    }
  }
  let intact = poisoned
    .map(|acc| acc.object_value_bytes().into_owned() == value && acc.key_span() == b"big")
    .unwrap_or(false);
  assert!(
    !intact,
    "插花流不得重组出无损记录（读取器按地址序并入，写端必须原子落盘）"
  );
}
