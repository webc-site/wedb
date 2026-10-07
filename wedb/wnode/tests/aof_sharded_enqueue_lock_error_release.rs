#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 分片 AOF 广播/事务入队锁段错误路径释放回归（对标 C#
//! libs/server/AOF/GarnetLog.cs:EnqueueStoredProc :1029-1045、EnqueueTxn
//! 分片臂 :1106-1121、EnqueueBroadcastEntry :1226-1244 三处
//! try { LockSublogs; 循环 Enqueue } finally { UnlockSublogs } 锁段）。
//!
//! 回归点：分片臂循环内 enqueue 终态错误经 `?` 早退时，子日志位图锁必须随
//! RAII 守卫（[`wnode::aof::garnet_log::GarnetLog`] 锁段守卫）释放。修复前
//! unlock 不可达，位图位永久占用，此后一切需锁同位图的事务标记
//!（TxnStart/TxnCommit/EXEC 组）与广播条目（FLUSH/检查点标记）在
//! lock_sublogs 永久挂起——磁盘致命故障劣化为静默挂起。
//!
//! 全程真实组件无 mock，两条终态注入链均为生产同款：
//! 1. 超窗记录 → waof 确定性 [`waof::Error::RecordTooLarge`]（入队即判，
//!    不触碰任何设备面）；
//! 2. 只读 [`wdev::SegmentedDevice`]（C# ManagedLocalStorageDevice readOnly
//!    对标）→ 常驻提交协程真实写失败 → `flush_failures` 非零绝对判定 →
//!    [`waof::Error::FlushFailed`] 终态（对标 C# cannedException 一次性置位
//!    后永久重抛，杜绝坏盘无限挂起）。

use std::{
  fs::File,
  mem::size_of,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use tempfile::tempdir;
use waof::{
  AofEntryType, AofShardedHeader, AofShardedLogTransactionHeader, Error, RECORD_HEADER_LEN,
  SequenceNumberGenerator, WalConfig, WalLog,
};
use wbase::{align::DEFAULT_SECTOR_SIZE, store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES};
use wconf::RuntimeServerOptions;
use wdev::{DeviceParams, SegmentedDevice};
use wnode::aof::{
  WaofSublog,
  garnet_log::{GarnetLog, RecordShape},
};
use wtxn::SublogAccess;

/// 环形窗口容量（扇区 4096 整数倍）：超窗即 waof 确定性终态，恰量填充可逼满
const WINDOW: usize = 64 * 1024;

/// 广播条目帧总长（记录头 + 分片事务头；enqueue_database_commit 的 extra 为空）
const BROADCAST_FRAME_LEN: usize = RECORD_HEADER_LEN + AofShardedLogTransactionHeader::TOTAL_SIZE;

/// 恰量填充后的残留窗孔（字节）：小于广播帧长，令广播帧预留必满载退回
const FILL_HOLE: usize = 32;

const _: () = assert!(
  FILL_HOLE < BROADCAST_FRAME_LEN,
  "残留窗孔必须容不下广播帧，否则满载退回不成立"
);

/// 测试段文件段容（对齐 SegmentedDevice::single_file 缺省段容）
const SEGMENT_SIZE: u64 = 1 << 30;

/// 锁重获探测的有界等待：修复前泄漏位图令探测线程永久阻塞，超时即断言失败
///（测试自我了断而非挂死整个跑批）
const LOCK_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// 两物理子日志分片装配：commit_frequency_ms=1 关自动提交（提交协程只在
/// 入队背压腾窗时被真实踢起），其余选项保持缺省
fn sharded_options() -> RuntimeServerOptions {
  RuntimeServerOptions {
    aof_physical_sublog_count: 2,
    aof_replay_task_count: 1,
    commit_frequency_ms: 1,
    ..RuntimeServerOptions::default()
  }
}

/// 窗口容量收缩的 WAL 配置（页随窗同值）
fn window_config() -> WalConfig {
  WalConfig {
    buffer_size: WINDOW,
    page_size: WINDOW,
    ..WalConfig::default()
  }
}

/// 跨两物理子日志的事务参与者位图（位图 0b11 = 广播/事务标记的锁段范围）
fn full_access() -> SublogAccess<'static> {
  SublogAccess {
    physical_vector: 0b11,
    virtual_vectors: &[[0; REPLAY_TASK_ACCESS_VECTOR_BYTES]; 2],
    participant_count: 2,
  }
}

/// 有界等待位图锁重新可获取：修复前错误路径漏 unlock，子线程永久阻塞，
/// 此处超时即断言失败（票面验证点「lock_sublogs 可再次获取（不挂起）」）
fn assert_lock_reacquirable(log: &Arc<GarnetLog>, bitmap: u64) {
  let acquired = Arc::new(AtomicBool::new(false));
  let probe_log = Arc::clone(log);
  let flag = Arc::clone(&acquired);
  let handle = thread::spawn(move || {
    probe_log.lock_sublogs(bitmap);
    flag.store(true, Ordering::Release);
    probe_log.unlock_sublogs(bitmap);
  });
  let deadline = Instant::now() + LOCK_PROBE_TIMEOUT;
  while !acquired.load(Ordering::Acquire) {
    assert!(
      Instant::now() < deadline,
      "子日志位图锁错误路径泄漏：lock_sublogs({bitmap:#b}) 无法再次获取"
    );
    thread::sleep(Duration::from_millis(5));
  }
  handle.join().expect("探测线程不得 panic");
}

/// 存储过程/事务标记分片臂：超窗终态错误（RecordTooLarge）经 `?` 早退后，
/// 位图锁须立即可重获、不残留半写，后续事务标记正常入队（对标 C#
/// EnqueueStoredProc 的 finally 释放臂）
#[test]
fn stored_proc_sharded_enqueue_error_releases_sublog_lock() {
  let (_dirs, backends) =
    wnode_test::test_sublogs_with_config("aof_lock_sp_err", 2, window_config());
  let log = Arc::new(
    GarnetLog::new(
      &sharded_options(),
      backends,
      Some(Arc::new(SequenceNumberGenerator::new(0))),
    )
    .expect("构造分片 GarnetLog"),
  );
  let access = full_access();

  // 超窗 body：首个参与子日志 enqueue_parts 即确定性终态（入队即判，
  // 不触碰设备面），循环内 `?` 早退——修复前位图 0b11 在此永久泄漏
  let body = vec![0u8; WINDOW * 2];
  let err = log
    .enqueue_stored_proc(AofEntryType::StoredProcedure, 1, 7, 42, &body, &access)
    .expect_err("超窗记录须以终态错误上抛而非挂起");
  assert!(
    matches!(err, Error::RecordTooLarge { .. }),
    "实际错误: {err:?}"
  );

  // 终态错误不得残留半写（长度判定先于地址预留，两子日志尾均未推进）
  for sublog_idx in 0..2 {
    assert_eq!(
      log.get_sub_log(sublog_idx).tail_address(),
      log.get_sub_log(sublog_idx).begin_address(),
      "子日志 {sublog_idx} 尾被终态错误路径污染"
    );
  }

  // 票面验证点：错误路径后位图可再次获取（不挂起）
  assert_lock_reacquirable(&log, 0b11);

  // 后续事务标记正常入队：两参与子日志各精确推进一条标记帧（记录 =
  // 记录头 + 分片事务头，空 body；返回地址取最后写入子日志的预留位，
  // 空日志首条为 0，不足为凭，以双尾推进为真凭）
  log
    .enqueue_txn(AofEntryType::TxnStart, 2, 7, &access)
    .expect("错误路径恢复后事务标记须正常入队");
  for sublog_idx in 0..2 {
    assert_eq!(
      log.get_sub_log(sublog_idx).tail_address() - log.get_sub_log(sublog_idx).begin_address(),
      (RECORD_HEADER_LEN + AofShardedLogTransactionHeader::TOTAL_SIZE) as i64,
      "子日志 {sublog_idx} 须精确落一条事务标记帧"
    );
  }
}

/// 广播条目分片臂：磁盘致命故障（只读设备 → 常驻提交协程真实写失败 →
/// flush_failures 非零 → FlushFailed 终态）经 `?` 早退后，位图锁须立即可
/// 重获、复入广播口以终态即时返回而非坏盘无限挂起（对标 C#
/// EnqueueBroadcastEntry 的 finally 释放臂与 cannedException 终态抛出）
#[test]
fn broadcast_sharded_enqueue_error_releases_sublog_lock() {
  // 子日志 0：只读真实段设备（提交协程写失败根因；打开不带写权限，
  // 预建空段文件供只读挂载）；子日志 1：可写同款
  let ro_dir = tempdir().expect("tempdir");
  let ro_path = ro_dir.path().join("aof_lock_ro_err_0.wal");
  File::create(&ro_path).expect("预建空段文件");
  let ro_device = Arc::new(
    SegmentedDevice::with_params(
      ro_path.as_path(),
      SEGMENT_SIZE,
      DEFAULT_SECTOR_SIZE,
      DeviceParams {
        read_only: true,
        ..DeviceParams::default()
      },
    )
    .expect("只读设备装配"),
  );
  let ro_wal = WalLog::new(Arc::clone(&ro_device), window_config()).expect("只读 WalLog 装配");
  let (_dirs, mut backends) =
    wnode_test::test_sublogs_with_config("aof_lock_ro_err", 2, window_config());
  backends[0] = Arc::new(WaofSublog::new(Arc::new(ro_wal)));
  let log = Arc::new(
    GarnetLog::new(
      &sharded_options(),
      backends,
      Some(Arc::new(SequenceNumberGenerator::new(0))),
    )
    .expect("构造分片 GarnetLog"),
  );

  // 探路键：命中物理子日志 0（广播循环以最低位先写子日志 0）
  let fill_key = (0..64)
    .map(|i| format!("ro_fill{i}"))
    .find(|k| log.get_physical_sublog_idx(GarnetLog::hash(k.as_bytes())) == 0)
    .expect("64 个候选键必有路由子日志 0 者");

  // 恰量填充：记录总长 = 窗口 - FILL_HOLE（记录头 + Sharded 头（Basic 16B
  // + 序列号 8B：分片拓扑 enqueue_with_header 必写序列号部件）+ 4B 键长前缀
  // + 键 + 4B 值长前缀 + 值，空 input），只占内存窗口不刷盘（自动提交已关），
  // 残留孔容不下广播帧
  let fill_value_len = WINDOW
    - FILL_HOLE
    - RECORD_HEADER_LEN
    - AofShardedHeader::TOTAL_SIZE
    - 2 * size_of::<u32>()
    - fill_key.len();
  let fill_value = vec![0u8; fill_value_len];
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 7,
      key: fill_key.as_bytes(),
      value: &fill_value,
      input: &[],
      database_id: 0,
    })
    .expect("恰量填充记录须落入子日志内存窗口");
  assert_eq!(
    log.get_sub_log(0).tail_address() - log.get_sub_log(0).begin_address(),
    (WINDOW - FILL_HOLE) as i64,
    "填充须精确落在子日志 0 并残留容不下广播帧的窗孔"
  );

  // 广播条目：子日志 0 满载 → BufferFull → 踢起提交协程腾窗 → 只读设备写
  // 失败 → flush_failures 非零 → FlushFailed 终态，循环内 `?` 早退——修复前
  // 位图 0b11 在此永久泄漏，此后事务/广播标记全部在 lock_sublogs 挂起
  let err = log
    .enqueue_database_commit(AofEntryType::FlushDb, 1)
    .expect_err("磁盘致命故障须以 FlushFailed 终态上抛而非挂起");
  assert!(matches!(err, Error::FlushFailed), "实际错误: {err:?}");
  assert!(
    log.get_sub_log(0).flush_failures() > 0,
    "只读设备须已令提交协程留痕刷盘失败（终态判定真源）"
  );

  // 票面验证点：错误路径后位图可再次获取（不挂起）
  assert_lock_reacquirable(&log, 0b11);

  // 复入广播口仍以终态即时返回：锁可重入（无死锁）、坏盘不无限重试
  let err = log
    .enqueue_database_commit(AofEntryType::FlushDb, 2)
    .expect_err("复入广播口须仍以终态错误即时返回");
  assert!(matches!(err, Error::FlushFailed), "实际错误: {err:?}");
}
