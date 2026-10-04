//! waof 环形写缓冲满载「背压不丢记录」集成回归
//!（对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:AllocateBlock
//! 满载挂起 flushEvent.Wait 后重试的背压语义，与
//! libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:TryAllocateRetryNow
//! 拒绝即等待、绝不向调用方抛瞬时满）。
//!
//! 判据：写缓冲打满时，入队侧绝不静默丢弃记录——主存写入序列与 AOF 落盘序列逐条
//! 一致、不丢不重不乱序；副本按同一序列重放后与主库 AOF 流完全一致（主从比对）。
//! 修复前 `WaofSublog::enqueue*` 直接把 `Error::BufferFull` 透传给存储写端口，主存
//! 已写而 AOF 缺条目 → 崩溃/副本永久静默发散；修复后转入腾窗等待重试，杜绝该丢法。

use std::{
  collections::BTreeMap,
  sync::{Arc, Barrier},
  thread,
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::tempdir;
use waof::{Error, FsyncPolicy, WalLog};
use wdev::SegmentedDevice;
use wnode::aof::WaofSublog;

/// 测试环形窗口容量（扇区 4096 的整数倍）；刻意远小于单测总写入量，逼出入队
/// 背压腾窗循环
const BUFFER_SIZE: usize = 64 * 1024;
/// 单条数据记录负载长度（含 8 字节记录头后仍远小于 BUFFER_SIZE，可正常腾窗）
const RECORD_LEN: usize = 16 * 1024;

/// 构造指定环形窗口容量的真实段文件子日志（deferred 落盘：仅写设备页缓存即推进
/// 刷盘水位，本测验证序列完整性、不含掉电，故免 fdatasync 提速腾窗节拍）。
///
/// 绝不用 mock——`SegmentedDevice` + `WalLog` 为生产同款设备/日志内核，`reserve_address`
/// 的 BufferFull 判定与 `flushed_until_address` 推进均真实发生
fn sublog_with_buffer(tag: &str) -> (tempfile::TempDir, Arc<WaofSublog<SegmentedDevice>>) {
  let dir = tempdir().expect("tempdir");
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).expect("设备装配"),
  );
  let config = waof::WalConfig {
    buffer_size: BUFFER_SIZE,
    page_size: BUFFER_SIZE,
    inflight_slots: 256,
    fsync: FsyncPolicy::Deferred,
  };
  let wal = WalLog::new(device, config).expect("WalLog 装配");
  (dir, Arc::new(WaofSublog::new(Arc::new(wal))))
}

/// 生成第 `seq` 条可辨识负载（每条字节唯一，令「不重」断言可证伪：任一重复或
/// 缺失都会在序列比对中暴露）
fn payload_at(seq: usize) -> Vec<u8> {
  let mut buf = vec![0u8; RECORD_LEN];
  // 前 8 字节写入序号小端编码作身份，其余填派生字节
  buf[..8].copy_from_slice(&(seq as u64).to_le_bytes());
  for (i, b) in buf[8..].iter_mut().enumerate() {
    *b = (seq.wrapping_mul(31).wrapping_add(i) % 251) as u8;
  }
  buf
}

/// 落盘并全量扫描回读数据记录序列（提交帧经 `is_commit_frame` 滤除），跨内存
/// 环形窗口与历史磁盘段，返回 `(起始地址, 负载)` 按地址序向量
async fn scan_data_records(sublog: &WaofSublog<SegmentedDevice>) -> Vec<(i64, Vec<u8>)> {
  let tail = sublog.tail_address();
  let mut it = sublog.scan_iter(sublog.begin_address(), tail);
  let mut seen = Vec::new();
  while let Some(rec) = it.next().await.expect("扫描设备段读取失败") {
    if !waof::is_commit_frame(&rec.payload) {
      seen.push((rec.address as i64, rec.payload));
    }
  }
  // 覆写丢数判据：非零即扫描中途因未提交数据被环形覆写而提前终止（数据不完整），
  // 本测背压保证写不超前刷，正常应恒为 0
  assert_eq!(
    it.overwritten_skips(),
    0,
    "扫描出现未提交数据被环形覆写，说明写入超前于刷盘（背压失效）"
  );
  seen
}

/// 核心回归：单线程突发写入打满环形缓冲，入队侧背压腾窗重试，AOF 落盘序列与
/// 主存写入序列逐条一致（不丢不重不乱序）——修复前该场景部分入队返回 BufferFull
/// 被写端口丢弃，AOF 序列短于主存序列即主从发散根因
#[test]
fn backpressure_keeps_aof_stream_identical_to_writes() -> Void {
  // 总写入量 ≈ 1MB ≫ BUFFER_SIZE(64KB)：环形窗口无法同时容纳全部记录，
  // 每条越过窗界的写入都必经 BufferFull → 刷盘腾窗 → 重试，真实复刻缓冲满载
  const RECORDS: usize = 64;
  let (_dir, sublog) = sublog_with_buffer("bp_single");

  let mut expected: Vec<Vec<u8>> = Vec::with_capacity(RECORDS);
  let mut written_addr: Vec<i64> = Vec::with_capacity(RECORDS);
  for seq in 0..RECORDS {
    let data = payload_at(seq);
    // 断言「入队绝不失败」：修复前满载条目在此 panic（Err(BufferFull)），
    // 修复后背压腾窗必然返回 Ok
    let addr = sublog
      .enqueue(&data)
      .expect("缓冲满载入队不得失败（背压须腾窗重试）");
    expected.push(data);
    written_addr.push(addr);
  }

  // 地址严格递增且互异（单写者下连续），复现「入队即占位、不回收不重用」
  for w in written_addr.windows(2) {
    assert!(
      w[0] < w[1],
      "入队返回地址须严格递增，实际 {:?}→{:?}",
      w[0],
      w[1]
    );
  }

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 末批刷盘：令全部数据经设备段落盘，验证崩溃后 AOF 与主存同源（磁盘一致性）
    let tail = sublog.tail_address();
    sublog.commit_flush_async(0).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while sublog.flushed_until_address() < tail {
      assert!(Instant::now() < deadline, "末批刷盘未在限时内推进");
      // 让位常驻提交协程（本线程 compio runtime 与提交线程不同核）
      sleep(Duration::from_millis(1)).await;
    }

    let seen = scan_data_records(&sublog).await;
    assert_eq!(
      seen.len(),
      RECORDS,
      "AOF 数据记录数须等于主存写入数（丢失/重复即主从发散）"
    );
    for (i, (addr, data)) in seen.iter().enumerate() {
      assert_eq!(
        *addr, written_addr[i],
        "第 {i} 条 AOF 记录地址须与入队返回地址一致（无插空/重排）"
      );
      assert_eq!(
        data.as_slice(),
        expected[i].as_slice(),
        "第 {i} 条 AOF 记录负载须与主存写入一致"
      );
    }
  });

  OK
}

/// 并发突发写入打满环形缓冲（对标 C# 高并发入队共享单一 flushEvent 背压）：
/// 多线程同时刷写远超窗容量的记录，全部入队成功，回读 AOF 序列无重复、无丢失，
/// 记录地址集合恰为入队返回地址集合
#[test]
fn concurrent_burst_backpressure_no_loss_no_dup() -> Void {
  const THREADS: usize = 6;
  const PER_THREAD: usize = 24;

  let (_dir, sublog) = sublog_with_buffer("bp_concurrent");
  let barrier = Arc::new(Barrier::new(THREADS));

  // 各线程持有子日志 Arc 克隆并发入队（生产写路径为 thread-per-core 多核并发）
  let mut joins = Vec::with_capacity(THREADS);
  for t in 0..THREADS {
    let sublog = Arc::clone(&sublog);
    let barrier = Arc::clone(&barrier);
    joins.push(thread::spawn(move || {
      let mut local: Vec<(i64, Vec<u8>)> = Vec::with_capacity(PER_THREAD);
      barrier.wait();
      for k in 0..PER_THREAD {
        // 全局唯一序号：seq = t * PER_THREAD + k，负载可辨识、跨线程不碰撞
        let seq = t * PER_THREAD + k;
        let data = payload_at(seq);
        let addr = sublog
          .enqueue(&data)
          .expect("并发突发满载入队不得失败（背压须腾窗重试）");
        local.push((addr, data));
      }
      local
    }));
  }

  let mut all: Vec<(i64, Vec<u8>)> = Vec::with_capacity(THREADS * PER_THREAD);
  for j in joins {
    all.extend(j.join().expect("入队线程不得 panic"));
  }

  let rt = Runtime::new()?;
  rt.block_on(async {
    let tail = sublog.tail_address();
    sublog.commit_flush_async(0).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while sublog.flushed_until_address() < tail {
      assert!(Instant::now() < deadline, "末批刷盘未在限时内推进");
      sleep(Duration::from_millis(1)).await;
    }

    let seen = scan_data_records(&sublog).await;
    assert_eq!(
      seen.len(),
      THREADS * PER_THREAD,
      "AOF 记录总数须等于并发入队总数（并发丢记录即主从发散）"
    );

    // 「不重」：入队地址两两互异；「不丢」：回读地址集合 == 入队地址集合
    let mut written: Vec<i64> = all.iter().map(|(a, _)| *a).collect();
    written.sort_unstable();
    assert!(
      written.windows(2).all(|w| w[0] != w[1]),
      "并发入队地址出现重复，槽位/CAS 预占非单点"
    );
    let mut read_addrs: Vec<i64> = seen.iter().map(|(a, _)| *a).collect();
    read_addrs.sort_unstable();
    assert_eq!(read_addrs, written, "AOF 回读地址集合须逐等入队地址集合");

    // 「负载无缺失且一一对应」：按地址对齐回读负载 == 入队负载
    let by_addr: BTreeMap<i64, &Vec<u8>> = all.iter().map(|(a, d)| (*a, d)).collect();
    for (addr, data) in &seen {
      assert_eq!(
        *by_addr.get(addr).expect("回读地址须在册"),
        data,
        "地址 {addr} 的 AOF 负载与入队负载不一致"
      );
    }
  });

  OK
}

/// 确定性过大记录一次性拒绝，绝不进入腾窗重试（对标 C# `ValidateAllocatedLength`
/// 于 AllocateBlock 之前抛出，与瞬时 BufferFull 严格区分）：单条记录超过环形窗口
/// 容量即返回 `RecordTooLarge`，不挂起、不死循环
#[test]
fn record_too_large_rejected_once_without_backpressure_retry() -> Void {
  let (_dir, sublog) = sublog_with_buffer("bp_toolarge");
  // 负载 + 记录头 > BUFFER_SIZE，命中 check_record_len 的窗口上限拒绝
  let oversized = vec![0u8; BUFFER_SIZE];
  let err = sublog
    .enqueue(&oversized)
    .expect_err("超窗记录须被一次性拒绝");
  assert!(
    matches!(err, Error::RecordTooLarge { .. }),
    "超窗记录须返回 RecordTooLarge 而非 BufferFull/FlushFailed（后者意味误入重试）: {err:?}"
  );
  OK
}

/// 副本重放主从比对：主库满载背压落盘的 AOF 数据流，逐条按地址保真重放到副本
/// 子日志（`enqueue` 走同一背压机制），副本回读序列与主库、与原始写入序列三方
/// 全等，证明背压腾窗后主从 AOF 流严格同源（修复前主库漏记 → 副本无从对齐）
#[test]
fn replica_replay_matches_primary_stream() -> Void {
  const RECORDS: usize = 40;
  let (_dir, primary) = sublog_with_buffer("bp_primary");

  let mut expected: Vec<Vec<u8>> = Vec::with_capacity(RECORDS);
  for seq in 0..RECORDS {
    let data = payload_at(seq);
    primary.enqueue(&data).expect("主库满载入队不得失败");
    expected.push(data);
  }

  let rt = Runtime::new()?;
  rt.block_on(async {
    let _tail = primary.tail_address();
    primary.commit_flush_async(0).await;
    let primary_stream = scan_data_records(&primary)
      .await
      .into_iter()
      .map(|(_, d)| d)
      .collect::<Vec<_>>();

    // 副本装配：独立设备同容量窗口，按主库数据流逐条重放（同走背压路径）
    let (_rdir, replica) = sublog_with_buffer("bp_replica");
    for data in &expected {
      replica
        .enqueue(data)
        .expect("副本按主库序列重放入队不得失败");
    }
    replica.commit_flush_async(0).await;
    let replica_stream = scan_data_records(&replica)
      .await
      .into_iter()
      .map(|(_, d)| d)
      .collect::<Vec<_>>();

    // 三方全等：主库 AOF 流 == 副本重放流 == 原始写入序列（不丢不重不乱序）
    assert_eq!(primary_stream.len(), RECORDS, "主库 AOF 流长度须等于写入数");
    assert_eq!(replica_stream.len(), RECORDS, "副本重放流长度须等于写入数");
    assert_eq!(primary_stream, expected, "主库 AOF 流须逐条等于写入序列");
    assert_eq!(replica_stream, expected, "副本重放流须逐条等于写入序列");
  });

  OK
}
