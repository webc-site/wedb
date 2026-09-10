use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread::{available_parallelism, sleep, spawn, yield_now},
  time::Duration,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use waof::{Error, RECORD_HEADER_LEN, RecordHeader, WalConfig, WalLog};
use wdev::SegmentedDevice;

use super::support::{
  ShortWriteDevice, WalFixture, make_pattern_payload, make_payload, reopen_single_file,
};

/// 对标 C# LogTests.cs::EnqueueAndWaitForCommitAsync
/// 异步写入、提交落盘与已提交位点无锁等待。
#[test]
fn test_enqueue_and_wait_for_commit() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("enqueue_commit.log", 64 * 1024)?;
    let wal = fixture.wal;

    let mut expected_tail = 0u64;
    for i in 0..100 {
      let payload = format!("commit_test_record_{i}").into_bytes();
      let record_len = (RECORD_HEADER_LEN + payload.len()) as u64;
      let addr = wal.enqueue(&payload)?;
      assert_eq!(addr, expected_tail);
      expected_tail += record_len;
    }

    assert_eq!(wal.tail_address(), expected_tail);
    let committed = wal.commit().await?;
    assert_eq!(committed, expected_tail);
    assert_eq!(wal.committed_until_address(), expected_tail);

    // 对已提交的位点再次调用 wait_for_commit 应当极速直接返回
    let fast_committed = wal.wait_for_commit(expected_tail).await?;
    assert_eq!(fast_committed, expected_tail);

    let mut iter = wal.scan_committed();
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 100);

    info!("EnqueueAndWaitForCommit 异步写入与提交测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogTests.cs::TestTryEnqueue
/// 测试写缓冲区填满时的 BufferFull 阻断，以及通过 commit 刷盘释放空间后继续写入的循环覆写能力。
#[test]
fn test_try_enqueue_buffer_full_and_commit_cycle() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 较小缓冲区：16KB 容量（扇区大小取设备默认 4096）
    let fixture = WalFixture::single_file("try_enqueue.log", 16 * 1024)?;
    let wal = fixture.wal;

    let payload = make_payload(1000, 0xAB);
    let mut first_batch_count = 0;

    // 持续写入直到缓冲区满
    let mut hit_buffer_full = false;
    for _ in 0..100 {
      match wal.enqueue(&payload) {
        Ok(_) => first_batch_count += 1,
        Err(Error::BufferFull { .. }) => {
          hit_buffer_full = true;
          break;
        }
        Err(e) => return Err(e.into()),
      }
    }

    assert!(hit_buffer_full, "缓冲区应当在容量耗尽时返回 BufferFull");

    // 执行 commit 刷盘释放环形缓冲区空间
    wal.commit().await?;

    // 再次写入，应当恢复写入能力
    for _ in 0..10 {
      wal.enqueue(&payload)?;
    }
    wal.commit().await?;

    // 扫描全部记录，校验所有写入数据的完整性与 CRC32
    let mut iter = wal.scan(0, wal.tail_address());
    let all_records = iter.collect_all().await?;
    assert_eq!(all_records.len(), first_batch_count + 10);
    for rec in all_records {
      assert_eq!(rec.payload, payload);
    }

    info!("TryEnqueue 缓冲区满与 commit 淘汰循环覆写测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogTests.cs 边界与防护机制
/// 测试单条记录超出缓冲容量报错、零在途槽位防御、空记录写入与空日志提交。
#[test]
fn test_enqueue_payload_limits_and_config_guards() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let buf_size = 8 * 1024;
    let fixture = WalFixture::single_file("limits_guard.log", buf_size)?;
    let wal = fixture.wal;

    // 1. 超大记录拦截
    let max_allowed_payload_len = buf_size - RECORD_HEADER_LEN;
    let too_large = make_payload(max_allowed_payload_len + 1, 1);
    assert!(matches!(
      wal.enqueue(&too_large),
      Err(Error::RecordTooLarge { .. })
    ));

    // 2. 空日志提交返回 0
    let c0 = wal.commit().await?;
    assert_eq!(c0, 0);

    // 3. 空 payload 写入
    let addr = wal.enqueue(&[])?;
    assert_eq!(addr, 0);
    wal.commit().await?;

    let mut iter = wal.scan_committed();
    let rec = iter.next().await?.expect("应有空 payload 记录");
    assert!(rec.payload.is_empty());
    assert_eq!(rec.header.entry_len, 0);

    // 4. 零在途槽位防御
    let dir = tempdir()?;
    let db_path = dir.path().join("zero_inflight.log");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let config = WalConfig {
      inflight_slots: 0,
      ..Default::default()
    };
    let zero_wal = WalLog::new(device, config)?;
    assert_eq!(zero_wal.inflight_slots.len(), 1);
    let zero_addr = zero_wal.enqueue(b"safe fallback with min 1 slot")?;
    assert_eq!(zero_addr, 0);

    info!("Enqueue 边界限制与配置守卫测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogFastCommitTests.cs::CommitRecordBoundedGrowthTest
/// 高并发多线程写入伴随并发提交，验证安全尾部边界一致性与零数据竞争损坏。
#[test]
fn test_commit_record_bounded_growth_concurrent_enqueue() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::segmented("concurrent_growth.log", 64 * 1024, 128 * 1024)?;
    let wal = fixture.wal;

    let num_threads = 4;
    let records_per_thread = 200;

    let mut handles = Vec::new();
    for t_id in 0..num_threads {
      let wal_clone = Arc::clone(&wal);
      let handle = spawn(move || {
        for i in 0..records_per_thread {
          let mut data = [0u8; 64];
          data[0] = t_id as u8;
          data[1..5].copy_from_slice(&(i as u32).to_le_bytes());
          wal_clone.enqueue(&data).expect("并发 enqueue 应当成功");
        }
      });
      handles.push(handle);
    }

    // 主线程在写入期间并发执行 commit 刷盘
    for _ in 0..5 {
      let _ = wal.commit().await;
      yield_now();
    }

    for h in handles {
      h.join().expect("线程应当正常结束");
    }

    // 最终提交全部在途与已写入数据
    let final_tail = wal.commit().await?;
    assert_eq!(final_tail, wal.tail_address());

    // 扫描全部记录
    let mut iter = wal.scan(0, final_tail);
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), num_threads * records_per_thread);

    // 统计各线程记录数量
    let mut thread_counts = [0usize; 4];
    for rec in records {
      let t_id = rec.payload[0] as usize;
      assert!(t_id < num_threads);
      thread_counts[t_id] += 1;
    }

    for (t_id, &count) in thread_counts.iter().enumerate() {
      assert_eq!(count, records_per_thread, "线程 {t_id} 记录数应完整");
    }

    info!("CommitRecordBoundedGrowth 高并发写入与并发 commit 测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogFastCommitTests.cs::FastCommitConcurrentWaiters
/// 多线程并发发起 enqueue 并在后台并发调用 wait_for_commit 等待位点持久化，
/// 验证协同组提交机制下的数据完整性与零死锁。
#[test]
fn test_fast_commit_concurrent_waiters_barrier() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("fast_commit_barrier.log", 2 * 1024 * 1024)?;
    let wal = fixture.wal;

    // 并发度按可用核数自适应（核数×2，夹在 [4,8]）：避免低核 CI 上
    // 核数+1 个 runtime 线程严重过饱和，协同提交被极端调度延迟拖到超时
    let thread_count = (available_parallelism().map_or(4, |n| n.get()) * 2).clamp(4, 8);
    let ops_per_thread = 200 / thread_count;

    // 看门狗：卡死时持续输出位点/锁/进度，nextest 超时回放 stdout 抢救现场
    let done = Arc::new(AtomicUsize::new(0));
    let watchdog_on = Arc::new(AtomicBool::new(false));
    {
      let wal = Arc::clone(&wal);
      let done = Arc::clone(&done);
      let watchdog_on = Arc::clone(&watchdog_on);
      spawn(move || {
        while !watchdog_on.load(Ordering::Relaxed) {
          sleep(Duration::from_secs(5));
          let lock_free = wal.commit_lock.try_lock().is_some();
          println!(
            "watchdog: committed={} flushed={} tail={} safe_tail={} lock_free={} done={}",
            wal.committed_until_address.load(Ordering::Relaxed),
            wal.flushed_until_address.load(Ordering::Relaxed),
            wal.tail_address.load(Ordering::Relaxed),
            wal.safe_tail_address(),
            lock_free,
            done.load(Ordering::Relaxed),
          );
        }
      });
    }

    let mut handles = Vec::with_capacity(thread_count);
    for t_id in 0..thread_count {
      let wal_clone = Arc::clone(&wal);
      let done = Arc::clone(&done);
      let handle = spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
          for i in 0..ops_per_thread {
            let payload = format!("barrier-thread-{t_id}-item-{i}").into_bytes();
            let addr = wal_clone.enqueue(&payload).unwrap();
            let target_addr = addr + (RECORD_HEADER_LEN + payload.len()) as u64;
            let committed = wal_clone.wait_for_commit(target_addr).await.unwrap();
            assert!(committed >= target_addr);
            done.fetch_add(1, Ordering::Relaxed);
          }
        });
      });
      handles.push(handle);
    }

    for h in handles {
      h.join().unwrap();
    }
    watchdog_on.store(true, Ordering::Relaxed);

    // 最终全部提交
    let final_tail = wal.commit().await?;
    assert_eq!(final_tail, wal.tail_address());

    // 扫描校验
    let mut iter = wal.scan_committed();
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), thread_count * ops_per_thread);
    for rec in records {
      rec.header.verify(&rec.payload)?;
    }

    info!("FastCommitConcurrentWaiters 多线程快速提交屏障测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogCommitFailureTests.cs
/// 验证 commit 短写入守卫：设备返回 Ok(部分字节) 时 commit 必须显式报错，
/// 且绝不推进 flushed/committed 位点（防止已提交区间在磁盘上存在数据空洞）。
#[test]
fn test_commit_short_write_guard() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("short_write.log");
    let device = Arc::new(ShortWriteDevice::single_file(&db_path)?);

    let config = WalConfig::new(64 * 1024);
    let wal = WalLog::new(device, config)?;

    wal.enqueue(b"record-one")?;
    wal.enqueue(b"record-two")?;

    // 短写入必须显式报错且位点保持不变
    assert!(matches!(wal.commit().await, Err(Error::ShortWrite { .. })));
    assert_eq!(wal.flushed_until_address(), 0);
    assert_eq!(wal.committed_until_address(), 0);

    // 失败的 commit 可安全重试，继续报错且位点不脏推进
    assert!(matches!(wal.commit().await, Err(Error::ShortWrite { .. })));
    assert_eq!(wal.flushed_until_address(), 0);
    assert_eq!(wal.committed_until_address(), 0);

    info!("Commit 短写入守卫与位点不回滚测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 WalLog::enqueue_raw（复制从节点保真落盘场景，对应 TsavoriteLog.UnsafeTryEnqueueRaw）
/// raw 帧与 enqueue 交替写入、帧字节逐字保真读回、重启恢复位点一致、
/// 从节点按帧序列重放地址与主机逐条一致，以及过短/超长帧错误路径。
#[test]
fn test_enqueue_raw_frame_fidelity() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("raw_frame.log", 64 * 1024)?;
    let wal = fixture.wal;

    // 主机侧：enqueue 与 raw 帧交替写入
    let mut frames: Vec<Vec<u8>> = Vec::new();
    for i in 0..5u32 {
      let payload = make_pattern_payload(i as usize, 32 + i as usize * 10);
      wal.enqueue(&payload)?;
      let raw_payload = make_pattern_payload(100 + i as usize, 20 + i as usize * 7);
      let mut frame = RecordHeader::for_payload(&raw_payload).to_bytes().to_vec();
      frame.extend_from_slice(&raw_payload);
      wal.enqueue_raw(&frame)?;
      frames.push(frame);
    }

    // 读回：记录交错且 payload 一致，raw 记录头 + 负载逐字等于写入帧
    let mut iter = wal.scan(0, wal.tail_address());
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 10);
    for (i, rec) in records.iter().enumerate() {
      if i % 2 == 0 {
        assert_eq!(rec.payload, make_pattern_payload(i / 2, 32 + (i / 2) * 10));
      } else {
        let mut full = rec.header.to_bytes().to_vec();
        full.extend_from_slice(&rec.payload);
        assert_eq!(full, frames[(i - 1) / 2]);
      }
    }

    // 提交后重开恢复，位点与记录链一致
    wal.commit().await?;
    let tail = wal.tail_address();
    let wal2 = reopen_single_file(fixture.dir.path(), "raw_frame.log", 64 * 1024).await?;
    assert_eq!(wal2.tail_address(), tail);
    let mut iter2 = wal2.scan_committed();
    assert_eq!(iter2.collect_all().await?.len(), 10);

    // 从节点模拟：空日志按主机帧序列 enqueue_raw，地址与主机逐条一致
    let replica_fixture = WalFixture::single_file("raw_replica.log", 64 * 1024)?;
    let replica = replica_fixture.wal;
    let mut expect_addr = 0u64;
    for frame in &frames {
      let addr = replica.enqueue_raw(frame)?;
      assert_eq!(addr, expect_addr, "从节点重放地址应与主机一致");
      expect_addr += frame.len() as u64;
    }
    let mut iter3 = replica.scan(0, replica.tail_address());
    let replayed = iter3.collect_all().await?;
    for (rec, frame) in replayed.iter().zip(&frames) {
      let mut full = rec.header.to_bytes().to_vec();
      full.extend_from_slice(&rec.payload);
      assert_eq!(&full, frame);
    }

    // 错误路径：过短帧、错位帧（头与负载长度不符 / 全零头）与超长帧
    assert!(matches!(
      replica.enqueue_raw(&[0u8; 4]),
      Err(Error::InvalidRecordHeader)
    ));
    let mut torn = RecordHeader::for_payload(b"abc").to_bytes().to_vec();
    torn.extend_from_slice(b"abcd");
    assert!(matches!(
      replica.enqueue_raw(&torn),
      Err(Error::InvalidRecordHeader)
    ));
    assert!(matches!(
      replica.enqueue_raw(&[0u8; 16]),
      Err(Error::InvalidRecordHeader)
    ));
    // 超长帧：帧头自洽但总长超出缓冲容量
    let oversized_len = 64 * 1024 + 1;
    let mut oversized =
      RecordHeader::new(oversized_len as u32 - RECORD_HEADER_LEN as u32, 0xDEAD_BEEF)
        .to_bytes()
        .to_vec();
    oversized.resize(oversized_len, 0xEE);
    assert!(matches!(
      replica.enqueue_raw(&oversized),
      Err(Error::RecordTooLarge { .. })
    ));

    info!("EnqueueRaw 帧保真、重放地址一致与错误路径测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
