use std::{sync::Arc, thread::spawn};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;

use super::support::{self, WalFixture, make_pattern_payload, make_payload};

/// 对标 C# LogTests.cs::TruncateUntilTest
/// 验证跨段物理文件截断、旧段物理删除与未截断段读取一致性。
#[test]
fn test_truncate_until_basic_and_file_deletion() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024; // 16KB 段大小
    let fixture = WalFixture::segmented("truncate_basic.log", seg_size, 64 * 1024)?;
    let wal = fixture.wal;

    // 写入跨越 4 个段
    let mut addrs = Vec::new();
    for i in 0..50 {
      let data = make_payload(1000, (i % 256) as u8);
      let addr = wal.enqueue(&data)?;
      addrs.push(addr);
    }
    wal.commit().await?;

    // 确认至少前 3 个段文件存在
    assert!(wal.device().segment_path(0).exists());
    assert!(wal.device().segment_path(1).exists());
    assert!(wal.device().segment_path(2).exists());

    // 选取位于段 2 中的某条记录起始地址（例如第 35 条记录）
    let target_idx = 35;
    let trunc_target = addrs[target_idx];
    assert!(trunc_target >= 2 * seg_size);
    wal.truncate(trunc_target).await?;

    assert_eq!(wal.begin_address(), trunc_target);
    // 段 0 和段 1 文件已被物理删除
    assert!(!wal.device().segment_path(0).exists());
    assert!(!wal.device().segment_path(1).exists());
    // 段 2 文件依然保留
    assert!(wal.device().segment_path(2).exists());

    // 从截断起始地址扫描剩余有效记录
    let remaining = support::collect_data(wal.scan(trunc_target, wal.tail_address())).await?;
    assert_eq!(remaining.len(), 50 - target_idx);
    for rec in remaining {
      assert!(rec.address >= trunc_target);
    }

    info!("TruncateUntil 物理段文件删除与边界读取测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogTests.cs
/// 验证在精确段边界（如 segment_size、2 * segment_size）上的截断与物理文件删除与读取。
#[test]
fn test_truncate_exact_segment_boundaries() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024;
    let fixture = WalFixture::segmented("trunc_exact.log", seg_size, 64 * 1024)?;
    let wal = fixture.wal;

    let mut addrs = Vec::new();
    for i in 0..60 {
      let payload = make_payload(800, (i & 0xFF) as u8);
      let addr = wal.enqueue(&payload)?;
      addrs.push(addr);
    }
    wal.commit().await?;

    assert!(wal.device().segment_path(0).exists());
    assert!(wal.device().segment_path(1).exists());
    assert!(wal.device().segment_path(2).exists());

    // 截断至正好是段 1 的起始逻辑边界 (16384)
    let seg1_start = seg_size;
    wal.truncate(seg1_start).await?;

    assert_eq!(wal.begin_address(), seg1_start);
    // 段 0 文件必须被删除
    assert!(!wal.device().segment_path(0).exists());
    // 段 1 文件必须保留
    assert!(wal.device().segment_path(1).exists());

    // 查找段 1 起始之后的第一条记录
    let first_in_seg1_idx = addrs.iter().position(|&a| a >= seg1_start).unwrap();
    let first_in_seg1_addr = addrs[first_in_seg1_idx];

    let data = support::collect_data(wal.scan(first_in_seg1_addr, wal.tail_address())).await?;
    assert_eq!(data.len(), 60 - first_in_seg1_idx);

    info!("精确段边界截断与物理文件删除测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# libs/storage/Tsavorite/cs/test/test.hlog/LogTests.cs:TsavoriteLogTest1
/// 批量写入 500 条定长记录，并在迭代扫描过程中每 100 条触发一次 truncate 截断历史数据，
/// 验证扫描器在底层持续截断推进的情况下的平滑读取与数据准确性。
#[test]
fn test_bulk_enqueue_scan_with_periodic_truncate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 32 * 1024;
    let fixture = WalFixture::segmented("periodic_trunc.log", seg_size, 64 * 1024)?;
    let wal = fixture.wal;

    let num_entries = 500;
    let entry_len = 100;
    let mut addrs = Vec::with_capacity(num_entries);

    for i in 0..num_entries {
      let entry = make_pattern_payload(i, entry_len);
      let addr = wal.enqueue(&entry)?;
      addrs.push(addr);
    }

    wal.commit().await?;

    let mut iter = wal.scan(0, u64::MAX);
    let mut count = 0;

    while let Some(record) = iter.next().await? {
      if !support::is_data_record(&record) {
        continue;
      }
      let expected = make_pattern_payload(count, entry_len);
      assert_eq!(record.payload, expected);

      count += 1;
      if count % 100 == 0 {
        wal.truncate(record.next_address).await?;
      }
    }

    assert_eq!(count, num_entries);
    info!("TsavoriteLogTest1 批量写入与周期性截断扫描测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogTests.cs::ResetTest / SingleLog.Reset
/// 验证 WAL 的 reset 重置操作以及重置后的复用。
#[test]
fn test_reset_and_reuse() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("reset_reuse.log", 32 * 1024)?;
    let wal = fixture.wal;

    for i in 0..10 {
      wal.enqueue(&make_payload(30, i as u8))?;
    }
    wal.commit().await?;
    assert!(wal.total_size() > 0);

    // 调用 reset 重置 WAL
    wal.reset().await?;
    assert_eq!(wal.begin_address(), 0);
    assert_eq!(wal.tail_address(), 0);
    assert_eq!(wal.total_size(), 0);

    // 重置后继续写入新数据
    for i in 0..5 {
      wal.enqueue(&make_payload(30, (i + 100) as u8))?;
    }
    let new_tail = wal.commit().await?;
    assert_eq!(wal.total_size(), new_tail);

    let data = support::collect_data(wal.scan(0, new_tail)).await?;
    assert_eq!(data.len(), 5);
    for (i, rec) in data.iter().enumerate() {
      assert_eq!(rec.payload[0], (i + 100) as u8);
    }

    info!("SingleLog.Reset 重置与复用测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# TsavoriteLog.Reset 持锁原子复位语义（注释 WARNING: Run after database
/// is quiesced 的防御性实现）：后台线程并发 commit_to 与主循环 reset 竞争提交锁，
/// 验证串行化无死锁，静默后位点原子归零且日志可复用。
#[test]
fn test_reset_concurrent_with_commit_lock_serialization() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("reset_concurrent.log", 64 * 1024)?;
    let wal = fixture.wal;

    // 预写一批并提交，形成非零基线位点
    for i in 0..32 {
      wal.enqueue(&make_payload(64, i as u8))?;
    }
    wal.commit().await?;
    let target = wal.tail_address();
    assert!(target > 0);

    // 后台线程独立 runtime 持续 commit_to(旧尾)，与主循环 reset 竞争提交锁
    let wal_bg = Arc::clone(&wal);
    let handle = spawn(move || {
      let bg_rt = Runtime::new().expect("后台 runtime 创建成功");
      bg_rt.block_on(async {
        for _ in 0..200 {
          let _ = wal_bg.commit_to(target).await;
        }
      });
    });

    // 主循环反复 reset：持提交锁与 Leader 级联串行化（无死锁、无位点撕裂 panic）
    for _ in 0..200 {
      wal.reset().await?;
    }
    handle.join().expect("后台提交线程正常结束");

    // 静默后终局 reset：四原子一致归零（原子复位，无竞争残留）
    wal.reset().await?;
    let begin = wal.begin_address();
    assert_eq!(begin, wal.tail_address(), "reset 后尾位点须归零");
    assert_eq!(begin, wal.flushed_until_address(), "reset 后刷盘位点须归零");
    assert_eq!(
      begin,
      wal.committed_until_address(),
      "reset 后提交位点须归零"
    );

    // 复用：重置后写入提交扫描闭环
    for i in 0..8 {
      wal.enqueue(&make_payload(64, (i + 200) as u8))?;
    }
    let new_tail = wal.commit().await?;
    assert_eq!(wal.tail_address(), new_tail);
    let records = support::collect_data(wal.scan(wal.begin_address(), new_tail)).await?;
    assert_eq!(records.len(), 8);

    info!("Reset 并发 commit 竞争串行化与复位一致性测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
