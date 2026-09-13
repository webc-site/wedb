use std::fs::{OpenOptions, remove_file};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use waof::RECORD_HEADER_LEN;

use super::support::{WalFixture, make_payload, reopen_segmented, reopen_single_file};

/// 对标 C# LogFastCommitTests.cs::TsavoriteLogSimpleFastCommitTest
/// 验证多阶段提交与崩溃恢复（Crash Recovery）从磁盘物理段重构 TailAddress 和有效数据链。
#[test]
fn test_fast_commit_multi_stage_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 32 * 1024;
    let fixture = WalFixture::segmented("multi_stage.log", seg_size, 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    // 阶段 1：写入 50 条记录并提交
    for i in 0..50 {
      wal.enqueue(&make_payload(80, (i & 0xFF) as u8))?;
    }
    let c1 = wal.commit().await?;

    // 阶段 2：追加写入 50 条记录并提交
    for i in 50..100 {
      wal.enqueue(&make_payload(80, (i & 0xFF) as u8))?;
    }
    let c2 = wal.commit().await?;

    // 阶段 3：追加写入 50 条记录并提交
    for i in 100..150 {
      wal.enqueue(&make_payload(80, (i & 0xFF) as u8))?;
    }
    let c3 = wal.commit().await?;

    assert!(c1 < c2);
    assert!(c2 < c3);
    drop(wal);

    // 模拟重启：重新以 open 打开 WAL，触发 recover
    let recovered_wal =
      reopen_segmented(dir.path(), "multi_stage.log", seg_size, 64 * 1024).await?;

    // 校验恢复后的位点与最后一次 commit 尾部完全一致
    assert_eq!(recovered_wal.tail_address(), c3);
    assert_eq!(recovered_wal.flushed_until_address(), c3);
    assert_eq!(recovered_wal.committed_until_address(), c3);

    // 校验全部 150 条记录可无损恢复与读取
    let mut iter = recovered_wal.scan(0, recovered_wal.tail_address());
    let all_records = iter.collect_all().await?;
    assert_eq!(all_records.len(), 150);
    for (idx, rec) in all_records.iter().enumerate() {
      assert_eq!(rec.payload[0], (idx & 0xFF) as u8);
    }

    info!("FastCommit 多阶段提交与崩溃恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs 容错恢复
/// 模拟故障导致末尾记录数据写入不完整或 CRC 损坏，恢复时应自动安全截断至最后一个有效记录。
#[test]
fn test_corrupted_trailing_record_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 32 * 1024;
    let fixture = WalFixture::segmented("corrupted_tail.log", seg_size, 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let valid_count = 15;
    for i in 0..valid_count {
      wal.enqueue(&make_payload(50, i as u8))?;
    }
    let valid_tail = wal.commit().await?;
    let seg0_path = wal.device().segment_path(0);
    drop(wal);

    // 人工向底层段文件末尾追加伪造的损坏记录头（损坏的 CRC32 校验和）
    {
      use std::io::Write;
      let mut file = OpenOptions::new().append(true).open(&seg0_path)?;
      let corrupted_len = 50u32.to_le_bytes();
      let corrupted_crc = 0xDEADBEEFu32.to_le_bytes();
      file.write_all(&corrupted_len)?;
      file.write_all(&corrupted_crc)?;
      file.write_all(&[0xFF; 20])?;
      file.flush()?;
    }

    // 重新打开 WAL 触发 recover
    let recovered_wal =
      reopen_segmented(dir.path(), "corrupted_tail.log", seg_size, 64 * 1024).await?;

    // 验证崩溃恢复机制成功识别末尾损坏，截断至最后一个完整合法记录尾部
    assert_eq!(recovered_wal.tail_address(), valid_tail);
    assert_eq!(recovered_wal.committed_until_address(), valid_tail);

    let mut iter = recovered_wal.scan(0, recovered_wal.tail_address());
    let recovered_records = iter.collect_all().await?;
    assert_eq!(recovered_records.len(), valid_count);

    info!("崩溃末尾损坏数据截断与容错恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs
/// 验证末尾空负载记录的持久性：空记录头携带非零哨兵 CRC，与扇区填充零可区分，
/// 已 commit 的空记录崩溃恢复后必须完整保留（兑现 commit 持久性承诺）。
#[test]
fn test_trailing_empty_record_durable_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("trailing_empty.log", 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    wal.enqueue(b"only-record")?;
    wal.enqueue(&[])?; // 末尾空记录：头携带哨兵 CRC，非全零
    let tail = wal.commit().await?;
    assert_eq!(tail, 19 + 8);
    drop(wal);

    let wal = reopen_single_file(dir.path(), "trailing_empty.log", 64 * 1024).await?;
    assert_eq!(wal.tail_address(), tail, "已提交的空记录崩溃后必须可恢复");
    assert_eq!(wal.committed_until_address(), tail);

    let mut iter = wal.scan_committed();
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, b"only-record");
    assert!(records[1].payload.is_empty());
    assert_eq!(records[1].header.entry_len, 0);

    // 恢复位点后可继续正常追加
    let addr = wal.enqueue(b"after")?;
    assert_eq!(addr, tail);
    wal.commit().await?;

    info!("末尾空记录哨兵 CRC 持久化恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs
/// 验证已提交尾部之后的扇区填充零（padding）不会被恢复逻辑复活成记录：
/// 恢复保守截断至最后一条完整记录，且后续追加会覆写填充区域。
#[test]
fn test_trailing_zero_padding_not_resurrected() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("zero_padding.log", 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    wal.enqueue(b"payload-before-pad")?;
    let valid_tail = wal.commit().await?;
    drop(wal);

    // 模拟崩溃残留：向已提交尾部之后追加一整个扇区的填充零
    {
      use std::io::Write;
      let db_path = dir.path().join("zero_padding.log");
      let mut file = OpenOptions::new().append(true).open(&db_path)?;
      file.write_all(&[0u8; 4096])?;
      file.flush()?;
    }

    // 恢复必须停在最后一条完整记录处，绝不把零填充误判为空记录链
    let wal = reopen_single_file(dir.path(), "zero_padding.log", 64 * 1024).await?;
    assert_eq!(wal.tail_address(), valid_tail);
    assert_eq!(wal.committed_until_address(), valid_tail);

    let mut iter = wal.scan_committed();
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, b"payload-before-pad");

    // 追加写入自 valid_tail 起覆写填充区域，提交后恢复位点照常推进
    let addr = wal.enqueue(b"overwrite-pad")?;
    assert_eq!(addr, valid_tail);
    let new_tail = wal.commit().await?;
    assert!(new_tail > valid_tail);

    let wal = reopen_single_file(dir.path(), "zero_padding.log", 64 * 1024).await?;
    assert_eq!(wal.tail_address(), new_tail);
    let mut iter = wal.scan_committed();
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].payload, b"overwrite-pad");

    info!("尾部零填充不复活与覆写续写测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs
/// 验证日志中部的介质损坏（非尾部 torn write）：恢复保守截断至损坏记录起始处。
#[test]
fn test_midlog_corruption_conservative_stop() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("midlog_corrupt.log", 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    for i in 0..10 {
      wal.enqueue(&make_payload(40, i as u8))?;
    }
    wal.commit().await?;
    drop(wal);

    // 翻转记录 5 负载中的一个字节（记录 i 起始 48i，负载区间 [48i+8, 48i+48)）
    let db_path = dir.path().join("midlog_corrupt.log");
    let corrupt_off: u64 = 5 * 48 + 8 + 4;
    {
      use std::io::{Seek, SeekFrom, Write};
      let mut f = OpenOptions::new().write(true).open(&db_path)?;
      f.seek(SeekFrom::Start(corrupt_off))?;
      f.write_all(&[0xFF])?;
      f.flush()?;
    }

    let wal = reopen_single_file(dir.path(), "midlog_corrupt.log", 64 * 1024).await?;

    assert_eq!(
      wal.tail_address(),
      5 * 48,
      "恢复须保守截断至损坏记录起始地址"
    );
    assert_eq!(wal.committed_until_address(), 5 * 48);

    let mut iter = wal.scan_committed();
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 5);
    for (i, rec) in records.iter().enumerate() {
      assert_eq!(rec.payload, make_payload(40, i as u8));
    }

    info!("日志中部损坏保守截断测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs::AlternatingEmptyAndFullRecordsRecovery
/// 验证包含复杂空负载记录与非空记录交替链时的崩溃恢复正确性。
#[test]
fn test_empty_payload_recovery_chain() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 32 * 1024;
    let fixture = WalFixture::segmented("empty_chain.log", seg_size, 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let mut payloads: Vec<Vec<u8>> = Vec::new();
    for i in 0..41 {
      if i % 3 == 0 {
        payloads.push(Vec::new()); // 空记录
      } else {
        payloads.push(format!("payload-for-idx-{i}").into_bytes());
      }
    }

    for p in &payloads {
      wal.enqueue(p)?;
    }
    let final_tail = wal.commit().await?;
    drop(wal);

    // 重新打开并恢复
    let recovered_wal =
      reopen_segmented(dir.path(), "empty_chain.log", seg_size, 64 * 1024).await?;

    assert_eq!(recovered_wal.tail_address(), final_tail);
    let mut iter = recovered_wal.scan_committed();
    let recovered_records = iter.collect_all().await?;
    assert_eq!(recovered_records.len(), payloads.len());

    for (i, rec) in recovered_records.iter().enumerate() {
      assert_eq!(rec.payload, payloads[i]);
      rec.header.verify(&rec.payload)?;
    }

    info!("AlternatingEmptyAndFullRecordsRecovery 复杂空记录链恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogTests.cs / LogRecoverReadOnlyTests.cs
/// 验证已发生段截断（旧段文件被物理删除）场景下的崩溃重启恢复：
/// 底层 SegmentedDevice 正确重建 start_segment，begin_address 成功跳转至最新有效段，后续数据无损恢复。
#[test]
fn test_recovery_after_segment_truncate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024; // 16KB 段大小
    let fixture = WalFixture::segmented("trunc_recover.log", seg_size, 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let mut addrs = Vec::new();
    let mut payloads = Vec::new();

    // 跨越 4 个段写入数据
    for i in 0..60 {
      let payload = make_payload(1000, (i % 250) as u8);
      let addr = wal.enqueue(&payload)?;
      addrs.push(addr);
      payloads.push(payload);
    }
    wal.commit().await?;

    // 截断至第 35 条记录所在的位置（位于段 2）
    let trunc_target = addrs[35];
    assert!(trunc_target >= 2 * seg_size);
    wal.truncate(trunc_target).await?;

    // 验证段 0 和段 1 物理文件已被删除，段 2 存在
    assert!(!wal.device().segment_path(0).exists());
    assert!(!wal.device().segment_path(1).exists());
    assert!(wal.device().segment_path(2).exists());
    drop(wal);

    // 重新打开
    let recovered_wal =
      reopen_segmented(dir.path(), "trunc_recover.log", seg_size, 64 * 1024).await?;

    // 验证 begin_address 被正确重构为未被截断的起始段边界
    assert!(recovered_wal.begin_address() >= 2 * seg_size);
    let last_expected_tail =
      addrs.last().unwrap() + (RECORD_HEADER_LEN + payloads.last().unwrap().len()) as u64;
    assert_eq!(recovered_wal.tail_address(), last_expected_tail);

    // 从截断起始记录扫描所有剩余数据
    let mut iter = recovered_wal.scan(addrs[35], recovered_wal.tail_address());
    let remaining = iter.collect_all().await?;
    assert_eq!(remaining.len(), 60 - 35);
    for (idx, rec) in remaining.iter().enumerate() {
      assert_eq!(rec.payload, payloads[35 + idx]);
      rec.header.verify(&rec.payload)?;
    }

    info!("段截断后重启恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs
/// 验证段截断后重启恢复：begin_address 前移至帧同步点，
/// 从 begin_address 起的常规扫描可直接读取全部剩余记录。
#[test]
fn test_scan_from_begin_after_truncate_restart() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024;
    let fixture = WalFixture::segmented("scan_from_begin.log", seg_size, 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let mut addrs = Vec::new();
    let mut payloads = Vec::new();
    for i in 0..60 {
      let payload = make_payload(1000, ((i % 250) + 1) as u8);
      addrs.push(wal.enqueue(&payload)?);
      payloads.push(payload);
    }
    let full_tail = wal.commit().await?;

    // 截断至段 2 内部，物理删除段 0、1
    wal.truncate(addrs[35]).await?;
    drop(wal);

    // 重启：帧同步定位到段 2 首条完整记录
    let wal = reopen_segmented(dir.path(), "scan_from_begin.log", seg_size, 64 * 1024).await?;

    assert_eq!(
      wal.begin_address(),
      addrs[33],
      "begin 应前移至段 2 首条完整记录"
    );
    assert_eq!(wal.tail_address(), full_tail);

    // 从 begin_address 起扫描全部剩余记录
    let mut iter = wal.scan(wal.begin_address(), wal.tail_address());
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 60 - 33);
    for (idx, rec) in records.iter().enumerate() {
      assert_eq!(rec.payload, payloads[33 + idx]);
      rec.header.verify(&rec.payload)?;
    }

    info!("段截断后从 begin_address 扫描恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs 自同步帧恢复
/// 验证自同步段首帧探测（Frame Sync）在跨段物理删除残留尾部场景下的鲁棒性。
#[test]
fn test_frame_sync_with_unaligned_cross_segment_remnant() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024;
    let fixture = WalFixture::segmented("frame_sync.log", seg_size, 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let target_record1_payload = b"first_valid_record_in_segment_1_after_remnant";
    let target_record2_payload = b"second_valid_record_in_segment_1";
    let rec1_addr;

    // 1. 构造跨段数据与段 1 内有效记录
    {
      let padding_len = 15000 - RECORD_HEADER_LEN;
      wal.enqueue(&make_payload(padding_len, 0xAA))?;

      let cross_seg_len = 1600 - RECORD_HEADER_LEN;
      let cross_addr = wal.enqueue(&make_payload(cross_seg_len, 0xBB))?;
      assert!(cross_addr < seg_size);
      let cross_end = cross_addr + 1600;
      assert!(cross_end > seg_size, "该记录必须跨越段 0 与段 1 边界");

      rec1_addr = wal.enqueue(target_record1_payload)?;
      assert!(rec1_addr >= cross_end);
      wal.enqueue(target_record2_payload)?;
      wal.commit().await?;
    }

    let seg0_path = wal.device().segment_path(0);
    drop(wal);

    // 2. 模拟物理删除段 0
    remove_file(&seg0_path)?;
    assert!(!seg0_path.exists());

    // 3. 重新打开触发 recover()
    let recovered_wal = reopen_segmented(dir.path(), "frame_sync.log", seg_size, 64 * 1024).await?;

    assert_eq!(recovered_wal.begin_address(), rec1_addr);
    let mut iter = recovered_wal.scan(recovered_wal.begin_address(), recovered_wal.tail_address());
    let rec1 = iter.next().await?.expect("rec1 必须成功恢复");
    assert_eq!(rec1.address, rec1_addr);
    assert_eq!(rec1.payload, target_record1_payload);
    rec1.header.verify(&rec1.payload)?;

    let rec2 = iter.next().await?.expect("rec2 必须成功恢复");
    assert_eq!(rec2.payload, target_record2_payload);
    rec2.header.verify(&rec2.payload)?;

    assert!(iter.next().await?.is_none());

    // 恢复后追加新记录
    let append_payload = b"append_record_after_frame_sync_recovery";
    let append_addr = recovered_wal.enqueue(append_payload)?;
    let new_tail = recovered_wal.commit().await?;
    assert!(new_tail > append_addr);

    let mut post_iter = recovered_wal.scan(rec1_addr, new_tail);
    let records = post_iter.collect_all().await?;
    assert_eq!(records.len(), 3);
    assert_eq!(records[2].payload, append_payload);

    info!("自同步段首帧探测（Frame Sync）跨段残缺尾部测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs
/// 验证段首存在合法空记录时的 Frame Sync 探测与恢复。
#[test]
fn test_frame_sync_with_empty_record_at_segment_start() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024;
    let fixture = WalFixture::segmented("frame_sync_empty.log", seg_size, 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let non_empty_payload = b"valid_data_after_empty_at_seg_start";
    let empty_rec_addr;

    // 填充段 0，并在段 1 起始处精确写入一条空记录
    {
      let pad_len = (seg_size - RECORD_HEADER_LEN as u64) as usize;
      wal.enqueue(&make_payload(pad_len, 0xCC))?;

      empty_rec_addr = wal.enqueue(&[])?;
      assert_eq!(empty_rec_addr, seg_size);

      wal.enqueue(non_empty_payload)?;
      wal.commit().await?;
    }

    let seg0_path = wal.device().segment_path(0);
    drop(wal);

    // 物理删除段 0
    remove_file(&seg0_path)?;

    // 重新打开触发恢复
    let recovered_wal =
      reopen_segmented(dir.path(), "frame_sync_empty.log", seg_size, 64 * 1024).await?;

    assert_eq!(recovered_wal.begin_address(), seg_size);
    let mut iter = recovered_wal.scan(recovered_wal.begin_address(), recovered_wal.tail_address());

    let r1 = iter.next().await?.expect("空记录必须成功读取");
    assert_eq!(r1.address, empty_rec_addr);
    assert!(r1.payload.is_empty());
    assert_eq!(r1.header.entry_len, 0);

    let r2 = iter.next().await?.expect("后续非空记录必须成功读取");
    assert_eq!(r2.payload, non_empty_payload);

    info!("段首空记录 Frame Sync 探测与恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs
/// 验证段首残缺负载超过单个 64KB 探测窗时，帧同步仍能滑动扫描整段定位首条完整记录。
#[test]
fn test_frame_sync_large_remnant_beyond_single_window() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 128 * 1024;
    let fixture = WalFixture::segmented("frame_sync_large.log", seg_size, 256 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let big = make_payload(70_000, 0xAB);
    let mut addrs = Vec::new();
    addrs.push(wal.enqueue(&big)?); // [0, 70008)
    for _ in 0..59 {
      addrs.push(wal.enqueue(&make_payload(1000, 0xCD))?);
    }

    // 大记录 [129480, 199488) 横跨段 1 起点 131072，残缺尾部达 68416 字节 > 64KB
    addrs.push(wal.enqueue(&big)?);
    let sync_addr = wal.enqueue(b"tail-after-large")?; // 199488，段 1 内首条完整记录
    let tail = wal.commit().await?;
    assert_eq!(sync_addr + 24, tail);

    wal.truncate(tail).await?;
    drop(wal);

    let recovered_wal =
      reopen_segmented(dir.path(), "frame_sync_large.log", seg_size, 256 * 1024).await?;

    assert_eq!(addrs.len(), 61);
    assert_eq!(sync_addr, 199488);
    assert!(sync_addr - seg_size > 64 * 1024, "残缺尾部须超过单个探测窗");
    assert_eq!(recovered_wal.begin_address(), sync_addr);
    assert_eq!(recovered_wal.tail_address(), sync_addr + 24);

    let mut iter = recovered_wal.scan(recovered_wal.begin_address(), recovered_wal.tail_address());
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, b"tail-after-large");

    info!("超窗残缺尾部帧同步测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs 自同步帧恢复
/// 验证单条巨记录横跨多个段文件时（残缺尾长超过单个段大小），帧同步仍能
/// 跨段滑动探测定位首条完整记录，绝不误判为空日志而清空位点。
#[test]
fn test_frame_sync_remnant_spanning_multiple_segments() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024;
    let fixture = WalFixture::segmented("frame_sync_multi_seg.log", seg_size, 64 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let tail_payload = b"first_record_after_multi_segment_remnant";
    let tail_addr;

    // 1. 巨记录 [0, 40008) 横跨段 0/1/2，残缺尾 [16384, 40008) 达 23624 字节 > 单段 16KB
    {
      let big_addr = wal.enqueue(&make_payload(40_000, 0xAA))?;
      assert_eq!(big_addr, 0);
      assert!(big_addr + 40_008 > 2 * seg_size, "巨记录须横跨至少 3 个段");
      tail_addr = wal.enqueue(tail_payload)?;
      wal.commit().await?;
    }

    let seg0_path = wal.device().segment_path(0);
    drop(wal);

    // 2. 模拟物理删除段 0：段 1 起点落在巨记录负载中部
    remove_file(&seg0_path)?;
    assert!(!seg0_path.exists());

    // 3. 重新打开触发 recover()：帧同步须跨段探测越过超段残缺尾部
    let recovered_wal =
      reopen_segmented(dir.path(), "frame_sync_multi_seg.log", seg_size, 64 * 1024).await?;

    assert_eq!(recovered_wal.begin_address(), tail_addr);
    assert_eq!(
      recovered_wal.tail_address(),
      tail_addr + (RECORD_HEADER_LEN + tail_payload.len()) as u64
    );

    let mut iter = recovered_wal.scan(recovered_wal.begin_address(), recovered_wal.tail_address());
    let rec = iter.next().await?.expect("巨记录之后的首条记录必须恢复");
    assert_eq!(rec.address, tail_addr);
    assert_eq!(rec.payload, tail_payload.to_vec());
    rec.header.verify(&rec.payload)?;
    assert!(iter.next().await?.is_none());

    // 恢复位点后可正常追加续写
    let tail_before = recovered_wal.tail_address();
    let append_addr = recovered_wal.enqueue(b"append-after-multi-seg-sync")?;
    assert_eq!(append_addr, tail_before);

    info!("跨多段残缺尾部帧同步测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs 自同步帧恢复
/// 验证单条超长记录的残缺负载完全覆盖整个起始段（起始段内无任何记录边界）时，
/// 帧同步必须跨段续扫直至 CRC 命中或设备 EOF，绝不误判空日志丢弃其后全部合法记录。
/// 与上一测试互补：此处删除两个段（起始段为段 2），且边界点落在更深的段 3 内。
#[test]
fn test_frame_sync_remnant_covers_whole_start_segment() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024;
    // 大缓冲允许写入超过段大小的记录
    let fixture = WalFixture::segmented("frame_sync_whole_seg.log", seg_size, 256 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    // 记录 1：[0, 30000)，横跨段 0 与段 1
    wal.enqueue(&make_payload(30_000 - RECORD_HEADER_LEN, 0xAA))?;
    // 记录 2（超长）：[30000, 52000)，完整覆盖段 2 [32768, 49152)
    wal.enqueue(&make_payload(52_000 - 30_000 - RECORD_HEADER_LEN, 0xBB))?;
    // 记录 3：段 3 内的首条合法记录，即帧同步目标点
    let after_payload = b"record-after-whole-seg-remnant";
    let sync_addr = wal.enqueue(after_payload)?;
    assert_eq!(sync_addr, 52_000);
    assert!(sync_addr >= 3 * seg_size, "同步点须位于段 3");
    wal.commit().await?;
    let (seg0_path, seg1_path) = (wal.device().segment_path(0), wal.device().segment_path(1));
    drop(wal);

    // 模拟物理删除段 0 与段 1：恢复的起始段（段 2）被记录 2 的残缺负载完全覆盖
    remove_file(&seg0_path)?;
    remove_file(&seg1_path)?;

    // 重新打开触发 recover()：帧同步跨段续扫，定位至段 3 首条完整记录
    let recovered_wal =
      reopen_segmented(dir.path(), "frame_sync_whole_seg.log", seg_size, 256 * 1024).await?;

    assert_eq!(recovered_wal.begin_address(), sync_addr);
    assert_eq!(
      recovered_wal.tail_address(),
      sync_addr + (RECORD_HEADER_LEN + after_payload.len()) as u64
    );

    let mut iter = recovered_wal.scan(recovered_wal.begin_address(), recovered_wal.tail_address());
    let rec = iter.next().await?.expect("同步点记录必须成功恢复");
    assert_eq!(rec.address, sync_addr);
    assert_eq!(rec.payload, after_payload.to_vec());
    rec.header.verify(&rec.payload)?;
    assert!(iter.next().await?.is_none());

    // 恢复后可正常追加并再次重启校验
    let tail_before = recovered_wal.tail_address();
    let append_addr = recovered_wal.enqueue(b"append-after-whole-seg-sync")?;
    assert_eq!(append_addr, tail_before);
    recovered_wal.commit().await?;
    let tail = recovered_wal.tail_address();
    drop(recovered_wal);

    let wal =
      reopen_segmented(dir.path(), "frame_sync_whole_seg.log", seg_size, 256 * 1024).await?;
    assert_eq!(wal.tail_address(), tail);

    info!("残缺负载覆盖整个起始段的跨段帧同步测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogRecoverReadOnlyTests.cs 海量数据恢复基准
/// 验证大量变长记录（1000 条各规格记录）在高吞吐下的批量分块流式恢复正确性与一致性。
#[test]
fn test_massive_records_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 64 * 1024;
    let fixture = WalFixture::segmented("massive_rec.log", seg_size, 256 * 1024)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    const TOTAL_RECORDS: usize = 1000;
    let mut expected_payloads = Vec::with_capacity(TOTAL_RECORDS);

    for i in 0..TOTAL_RECORDS {
      let len = (i % 1024) + 1;
      let payload = make_payload(len, (i % 251) as u8);
      expected_payloads.push(payload);
    }

    for (idx, p) in expected_payloads.iter().enumerate() {
      wal.enqueue(p)?;
      if idx % 200 == 0 {
        wal.commit().await?;
      }
    }
    let final_tail = wal.commit().await?;
    drop(wal);

    // 重新以 open 打开恢复
    let recovered_wal =
      reopen_segmented(dir.path(), "massive_rec.log", seg_size, 256 * 1024).await?;

    assert_eq!(recovered_wal.tail_address(), final_tail);
    assert_eq!(recovered_wal.committed_until_address(), final_tail);

    let mut iter = recovered_wal.scan(0, final_tail);
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), TOTAL_RECORDS);

    for (idx, rec) in records.iter().enumerate() {
      assert_eq!(rec.payload, expected_payloads[idx]);
      rec.header.verify(&rec.payload)?;
    }

    info!("1000 条海量变长记录批量分块预读恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
