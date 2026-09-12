use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use waof::{RECORD_HEADER_LEN, RecordHeader};

use super::support::{WalFixture, make_payload};

/// 对标 C# LogScanTests.cs::ScanBasicDefault
/// 验证基础从起始地址扫描至末尾的正确性与完整记录链。
#[test]
fn test_scan_basic_default() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("scan_default.log", 64 * 1024)?;
    let wal = fixture.wal;

    let count = 200;
    for i in 0..count {
      let payload = make_payload(50, (i & 0xFF) as u8);
      wal.enqueue(&payload)?;
    }
    wal.commit().await?;

    let mut iter = wal.scan(0, u64::MAX);
    let mut read_count = 0;
    let mut prev_next_addr = 0;

    while let Some(record) = iter.next().await? {
      if read_count > 0 {
        assert_eq!(record.address, prev_next_addr);
      }
      prev_next_addr = record.next_address;
      assert_eq!(record.payload[0], (read_count & 0xFF) as u8);
      read_count += 1;
    }

    assert_eq!(read_count, count);
    info!("ScanBasicDefault 基础全量顺序扫描测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogScanTests.cs::ScanNoDefault
/// 测试指定精确范围 [from, to) 的扫描切片。
#[test]
fn test_scan_no_default_subrange() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("scan_subrange.log", 64 * 1024)?;
    let wal = fixture.wal;

    let mut addrs = Vec::new();
    for i in 0..20 {
      let payload = make_payload(32, i as u8);
      let addr = wal.enqueue(&payload)?;
      addrs.push(addr);
    }
    wal.commit().await?;

    // 仅扫描索引 [5, 10) 的区间
    let start_addr = addrs[5];
    let end_addr = addrs[10];

    let mut iter = wal.scan(start_addr, end_addr);
    let sub_records = iter.collect_all().await?;

    assert_eq!(sub_records.len(), 5);
    for (idx, rec) in sub_records.iter().enumerate() {
      assert_eq!(rec.payload[0], (idx + 5) as u8);
      assert_eq!(rec.address, addrs[idx + 5]);
    }

    info!("ScanNoDefault 精确子区间扫描测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogScanTests.cs::ScanUncommitted
/// 验证未提交记录在环形内存中的读取，以及在提交后转为已提交状态的全流程。
#[test]
fn test_scan_uncommitted_memory_and_commit() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("scan_uncommitted.log", 64 * 1024)?;
    let wal = fixture.wal;

    // 写入 5 条记录，但不 commit
    for i in 0..5 {
      let payload = make_payload(40, i as u8);
      wal.enqueue(&payload)?;
    }

    // 验证 scan_committed() 此时返回空
    let mut committed_iter = wal.scan_committed();
    assert!(committed_iter.next().await?.is_none());

    // 验证 scan_all() 可以直接从内存环形缓冲区读取到未提交记录
    let mut uncommitted_iter = wal.scan_all();
    let mem_records = uncommitted_iter.collect_all().await?;
    assert_eq!(mem_records.len(), 5);

    // 提交落盘
    wal.commit().await?;

    // 提交后 scan_committed() 即可读取到全部 5 条记录
    let mut post_commit_iter = wal.scan_committed();
    let disk_records = post_commit_iter.collect_all().await?;
    assert_eq!(disk_records.len(), 5);

    info!("ScanUncommitted 内存透明读取与提交测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogScanTests.cs::ScanBehindBeginAddress
/// 验证在迭代扫描期间对 WAL 执行截断至 TailAddress，迭代器自动跳跃至尾部并安全结束迭代（返回 None），不抛出异常。
#[test]
fn test_scan_behind_begin_address_graceful_jump() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("scan_behind.log", 64 * 1024)?;
    let wal = fixture.wal;

    for i in 0..20 {
      let payload = make_payload(64, i as u8);
      wal.enqueue(&payload)?;
    }
    wal.commit().await?;

    let mut iter = wal.scan(0, u64::MAX);

    // 读取第 1 条记录
    let first = iter.next().await?.expect("第一条记录");
    assert_eq!(first.payload[0], 0);

    // 并发执行 Truncate 到 TailAddress
    let tail = wal.tail_address();
    wal.truncate(tail).await?;
    wal.commit().await?;
    assert_eq!(wal.begin_address(), tail);

    // 此时 cur_address < begin_address，迭代器应平滑跳跃至 tail 并返回 None
    let next_rec = iter.next().await?;
    assert!(
      next_rec.is_none(),
      "在截断至尾部后，迭代器应平滑结束返回 None"
    );

    info!("ScanBehindBeginAddress 迭代中并发截断平滑前进测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogScanTests.cs
/// 验证分段设备上迭代扫描期间并发截断（物理删除全部段文件）时迭代器平滑终止：
/// 绝不触发段不存在错误，平滑返回 None。
#[test]
fn test_scan_graceful_stop_after_physical_truncate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 16 * 1024;
    // 小缓冲制造内存淘汰，迫使后续读取走磁盘
    let fixture = WalFixture::segmented("scan_graceful_trunc.log", seg_size, 16 * 1024)?;
    let wal = fixture.wal;

    for i in 0..60 {
      wal.enqueue(&make_payload(1000, i as u8))?;
      if i > 0 && i % 10 == 0 {
        wal.commit().await?;
      }
    }
    let tail = wal.commit().await?;
    assert!(wal.device().segment_path(0).exists());

    let mut iter = wal.scan(0, tail);
    let first = iter.next().await?.expect("第一条记录");
    assert_eq!(first.payload, make_payload(1000, 0));

    // 迭代暂停期间截断至尾部：全部历史段被物理删除
    wal.truncate(tail).await?;
    assert!(!wal.device().segment_path(0).exists());
    assert_eq!(wal.begin_address(), tail);

    // 后续迭代自动跳跃至尾部并平滑结束
    while (iter.next().await?).is_some() {}
    assert_eq!(iter.current_address(), tail);

    info!("迭代扫描并发物理截断平滑终止测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogScanTests.cs
/// 多个批次频繁写入并提交使得内存环多次回绕（wraparound），
/// 慢读者顺序扫描全部数据，验证无缝、安全地透明回退到底层磁盘读取。
#[test]
fn test_slow_reader_eviction_fallback_to_disk() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 32 * 1024;
    // 设置较小的写缓冲区（16KB），制造频繁回绕
    let fixture = WalFixture::segmented("evict_scan.log", seg_size, 16 * 1024)?;
    let wal = fixture.wal;

    const TOTAL_RECORDS: usize = 120;
    let mut expected_payloads = Vec::with_capacity(TOTAL_RECORDS);

    for i in 0..TOTAL_RECORDS {
      let payload = format!("seq-record-payload-{i:04}-padding-data-content").into_bytes();
      expected_payloads.push(payload);
    }

    // 写入第一批记录并 commit
    for p in &expected_payloads[0..20] {
      wal.enqueue(p)?;
    }
    wal.commit().await?;

    // 启动扫描迭代器从 0 开始扫描
    let mut iter = wal.scan(0, u64::MAX);

    // 读取前 5 条记录
    for expected in &expected_payloads[..5] {
      let rec = iter.next().await?.expect("应有记录");
      assert_eq!(&rec.payload, expected);
    }

    // 在迭代器暂停期间，快速写入剩余 100 条记录（超 16KB 缓冲区容量数倍），并进行多次 commit
    for chunk in expected_payloads[20..].chunks(20) {
      for p in chunk {
        wal.enqueue(p)?;
      }
      wal.commit().await?;
    }

    // 迭代器继续扫描剩余所有记录（必须透明回退到磁盘）
    let mut read_idx = 5;
    while let Some(rec) = iter.next().await? {
      assert_eq!(rec.payload, expected_payloads[read_idx]);
      rec.header.verify(&rec.payload)?;
      read_idx += 1;
    }

    assert_eq!(read_idx, TOTAL_RECORDS);

    info!("环形写缓冲区高并发覆写淘汰下的慢读扫描测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogScanTests.cs
/// 验证已提交记录的内存环槽位被并发覆写破坏时，扫描器按最新 flushed 位点透明回退
/// 磁盘读取权威数据，绝不误报 CRC 错误。
#[test]
fn test_scan_memory_clobber_falls_back_to_disk() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("mem_clobber.log", 64 * 1024)?;
    let wal = fixture.wal;

    const COUNT: usize = 20;
    let mut addrs = Vec::with_capacity(COUNT);
    for i in 0..COUNT {
      addrs.push(wal.enqueue(&make_payload(1000, i as u8))?);
    }
    let tail = wal.commit().await?;
    assert_eq!(wal.flushed_until_address(), tail);
    assert!(tail < 64 * 1024, "记录须全部驻留内存环内");

    // 模拟并发破坏内存槽位：
    // 记录 5 的头部被改写为超大长度
    wal
      .ring_buffer
      .write_bytes(addrs[5], &[0xFF; RECORD_HEADER_LEN]);
    // 记录 10 的头部长度合法但 CRC 错误
    let bad_hdr = RecordHeader::new(1000, 0xDEADBEEF);
    wal.ring_buffer.write_bytes(addrs[10], &bad_hdr.to_bytes());

    // 扫描必须按磁盘权威数据无损读出全部记录
    let mut iter = wal.scan(0, tail);
    for (i, &expected_addr) in addrs.iter().enumerate().take(COUNT) {
      let rec = iter.next().await?.expect("记录须从磁盘回退读取");
      assert_eq!(rec.address, expected_addr);
      assert_eq!(rec.payload, make_payload(1000, i as u8));
      rec.header.verify(&rec.payload)?;
    }
    assert!(iter.next().await?.is_none());

    info!("内存环破坏透明回退磁盘读取测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogScanTests.cs
/// 验证扫描器在面对单条超大记录（超出 64KB 扫描缓冲块）时的磁盘流式扫描正确性。
#[test]
fn test_scan_buffered_large_records() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg_size = 256 * 1024;
    let fixture = WalFixture::segmented("large_scan.log", seg_size, 512 * 1024)?;
    let wal = fixture.wal;

    let small_payload = vec![0x11; 128];
    let large_payload = vec![0x22; 70 * 1024]; // 70KB 超出 64KB 缓冲块
    let med_payload = vec![0x33; 30 * 1024];

    let a1 = wal.enqueue(&small_payload)?;
    let a2 = wal.enqueue(&large_payload)?;
    let a3 = wal.enqueue(&med_payload)?;
    let a4 = wal.enqueue(&small_payload)?;

    let tail = wal.commit().await?;

    let mut iter = wal.scan(0, tail);

    let r1 = iter.next().await?.expect("r1");
    assert_eq!(r1.address, a1);
    assert_eq!(r1.payload, small_payload);

    let r2 = iter.next().await?.expect("r2");
    assert_eq!(r2.address, a2);
    assert_eq!(r2.payload, large_payload);

    let r3 = iter.next().await?.expect("r3");
    assert_eq!(r3.address, a3);
    assert_eq!(r3.payload, med_payload);

    let r4 = iter.next().await?.expect("r4");
    assert_eq!(r4.address, a4);
    assert_eq!(r4.payload, small_payload);

    assert!(iter.next().await?.is_none());

    info!("扫描器超大记录与分块缓存扫描测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# LogScanTests.cs
/// 验证磁盘预读滑窗在存在大量未提交内存尾部时不得越过 flushed 边界，防止触发 UnexpectedEof。
#[test]
fn test_scan_disk_prefetch_flushed_boundary() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("prefetch_bound.log", 128 * 1024)?;
    let wal = fixture.wal;

    for i in 0..50 {
      wal.enqueue(&make_payload(1000, i as u8))?;
    }
    let flushed = wal.commit().await?;

    for i in 50..140 {
      wal.enqueue(&make_payload(1000, i as u8))?;
    }
    let tail = wal.tail_address();
    assert!(tail - flushed > 64 * 1024, "未提交尾部须超过 64KB 预读窗");

    // 从头扫描：早期记录走磁盘路径，预读窗以 flushed 封顶后不得报 EOF 错误
    let mut iter = wal.scan(0, tail);
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 140);
    for (i, rec) in records.iter().enumerate() {
      assert_eq!(rec.payload, make_payload(1000, i as u8));
    }

    info!("磁盘预读 flushed 边界约束测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
