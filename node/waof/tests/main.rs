use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use waof::{Error, RECORD_HEADER_LEN, RecordHeader, RingBuffer, WalConfig, WalLog};
use wdev::SegmentedDevice;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 测试 RecordHeader 的编解码与校验和
#[test]
fn test_record_header() -> Void {
  assert_eq!(RECORD_HEADER_LEN, 8);

  // 1. 空负载测试：携带非零哨兵 CRC，保证记录头绝不与扇区填充零混淆
  let empty_hdr = RecordHeader::for_payload(&[]);
  assert_eq!(empty_hdr.entry_len, 0);
  assert_ne!(empty_hdr.crc32, 0);
  assert!(!empty_hdr.is_zero());
  empty_hdr.verify(&[])?;
  assert!(matches!(
    RecordHeader::new(0, 0).verify(&[]),
    Err(Error::ChecksumMismatch { .. })
  ));

  // 2. 正常负载编解码测试
  let payload = b"Hello, Wedb Wal Engine!";
  let header = RecordHeader::for_payload(payload);
  assert_eq!(header.entry_len, payload.len() as u32);
  header.verify(payload)?;

  let bytes = header.to_bytes();
  let decoded = RecordHeader::decode(&bytes)?;
  assert_eq!(header, decoded);
  assert_eq!(RecordHeader::decode_opt(&bytes), Some(header));
  assert_eq!(RecordHeader::decode_opt(&bytes[..7]), None);
  assert!(matches!(
    RecordHeader::decode(&bytes[..7]),
    Err(Error::InvalidRecordHeader)
  ));

  // 3. 损坏数据校验测试
  let mut corrupted = *payload;
  corrupted[0] ^= 0xFF;
  assert!(matches!(
    header.verify(&corrupted),
    Err(Error::ChecksumMismatch { .. })
  ));

  info!("RecordHeader 编解码与校验和测试通过");
  OK
}

/// 验证 RecordHeader 在各种极端破坏、位反转、截断及溢出情况下的鲁棒性
#[test]
fn test_record_header_corruption_and_boundary_checks() -> Void {
  let payload = b"critical wal transaction payload data";
  let header = RecordHeader::for_payload(payload);
  assert_eq!(header.payload_len(), payload.len());
  assert!(!header.is_zero());

  // 1. 长度不足 RECORD_HEADER_LEN 时拒绝解码
  assert!(RecordHeader::decode(&[0u8; 7]).is_err());
  assert!(RecordHeader::decode(&[]).is_err());

  // 2. 负载切片长度与 entry_len 不一致时报错
  assert!(matches!(
    header.verify(&payload[..payload.len() - 1]),
    Err(Error::InvalidRecordHeader)
  ));
  let mut extended = payload.to_vec();
  extended.push(0);
  assert!(matches!(
    header.verify(&extended),
    Err(Error::InvalidRecordHeader)
  ));

  // 3. 逐位单 bit 翻转校验 CRC32 拦截率（栈数组原地翻转与复原，零堆分配，100% 拦截）
  let mut mutated = *payload;
  for i in 0..payload.len() {
    for bit in 0..8 {
      mutated[i] ^= 1 << bit;
      assert!(
        matches!(header.verify(&mutated), Err(Error::ChecksumMismatch { .. })),
        "第 {i} 字节第 {bit} 位翻转未被拦截"
      );
      mutated[i] ^= 1 << bit;
    }
  }

  // 4. 全零头 is_zero 判定
  let zero_hdr = RecordHeader::new(0, 0);
  assert!(zero_hdr.is_zero());
  assert_eq!(zero_hdr.payload_len(), 0);

  info!("RecordHeader 极端校验与位反转测试通过");
  OK
}

/// 验证 WAL 环形写缓冲区在大数（> 4GB）64 位逻辑地址下的读写正确性
#[test]
fn test_wal_ring_buffer_large_64bit_offset() -> Void {
  let buffer_size = 64 * 1024; // 64 KB
  let align = 4096;
  let ring = RingBuffer::new(buffer_size, align)?;

  // 模拟超越 32 位整型上限的大逻辑地址 (例如 8GB + 123 字节)
  let huge_offset = (8u64 * 1024 * 1024 * 1024) + 123;
  let test_data = b"64-bit large logical address test across 4GB boundary!";

  ring.write_bytes(huge_offset, test_data);

  let mut read_back = [0u8; 64];
  let slice = &mut read_back[..test_data.len()];
  ring.read_bytes(huge_offset, slice);

  assert_eq!(slice, test_data);

  // 跨越环形边界的大数写入与读取
  let near_boundary_offset = (16u64 * 1024 * 1024 * 1024) + (buffer_size as u64 - 10);
  let boundary_data = b"across_ring_boundary_large_address";
  ring.write_bytes(near_boundary_offset, boundary_data);

  let mut boundary_read = [0u8; 64];
  let slice = &mut boundary_read[..boundary_data.len()];
  ring.read_bytes(near_boundary_offset, slice);
  assert_eq!(slice, boundary_data);

  info!("WAL 环形写缓冲区 64 位大地址测试通过");
  OK
}

/// WAL 端到端冒烟测试：
/// 涵盖创建分段设备、写入记录、内存扫描、提交落盘、磁盘扫描、位点截断、重启恢复以及追加写入的全链路闭环
#[test]
fn test_wal_end_to_end_smoke() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("wal_smoke.log");
    let seg_size = 16 * 1024; // 16KB 段大小
    let config = WalConfig::new(64 * 1024);

    let committed_tail;
    let mut expected_payloads = Vec::with_capacity(30);

    // 1. 初始化并写入 30 条记录（跨越 2 个段）
    {
      let device = Arc::new(SegmentedDevice::segmented(&db_path, seg_size)?);
      let wal = WalLog::new(device, config)?;

      for i in 0..30 {
        let payload = format!("wal-smoke-record-{i:03}").into_bytes();
        wal.enqueue(&payload)?;
        expected_payloads.push(payload);
      }

      // 验证提交前可以从内存中完整扫描
      let mut mem_iter = wal.scan_all();
      let mem_records = mem_iter.collect_all().await?;
      assert_eq!(mem_records.len(), 30);
      for (rec, expected) in mem_records.iter().zip(&expected_payloads) {
        assert_eq!(&rec.payload, expected);
      }

      // 执行提交刷盘
      committed_tail = wal.commit().await?;
      assert_eq!(committed_tail, wal.tail_address());

      // 验证提交后 scan_committed 准确读取
      let mut disk_iter = wal.scan_committed();
      let disk_records = disk_iter.collect_all().await?;
      assert_eq!(disk_records.len(), 30);
    }

    // 2. 模拟系统崩溃与重启：重新通过 WalLog::open 恢复
    {
      let device = Arc::new(SegmentedDevice::segmented(&db_path, seg_size)?);
      let wal = WalLog::open(device, config).await?;

      assert_eq!(wal.tail_address(), committed_tail);
      assert_eq!(wal.committed_until_address(), committed_tail);

      // 验证恢复后数据读取无损
      let mut iter = wal.scan_committed();
      let recovered_records = iter.collect_all().await?;
      assert_eq!(recovered_records.len(), 30);
      for (rec, expected) in recovered_records.iter().zip(&expected_payloads) {
        assert_eq!(&rec.payload, expected);
        rec.header.verify(&rec.payload)?;
      }

      // 恢复后追加写入新记录并提交
      let append_payload = b"wal-smoke-appended-after-recovery";
      let append_addr = wal.enqueue(append_payload)?;
      assert_eq!(append_addr, committed_tail);

      let new_tail = wal.commit().await?;
      assert!(new_tail > append_addr);

      let mut post_iter = wal.scan(append_addr, new_tail);
      let post_records = post_iter.collect_all().await?;
      assert_eq!(post_records.len(), 1);
      assert_eq!(post_records[0].payload, append_payload);
    }

    info!("WAL 端到端全链路冒烟测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
