use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use waof::{RECORD_HEADER_LEN, RecordHeader, RingBuffer, WalConfig, WalLog};
use wdev::SegmentedDevice;

use super::support::WalFixture;

/// 任意切分部件：把 payload 按给定分段长度切成多部件表
fn split_parts<'a>(payload: &'a [u8], seg_lens: &[usize]) -> Vec<&'a [u8]> {
  let mut parts = Vec::new();
  let mut offset = 0;
  for &seg in seg_lens {
    let end = (offset + seg).min(payload.len());
    parts.push(&payload[offset..end]);
    offset = end;
    if offset >= payload.len() {
      break;
    }
  }
  if offset < payload.len() {
    parts.push(&payload[offset..]);
  }
  parts
}

/// RecordHeader::for_payload_parts 分段累加 CRC 与整包单遍逐位一致（CRC32 线性可分段）
#[test]
fn test_record_header_for_payload_parts_equivalence() -> Void {
  let payload = make_pattern_payload(0, 300);

  // 多部件切分（含空部件）
  let parts = [
    &payload[..7],
    &payload[7..7][..],
    &payload[7..128],
    &payload[128..],
  ];
  let whole = RecordHeader::for_payload(&payload);
  let partwise = RecordHeader::for_payload_parts(&parts);
  assert_eq!(whole, partwise);

  // 单部件退化为 for_payload
  assert_eq!(
    RecordHeader::for_payload(&payload),
    RecordHeader::for_payload_parts(&[&payload])
  );

  // 全空部件表与空负载一致：携带非零哨兵 CRC
  let empty = RecordHeader::for_payload_parts(&[&[], &[]]);
  assert_eq!(empty, RecordHeader::for_payload(&[]));
  assert!(!empty.is_zero());

  info!("RecordHeader for_payload_parts 分段 CRC 等价测试通过");
  OK
}

/// make_pattern_payload 的测试内复刻（避免 support 增加测试专用导出）
fn make_pattern_payload(index: usize, len: usize) -> Vec<u8> {
  (0..len).map(|j| ((index + j) % 256) as u8).collect()
}

/// 对比两个日志实例自 from 起的扫描记录帧（WAL 头 + 负载）逐字节一致
fn assert_scan_frames_identical(
  wal_a: &WalLog<SegmentedDevice>,
  wal_b: &WalLog<SegmentedDevice>,
  from: u64,
) -> Void {
  let rt = Runtime::new()?;
  let records_a = rt.block_on(wal_a.scan(from, wal_a.tail_address()).collect_all())?;
  let records_b = rt.block_on(wal_b.scan(from, wal_b.tail_address()).collect_all())?;
  assert_eq!(records_a.len(), records_b.len());
  for (a, b) in records_a.iter().zip(records_b.iter()) {
    assert_eq!(a.address, b.address);
    assert_eq!(a.header, b.header);
    assert_eq!(a.payload, b.payload);
  }
  OK
}

/// enqueue_parts 产出页与 enqueue 预拼整包逐字节一致（AOF 日志字节级兼容契约）
#[test]
fn test_enqueue_parts_byte_equivalence() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device_whole = Arc::new(SegmentedDevice::single_file(dir.path().join("whole.log"))?);
    let device_parts = Arc::new(SegmentedDevice::single_file(dir.path().join("parts.log"))?);
    let wal_whole = WalLog::new(device_whole, WalConfig::new(64 * 1024))?;
    let wal_parts = WalLog::new(device_parts, WalConfig::new(64 * 1024))?;

    // 多部件形状：AOF 头 16B + key 长度前缀 4B + key + value + input 的典型散射布局
    for i in 0..20 {
      let header_bytes = make_pattern_payload(i, 16);
      let key = make_pattern_payload(i + 1, 5 + i);
      let value = make_pattern_payload(i + 2, 100 * (i + 1));
      let input = make_pattern_payload(i + 3, 7 + i);

      let mut payload = Vec::new();
      payload.extend_from_slice(&header_bytes);
      payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
      payload.extend_from_slice(&key);
      payload.extend_from_slice(&value);
      payload.extend_from_slice(&input);

      let addr_whole = wal_whole.enqueue(&payload)?;
      let addr_parts = wal_parts.enqueue_parts(&[
        &header_bytes,
        &(key.len() as u32).to_le_bytes(),
        &key,
        &value,
        &input,
      ])?;
      assert_eq!(addr_whole, addr_parts);
      assert_eq!(wal_whole.tail_address(), wal_parts.tail_address());
    }

    // 空部件表退化为空记录（哨兵 CRC）
    let addr_whole = wal_whole.enqueue(&[])?;
    let addr_parts = wal_parts.enqueue_parts(&[])?;
    assert_eq!(addr_whole, addr_parts);

    assert_scan_frames_identical(&wal_whole, &wal_parts, 0)?;
    info!("enqueue_parts 与 enqueue 字节等价测试通过");
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 回绕边界：部件表写入跨环形缓冲区边界记录，恢复侧扫描帧与整包路径逐字节一致
#[test]
fn test_enqueue_parts_ring_wrap_boundary() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let buf_size = 16 * 1024;
    let fixture_whole = WalFixture::single_file("wrap_whole.log", buf_size)?;
    let fixture_parts = WalFixture::single_file("wrap_parts.log", buf_size)?;
    let wal_whole = fixture_whole.wal;
    let wal_parts = fixture_parts.wal;

    // 两侧写相同记录序列填充至距容量边界仅剩 100 字节（此时尚未刷盘）
    let mut i = 0;
    while wal_whole.tail_address() + (RECORD_HEADER_LEN + 80) as u64
      <= (buf_size - 100) as u64
    {
      let payload = make_pattern_payload(i, 80);
      let addr_whole = wal_whole.enqueue(&payload)?;
      let addr_parts = wal_parts.enqueue_parts(&split_parts(&payload, &[31, 13, 7]))?;
      assert_eq!(addr_whole, addr_parts);
      i += 1;
    }
    assert_eq!(wal_parts.tail_address(), wal_whole.tail_address());

    // 提交刷盘推进窗口起点，后续写入物理位置必然骑跨环形回绕点
    wal_whole.commit().await?;
    wal_parts.commit().await?;

    // 骑跨回绕点的多部件大记录（总长约 600B > 剩余 100B）
    let header_bytes = make_pattern_payload(1, 16);
    let key = make_pattern_payload(2, 64);
    let value = make_pattern_payload(3, 400);
    let mut payload = Vec::new();
    payload.extend_from_slice(&header_bytes);
    payload.extend_from_slice(&key);
    payload.extend_from_slice(&value);
    let addr_whole = wal_whole.enqueue(&payload)?;
    let addr_parts = wal_parts.enqueue_parts(&[&header_bytes, &key, &value])?;
    assert_eq!(addr_whole, addr_parts);
    assert!(addr_parts + RECORD_HEADER_LEN as u64 + payload.len() as u64 > buf_size as u64);

    // 回绕后继续写入，验证窗口内新旧记录均完整可扫描、CRC 全部通过
    for i in 0..10 {
      let payload = make_pattern_payload(i + 4, 50 + i);
      wal_whole.enqueue(&payload)?;
      let parts = split_parts(&payload, &[13, 17]);
      wal_parts.enqueue_parts(&parts)?;
    }

    // 自骑跨记录起逐帧比较（填充段已被环形覆写，不在窗口覆盖面）
    assert_scan_frames_identical(&wal_whole, &wal_parts, addr_parts)?;

    let records = rt.block_on(wal_parts.scan(addr_parts, wal_parts.tail_address()).collect_all())?;
    for rec in &records {
      rec.header.verify(&rec.payload)?;
    }
    info!("enqueue_parts 环形回绕边界测试通过");
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RingBuffer::write_record_parts 直接回绕写入：产出字节与 write_record 整包一致
#[test]
fn test_ring_buffer_write_record_parts_wrap() -> Void {
  let cap = 4096;
  let align = 512;
  let ring = RingBuffer::new(cap, align)?;

  // 起点贴近容量末尾，记录必然回绕
  let start = (cap - 100) as u64;
  let header = RecordHeader::for_payload_parts(&[b"AA", b"BB"]).to_bytes();
  let part_a = make_pattern_payload(5, 60);
  let part_b = make_pattern_payload(6, 90);

  ring.write_record_parts(start, &header, &[&part_a, &part_b]);

  let total_len = RECORD_HEADER_LEN + part_a.len() + part_b.len();
  let mut frame = vec![0u8; total_len];
  ring.read_bytes(start, &mut frame);
  assert_eq!(&frame[..RECORD_HEADER_LEN], &header);
  assert_eq!(&frame[RECORD_HEADER_LEN..RECORD_HEADER_LEN + part_a.len()], &part_a);
  assert_eq!(&frame[RECORD_HEADER_LEN + part_a.len()..], &part_b);

  // 同起点整包写入对照缓冲区，逐字节一致
  let ring_whole = RingBuffer::new(cap, align)?;
  let mut payload = Vec::with_capacity(part_a.len() + part_b.len());
  payload.extend_from_slice(&part_a);
  payload.extend_from_slice(&part_b);
  ring_whole.write_record(start, &header, &payload);
  let mut frame_whole = vec![0u8; total_len];
  ring_whole.read_bytes(start, &mut frame_whole);
  assert_eq!(frame, frame_whole);

  info!("RingBuffer write_record_parts 回绕写入测试通过");
  OK
}
