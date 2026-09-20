use std::{sync::Arc, thread::spawn};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use waof::{Error, RECORD_HEADER_LEN, RingBuffer, WalConfig, WalFrameHeader, WalLog};
use wbase::map::HashMap;
use wdev::SegmentedDevice;

use super::support::{self, WalFixture};

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

/// WalFrameHeader::for_payload_parts 分段累加 CRC 与整包单遍逐位一致（CRC32 线性可分段）
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
  let whole = WalFrameHeader::for_payload(&payload);
  let partwise = WalFrameHeader::for_payload_parts(&parts);
  assert_eq!(whole, partwise);

  // 单部件退化为 for_payload
  assert_eq!(
    WalFrameHeader::for_payload(&payload),
    WalFrameHeader::for_payload_parts(&[&payload])
  );

  // 全空部件表与空负载一致：携带非零哨兵 CRC
  let empty = WalFrameHeader::for_payload_parts(&[&[], &[]]);
  assert_eq!(empty, WalFrameHeader::for_payload(&[]));
  assert!(!empty.is_zero());

  info!("WalFrameHeader for_payload_parts 分段 CRC 等价测试通过");
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
  let records_a = rt.block_on(support::collect_iter(
    wal_a.scan(from, wal_a.tail_address()),
  ))?;
  let records_b = rt.block_on(support::collect_iter(
    wal_b.scan(from, wal_b.tail_address()),
  ))?;
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
    while wal_whole.tail_address() + (RECORD_HEADER_LEN + 80) as u64 <= (buf_size - 100) as u64 {
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

    let records = rt.block_on(support::collect_iter(
      wal_parts.scan(addr_parts, wal_parts.tail_address()),
    ))?;
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
  let header = WalFrameHeader::for_payload_parts(&[b"AA", b"BB"]).to_bytes();
  let part_a = make_pattern_payload(5, 60);
  let part_b = make_pattern_payload(6, 90);

  ring.write_record_parts(start, &header, &[&part_a, &part_b]);

  let total_len = RECORD_HEADER_LEN + part_a.len() + part_b.len();
  let mut frame = vec![0u8; total_len];
  ring.read_bytes(start, &mut frame);
  assert_eq!(&frame[..RECORD_HEADER_LEN], &header);
  assert_eq!(
    &frame[RECORD_HEADER_LEN..RECORD_HEADER_LEN + part_a.len()],
    &part_a
  );
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

/// enqueue_frames 单次预留连续落盘：组内帧地址连续、帧序列字节与逐帧
/// 独立入队逐字节一致（单线程对照）
#[test]
fn test_enqueue_frames_contiguous() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture_a = WalFixture::single_file("frames_atomic.log", 64 * 1024)?;
    let fixture_b = WalFixture::single_file("frames_seq.log", 64 * 1024)?;
    let (wal_a, wal_b) = (fixture_a.wal, fixture_b.wal);

    // 分块形状：首帧（头+分块头+key）、value 页片、input 帧
    for i in 0..8u64 {
      let first = make_pattern_payload(i as usize, 40);
      let piece0 = make_pattern_payload(i as usize + 1, 120);
      let piece1 = make_pattern_payload(i as usize + 2, 60);
      let input = make_pattern_payload(i as usize + 3, 30);

      let first_addr = wal_a.enqueue_frames(&[&[&first], &[&piece0], &[&piece1], &[&input]])?;
      // 组内帧地址连续（逐帧累加 8 字节帧头 + 负载长）
      let expect = |off: u64| first_addr + off;
      assert_eq!(
        wal_b.enqueue_parts(&[&first])?,
        first_addr,
        "首帧地址须与逐帧路径一致"
      );
      assert_eq!(
        wal_b.enqueue_parts(&[&piece0])?,
        expect(RECORD_HEADER_LEN as u64 + 40)
      );
      assert_eq!(
        wal_b.enqueue_parts(&[&piece1])?,
        expect((RECORD_HEADER_LEN + 40 + RECORD_HEADER_LEN + 120) as u64)
      );
      assert_eq!(
        wal_b.enqueue_parts(&[&input])?,
        expect((RECORD_HEADER_LEN + 40 + RECORD_HEADER_LEN + 120 + RECORD_HEADER_LEN + 60) as u64)
      );
    }
    assert_eq!(wal_a.tail_address(), wal_b.tail_address());

    // 全部帧 CRC 校验通过（帧序列自洽）
    let records = support::collect_iter(wal_a.scan(0, wal_a.tail_address())).await?;
    for rec in &records {
      rec.header.verify(&rec.payload)?;
    }
    info!("enqueue_frames 连续落盘与字节等价测试通过");
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// enqueue_frames 超窗整体拒绝：组总预留超出环形窗口时 RecordTooLarge，
/// 绝不残留半个分块（单帧路径受同一窗口约束）
#[test]
fn test_enqueue_frames_oversize_rejected_atomically() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let buf_size = 8 * 1024;
    let fixture = WalFixture::single_file("frames_oversize.log", buf_size)?;
    let wal = fixture.wal;

    let big0 = make_pattern_payload(1, buf_size / 2);
    let big1 = make_pattern_payload(2, buf_size / 2);
    let err = wal
      .enqueue_frames(&[&[&big0], &[&big1]])
      .expect_err("组总预留超窗须整体拒绝");
    assert!(
      matches!(err, Error::RecordTooLarge { .. }),
      "超窗须报 RecordTooLarge，实际 {err:?}"
    );
    // 整体拒绝：tail 未推进，无半截组残留
    assert_eq!(wal.tail_address(), 0);
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 分块多帧组并发原子性：多线程 enqueue_frames 组与单帧 enqueue 混合写入，
/// 全部落盘后按组核验——组内帧地址必须连续且负载归属一致
///（修前逐片独立 CAS 预留，组间可被他写入者插花打断连续性，即条 1 损坏面）
#[test]
fn test_enqueue_frames_group_contiguity_under_concurrency() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("frames_concurrent.log", 256 * 1024)?;
    let wal = fixture.wal;
    let groups_per_thread = 64u32;
    let frames_per_group = 3usize;
    // 组帧负载：[线程号, 帧序, 组号 u32 LE, 填充...]；普通帧帧序 0xFF
    let make = |t: u8, fi: u8, g: u32, len: usize| {
      let mut v = Vec::with_capacity(len);
      v.extend_from_slice(&[t, fi]);
      v.extend_from_slice(&g.to_le_bytes());
      v.resize(len, 0x5A);
      v
    };

    let mut handles = Vec::new();
    for t in 0..4u8 {
      let wal = Arc::clone(&wal);
      handles.push(spawn(move || -> aok::Result<()> {
        if t < 2 {
          // 分块会话：每组 3 帧
          for g in 0..groups_per_thread {
            let f0 = make(t, 0, g, 40);
            let f1 = make(t, 1, g, 80);
            let f2 = make(t, 2, g, 60);
            wal.enqueue_frames(&[&[&f0], &[&f1], &[&f2]])?;
          }
        } else {
          // 普通会话：单帧高频并发，制造插入窗口
          for g in 0..groups_per_thread {
            let f = make(t, 0xFF, g, 50);
            wal.enqueue(&f)?;
          }
        }
        aok::Result::<()>::Ok(())
      }));
    }
    for h in handles {
      h.join().expect("写线程不应 panic")?;
    }
    wal.commit().await?;

    let data = support::collect_data(wal.scan(0, wal.tail_address())).await?;
    assert_eq!(
      data.len(),
      2 * groups_per_thread as usize * frames_per_group + 2 * groups_per_thread as usize,
      "帧总数须完整"
    );

    // 按组聚合核验：组内帧地址连续、帧序递增（插花即断言失败）
    let mut groups: HashMap<(u8, u32), Vec<&waof::WalRecord>> = HashMap::default();
    for r in &data {
      let (t, fi) = (r.payload[0], r.payload[1]);
      if fi != 0xFF {
        let g = u32::from_le_bytes(r.payload[2..6].try_into()?);
        groups.entry((t, g)).or_default().push(r);
      }
    }
    assert_eq!(groups.len(), 2 * groups_per_thread as usize);
    for ((t, g), mut frames) in groups {
      frames.sort_by_key(|r| r.address);
      assert_eq!(
        frames.len(),
        frames_per_group,
        "组 (t={t},g={g}) 帧数不完整"
      );
      for (idx, w) in frames.windows(2).enumerate() {
        let expected_next = w[0].address + (RECORD_HEADER_LEN + w[0].payload.len()) as u64;
        assert_eq!(
          w[1].address, expected_next,
          "组 (t={t},g={g}) 第 {idx} 帧后被插花打断连续性"
        );
        assert_eq!(w[1].payload[1], idx as u8 + 1, "组内帧序须递增");
      }
      for (fi, f) in frames.iter().enumerate() {
        assert_eq!(f.payload[0], t, "组帧线程归属须一致");
        assert_eq!(f.payload[1], fi as u8, "组帧序须连续");
      }
    }
    info!("enqueue_frames 并发组连续性（插花原子性）测试通过");
    aok::Result::<()>::Ok(())
  })?;
  OK
}
