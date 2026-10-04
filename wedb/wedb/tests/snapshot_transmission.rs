use std::{
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::Duration,
};

use compio::{
  buf::BufResult,
  fs::{remove_file, write},
  runtime::{Runtime, spawn},
  time::sleep,
};
use wbase::{align::DEFAULT_SECTOR_SIZE, pool::BufferPool};
use wdev::SegmentedDevice;
use wedb::server::replication::{
  error::ReplicationError,
  snapshot_transmission::{
    CheckpointFileSource, HlogSegmentSource, SNAPSHOT_CHUNK_SIZE, SnapshotDataSource,
  },
};

/// 探针节拍：远小于整段下发耗时，又不至于热自旋
const PROBE_TICK: Duration = Duration::from_micros(50);

/// 位置相关确定性图案（逐字节随偏移变化：错偏移、丢段、重复段必被捕获）
fn pattern_bytes(len: usize) -> Vec<u8> {
  (0..len)
    .map(|i| ((i as u64 * 31 + 7) % 251) as u8)
    .collect()
}

/// 测试夹具落盘（与产品路径同一 compio 异步口径）
async fn write_fixture(path: &Path, data: &[u8]) {
  let BufResult(res, _) = write(path, data.to_vec()).await;
  res.expect("write fixture");
}

/// 段流排空（与 `send_file_chunks` 的读循环同构，剥离网络帧）
async fn drain<S: SnapshotDataSource>(src: &mut S, max_len: usize) -> Vec<(u64, Vec<u8>)> {
  let mut chunks = Vec::new();
  while src.has_next_chunk() {
    let start = src.span().cursor;
    let chunk = src.read_next_chunk(max_len).await.expect("chunk read");
    assert!(!chunk.is_empty(), "零字节块使游标停滞，段流永不收敛");
    assert!(chunk.len() <= max_len, "块长越过请求上界");
    chunks.push((start, chunk.to_vec()));
  }
  chunks
}

/// 段流读回的逐块断言：内容与写入逐字节一致、地址自起点连续、
/// 块数与末尾短段由幅面和上界唯一决定
fn assert_stream(chunks: &[(u64, Vec<u8>)], data: &[u8], start: u64, end: u64, max_len: usize) {
  let span = (end - start) as usize;
  assert_eq!(
    chunks.len(),
    span.div_ceil(max_len),
    "块数与段幅面/上界不符（span={span}, max_len={max_len}）"
  );
  let mut cursor = start;
  for (idx, (addr, chunk)) in chunks.iter().enumerate() {
    assert_eq!(*addr, cursor, "第 {idx} 块起始地址与游标不连续");
    let s = *addr as usize;
    assert_eq!(
      chunk.as_slice(),
      &data[s..s + chunk.len()],
      "第 {idx} 块内容与写入不符"
    );
    cursor += chunk.len() as u64;
  }
  assert_eq!(cursor, end, "读出总字节与段幅面不符");
  let tail = span % max_len;
  if tail != 0 {
    assert_eq!(
      chunks.last().expect("tail chunk").1.len(),
      tail,
      "末尾短段长度不符"
    );
  }
}

/// 段读回路与写入完全一致：空文件、末尾短段、整倍数无尾段、单段大于
/// 读缓冲（幅面远超上界）、请求上界超出幅面（按幅面截断）、末尾短段
/// 非扇区整数倍（池 class 容量大于本次请求，越界多读必被捕获）
#[test]
fn segment_stream_reads_back_written_bytes() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
    let cases: [(usize, usize); 6] = [
      (0, SNAPSHOT_CHUNK_SIZE),
      (SNAPSHOT_CHUNK_SIZE * 2 + 4096, SNAPSHOT_CHUNK_SIZE),
      (SNAPSHOT_CHUNK_SIZE * 3, SNAPSHOT_CHUNK_SIZE),
      (SNAPSHOT_CHUNK_SIZE * 4, 4096),
      (SNAPSHOT_CHUNK_SIZE, SNAPSHOT_CHUNK_SIZE * 4),
      (SNAPSHOT_CHUNK_SIZE * 2 + 100, SNAPSHOT_CHUNK_SIZE),
    ];
    for (idx, (len, max_len)) in cases.into_iter().enumerate() {
      let data = pattern_bytes(len);
      let path = dir.path().join(format!("case-{idx}"));
      write_fixture(&path, &data).await;
      let mut src = CheckpointFileSource::open_required(&path, &pool)
        .await
        .expect("open required");
      assert_eq!(src.span().end, len as u64, "幅面须由 open 期 fstat 定得");
      let chunks = drain(&mut src, max_len).await;
      assert_stream(&chunks, &data, 0, len as u64, max_len);
      assert_eq!(src.span().cursor, src.span().end, "排空后游标停在段流终点");
      assert!(!src.has_next_chunk(), "排空后不得再有下一块");
    }
    Ok(())
  })
}

/// 缺席文件按「无该源」跳过（STORE_INDEX 可缺），编目在册而磁盘缺席
/// 必须中止下发；空幅面文件建得起源但零块（只发空载荷收尾帧）
#[test]
fn absent_and_empty_files_gate_the_source() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
    let missing = dir.path().join("index_absent");
    assert!(
      CheckpointFileSource::open(&missing, &pool)
        .await
        .expect("open absent must not error")
        .is_none(),
      "NotFound 须归一为无该源，而非 IOERR"
    );
    let err = CheckpointFileSource::open_required(&missing, &pool)
      .await
      .err()
      .expect("编目在册而磁盘缺席必须中止");
    assert!(
      matches!(err, ReplicationError::CheckpointFileMissing { .. }),
      "中止原因须点名缺席文件: {err}"
    );

    let empty = dir.path().join("index_empty");
    write_fixture(&empty, &[]).await;
    let mut src = CheckpointFileSource::open_required(&empty, &pool)
      .await
      .expect("open empty");
    assert_eq!(src.span().end, 0);
    assert!(!src.has_next_chunk(), "零幅面段流不得读出任何块");
    assert!(drain(&mut src, SNAPSHOT_CHUNK_SIZE).await.is_empty());
    Ok(())
  })
}

/// 每文件一次 open、句柄跨块复用（C# RangeIndexFileDataSource 的
/// `stream ??= new FileStream` 形态）：首块之后删除目录项，后续块仍须
/// 读全——逐块重开的实现会在此以 NotFound 失败
#[test]
fn file_handle_is_reused_across_chunks() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
    let path = dir.path().join("tree.bftree");
    let len = SNAPSHOT_CHUNK_SIZE * 3 + 4096;
    let data = pattern_bytes(len);
    write_fixture(&path, &data).await;

    let mut src = CheckpointFileSource::open_required(&path, &pool)
      .await
      .expect("open");
    let mut joined = src
      .read_next_chunk(SNAPSHOT_CHUNK_SIZE)
      .await
      .expect("first chunk")
      .to_vec();
    remove_file(&path).await.expect("unlink fixture");
    while src.has_next_chunk() {
      joined.extend_from_slice(
        src
          .read_next_chunk(SNAPSHOT_CHUNK_SIZE)
          .await
          .expect("chunk after unlink")
          .as_slice(),
      );
    }
    assert_eq!(joined, data, "句柄复用下的按段读回须与写入完全一致");
    Ok(())
  })
}

/// 大文件段下发不独占 compio 线程：读段期间同线程上的其它异步任务
/// 必须被驱动。若读段退回同步 syscall，主循环一次也不让出，探针计数
/// 恒 0（同步 fs 读与 compio reactor 不可并存的正证伪判据）。判据只取
/// 「让出发生过」这一不变量，不取节拍比例——页缓存命中时长随机器负载
/// 浮动，任何绝对节拍阈值都会 flaky
#[test]
fn large_file_segments_yield_to_other_tasks() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
    let path = dir.path().join("large.bftree");
    let len = SNAPSHOT_CHUNK_SIZE * 8;
    let data = pattern_bytes(len);
    write_fixture(&path, &data).await;

    // 源先建、探针后派：探针拿到的每一次让出都只能来自读段
    let mut src = CheckpointFileSource::open_required(&path, &pool)
      .await
      .expect("open");
    let beats = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let probe_beats = Arc::clone(&beats);
    let probe_stop = Arc::clone(&stop);
    spawn(async move {
      while !probe_stop.load(Ordering::Relaxed) {
        probe_beats.fetch_add(1, Ordering::Relaxed);
        sleep(PROBE_TICK).await;
      }
    })
    .detach();

    let mut cursor = 0usize;
    let mut previous_beats = 0u64;
    while src.has_next_chunk() {
      let chunk = src
        .read_next_chunk(SNAPSHOT_CHUNK_SIZE)
        .await
        .expect("chunk read");
      assert_eq!(
        chunk.as_slice(),
        &data[cursor..cursor + chunk.len()],
        "段内容错位"
      );
      cursor += chunk.len();
      let beats = beats.load(Ordering::Relaxed);
      assert!(
        beats >= previous_beats,
        "探针计数只增不减（{previous_beats} → {beats}）"
      );
      previous_beats = beats;
    }
    // 循环出口到此处无 await：读到的计数只能由读段期间的让出贡献
    let beats = beats.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);

    assert_eq!(cursor, len, "整幅面须全部按段读出");
    assert!(
      beats > 0,
      "读段全程未让出 ⇒ 读源退回同步 syscall，独占当根 compio 线程"
    );
    Ok(())
  })
}

/// STORE_HLOG 设备段源：池化异步读按段回读与写入一致，起点非零时
/// 只下发 [start, end) 区间（对标 C# hybridLogFileStart/EndAddress）
#[test]
fn hlog_segment_source_reads_back_device_bytes() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let device = SegmentedDevice::new(dir.path().join("hlog"), 1 << 30, 4096)?;
    let len = SNAPSHOT_CHUNK_SIZE * 2 + 4096;
    let data = pattern_bytes(len);
    write_fixture(&device.segment_path(0), &data).await;

    let mut whole = HlogSegmentSource::new(&device, 0, len as u64);
    let chunks = drain(&mut whole, SNAPSHOT_CHUNK_SIZE).await;
    assert_stream(&chunks, &data, 0, len as u64, SNAPSHOT_CHUNK_SIZE);

    let start = SNAPSHOT_CHUNK_SIZE as u64;
    let mut tail = HlogSegmentSource::new(&device, start, len as u64);
    let chunks = drain(&mut tail, SNAPSHOT_CHUNK_SIZE).await;
    assert_stream(&chunks, &data, start, len as u64, SNAPSHOT_CHUNK_SIZE);
    Ok(())
  })
}
