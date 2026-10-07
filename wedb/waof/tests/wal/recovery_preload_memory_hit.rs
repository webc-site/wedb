//! 恢复收尾 preload 的「reopen 后内存扫描命中」回归（wal/recover.rs 收尾：
//! 把提交上界前一个环形窗口的数据预载入 ring_buffer）。
//!
//! 判据：写入→commit→reopen 后，preload 窗内的记录经 `scan_memory_records`
//! 全量命中（零磁盘读、内容逐字节一致）。若 preload 缺失或窗口错位，
//! `scan_memory_records` 要么因 from 越出内存窗返回 false，要么解码断链
//! 丢记录——两条失败路径均被本测证伪。
//!
//! 边界构造：buffer_size（16KB）刻意小于总写入量（约 21KB），早期记录被
//! 环形覆写淘汰，preload 下界 committed - buffer_size 落在首批记录中段——
//! 内存命中断言只覆盖 preload 窗内（下界之后的全部记录）。

use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering},
};

use aok::{OK, Void};
use log::info;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::SegmentedDevice;

use super::support::make_pattern_payload;

/// 环形窗口容量（扇区 4096 整数倍）：小于总写入量，构造 preload 边界
const BUF: usize = 16 * 1024;
/// 单条负载长度（含 8B 头每条 708B，3 批 × 10 条 ≈ 21KB > BUF，必然回绕）
const REC: usize = 700;

/// 统计设备读取次数的包装（read_aligned/read_raw 双计数；其余透传）
struct CountingReadDevice {
  inner: SegmentedDevice,
  reads: AtomicUsize,
}

impl wdev::Device for CountingReadDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.reads.fetch_add(1, Ordering::AcqRel);
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.reads.fetch_add(1, Ordering::AcqRel);
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> wdev::Result<()> {
    self.inner.sync().await
  }

  async fn sync_data(&self) -> wdev::Result<()> {
    self.inner.sync_data().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

/// reopen（写入→commit→重开→preload 命中）主流程
#[compio::test]
async fn test_recovery_preload_hits_memory_scan_without_disk_read() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("preload_hit.log");

  // 阶段 1：3 批写入，每批随批 commit（腾窗释放环形空间），总量越过 BUF
  let device = Arc::new(SegmentedDevice::single_file(&path)?);
  let wal = WalLog::new(device, WalConfig::new(BUF))?;
  let mut addrs = Vec::new();
  for batch in 0..3 {
    for i in 0..10 {
      let seq = batch * 10 + i;
      addrs.push(wal.enqueue(&make_pattern_payload(seq, REC))?);
    }
    wal.commit().await?;
  }
  let committed = wal.committed_until_address();
  assert!(
    committed > BUF as u64,
    "总写入须越过环形窗口（实测 {committed} > {BUF}）"
  );
  drop(wal);

  // 阶段 2：reopen 触发 recover（扫描链 + 收尾 preload）
  let device = Arc::new(CountingReadDevice {
    inner: SegmentedDevice::single_file(&path)?,
    reads: AtomicUsize::new(0),
  });
  let wal = WalLog::open(Arc::clone(&device) as Arc<_>, WalConfig::new(BUF)).await?;
  assert_eq!(wal.committed_until_address(), committed, "恢复提交上界不变");

  // preload 下界（与 recover.rs 收尾同式）：committed - BUF，落在首批记录中段
  let preload_start = committed
    .saturating_sub(BUF as u64)
    .max(wal.begin_address());
  let from = addrs
    .iter()
    .copied()
    .find(|a| *a >= preload_start)
    .expect("preload 窗内必有记录");

  // 阶段 3：内存扫描命中断言——先冻结磁盘读基线
  let reads_base = device.reads.load(Ordering::Acquire);
  let mut seen: Vec<(u64, Vec<u8>)> = Vec::new();
  let covered = wal.scan_memory_records(from, committed, |rec| {
    if !waof::is_commit_frame(&rec.payload) {
      seen.push((rec.address, rec.payload.clone()));
    }
    true
  });

  assert!(covered, "preload 窗内起点须在内存窗内（from={from}）");
  assert_eq!(
    device.reads.load(Ordering::Acquire),
    reads_base,
    "内存扫描须零磁盘读"
  );

  // 全命中：窗内每条数据记录逐一解出，地址与负载逐字节一致
  let expect: Vec<(u64, Vec<u8>)> = addrs
    .iter()
    .enumerate()
    .filter(|(_, a)| **a >= from)
    .map(|(seq, a)| (*a, make_pattern_payload(seq, REC)))
    .collect();
  assert!(!expect.is_empty(), "对照集非空前置");
  assert_eq!(seen.len(), expect.len(), "窗内记录须全量命中（无断链）");
  for ((addr_s, payload_s), (addr_e, payload_e)) in seen.iter().zip(&expect) {
    assert_eq!(addr_s, addr_e);
    assert_eq!(payload_s, payload_e);
  }

  info!("恢复 preload：reopen 后内存扫描零磁盘读全命中测试通过");
  OK
}
