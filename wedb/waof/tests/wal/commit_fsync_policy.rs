use std::{
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
};

use aok::{OK, Void};
use log::info;
use tempfile::tempdir;
use waof::{FsyncPolicy, WalConfig, WalLog};
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::SegmentedDevice;

use super::support::make_payload;

/// 统计 sync_data 调用次数的设备包装（fsync 策略位观测面；其余方法透传）
struct CountingSyncDevice {
  inner: SegmentedDevice,
  sync_count: AtomicUsize,
}

impl CountingSyncDevice {
  fn single_file(path: &Path) -> aok::Result<Self> {
    Ok(Self {
      inner: SegmentedDevice::single_file(path)?,
      sync_count: AtomicUsize::new(0),
    })
  }
}

impl wdev::Device for CountingSyncDevice {
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
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> wdev::Result<()> {
    self.inner.sync().await
  }

  async fn sync_data(&self) -> wdev::Result<()> {
    self.sync_count.fetch_add(1, Ordering::AcqRel);
    self.inner.sync_data().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

/// fsync 策略位（对标 TsavoriteLogSettings.cs:AutoCommit 持久性显式配置面）：
/// Always 每次提交批次无条件 sync_data；Deferred 仅写设备页缓存、跳过
/// sync_data，提交水位照常推进
#[compio::test]
async fn test_fsync_policy_controls_sync_data() -> Void {
  let dir = tempdir()?;

  // Always（默认档）：每次 commit 恰好一次 sync_data
  let device = Arc::new(CountingSyncDevice::single_file(
    &dir.path().join("always.log"),
  )?);
  let wal = WalLog::new(Arc::clone(&device) as Arc<_>, WalConfig::new(64 * 1024))?;
  assert_eq!(wal.config().fsync, FsyncPolicy::Always);
  wal.enqueue(&make_payload(64, 1))?;
  wal.commit().await?;
  wal.enqueue(&make_payload(64, 2))?;
  wal.commit().await?;
  assert_eq!(
    device.sync_count.load(Ordering::Acquire),
    2,
    "Always 档每次提交批次须恰好一次 sync_data"
  );

  // Deferred：提交跳过 sync_data，水位照常推进
  let device = Arc::new(CountingSyncDevice::single_file(
    &dir.path().join("deferred.log"),
  )?);
  let config = WalConfig {
    fsync: FsyncPolicy::Deferred,
    ..WalConfig::new(64 * 1024)
  };
  let wal = WalLog::new(Arc::clone(&device) as Arc<_>, config)?;
  assert_eq!(wal.config().fsync, FsyncPolicy::Deferred);
  wal.enqueue(&make_payload(64, 3))?;
  let committed = wal.commit().await?;
  assert!(committed >= 8 + 64, "Deferred 档提交水位须照常推进至记录尾");
  assert_eq!(
    device.sync_count.load(Ordering::Acquire),
    0,
    "Deferred 档提交不得触发 sync_data"
  );
  // 无新增数据的空提交同样零 sync
  wal.commit().await?;
  assert_eq!(device.sync_count.load(Ordering::Acquire), 0);
  // Deferred 档强持久化显式入口：sync 恰好一次设备 fdatasync
  wal.sync().await?;
  assert_eq!(device.sync_count.load(Ordering::Acquire), 1);

  info!("WAL fsync 策略位测试通过");
  OK
}
