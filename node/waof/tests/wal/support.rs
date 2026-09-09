use std::{path::Path, sync::Arc};

use aok::Result;
use tempfile::{TempDir, tempdir};
use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;

/// 测试夹具：封装临时目录与 WalLog 实例
pub struct WalFixture {
  pub dir: TempDir,
  pub wal: Arc<WalLog<SegmentedDevice>>,
}

impl WalFixture {
  /// 创建基于单文件设备的 WalLog 夹具
  pub fn single_file(file_name: &str, buf_size: usize) -> Result<Self> {
    let dir = tempdir()?;
    let db_path = dir.path().join(file_name);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let config = WalConfig::new(buf_size);
    let wal = Arc::new(WalLog::new(device, config)?);
    Ok(Self { dir, wal })
  }

  /// 创建基于分段设备的 WalLog 夹具
  pub fn segmented(file_name: &str, seg_size: u64, buf_size: usize) -> Result<Self> {
    let dir = tempdir()?;
    let db_path = dir.path().join(file_name);
    let device = Arc::new(SegmentedDevice::segmented(&db_path, seg_size)?);
    let config = WalConfig::new(buf_size);
    let wal = Arc::new(WalLog::new(device, config)?);
    Ok(Self { dir, wal })
  }
}

/// 重新打开已有的分段 WalLog
pub async fn reopen_segmented(
  dir: &Path,
  file_name: &str,
  seg_size: u64,
  buf_size: usize,
) -> Result<WalLog<SegmentedDevice>> {
  let db_path = dir.join(file_name);
  let device = Arc::new(SegmentedDevice::segmented(&db_path, seg_size)?);
  let config = WalConfig::new(buf_size);
  let wal = WalLog::open(device, config).await?;
  Ok(wal)
}

/// 重新打开已有的单文件 WalLog
pub async fn reopen_single_file(
  dir: &Path,
  file_name: &str,
  buf_size: usize,
) -> Result<WalLog<SegmentedDevice>> {
  let db_path = dir.join(file_name);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let config = WalConfig::new(buf_size);
  let wal = WalLog::open(device, config).await?;
  Ok(wal)
}

/// 生成指定长度与统一字节内容的测试负载
pub fn make_payload(len: usize, byte: u8) -> Vec<u8> {
  vec![byte; len]
}

/// 生成带有递增序列特征的测试负载（对标 C# TsavoriteLog entry 模式）
pub fn make_pattern_payload(index: usize, len: usize) -> Vec<u8> {
  (0..len).map(|j| ((index + j) % 256) as u8).collect()
}

/// 伪造短写入的设备包装：底层写成功后仅报告一半字节，验证 commit 持久化守卫
pub struct ShortWriteDevice {
  pub inner: SegmentedDevice,
}

impl ShortWriteDevice {
  pub fn single_file(path: &Path) -> Result<Self> {
    Ok(Self {
      inner: SegmentedDevice::single_file(path)?,
    })
  }
}

impl wdev::Device for ShortWriteDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  fn segment_size(&self) -> Option<u64> {
    self.inner.segment_size()
  }

  fn pool(&self) -> &Arc<wram::BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(
    &self,
    offset: u64,
    buf: wram::AlignedBuf,
  ) -> (wdev::Result<usize>, wram::AlignedBuf) {
    let expected = buf.len();
    let (res, buf) = self.inner.write_aligned(offset, buf).await;
    (res.map(|_| expected / 2), buf)
  }

  async fn read_aligned(
    &self,
    offset: u64,
    buf: wram::AlignedBuf,
  ) -> (wdev::Result<usize>, wram::AlignedBuf) {
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(
    &self,
    offset: u64,
    buf: wram::AlignedBuf,
  ) -> (wdev::Result<usize>, wram::AlignedBuf) {
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> wdev::Result<()> {
    self.inner.sync().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}
