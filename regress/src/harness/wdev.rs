//! wdev 设备层直读/直写微基准夹具：对标 C# Device.benchmark/BenchWorker.cs（设备层
//! 读写吞吐）与 Tsavorite 各 StorageDevice.WriteAsync/ReadAsync 统一落点。wdev 是全
//! 引擎物理 I/O 底座，现仓零基准守护，本件补建扇区对齐页的直写与回读吞吐。

use std::sync::Arc;

use ::wbase::pool::AlignedBuf;
use ::wdev::{Device, SegmentedDevice};
use tempfile::TempDir;

use super::{DEFAULT_SECTOR_SIZE, with_runtime};

/// 单页 16KB（扇区对齐，对标 C# Device.benchmark 页尺寸档）
pub const WDEV_PAGE_BYTES: usize = 16 * 1024;
/// 轮写页槽数（16KB × 1024 = 16MB < 单段 64MB，跨迭代覆写常驻）
pub const WDEV_PAGE_SLOTS: u64 = 1_024;

/// 生成第 idx 页的确定性图案（byte[i] = (idx*31 + i) & 0xFF），供回读逐字节校验
#[inline]
fn page_pattern(idx: u64, len: usize) -> Vec<u8> {
  let base = idx.wrapping_mul(31);
  (0..len).map(|i| (base + i as u64) as u8).collect()
}

/// wdev 设备层直读写夹具：持一份 SegmentedDevice（临时目录、64MB 段、扇区对齐）
pub struct WdevHarness {
  pub device: Arc<SegmentedDevice>,
  _temp_dir: TempDir,
}

impl WdevHarness {
  /// 建 64MB 段、4096 扇区设备（对齐 regress WkvHarness 设备口径）
  pub fn bench() -> aok::Result<Self> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("wdev_data");
    let device = Arc::new(SegmentedDevice::new(
      &path,
      super::DEFAULT_SEGMENT_SIZE,
      DEFAULT_SECTOR_SIZE,
    )?);
    Ok(Self {
      device,
      _temp_dir: temp_dir,
    })
  }

  #[inline]
  fn offset_of(idx: u64) -> u64 {
    idx * WDEV_PAGE_BYTES as u64
  }

  /// 扇区对齐直写一页，返回实际传输字节数（对标 write_aligned numBytes）
  pub fn write_page(&self, idx: u64) -> usize {
    let payload = page_pattern(idx % WDEV_PAGE_SLOTS, WDEV_PAGE_BYTES);
    let buf = AlignedBuf::from_slice(&payload, DEFAULT_SECTOR_SIZE).expect("wdev 对齐缓冲分配失败");
    let device = Arc::clone(&self.device);
    let offset = Self::offset_of(idx % WDEV_PAGE_SLOTS);
    let (res, _buf) = with_runtime(async move { device.write_aligned(offset, buf).await });
    res.expect("wdev 直写失败")
  }

  /// 池化直读一页，返回回读字节校验和（供 black_box，杜绝空读被优化）
  pub fn read_page(&self, idx: u64) -> u64 {
    let slot = idx % WDEV_PAGE_SLOTS;
    let device = Arc::clone(&self.device);
    let offset = Self::offset_of(slot);
    let buf = with_runtime(async move { device.read_range(offset, WDEV_PAGE_BYTES).await })
      .expect("wdev 直读失败");
    buf.as_slice().iter().map(|&b| b as u64).sum()
  }

  /// 工况预校验：写一页后回读须逐字节命中、传输计数须写满整页（对标 C# 设备层短写校验）
  pub fn validate(&self) {
    let probe = 3u64;
    let written = self.write_page(probe);
    assert_eq!(written, WDEV_PAGE_BYTES, "wdev 短写: 页未写满");
    let expect = page_pattern(probe, WDEV_PAGE_BYTES);
    let device = Arc::clone(&self.device);
    let offset = Self::offset_of(probe);
    let buf = with_runtime(async move { device.read_range(offset, WDEV_PAGE_BYTES).await })
      .expect("wdev 回读失败");
    assert_eq!(buf.as_slice(), &expect[..], "wdev 页回读失真");
  }
}
