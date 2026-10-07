//! 测试侧读让点注入设备（fixtures 同层；R11 read 环回复燃前置，8v 裁定的
//! 测试域现代化面）
//!
//! 包装任意 [`wdev::Device`]，仅读原语（[`Device::read_aligned`] /
//! [`Device::read_raw`]，`read_range` 默认臂的最终落点）首次 poll 注入一次
//! Pending 让点后转发真设备——每读恰一次、确定性、与物理来源无关。持窗交叠
//! 族用例（drive_interleaved / rmw 交叠 / ttl_selfheal / store_ttl_clear /
//! bitop_fold / envelope_race）的可观测锚点由「victim 持窗期内存在一个
//! Pending 让点」承接（8r 重审判据），不再依赖 compio ReadAt 池环回这一
//! 恰好存在的物理让点：R6 read 直调裁定撤销的 21 用例配套成本由此在测试侧
//! 消解（R10 复燃前置落地）。
//!
//! 写面 / 刷盘 / 截断 / 元数据全转发不注入：交叠判据只认读侧让点（R7：
//! victim 慢路径持窗期内唯一 await 让点 = 冷装载设备读）。

use std::{
  future::Future,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};

use tempfile::{TempDir, tempdir};
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::{Device, Result, SegmentedDevice};
use wkv::{StoreConfig, WedbStore};
use wtest_base::test_store_config;

use crate::PendingReadStore;

/// 首轮 poll 自醒挂起一次的让点 future（对标 `futures::future::yield_now`
/// 语义，不引依赖：`wake_by_ref` 即时复唤，调用方下一轮 poll 即 Ready）
async fn yield_once() {
  struct Yield(bool);

  impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
      if self.0 {
        return Poll::Ready(());
      }
      self.0 = true;
      cx.waker().wake_by_ref();
      Poll::Pending
    }
  }

  Yield(false).await;
}

/// 读让点注入设备：包装真设备，读原语首次 poll 注入一次 Pending 让点
pub struct PendingReadDevice<D: Device> {
  inner: Arc<D>,
}

impl<D: Device> PendingReadDevice<D> {
  /// 包装真设备（`inner` 与裸装配同一 `Arc<SegmentedDevice>` 形态）
  pub fn new(inner: Arc<D>) -> Self {
    Self { inner }
  }
}

impl<D: Device> Device for PendingReadDevice<D> {
  #[inline]
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  #[inline]
  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  #[inline]
  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  #[inline]
  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  #[inline]
  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  #[inline]
  fn capacity(&self) -> Option<u64> {
    self.inner.capacity()
  }

  #[inline]
  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  #[inline]
  fn write_aligned(
    &self,
    offset: u64,
    buf: AlignedBuf,
  ) -> impl Future<Output = (Result<usize>, AlignedBuf)> {
    let inner = Arc::clone(&self.inner);
    async move { inner.write_aligned(offset, buf).await }
  }

  /// 读原语注入臂：首次 poll 挂起一次（窗口在手判据的可观测锚点），
  /// 复唤后转发真设备
  #[inline]
  fn read_aligned(
    &self,
    offset: u64,
    buf: AlignedBuf,
  ) -> impl Future<Output = (Result<usize>, AlignedBuf)> {
    let inner = Arc::clone(&self.inner);
    async move {
      yield_once().await;
      inner.read_aligned(offset, buf).await
    }
  }

  /// 读原语注入臂（缓冲 I/O 形，macOS 段设备 `read_range` 的实际路由），
  /// 注入语义同 [`Device::read_aligned`]
  #[inline]
  fn read_raw(
    &self,
    offset: u64,
    buf: AlignedBuf,
  ) -> impl Future<Output = (Result<usize>, AlignedBuf)> {
    let inner = Arc::clone(&self.inner);
    async move {
      yield_once().await;
      inner.read_raw(offset, buf).await
    }
  }

  #[inline]
  fn sync(&self) -> impl Future<Output = Result<()>> {
    let inner = Arc::clone(&self.inner);
    async move { inner.sync().await }
  }

  #[inline]
  fn sync_data(&self) -> impl Future<Output = Result<()>> {
    let inner = Arc::clone(&self.inner);
    async move { inner.sync_data().await }
  }

  #[inline]
  fn get_file_size(&self, segment_id: u32) -> Result<u64> {
    self.inner.get_file_size(segment_id)
  }

  #[inline]
  fn remove_segment(&self, segment_id: u32) -> impl Future<Output = Result<()>> {
    let inner = Arc::clone(&self.inner);
    async move { inner.remove_segment(segment_id).await }
  }

  #[inline]
  fn erase_tail_after(&self, from_address: u64) -> impl Future<Output = Result<()>> {
    let inner = Arc::clone(&self.inner);
    async move { inner.erase_tail_after(from_address).await }
  }

  #[inline]
  fn reset(&self) {
    self.inner.reset();
  }

  #[inline]
  fn truncate_until_segment(&self, segment_id: u32) -> impl Future<Output = Result<()>> {
    let inner = Arc::clone(&self.inner);
    async move { inner.truncate_until_segment(segment_id).await }
  }

  #[inline]
  fn recover(&self) -> Result<()> {
    self.inner.recover()
  }
}

/// 注入设备测试存储装配单源：`wtest_base::open_test_store` 的注入设备孪生
/// （tempdir + 单文件段设备 + 小预算配置 + gc off，目录随 [`TempDir`]
/// 存活 Drop 清理；持窗交叠族 8 册装配面）
pub fn open_pending_store(tag: &str) -> aok::Result<(TempDir, Arc<PendingReadStore>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  // 小库 + gc off：与 open_test_store 同拍（「小索引、后台紧缩禁用」测试意图）
  let mut config: StoreConfig = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(
    config,
    Arc::new(PendingReadDevice::new(device)),
  )?);
  Ok((dir, store))
}
