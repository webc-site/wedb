use wdev::SegmentedDevice;

use super::{
  garnet_log::{InMemorySublog, LogRecord, SublogBackend},
  waof_sublog::WaofSublog,
};

/// 统一子日志静态分发枚举
pub enum Sublog {
  /// 纯内存日志（测试、单机极速模式）
  Mem(InMemorySublog),
  /// waof 块设备物理日志
  Waof(WaofSublog<SegmentedDevice>),
}

impl From<InMemorySublog> for Sublog {
  #[inline]
  fn from(m: InMemorySublog) -> Self {
    Self::Mem(m)
  }
}

impl From<WaofSublog<SegmentedDevice>> for Sublog {
  #[inline]
  fn from(w: WaofSublog<SegmentedDevice>) -> Self {
    Self::Waof(w)
  }
}

impl Sublog {
  #[inline(always)]
  pub fn enqueue(&self, payload: &[u8]) -> i64 {
    SublogBackend::enqueue(self, payload)
  }

  #[inline(always)]
  pub fn tail_address(&self) -> i64 {
    SublogBackend::tail_address(self)
  }

  #[inline(always)]
  pub fn begin_address(&self) -> i64 {
    SublogBackend::begin_address(self)
  }

  #[inline(always)]
  pub fn committed_until_address(&self) -> i64 {
    SublogBackend::committed_until_address(self)
  }

  #[inline(always)]
  pub fn commit(&self, until_address: i64, cookie: i64) {
    SublogBackend::commit(self, until_address, cookie)
  }

  #[inline(always)]
  pub fn recovered_cookie(&self) -> Option<i64> {
    SublogBackend::recovered_cookie(self)
  }

  #[inline(always)]
  pub fn flushed_until_address(&self) -> i64 {
    SublogBackend::flushed_until_address(self)
  }

  #[inline(always)]
  pub fn scan(&self, begin_address: i64, end_address: i64) -> Vec<LogRecord> {
    SublogBackend::scan(self, begin_address, end_address)
  }

  #[inline(always)]
  pub fn shift_begin_address(&self, new_begin: i64) {
    SublogBackend::shift_begin_address(self, new_begin)
  }

  #[inline(always)]
  pub fn log_page_size_bits(&self) -> i32 {
    SublogBackend::log_page_size_bits(self)
  }

  #[inline(always)]
  pub fn memory_size_bytes(&self) -> i64 {
    SublogBackend::memory_size_bytes(self)
  }

  #[inline(always)]
  pub fn reset(&self) {
    SublogBackend::reset(self)
  }

  #[inline(always)]
  pub async fn recover_async(&self) {
    SublogBackend::recover_async(self).await
  }

  #[inline(always)]
  pub fn safe_initialize(
    &self,
    begin_address: i64,
    committed_until_address: i64,
    last_commit_num: i64,
  ) {
    SublogBackend::safe_initialize(
      self,
      begin_address,
      committed_until_address,
      last_commit_num,
    )
  }
}

impl SublogBackend for Sublog {
  #[inline(always)]
  fn enqueue(&self, payload: &[u8]) -> i64 {
    match self {
      Self::Mem(m) => m.enqueue(payload),
      Self::Waof(w) => w.enqueue(payload),
    }
  }

  #[inline(always)]
  fn tail_address(&self) -> i64 {
    match self {
      Self::Mem(m) => m.tail_address(),
      Self::Waof(w) => w.tail_address(),
    }
  }

  #[inline(always)]
  fn begin_address(&self) -> i64 {
    match self {
      Self::Mem(m) => m.begin_address(),
      Self::Waof(w) => w.begin_address(),
    }
  }

  #[inline(always)]
  fn committed_until_address(&self) -> i64 {
    match self {
      Self::Mem(m) => m.committed_until_address(),
      Self::Waof(w) => w.committed_until_address(),
    }
  }

  #[inline(always)]
  fn commit(&self, until_address: i64, cookie: i64) {
    match self {
      Self::Mem(m) => m.commit(until_address, cookie),
      Self::Waof(w) => w.commit(until_address, cookie),
    }
  }

  #[inline(always)]
  fn recovered_cookie(&self) -> Option<i64> {
    match self {
      Self::Mem(m) => m.recovered_cookie(),
      Self::Waof(w) => w.recovered_cookie(),
    }
  }

  #[inline(always)]
  fn flushed_until_address(&self) -> i64 {
    match self {
      Self::Mem(m) => m.flushed_until_address(),
      Self::Waof(w) => w.flushed_until_address(),
    }
  }

  #[inline(always)]
  fn scan(&self, begin_address: i64, end_address: i64) -> Vec<LogRecord> {
    match self {
      Self::Mem(m) => m.scan(begin_address, end_address),
      Self::Waof(w) => w.scan(begin_address, end_address),
    }
  }

  #[inline(always)]
  async fn scan_async(&self, begin_address: i64, end_address: i64) -> Vec<LogRecord> {
    match self {
      Self::Mem(m) => m.scan_async(begin_address, end_address).await,
      Self::Waof(w) => w.scan_async(begin_address, end_address).await,
    }
  }

  #[inline(always)]
  fn shift_begin_address(&self, new_begin: i64) {
    match self {
      Self::Mem(m) => m.shift_begin_address(new_begin),
      Self::Waof(w) => w.shift_begin_address(new_begin),
    }
  }

  #[inline(always)]
  async fn truncate_until_async(&self, new_begin: i64) {
    match self {
      Self::Mem(m) => m.truncate_until_async(new_begin).await,
      Self::Waof(w) => w.truncate_until_async(new_begin).await,
    }
  }

  #[inline(always)]
  async fn commit_flush_async(&self, cookie: i64) {
    match self {
      Self::Mem(m) => m.commit_flush_async(cookie).await,
      Self::Waof(w) => w.commit_flush_async(cookie).await,
    }
  }

  #[inline(always)]
  async fn recover_async(&self) {
    match self {
      Self::Mem(m) => m.recover_async().await,
      Self::Waof(w) => w.recover_async().await,
    }
  }

  #[inline(always)]
  fn log_page_size_bits(&self) -> i32 {
    match self {
      Self::Mem(m) => m.log_page_size_bits(),
      Self::Waof(w) => w.log_page_size_bits(),
    }
  }

  #[inline(always)]
  fn memory_size_bytes(&self) -> i64 {
    match self {
      Self::Mem(m) => m.memory_size_bytes(),
      Self::Waof(w) => w.memory_size_bytes(),
    }
  }

  #[inline(always)]
  fn reset(&self) {
    match self {
      Self::Mem(m) => m.reset(),
      Self::Waof(w) => w.reset(),
    }
  }

  #[inline(always)]
  fn safe_initialize(
    &self,
    begin_address: i64,
    committed_until_address: i64,
    last_commit_num: i64,
  ) {
    match self {
      Self::Mem(m) => m.safe_initialize(begin_address, committed_until_address, last_commit_num),
      Self::Waof(w) => w.safe_initialize(begin_address, committed_until_address, last_commit_num),
    }
  }

  #[inline(always)]
  async fn wait_for_commit_async(&self, until_address: i64) {
    match self {
      Self::Mem(m) => m.wait_for_commit_async(until_address).await,
      Self::Waof(w) => w.wait_for_commit_async(until_address).await,
    }
  }
}
