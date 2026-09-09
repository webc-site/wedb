//! 复活池测试辅助方法与测试夹具

use wreviv::{FreeRecordBin, FreeRecordPool, Result, SetStatus};

/// 基础申请记录尺寸，对标 C# RevivificationTests.TakeRecordSize = 40
pub const TAKE_RECORD_SIZE: u32 = 40;

/// 记录对齐字节数，对标 C# Constants.kRecordAlignment = 8
pub const RECORD_ALIGNMENT: u32 = 8;

/// 测试地址基底偏移增量，对标 C# RevivificationTests.AddressIncrement = 1,000,000
pub const ADDRESS_INCREMENT: u64 = 1_000_000;

/// 创建单分桶测试池夹具，对标 C# RevivificationTestUtils.CreateSingleBinFreeRecordPool
pub fn create_single_bin_pool(
  record_size: u32,
  capacity: usize,
  scan_limit: usize,
) -> Result<FreeRecordPool> {
  FreeRecordPool::with_bin_sizes_and_scan_limit(&[record_size], capacity, scan_limit)
}

/// 填充 6 条 Best-Fit 测试基准记录，对标 C# CreateBestFitTestPool
///
/// 依次存入:
/// - 槽位 0: `base_addr + 1`, 尺寸 `take_size + 1`
/// - 槽位 1: `base_addr + 2`, 尺寸 `take_size + 2`
/// - 槽位 2: `base_addr + 3`, 尺寸 `take_size + 3`
/// - 槽位 3: `base_addr + 4`, 尺寸 `take_size` (精确匹配 1)
/// - 槽位 4: `base_addr + 5`, 尺寸 `take_size` (精确匹配 2)
/// - 槽位 5: `base_addr + 6`, 尺寸 `take_size` (精确匹配 3)
pub fn populate_best_fit_records(
  bin: &FreeRecordBin,
  base_addr: u64,
  take_size: u32,
  min_addr: u64,
) {
  for (i, size) in (1..=3).zip([take_size + 1, take_size + 2, take_size + 3]) {
    assert_eq!(
      bin.put(base_addr + i, size, min_addr),
      SetStatus::InsertedEmpty
    );
  }
  for i in 4..=6 {
    assert_eq!(
      bin.put(base_addr + i, take_size, min_addr),
      SetStatus::InsertedEmpty
    );
  }
}
