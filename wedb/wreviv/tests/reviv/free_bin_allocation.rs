//! 分桶分配算法与下界测试，对标 C# RevivificationTests.cs
//!
//! 覆盖：
//! - ArtificialSimpleTest (简单分配与统计)
//! - ArtificialBestFitTest (Best-Fit 最佳适配与窗口滑动)
//! - ArtificialFirstFitTest (First-Fit 首次适配)
//! - SimpleMinAddressAddTest (存入下界限制)
//! - SimpleMinAddressTakeTest (取出下界过期淘汰)
//! - BinSelectionTest (分桶索引二分选取与边界阶梯)
//! - take_with_max_bins 跨桶检索步长限制
//! - best_fit_equal_size_tie_break (同尺寸多候选取最低槽位索引的确定性裁决)

use aok::{OK, Void};
use log::info;
use wreviv::{FreeRecordBin, FreeRecordPool, SetStatus, USE_FIRST_FIT};

use super::support::{
  ADDRESS_INCREMENT, RECORD_ALIGNMENT, TAKE_RECORD_SIZE, create_single_bin_pool,
  populate_best_fit_records,
};

/// 验证单分桶简单存取流程
/// 对标 libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs:ArtificialSimpleTest
///
/// 1. 初始化容量 64、尺寸 TakeRecordSize + 8 的首次适配分桶池；
/// 2. 存入有效记录并以 min_address 校验取出；
/// 3. 取出地址与统计计数严格 1:1 对齐。
#[test]
fn simple_allocation() -> Void {
  info!("> simple_allocation [对标 C# ArtificialSimpleTest]");

  let bin_size = TAKE_RECORD_SIZE + RECORD_ALIGNMENT;
  let pool = create_single_bin_pool(bin_size, 64, USE_FIRST_FIT)?;

  let target_addr = ADDRESS_INCREMENT + 1;
  let min_addr = ADDRESS_INCREMENT;

  assert!(pool.put(target_addr, TAKE_RECORD_SIZE, min_addr));
  assert_eq!(pool.put_count(), 1);

  let taken = pool.take(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(taken, Some((target_addr, TAKE_RECORD_SIZE)));
  assert_eq!(pool.hit_count(), 1);
  assert_eq!(pool.take_count(), 1);
  assert_eq!(pool.drop_count(), 0);

  OK
}

/// 验证 Best-Fit 最佳适配与精确匹配优先逻辑
/// 对标 libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs:ArtificialBestFitTest
///
/// 存入 6 条记录：
/// - 槽位 0: TakeSize + 1 (41) -> 地址 1_000_001
/// - 槽位 1: TakeSize + 2 (42) -> 地址 1_000_002
/// - 槽位 2: TakeSize + 3 (43) -> 地址 1_000_003
/// - 槽位 3: TakeSize (40) -> 地址 1_000_004（精确匹配 1）
/// - 槽位 4: TakeSize (40) -> 地址 1_000_005（精确匹配 2）
/// - 槽位 5: TakeSize (40) -> 地址 1_000_006（精确匹配 3）
///
/// 取出序列断言：
/// 1. 扫描窗口内优先精确命中槽位 3（地址 4）；
/// 2. 槽位 3 清空后，精确命中槽位 4（地址 5）；
/// 3. 槽位 0（41）为最优适配，命中槽位 0（地址 1）；
/// 4. 窗口滑动，槽位 5（40）进入扫描范围并精确命中（地址 6）；
/// 5. 剩余最优适配槽位 1（地址 2）；
/// 6. 剩余最优适配槽位 2（地址 3）；
/// 7. 槽位耗尽，返回 None。
#[test]
fn best_fit_allocation_sequence() -> Void {
  info!("> best_fit_allocation_sequence [对标 C# ArtificialBestFitTest]");

  let bin = FreeRecordBin::with_scan_limit(TAKE_RECORD_SIZE + 128, 64, 4);
  let min_addr = ADDRESS_INCREMENT;

  populate_best_fit_records(&bin, ADDRESS_INCREMENT, TAKE_RECORD_SIZE, min_addr);
  assert_eq!(bin.len(), 6);

  // 1. 精确匹配槽位 3 (addr 4)
  let (res1, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res1, Some((ADDRESS_INCREMENT + 4, TAKE_RECORD_SIZE)));

  // 2. 精确匹配槽位 4 (addr 5)
  let (res2, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res2, Some((ADDRESS_INCREMENT + 5, TAKE_RECORD_SIZE)));

  // 3. 窗口内最优适配槽位 0 (addr 1, 尺寸 41)
  let (res3, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res3, Some((ADDRESS_INCREMENT + 1, TAKE_RECORD_SIZE + 1)));

  // 4. 首个候选推进，槽位 5 (addr 6, 尺寸 40) 进入窗口精确匹配
  let (res4, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res4, Some((ADDRESS_INCREMENT + 6, TAKE_RECORD_SIZE)));

  // 5. 剩余 42 与 43，最优适配槽位 1 (addr 2, 尺寸 42)
  let (res5, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res5, Some((ADDRESS_INCREMENT + 2, TAKE_RECORD_SIZE + 2)));

  // 6. 仅剩槽位 2 (addr 3, 尺寸 43)
  let (res6, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res6, Some((ADDRESS_INCREMENT + 3, TAKE_RECORD_SIZE + 3)));

  // 7. 全部槽位已空，返回 None
  let (res7, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res7, None);
  assert!(bin.is_empty());

  OK
}

/// 验证 First-Fit 首次适配按槽位顺序分配逻辑
/// 对标 libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs:ArtificialFirstFitTest
///
/// 当 scan_limit 为 USE_FIRST_FIT 时，按物理槽位顺序依次取出。
#[test]
fn first_fit_allocation_sequence() -> Void {
  info!("> first_fit_allocation_sequence [对标 C# ArtificialFirstFitTest]");

  let bin = FreeRecordBin::with_scan_limit(TAKE_RECORD_SIZE + 128, 64, USE_FIRST_FIT);
  let min_addr = ADDRESS_INCREMENT;

  populate_best_fit_records(&bin, ADDRESS_INCREMENT, TAKE_RECORD_SIZE, min_addr);

  for i in 1..=6 {
    let (res, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
    let expected_addr = ADDRESS_INCREMENT + i as u64;
    let expected_size = if i <= 3 {
      TAKE_RECORD_SIZE + i
    } else {
      TAKE_RECORD_SIZE
    };
    assert_eq!(
      res,
      Some((expected_addr, expected_size)),
      "First-Fit 取出序号 {i} 不匹配"
    );
  }

  let (res_none, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res_none, None);

  OK
}

/// 验证低于 min_address 的记录在存入时被拦截
/// 对标 libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs:SimpleMinAddressAddTest
#[test]
fn min_address_put_boundary() -> Void {
  info!("> min_address_put_boundary [对标 C# SimpleMinAddressAddTest]");

  let pool = FreeRecordPool::new();
  let min_addr = 0x2000u64;

  // 低于 min_address，拒绝存入并计入丢弃
  assert!(!pool.put(0x1000, 64, min_addr));
  assert_eq!(pool.drop_count(), 1);
  assert_eq!(pool.total_active_records(), 0);

  // 高于等于 min_address，成功存入
  assert!(pool.put(0x2000, 64, min_addr));
  assert!(pool.put(0x3000, 64, min_addr));
  assert_eq!(pool.total_active_records(), 2);

  OK
}

/// 验证 min_address 推进导致记录过期并在取出时就地淘汰
/// 对标 libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs:SimpleMinAddressTakeTest
#[test]
fn min_address_take_invalidation() -> Void {
  info!("> min_address_take_invalidation [对标 C# SimpleMinAddressTakeTest]");

  let pool = FreeRecordPool::new();

  // 1. 存入有效记录
  assert!(pool.put(0x2000, 64, 0x1000));
  assert_eq!(pool.total_active_records(), 1);

  // 2. min_address 提升到 0x3000，记录已落入失效只读区
  let taken = pool.take(64, 0x3000);
  assert_eq!(taken, None, "已滑入失效区的记录不可被复活");
  assert_eq!(pool.total_active_records(), 0);
  assert!(pool.drop_count() >= 1, "过期槽位应就地淘汰清零并计入 drop");

  OK
}

/// 验证二分查找分桶阶梯边界映射
/// 对标 libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs:BinSelectionTest
#[test]
fn bin_selection_partition_point() -> Void {
  info!("> bin_selection_partition_point [对标 C# BinSelectionTest]");

  let pool = FreeRecordPool::new();
  assert_eq!(pool.bin_count(), 13);

  // 极小尺寸边界（16B 起步，对齐 wrecord HEADER_SIZE 与 C# MinRecordSize）
  assert_eq!(pool.find_bin_index(1), Some(0));
  assert_eq!(pool.find_bin_index(16), Some(0));

  // 阶梯跃迁边界 (16B -> 32B -> 64B -> 128B -> 256B)
  assert_eq!(pool.find_bin_index(17), Some(1));
  assert_eq!(pool.find_bin_index(32), Some(1));
  assert_eq!(pool.find_bin_index(33), Some(2));
  assert_eq!(pool.find_bin_index(64), Some(2));
  assert_eq!(pool.find_bin_index(65), Some(3));
  assert_eq!(pool.find_bin_index(128), Some(3));
  assert_eq!(pool.find_bin_index(129), Some(4));
  assert_eq!(pool.find_bin_index(256), Some(4));

  // 最大内联尺寸边界
  assert_eq!(pool.find_bin_index(32769), Some(12));
  assert_eq!(pool.find_bin_index(65535), Some(12));

  // 溢出尺寸
  assert_eq!(pool.find_bin_index(65536), None);
  assert_eq!(pool.find_bin_index(100_000), None);

  OK
}

/// 验证 Best-Fit 对同尺寸多候选的选择确定性（锁定碎片演化方向）
///
/// 1. 真最优（浪费最小）优先于同尺寸等价候选；
/// 2. 剩余等尺寸非精确候选：严格按扫描序取最低槽位索引（严格 `<` 比较保留首个）；
/// 3. CAS 无竞争下逐次取出顺序完全确定，保障碎片演化可复现。
#[test]
fn best_fit_equal_size_tie_break_determinism() -> Void {
  info!("> best_fit_equal_size_tie_break_determinism [同尺寸多候选确定性裁决]");

  let bin = FreeRecordBin::with_scan_limit(TAKE_RECORD_SIZE + 128, 16, 8);
  let min_addr = 0;

  // 槽位 0/1/2 依次存入 50/48/50
  assert_eq!(bin.put(0x1000, 50, min_addr), SetStatus::InsertedEmpty);
  assert_eq!(bin.put(0x2000, 48, min_addr), SetStatus::InsertedEmpty);
  assert_eq!(bin.put(0x3000, 50, min_addr), SetStatus::InsertedEmpty);
  assert_eq!(bin.len(), 3);

  // 1. 真最优优先：浪费最小的 48 胜出（槽位 1）
  let (res1, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res1, Some((0x2000, 48)));

  // 2. 剩余两个等尺寸 50 候选：确定性取最低槽位索引（槽位 0）
  let (res2, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res2, Some((0x1000, 50)));

  // 3. 仅剩槽位 2
  let (res3, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res3, Some((0x3000, 50)));

  // 4. 槽位耗尽
  let (res4, _) = bin.take_best_fit(TAKE_RECORD_SIZE, min_addr);
  assert_eq!(res4, None);
  assert!(bin.is_empty());

  OK
}

/// 验证限制跨桶检索步长时的检索截断行为
#[test]
fn take_with_max_bins_limit() -> Void {
  info!("> take_with_max_bins_limit [跨桶检索深度限制验证]");

  let pool = FreeRecordPool::with_bin_sizes(&[64, 128], 16)?;

  // 仅在 128B 分桶中存入记录
  assert!(pool.put(0x2000, 100, 0));

  // max_bins = 0：仅在 64B 桶查找，不跨桶，返回 None
  let take_no_cross = pool.take_with_max_bins(50, 0, 0);
  assert_eq!(take_no_cross, None);

  // max_bins = 1：允许向上跨 1 桶，命中 128B 桶中记录
  let take_cross1 = pool.take_with_max_bins(50, 0, 1);
  assert_eq!(take_cross1, Some((0x2000, 100)));

  OK
}
