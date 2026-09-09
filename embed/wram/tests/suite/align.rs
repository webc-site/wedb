//! 扇区对齐算术与 SectorRange 测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/src/core/Allocator/ 中 SectorAlignedBufferPool 的
//! 对齐数学（RoundUp/ClassOfSectors 所依赖的对齐原语）；
//! 取整与切片断言对标 libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs
//! 的 `GetCapacityCoversRequestAcrossSizes` 等用例中的对齐约定。

use aok::{OK, Void};
use log::info;
use wram::{
  DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, SectorRange, SectorRangeError, align_down, align_up,
  checked_align_up, is_aligned,
};

/// is_aligned 按字对齐判定语义
#[test]
fn is_aligned_reports_sector_alignment() -> Void {
  info!("验证 is_aligned 对 2 的幂与非 2 的幂对齐的判定");

  // 2 的幂对齐：位运算快速路径
  assert!(is_aligned(0, 512));
  assert!(is_aligned(512, 512));
  assert!(is_aligned(1024, 512));
  assert!(is_aligned(4096, 4096));
  assert!(is_aligned(8192, 4096));
  assert!(!is_aligned(1, 512));
  assert!(!is_aligned(511, 512));
  assert!(!is_aligned(4095, 4096));

  // 对齐为 0 时防御性返回 false
  assert!(!is_aligned(100, 0));

  // 非 2 的幂对齐：模运算回退路径
  assert!(is_aligned(14, 7));
  assert!(!is_aligned(15, 7));

  OK
}

/// align_down / align_up 的向下与向上取整语义
#[test]
fn align_down_and_align_up_round_trip() -> Void {
  info!("验证 align_down/align_up 在 2 的幂与非 2 的幂对齐下的取整");

  // align_down（2 的幂）
  assert_eq!(align_down(0, 4096), 0);
  assert_eq!(align_down(100, 4096), 0);
  assert_eq!(align_down(4095, 4096), 0);
  assert_eq!(align_down(4096, 4096), 4096);
  assert_eq!(align_down(4097, 4096), 4096);
  assert_eq!(align_down(8191, 4096), 4096);
  assert_eq!(align_down(8192, 4096), 8192);
  assert_eq!(align_down(500, 512), 0);
  assert_eq!(align_down(512, 512), 512);
  assert_eq!(align_down(513, 512), 512);

  // align_up（2 的幂）
  assert_eq!(align_up(0, 4096), 0);
  assert_eq!(align_up(1, 4096), 4096);
  assert_eq!(align_up(100, 4096), 4096);
  assert_eq!(align_up(4095, 4096), 4096);
  assert_eq!(align_up(4096, 4096), 4096);
  assert_eq!(align_up(4097, 4096), 8192);
  assert_eq!(align_up(8192, 4096), 8192);
  assert_eq!(align_up(1, 512), 512);
  assert_eq!(align_up(512, 512), 512);
  assert_eq!(align_up(513, 512), 1024);

  // 非 2 的幂对齐：模运算回退
  assert_eq!(align_down(15, 7), 14);
  assert_eq!(align_up(15, 7), 21);
  assert_eq!(align_up(14, 7), 14);
  assert_eq!(align_down(100, 0), 100, "对齐 <= 1 时原值返回");

  OK
}

/// checked_align_up 溢出检测与 align_up 饱和语义
#[test]
fn checked_align_up_overflow_protection() -> Void {
  info!("验证 checked_align_up 极值溢出返回 None，align_up 饱和到最大对齐倍数");

  // 正常对齐计算
  assert_eq!(checked_align_up(0, 4096), Some(0));
  assert_eq!(checked_align_up(1, 4096), Some(4096));
  assert_eq!(checked_align_up(4096, 4096), Some(4096));
  assert_eq!(checked_align_up(4097, 4096), Some(8192));
  assert_eq!(checked_align_up(100, 1), Some(100));
  assert_eq!(checked_align_up(100, 0), Some(100));

  // 极大值与溢出检测
  let align = 4096u64;
  let max_aligned = (u64::MAX / align) * align; // 0xFFFFFFFFFFFFF000
  assert_eq!(checked_align_up(max_aligned, align), Some(max_aligned));
  assert_eq!(
    checked_align_up(max_aligned + 1, align),
    None,
    "溢出必须返回 None"
  );
  assert_eq!(checked_align_up(u64::MAX, align), None);

  // 非 2 的幂对齐走模运算路径 (u64::MAX % 7 == 1)
  assert_eq!(checked_align_up(10, 7), Some(14));
  assert_eq!(checked_align_up(14, 7), Some(14));
  let max_mult_7 = (u64::MAX / 7) * 7;
  assert_eq!(checked_align_up(max_mult_7, 7), Some(max_mult_7));
  assert_eq!(checked_align_up(max_mult_7 + 1, 7), None);

  // align_up 溢出饱和至 u64::MAX 范围内最大对齐倍数
  assert_eq!(align_up(max_aligned + 1, align), max_aligned);
  assert_eq!(align_up(u64::MAX, align), max_aligned);
  assert_eq!(align_up(max_mult_7 + 1, 7), max_mult_7);

  OK
}

/// SectorRange 逻辑范围到物理扇区范围的换算与切片提取
#[test]
fn sector_range_calculate_slices_and_counts() -> Void {
  info!("验证 SectorRange::calculate 的对齐换算、internal_offset 与 sub_range");

  // 非法对齐：非 2 的幂或 < 512
  assert!(SectorRange::calculate(0, 100, 300).is_err());
  assert!(SectorRange::calculate(0, 100, 1000).is_err());

  // 长度为 0：仅对齐起始偏移，不跨扇区
  let r0 = SectorRange::calculate(100, 0, DEFAULT_SECTOR_SIZE)?;
  assert_eq!(r0.aligned_offset, 0);
  assert_eq!(r0.aligned_len, 0);
  assert_eq!(r0.internal_offset, 100);
  assert_eq!(r0.sector_count(DEFAULT_SECTOR_SIZE), 0);

  // 单扇区完全对齐
  let r1 = SectorRange::calculate(4096, 4096, DEFAULT_SECTOR_SIZE)?;
  assert_eq!(r1.aligned_offset, 4096);
  assert_eq!(r1.aligned_len, 4096);
  assert_eq!(r1.internal_offset, 0);
  assert_eq!(r1.sector_count(DEFAULT_SECTOR_SIZE), 1);
  assert_eq!(r1.sub_range(4096), 0..4096);

  // 跨扇区读取：从 4000 读 200 字节，覆盖 0..8192 两个扇区
  let r2 = SectorRange::calculate(4000, 200, DEFAULT_SECTOR_SIZE)?;
  assert_eq!(r2.aligned_offset, 0);
  assert_eq!(r2.aligned_len, 8192);
  assert_eq!(r2.internal_offset, 4000);
  assert_eq!(r2.sector_count(DEFAULT_SECTOR_SIZE), 2);
  assert_eq!(r2.sub_range(200), 4000..4200);

  // 512 字节扇区
  let r3 = SectorRange::calculate(1000, 50, MIN_SECTOR_SIZE)?;
  assert_eq!(r3.aligned_offset, 512);
  assert_eq!(r3.aligned_len, 1024);
  assert_eq!(r3.internal_offset, 488);
  assert_eq!(r3.sector_count(MIN_SECTOR_SIZE), 2);
  assert_eq!(r3.sub_range(50), 488..538);

  // 5000 偏移 300 字节：跨 4096..8192 扇区，逻辑切片 904..1204
  let r4 = SectorRange::calculate(5000, 300, DEFAULT_SECTOR_SIZE)?;
  assert_eq!(r4.aligned_offset, 4096);
  assert_eq!(r4.aligned_len, 4096);
  assert_eq!(r4.internal_offset, 904);
  assert_eq!(r4.sub_range(300), 904..1204);

  // 任意非对齐长度必须向上舍入为扇区整数倍
  for req in [1, 200, 4095, 4096, 4097, 8191, 8192] {
    let range = SectorRange::calculate(0, req, DEFAULT_SECTOR_SIZE)?;
    let rounded = align_up(req as u64, DEFAULT_SECTOR_SIZE as u64);
    assert_eq!(
      range.aligned_len as u64, rounded,
      "请求 {req} 必须取整到 {rounded}"
    );
    assert_eq!(
      range.sector_count(DEFAULT_SECTOR_SIZE),
      (rounded / DEFAULT_SECTOR_SIZE as u64) as usize
    );
  }

  OK
}

/// SectorRange 极值偏移与长度的溢出防护
#[test]
fn sector_range_overflow_protection() -> Void {
  info!("验证 SectorRange::calculate 溢出时报 Overflow 错误而非回绕");

  // offset + len 溢出 u64
  assert!(matches!(
    SectorRange::calculate(u64::MAX - 10, 100, DEFAULT_SECTOR_SIZE),
    Err(SectorRangeError::Overflow)
  ));

  // 末端恰在 u64::MAX 且不对齐：末端取整溢出
  let max_aligned = u64::MAX - (DEFAULT_SECTOR_SIZE as u64 - 1);
  assert!(matches!(
    SectorRange::calculate(
      max_aligned + 1,
      DEFAULT_SECTOR_SIZE - 1,
      DEFAULT_SECTOR_SIZE
    ),
    Err(SectorRangeError::Overflow)
  ));

  // usize::MAX 长度必然溢出
  assert!(matches!(
    SectorRange::calculate(0, usize::MAX, DEFAULT_SECTOR_SIZE),
    Err(SectorRangeError::Overflow)
  ));

  OK
}
