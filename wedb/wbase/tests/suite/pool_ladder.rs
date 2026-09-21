//! size class 阶梯算术测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs 的
//! `LadderIsMonotonicAndBounded`（阶梯单调、容量充分、浪费有界、超界 bypass）。

use aok::{OK, Void};
use log::info;
use wbase::{
  align::MIN_SECTOR_SIZE,
  pool::{
    CLASS_CAPACITIES_SECTORS, MAX_POOLED_SECTORS, NUM_CLASSES, class_capacity_sectors,
    class_of_sectors,
  },
};

/// 线性区顶部扇区数 (2 精确 + 4 线性 × 4 扇区步长，对标 C# TestLinearTopSectors)
const LINEAR_TOP_SECTORS: usize = 16;
/// 线性区步长扇区数 (对标 C# TestLinearStrideSectors)
const LINEAR_STRIDE: usize = 4;

/// 全扇区数域穷举：class 单调、容量充分且最小、浪费有界、往返恒等
///
/// libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs:LadderIsMonotonicAndBounded
#[test]
fn ladder_is_monotonic_and_bounded() -> Void {
  info!("穷举 1..=MAX_POOLED_SECTORS 验证阶梯映射单调、充分、最小且几何区浪费 <= 1.5x");

  let mut prev_cls = 0usize;
  for s in 1..=MAX_POOLED_SECTORS {
    let cls = class_of_sectors(s).expect("池内扇区数必须命中 class");
    assert!(cls < NUM_CLASSES, "sectors={s} class 必须在范围内");
    assert!(cls >= prev_cls, "class 必须随扇区数单调不减 (sectors={s})");
    prev_cls = cls;

    let cap = class_capacity_sectors(cls);
    assert!(cap >= s, "sectors={s}: class {cls} 容量 {cap} 必须覆盖请求");

    // 必须是最小充分 class：低一级容量严格不足
    if cls > 0 {
      let prev_cap = class_capacity_sectors(cls - 1);
      assert!(
        prev_cap < s,
        "sectors={s}: 低一级容量 {prev_cap} 已充分，非最小"
      );
    }

    // 线性区取整浪费 < 一个步长；几何区浪费上界 1.5x
    if s <= LINEAR_TOP_SECTORS {
      assert!(
        cap - s < LINEAR_STRIDE,
        "线性区浪费必须 < {LINEAR_STRIDE} 扇区"
      );
    } else {
      assert!(
        cap * 2 <= s * 3,
        "几何区 class {cls} 容量 {cap} 对 sectors={s} 浪费超 1.5x"
      );
    }

    // 往返恒等：class 容量必须映射回自身
    assert_eq!(class_of_sectors(cap), Some(cls), "class {cls} 往返映射失恒");
  }

  // 超出可池化上限必须 bypass
  assert_eq!(
    class_of_sectors(MAX_POOLED_SECTORS + 1),
    None,
    "超界请求必须 bypass"
  );

  // 最常用的小尺寸保留精确 class
  assert_eq!(
    class_capacity_sectors(class_of_sectors(1).expect("1 扇区")),
    1
  );
  assert_eq!(
    class_capacity_sectors(class_of_sectors(2).expect("2 扇区")),
    2
  );

  // 基于 ~8MB 内联值的完整记录必须池化而非 bypass
  let record_sectors = (8 * 1024 * 1024 + 128 * 1024) / MIN_SECTOR_SIZE;
  assert!(
    class_of_sectors(record_sectors).is_some(),
    "≈8.1MB 记录 ({record_sectors} 扇区) 必须池化"
  );

  // 防御性边界：越界 class 不得触发移位溢出 panic (钳制为饱和值 usize::MAX)
  assert!(class_capacity_sectors(NUM_CLASSES) > MAX_POOLED_SECTORS);
  assert_eq!(class_capacity_sectors(usize::MAX), usize::MAX);

  OK
}

/// 编译期常量表与 const fn 求值一致
#[test]
fn class_capacities_const_table_matches_fn() -> Void {
  info!(
    "验证 CLASS_CAPACITIES_SECTORS 常量表与 class_capacity_sectors/class_of_sectors 的 const 求值"
  );

  for (c, &cap) in CLASS_CAPACITIES_SECTORS.iter().enumerate() {
    assert_eq!(
      cap,
      class_capacity_sectors(c),
      "class {c} 常量表必须与函数结果完全一致"
    );
  }

  // const 上下文求值验证
  const C0: Option<usize> = class_of_sectors(1);
  const C1: Option<usize> = class_of_sectors(2);
  const C_TOP: Option<usize> = class_of_sectors(MAX_POOLED_SECTORS);
  const C_OVER: Option<usize> = class_of_sectors(MAX_POOLED_SECTORS + 1);
  assert_eq!(C0, Some(0));
  assert_eq!(C1, Some(1));
  assert_eq!(C_TOP, Some(NUM_CLASSES - 1));
  assert_eq!(C_OVER, None);

  OK
}
