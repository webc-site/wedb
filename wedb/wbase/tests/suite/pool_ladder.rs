//! size class 阶梯算术测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs 的
//! `LadderIsMonotonicAndBounded`（阶梯单调、容量充分、浪费有界、超界 bypass）；
//! `LocalRetentionIsBoundedByThePerThreadByteCap` 与 `LocalByteCapIsSharedFairlyAcrossClasses`（字节计价保留上限与公平腾挪）。

use std::sync::Arc;

use aok::{OK, Result, Void};
use log::info;
use wbase::{
  BufferPool, CLASS_CAPACITIES_SECTORS, DEFAULT_SECTOR_SIZE, MAX_POOLED_SECTORS, MIN_SECTOR_SIZE,
  NUM_CLASSES, class_capacity_sectors, class_of_sectors,
};

/// 线性区顶部扇区数 (2 精确 + 4 线性 × 4 扇区步长，对标 C# TestLinearTopSectors)
const LINEAR_TOP_SECTORS: usize = 16;
/// 线性区步长扇区数 (对标 C# TestLinearStrideSectors)
const LINEAR_STRIDE: usize = 4;

/// 全扇区数域穷举：class 单调、容量充分且最小、浪费有界、往返恒等
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

fn rent_burst_and_return(pool: &Arc<BufferPool>, bytes: usize, count: usize) -> Result<()> {
  let mut list = Vec::with_capacity(count);
  for _ in 0..count {
    list.push(pool.get(bytes)?);
  }
  drop(list);
  Ok(())
}

/// 线程本地保留受 per-thread 字节上限约束，突发流量超出上限部分溢出至全局条带仓库
/// (对标 C# `LocalRetentionIsBoundedByThePerThreadByteCap`)
#[test]
fn local_retention_is_bounded_by_the_per_thread_byte_cap() -> Void {
  info!("验证单线程本地缓存受 thread_local_byte_cap 字节上限约束并正确溢出");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let cap = pool.thread_local_byte_cap();
  assert!(cap > 0, "pool 必须公布线程本地保留字节上限");

  let count = (cap / (2 * DEFAULT_SECTOR_SIZE)).max(1) * 3;
  rent_burst_and_return(&pool, DEFAULT_SECTOR_SIZE, count)?;

  assert!(
    pool.caller_local_bytes() > 0,
    "线程必须在其配额内保留缓冲区"
  );
  assert!(pool.caller_local_bytes() <= cap, "线程保留字节不得超出上限");

  pool.free();
  assert_eq!(pool.reserved_bytes(), 0, "关闭后预算许可必须全部归零");

  OK
}

/// 线程本地配额在多 class 之间公平共享，新活跃 class 腾挪老 class 份额
/// (对标 C# `LocalByteCapIsSharedFairlyAcrossClasses`)
#[test]
fn local_byte_cap_is_shared_fairly_across_classes() -> Void {
  info!("验证 thread_local_byte_cap 在多 active class 间的 max-min 公平腾挪");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let cap = pool.thread_local_byte_cap();
  let cls_a = class_of_sectors(1).expect("1 扇区命中 class A");
  let cls_b = class_of_sectors(2).expect("2 扇区命中 class B");
  assert_ne!(cls_a, cls_b, "两组探测尺寸必须属于不同 class");

  // 1. 仅 class A 活跃：独占整个本地配额
  let count_a = (cap / (2 * DEFAULT_SECTOR_SIZE)) + 64;
  rent_burst_and_return(&pool, DEFAULT_SECTOR_SIZE, count_a)?;
  assert!(
    pool.caller_local_bytes_for_class(cls_a) > cap / 2,
    "单 class 活跃时应能持有超过半数配额"
  );

  // 2. class B 变活跃：公平腾挪份额
  let count_b = (cap / (2 * 2 * DEFAULT_SECTOR_SIZE)) + 64;
  rent_burst_and_return(&pool, 2 * DEFAULT_SECTOR_SIZE, count_b)?;

  assert!(
    pool.caller_local_bytes() <= cap,
    "多 class 共享后总量仍不得超过配额上限"
  );
  assert!(
    pool.caller_local_bytes_for_class(cls_b) > 0,
    "新活跃 class B 必须成功夺得部分配额"
  );
  assert!(
    pool.caller_local_bytes_for_class(cls_a) <= cap * 3 / 4,
    "老 class A 必须让出部分配额"
  );

  pool.free();
  assert_eq!(pool.reserved_bytes(), 0, "关闭后预算许可必须全部归零");

  OK
}
