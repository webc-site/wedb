//! 下界清理与边界防御测试，对标 C# RevivificationTests.cs
//!
//! 覆盖：
//! - UnelideTest (分桶容量饱和展开与溢出丢弃)
//! - purge_below_multi_bin_bulk (多级分桶批量冷区淘汰)
//! - put_replaces_expired_slot (过期槽位的覆盖替换与 drop 计数)
//! - free_record_cas_aba_defense (FreeRecord 原子打包与 ABA 防御)
//! - best_fit_scan_limit_clamping (扫描上限与最大尺寸安全钳位)
//! - boundary_parameter_defense (极端边界条件与参数防御拦截)
//! - multithread_purge_and_cas_race (多线程存入与主动淘汰 CAS 竞态安全)
//! - non_monotonic_min_address_safety (非单调 min_address 的安全契约)

use std::{
  sync::{Arc, Barrier},
  thread,
};

use aok::{OK, Void};
use log::info;
use wreviv::{Error, FreeRecord, FreeRecordBin, FreeRecordPool, SetStatus, USE_FIRST_FIT};

/// 验证分桶容量满载时的展开与溢出丢弃机制
/// 对标 C# `UnelideTest`
///
/// 1. 分桶容量达到上限时，多余放入的记录被拒绝并计入 drop 计数；
/// 2. 取出记录腾出空位后，新记录可以再次放入。
#[test]
fn unelide_capacity_overflow() -> Void {
  info!("> unelide_capacity_overflow [对标 C# UnelideTest]");

  let capacity = 4;
  let pool = FreeRecordPool::with_bin_sizes(&[64], capacity)?;

  // 填满分桶
  for i in 1..=capacity {
    assert!(pool.put(0x1000 * i as u64, 64, 0x1000));
  }
  assert_eq!(pool.total_active_records(), capacity);

  // 分桶已满，第 5 个记录溢出被拒绝
  assert!(!pool.put(0x5000, 64, 0x1000));
  assert_eq!(pool.drop_count(), 1);

  // 取出 1 个槽位腾出空间
  let taken = pool.take(64, 0x1000);
  assert!(taken.is_some());
  assert_eq!(pool.total_active_records(), capacity - 1);

  // 腾出空位后再次放入成功
  assert!(pool.put(0x5000, 64, 0x1000));
  assert_eq!(pool.total_active_records(), capacity);

  OK
}

/// 验证多级分桶跨桶批量淘汰冷区槽位（purge_below）
///
/// 1. 跨多个分桶（64B, 128B, 256B）存放多条记录；
/// 2. 调用 purge_below 批量淘汰所有低于 min_address 的记录；
/// 3. drop_count 严格累加被淘汰的槽位数，活跃槽位精准递减；
/// 4. 未过期的热区槽位依然完好保留，可供正常复活。
#[test]
fn purge_below_multi_bin_bulk() -> Void {
  info!("> purge_below_multi_bin_bulk [批量主动清理冷区审查]");

  let pool = FreeRecordPool::with_bin_sizes(&[64, 128, 256], 8)?;

  // 存入 6 条记录跨越不同地址与尺寸
  assert!(pool.put(0x1000, 50, 0)); // 64B 桶，冷区
  assert!(pool.put(0x2000, 60, 0)); // 64B 桶，冷区
  assert!(pool.put(0x3000, 100, 0)); // 128B 桶，冷区
  assert!(pool.put(0x4000, 120, 0)); // 128B 桶，热区
  assert!(pool.put(0x5000, 200, 0)); // 256B 桶，热区
  assert!(pool.put(0x6000, 220, 0)); // 256B 桶，热区

  assert_eq!(pool.total_active_records(), 6);
  assert_eq!(pool.drop_count(), 0);

  // 批量淘汰 min_address = 0x3500 以下的冷区记录（应淘汰 0x1000, 0x2000, 0x3000 共 3 条）
  let purged = pool.purge_below(0x3500);
  assert_eq!(purged, 3, "应当精确淘汰 3 条冷区记录");
  assert_eq!(pool.drop_count(), 3);
  assert_eq!(pool.total_active_records(), 3);

  // 热区记录仍可正常复活
  let take1 = pool.take(100, 0x3500);
  assert_eq!(take1, Some((0x4000, 120)));

  let take2 = pool.take(180, 0x3500);
  assert_eq!(take2, Some((0x5000, 200)));

  let take3 = pool.take(210, 0x3500);
  assert_eq!(take3, Some((0x6000, 220)));

  assert!(pool.is_empty());
  assert_eq!(pool.hit_count(), 3);

  OK
}

/// 验证槽位因 min_address 推进失效后，新记录的 put 会覆盖替换（ReplacedExpired）
///
/// 1. 单槽分桶存入 A（InsertedEmpty），活跃计数 1；
/// 2. min_address 推进使 A 滑入失效区，再次 put B：CAS 覆盖替换该槽位，
///    活跃计数保持 1（替换而非新增）；
/// 3. 池层同路径：覆盖替换返回 true 且被替换的过期槽位计入 drop_count；
/// 4. 取出 B 后桶空，A 已随覆盖消亡，不可复活。
#[test]
fn put_replaces_expired_slot() -> Void {
  info!("> put_replaces_expired_slot [过期槽位覆盖替换审查]");

  // 1. bin 层三态状态机验证
  let bin = FreeRecordBin::with_scan_limit(64, 1, USE_FIRST_FIT);
  assert_eq!(bin.put(0x1000, 64, 0x1000), SetStatus::InsertedEmpty);
  assert_eq!(bin.len(), 1);

  // 2. min_address 推进至 0x2000，槽位 A 过期，put B 覆盖替换
  assert_eq!(bin.put(0x3000, 64, 0x2000), SetStatus::ReplacedExpired);
  assert_eq!(bin.len(), 1, "覆盖替换过期槽位不改变活跃计数");
  assert_eq!(bin.slots[0].address(), 0x3000);

  // 3. pool 层：覆盖替换计入 drop 且返回 true
  let pool = FreeRecordPool::with_bin_sizes(&[64], 1)?;
  assert!(pool.put(0x1000, 64, 0x1000));
  assert!(pool.put(0x3000, 64, 0x2000));
  assert_eq!(pool.put_count(), 2);
  assert_eq!(pool.drop_count(), 1, "被覆盖的过期槽位应计入丢弃统计");
  assert_eq!(pool.total_active_records(), 1);

  // 4. 取出 B，A 不可复活
  assert_eq!(pool.take(64, 0x2000), Some((0x3000, 64)));
  assert!(pool.is_empty());

  OK
}

/// 验证 FreeRecord 槽位高并发下的 CAS 原子性与 ABA 防御
///
/// 4 个线程频繁对同一槽位进行高频原子交换（set/get/clear），
/// 验证 48 位地址与 16 位尺寸打包解包无破坏、无 ABA 误判。
#[test]
fn free_record_cas_aba_defense() -> Void {
  info!("> free_record_cas_aba_defense [CAS ABA 与原子性校验]");

  let slot = Arc::new(FreeRecord::empty());
  let iterations = 20_000;
  let thread_count = 4;

  let mut handles = Vec::with_capacity(thread_count);
  for t in 0..thread_count {
    let slot = Arc::clone(&slot);
    handles.push(thread::spawn(move || {
      let mut my_inserts = 0usize;
      let mut my_clears = 0usize;
      for i in 1..=iterations {
        let addr = (t as u64 * 100_000) + i as u64;
        let size = (16 + (t as u32 * 8)) % 1024 + 16;
        if slot.set(addr, size, 0) == SetStatus::InsertedEmpty {
          my_inserts += 1;
          let (cur_addr, cur_size) = slot.get();
          assert_eq!(cur_addr, addr);
          assert_eq!(cur_size, size);
          slot.clear();
          my_clears += 1;
        }
      }
      (my_inserts, my_clears)
    }));
  }

  let mut total_inserts = 0;
  let mut total_clears = 0;
  for h in handles {
    let (ins, cls) = h.join().unwrap();
    total_inserts += ins;
    total_clears += cls;
  }

  assert_eq!(total_inserts, total_clears);
  assert!(total_inserts > 0);
  assert!(slot.is_empty());

  OK
}

/// 验证扫描步长上限钳位与极端参数自适应
///
/// 1. 当 scan_limit > capacity 时，自动钳位至 capacity（对标 C# FreeRecordBin.cs 钳位逻辑）；
/// 2. scan_limit = 1 时，遇 CAS 碰撞立即优雅回退 First-Fit；
/// 3. max_size 溢出 16 位时自动钳位至 FreeRecord::MAX_INLINE_SIZE。
#[test]
fn best_fit_scan_limit_clamping() -> Void {
  info!("> best_fit_scan_limit_clamping [扫描上限钳位审查]");

  // 1. 测试超出容量的 scan_limit 自动钳位
  let bin = FreeRecordBin::with_scan_limit(128, 16, usize::MAX);
  assert_eq!(
    bin.best_fit_scan_limit(),
    16,
    "scan_limit 超出容量时应被安全钳位"
  );
  assert_eq!(bin.max_size(), 128);

  // 2. 测试超出 16 位的 max_size 自动钳位
  let bin_overflow = FreeRecordBin::with_scan_limit(70_000, 16, 4);
  assert_eq!(
    bin_overflow.max_size(),
    FreeRecord::MAX_INLINE_SIZE,
    "max_size 超出 65535 时应被安全钳位"
  );

  // 3. 测试 scan_limit = 1 的极小窗口边界
  let bin_limit1 = FreeRecordBin::with_scan_limit(128, 8, 1);
  assert_eq!(bin_limit1.best_fit_scan_limit(), 1);
  assert_eq!(bin_limit1.put(0x1000, 40, 0), SetStatus::InsertedEmpty);
  assert_eq!(bin_limit1.put(0x2000, 50, 0), SetStatus::InsertedEmpty);

  let (res1, _) = bin_limit1.take_best_fit(35, 0);
  assert_eq!(res1, Some((0x1000, 40)));
  let (res2, _) = bin_limit1.take_best_fit(35, 0);
  assert_eq!(res2, Some((0x2000, 50)));
  assert!(bin_limit1.is_empty());

  OK
}

/// 验证极限边界值与池参数防御拦截
#[test]
fn boundary_parameter_defense() -> Void {
  info!("> boundary_parameter_defense [边界条件防御审查]");

  let pool = FreeRecordPool::new();

  // 1. address = 0 无效地址防御
  assert!(!pool.put(0, 64, 0));

  // 2. address < min_address 过期地址防御
  assert!(!pool.put(0x100, 64, 0x200));

  // 3. address 超出 48 位上限防御
  let overflow_addr = (1u64 << 48) + 0x1000;
  assert!(!pool.put(overflow_addr, 64, 0));

  // 4. size = 0 记录尺寸防御
  assert!(!pool.put(0x1000, 0, 0));

  // 5. size 超出 65535（16 位最大内联尺寸）防御
  assert!(!pool.put(0x1000, 65536, 0));

  // 6. 申请取出尺寸为 0 或溢出尺寸返回 None
  assert_eq!(pool.take(0, 100), None);
  assert_eq!(pool.take(65536, 100), None);
  assert_eq!(pool.take(u32::MAX, 100), None);

  // 7. FreeRecord 槽位直接防御验证
  let slot = FreeRecord::empty();
  assert_eq!(slot.set(0, 64, 0), SetStatus::Occupied);
  assert_eq!(slot.set(0x1000, 0, 0), SetStatus::Occupied);
  assert_eq!(slot.set(overflow_addr, 64, 0), SetStatus::Occupied);
  assert_eq!(slot.set(0x1000, 70_000, 0), SetStatus::Occupied);
  assert_eq!(slot.set(0x500, 64, 0x1000), SetStatus::Occupied);
  assert!(slot.is_empty());

  // 8. 成功存入与取出最大合法尺寸
  assert!(pool.put(FreeRecord::ADDRESS_MASK, FreeRecord::MAX_INLINE_SIZE, 0));
  let taken = pool.take(FreeRecord::MAX_INLINE_SIZE, 0);
  assert_eq!(
    taken,
    Some((FreeRecord::ADDRESS_MASK, FreeRecord::MAX_INLINE_SIZE))
  );

  // 9. FreeRecordPool 构建参数校验防御
  assert!(matches!(
    FreeRecordPool::with_bin_sizes(&[], 16),
    Err(Error::EmptyBinSizes)
  ));
  assert!(matches!(
    FreeRecordPool::with_bin_sizes(&[64], 0),
    Err(Error::InvalidCapacity)
  ));
  assert!(matches!(
    FreeRecordPool::with_bin_sizes(&[64, 32], 16),
    Err(Error::UnsortedBinSizes)
  ));
  assert!(matches!(
    FreeRecordPool::with_bin_sizes(&[64, 64], 16),
    Err(Error::UnsortedBinSizes)
  ));
  assert!(matches!(
    FreeRecordPool::with_bin_sizes(&[64, 70_000], 16),
    Err(Error::SizeOverflow(70_000))
  ));

  OK
}

/// 验证上层传入非单调 min_address 时池行为的安全契约
///
/// 1. take 用回退的更小 min_address：仅放宽当次过滤，此前存入的记录仍可见可取出；
/// 2. purge_below 清零后的槽位，min_address 回退亦不可复活（保守安全，容量让渡）；
/// 3. put 的 min_address 高于地址：拒绝并计 drop；
/// 4. 全程 active_count 与统计口径严格自洽，池不变量不受非单调输入破坏。
#[test]
fn non_monotonic_min_address_safety() -> Void {
  info!("> non_monotonic_min_address_safety [非单调下界安全契约]");

  let pool = FreeRecordPool::with_bin_sizes(&[64, 128], 8)?;

  // 1. 以 min_address = 0x4000 存入 0x5000
  assert!(pool.put(0x5000, 64, 0x4000));
  assert_eq!(pool.total_active_records(), 1);

  // 2. min_address 回退到 0x1000（低于前次 0x4000），地址 0x3000 仍被当次放宽接受
  assert!(pool.put(0x3000, 40, 0x1000));
  assert_eq!(pool.total_active_records(), 2);

  // 3. take 用回退的 min_address：记录仍可见（40 精确匹配命中 0x3000）
  assert_eq!(pool.take(40, 0x1000), Some((0x3000, 40)));
  assert_eq!(pool.total_active_records(), 1);

  // 4. 用更大的 min_address 清零后，回退 min_address 亦不可复活
  assert_eq!(pool.purge_below(0x6000), 1);
  assert_eq!(pool.total_active_records(), 0);
  assert_eq!(
    pool.take(64, 0x1000),
    None,
    "已清零槽位不可因 min_address 回退而复活"
  );

  // 5. put 的 min_address 高于地址：拒绝并计 drop
  assert!(!pool.put(0x8000, 64, 0x9000));
  assert_eq!(pool.total_active_records(), 0);

  // 6. 统计口径自洽：3 次 put（含 1 次拒绝）、2 次 take（1 命中 1 失败）、2 次 drop（1 拒绝 1 清理）
  assert_eq!(pool.put_count(), 3);
  assert_eq!(pool.take_count(), 2);
  assert_eq!(pool.hit_count(), 1);
  assert_eq!(pool.drop_count(), 2);
  assert_eq!(pool.stats().failed_takes(), 1);

  OK
}

/// 验证多线程频繁存入与尝试淘汰同一槽位时的竞态安全性
#[test]
fn multithread_purge_and_cas_race() -> Void {
  info!("> multithread_purge_and_cas_race [多线程淘汰与写入竞态]");

  let slot = Arc::new(FreeRecord::empty());
  let iterations = 10_000;
  let threads = 4;
  let barrier = Arc::new(Barrier::new(threads * 2));

  // 4 个线程频繁写入过期与有效记录
  let mut put_handles = Vec::new();
  for t in 0..threads {
    let slot = Arc::clone(&slot);
    let barrier = Arc::clone(&barrier);
    put_handles.push(thread::spawn(move || {
      barrier.wait();
      for i in 1..=iterations {
        let addr = (t as u64 * 10_000) + (i as u64) + 100;
        let _ = slot.set(addr, 64, 100);
      }
    }));
  }

  // 4 个线程频繁调用 try_purge_below
  let mut purge_handles = Vec::new();
  for _ in 0..threads {
    let slot = Arc::clone(&slot);
    let barrier = Arc::clone(&barrier);
    purge_handles.push(thread::spawn(move || {
      barrier.wait();
      for _ in 1..=iterations {
        let _ = slot.try_purge_below(50_000);
      }
    }));
  }

  for h in put_handles {
    h.join().unwrap();
  }
  for h in purge_handles {
    h.join().unwrap();
  }

  // 验证清理完成
  slot.clear();
  assert!(slot.is_empty());

  OK
}
