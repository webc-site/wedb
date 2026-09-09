//! BufferPool 字节预算、bypass 与池关闭拆除测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs 的
//! `BudgetBoundsReusableBytesAndReturnsToZero`、`SmallBudgetIsolatedFromLargeExhaustion`、
//! `OversizeRequestBypassesCacheAndHoldsNoBudget`、`DisabledPoolServesUncachedBuffersAndHoldsNoBudget`、
//! `FreeIsIdempotent`、`ReturnOfInFlightBufferAfterFreeIsSafe`、`FreeReleasesBuffersCachedOnOtherThreads`。

use std::{sync::mpsc::channel, thread};

use aok::{OK, Void};
use log::info;
use wram::{
  BufferPool, DEFAULT_LARGE_BUDGET_BYTES, DEFAULT_SECTOR_SIZE, DEFAULT_SMALL_BUDGET_BYTES,
  LARGE_TIER_MIN_BYTES, MAX_POOLED_SECTORS, MIN_SECTOR_SIZE, NUM_CLASSES, Result,
  class_capacity_bytes, class_of_sectors,
};

/// 预算上限约束可缓存字节数，池关闭后配额严格归零
#[test]
fn budget_bounds_reusable_bytes_and_returns_to_zero() -> Void {
  info!("对标 BudgetBoundsReusableBytesAndReturnsToZero：64KB 小预算下预留不越界，Free 后归零");

  let small_budget = 64 * 1024; // 64 KB
  let pool = BufferPool::with_budgets(DEFAULT_SECTOR_SIZE, small_budget, 1024 * 1024)?;

  // 256 个 4KB 缓冲远超小预算：仅前若干个可缓存，其余以非缓存方式签发
  let mut held = Vec::new();
  for _ in 0..256 {
    held.push(pool.get_with_policy(4096, false)?);
  }

  assert!(
    pool.small_reserved_bytes() <= small_budget,
    "预留小缓冲配额 {} 严禁超过上限 {small_budget}",
    pool.small_reserved_bytes()
  );

  drop(held);
  pool.free();

  assert_eq!(
    pool.reserved_bytes(),
    0,
    "池关闭清空后所有预留配额必须严格归零"
  );

  OK
}

/// 大额预算耗尽不得挤占小额预算：双层预算强隔离
#[test]
fn small_budget_isolated_from_large_exhaustion() -> Void {
  info!("对标 SmallBudgetIsolatedFromLargeExhaustion：灌满大缓冲后小缓冲仍从独立配额缓存");

  // 对标 C# 8MB 总预算拆分为 2MB 小 / 6MB 大：Rust 直接显式给定双层预算
  let pool = BufferPool::with_budgets(MIN_SECTOR_SIZE, 2 << 20, 6 << 20)?;

  // 分层判定：2MB 是大 class，4KB 是小 class
  let large_cls = class_of_sectors((2 * 1024 * 1024) / MIN_SECTOR_SIZE).expect("2MB 必有 class");
  let small_cls = class_of_sectors(4096 / MIN_SECTOR_SIZE).expect("4KB 必有 class");
  assert!(
    class_capacity_bytes(large_cls, MIN_SECTOR_SIZE) > LARGE_TIER_MIN_BYTES,
    "2MB 必须落入大额预算层"
  );
  assert!(
    class_capacity_bytes(small_cls, MIN_SECTOR_SIZE) <= LARGE_TIER_MIN_BYTES,
    "4KB 必须落入小额预算层"
  );

  // 用 32 个存活 2MB 大缓冲灌满大额子预算（每个存活期间持有许可）
  let large = (0..32)
    .map(|_| pool.get_with_policy(2 * 1024 * 1024, false))
    .collect::<Result<Vec<_>>>()?;

  assert!(
    pool.large_reserved_bytes() <= 6 << 20,
    "大额预留必须被大额子预算约束"
  );
  assert_eq!(pool.small_reserved_bytes(), 0, "此时不得有小缓冲缓存");

  // 大额耗尽时，小缓冲仍须从独立小圈子预算缓存
  let small = (0..16)
    .map(|_| pool.get_with_policy(4096, false))
    .collect::<Result<Vec<_>>>()?;
  drop(small);

  assert!(
    pool.small_reserved_bytes() > 0,
    "大额耗尽时小缓冲必须仍能从小额子预算缓存"
  );
  assert!(
    pool.small_reserved_bytes() <= 2 << 20,
    "小额预留必须被小额子预算约束"
  );

  drop(large);
  pool.free();
  assert_eq!(pool.reserved_bytes(), 0, "Free 后配额必须归零");

  OK
}

/// 超出可池化上限的请求走 bypass 直配：不入池、不占预算
#[test]
fn oversize_request_bypasses_cache_and_holds_no_budget() -> Void {
  info!("对标 OversizeRequestBypassesCacheAndHoldsNoBudget：超界请求精确容量、零缓存、零许可");

  let pool = BufferPool::new(MIN_SECTOR_SIZE)?;
  let over_cap = (MAX_POOLED_SECTORS + 1) * MIN_SECTOR_SIZE;

  let mut first = pool.get_with_policy(over_cap, false)?;
  assert_eq!(
    first.as_buf_ptr() as usize % MIN_SECTOR_SIZE,
    0,
    "bypass 指针必须扇区对齐"
  );
  assert!(first.capacity() >= over_cap, "容量必须覆盖超界请求");

  // 两个在途 bypass 缓冲必然是不同分配（不复用）
  let second = pool.get_with_policy(over_cap, false)?;
  assert_ne!(first.as_buf_ptr(), second.as_buf_ptr(), "超界缓冲不得复用");

  // 触达最后一个可用字节
  first.as_allocated_slice_mut()[over_cap - 1] = 0xEE;
  drop((first, second));

  for c in 0..NUM_CLASSES {
    assert_eq!(pool.cached_len(c), 0, "bypass 归还不得入池");
  }
  assert_eq!(pool.reserved_bytes(), 0, "超界缓冲不得消耗字节预算");

  // bypass 直配可观测：次数与字节数精确计入独立计数 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:BypassAllocs)
  let stats = pool.stats();
  assert_eq!(
    stats.bypass_alloc_count, 2,
    "两次超界分配必须计入 bypass 计数"
  );
  assert_eq!(stats.bypass_alloc_bytes, 2 * over_cap as u64);
  assert_eq!(
    stats.direct_alloc_count, 0,
    "bypass 与预算耗尽显式直配是两类独立观测口径"
  );

  OK
}

/// 未对齐超界请求 bypass 直配：容量精确按扇区向上取整
#[test]
fn unaligned_oversize_bypass_rounds_exactly_to_sector() -> Void {
  info!("验证未对齐超界请求的 bypass 容量精确取整且不持有预算许可");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let req = (MAX_POOLED_SECTORS + 1) * DEFAULT_SECTOR_SIZE + 123;
  let buf = pool.get(req)?;

  let rounded = (req + DEFAULT_SECTOR_SIZE - 1) & !(DEFAULT_SECTOR_SIZE - 1);
  assert_eq!(buf.capacity(), rounded, "bypass 容量必须精确扇区取整");
  assert_eq!(
    buf.as_buf_ptr() as usize % DEFAULT_SECTOR_SIZE,
    0,
    "bypass 指针必须扇区对齐"
  );
  assert!(buf.required_len() >= req);

  drop(buf);
  assert_eq!(pool.reserved_bytes(), 0, "bypass 缓冲不持有预算许可");

  OK
}

/// 池关闭后 Get 走 bypass 直配、归还即释放且不入缓存
#[test]
fn closed_pool_serves_uncached_buffers_and_holds_no_budget() -> Void {
  info!("对标 DisabledPoolServesUncachedBuffersAndHoldsNoBudget：以 free() 关闭态等价验证直配语义");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let cls = class_of_sectors(1).expect("4096 字节 = 1 扇区必有 class");

  // 关闭前先产生一个缓存
  drop(pool.get(DEFAULT_SECTOR_SIZE)?);
  assert!(pool.cached_len(cls) > 0);

  pool.free();
  assert!(pool.is_closed());

  // 关闭后分配仍然成功，但缓冲不再携带池归属，归还直接释放
  let buf = pool.get(DEFAULT_SECTOR_SIZE)?;
  assert!(buf.capacity() >= DEFAULT_SECTOR_SIZE);
  assert!(buf.is_ptr_aligned());
  assert!(
    buf.clear_on_return(),
    "非池化独立分配的归还清零策略恒为 true"
  );
  let ptr_val = buf.as_buf_ptr() as usize;
  drop(buf);

  assert_eq!(pool.cached_len(cls), 0, "关闭后归还不得入池");
  assert_eq!(pool.reserved_bytes(), 0, "关闭后归还必须立即释放预算许可");
  assert!(ptr_val > 0);
  assert_eq!(
    pool.stats().bypass_alloc_count,
    1,
    "关闭态直配对标 C# Disabled 路径，必须计入 bypass 计数"
  );

  OK
}

/// 池 Free 幂等：重复关闭不得 panic 或破坏状态
#[test]
fn free_is_idempotent() -> Void {
  info!("对标 FreeIsIdempotent：二次 Free 必须无副作用");

  let pool = BufferPool::new(MIN_SECTOR_SIZE)?;
  drop(pool.get_with_policy(4096, false)?);

  pool.free();
  pool.free();

  assert!(pool.is_closed());
  assert_eq!(pool.reserved_bytes(), 0);

  OK
}

/// 池关闭后在途缓冲区（本地/大容量/跨线程）安全归还且配额归零
#[test]
fn return_of_in_flight_buffer_after_free_is_safe() -> Void {
  info!("对标 ReturnOfInFlightBufferAfterFreeIsSafe：关闭后在途缓冲归还零异常且配额归零");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let small = pool.get_with_policy(4096, false)?; // 小 class => 本地路径
  let large = pool.get_with_policy(1024 * 1024, false)?; // 大 class => Depot 路径
  let foreign = pool.get_with_policy(4096, false)?;

  // 在三个在途缓冲存活时显式关闭池
  pool.free();

  // 本地同源归还在途缓冲（直接释放，不入 TLS/Depot）
  drop(small);
  drop(large);

  // 跨线程归还在途缓冲
  thread::spawn(move || drop(foreign))
    .join()
    .expect("跨线程归还执行成功");

  assert_eq!(
    pool.reserved_bytes(),
    0,
    "在途缓冲全部归还后预留配额必须严格归零"
  );

  OK
}

/// 从其他线程归还缓存缓冲：Rust 以 TLS RAII 在线程退出时确定性回收许可
#[test]
fn free_releases_buffers_cached_on_other_threads() -> Void {
  info!("对标 FreeReleasesBuffersCachedOnOtherThreads：其他线程 TLS 缓存在其退出时确定性释放");

  let pool = BufferPool::new(MIN_SECTOR_SIZE)?;
  let (populated_tx, populated_rx) = channel::<()>();
  let (release_tx, release_rx) = channel::<()>();

  // 长生命周期工作线程：在自己 TLS 私有栈驻留 8 个缓冲后待命
  let p = pool.clone();
  let worker = thread::spawn(move || {
    for _ in 0..8 {
      drop(p.get_with_policy(4096, false).expect("租借成功"));
    }
    populated_tx.send(()).expect("通知成功");
    release_rx.recv().expect("等待释放信号");
  });
  populated_rx.recv().expect("等待缓存就绪");

  // 由不同线程执行 Free：仅清空调用方 TLS 与 Depot，工作线程 TLS 仍持有许可
  pool.free();
  assert!(
    pool.reserved_bytes() > 0,
    "Rust 语义差异：存活工作线程 TLS 中的缓存在其退出前仍持有许可"
  );

  // 工作线程退出：TLS RAII 将缓存归还已关闭的池路径，许可全部释放
  release_tx.send(()).expect("发送成功");
  worker.join().expect("工作线程退出成功");

  assert_eq!(pool.reserved_bytes(), 0, "工作线程退出后许可必须全部回收");

  OK
}

/// 预算耗尽显式直通可观测：直配计数与字节随耗尽分配增长（容量配错诊断入口）
#[test]
fn stats_reports_direct_allocations_on_budget_exhaustion() -> Void {
  info!("验证预算耗尽显式直通计入 stats() 直配计数与字节，且不改变分配语义");

  // 极小预算：仅许可 1 个 4KB 缓存，其余分配必然显式直配
  let tiny_budget = 4 * 1024;
  let pool = BufferPool::with_budgets(DEFAULT_SECTOR_SIZE, tiny_budget, tiny_budget)?;

  let mut held = Vec::new();
  for _ in 0..16 {
    held.push(pool.get_with_policy(DEFAULT_SECTOR_SIZE, false)?);
  }

  let stats = pool.stats();
  assert_eq!(
    stats.direct_alloc_count, 15,
    "首个分配持有预算许可，其余 15 个必须计入直配计数"
  );
  assert_eq!(
    stats.direct_alloc_bytes,
    stats.direct_alloc_count * DEFAULT_SECTOR_SIZE as u64,
    "直配字节必须等于计数 × class 容量"
  );
  assert_eq!(
    stats.reserved_bytes, stats.small_reserved_bytes,
    "本次全部为小 class 分配，总预留须与小额预留一致"
  );

  // 直配缓冲 cacheable=false：归还即物理释放，许可不被释放也不入池
  drop(held);
  assert_eq!(
    pool.reserved_bytes(),
    tiny_budget,
    "直配归还不得影响许可口径"
  );

  pool.free();
  assert_eq!(pool.stats().reserved_bytes, 0, "关闭后预留归零");

  OK
}

/// 超大扇区大小的池创建必须报错（预算记账 i64 溢出防御）
#[test]
fn with_budgets_rejects_sector_size_overflowing_budget_accounting() -> Void {
  info!("验证扇区大小超出 i64 预算安全乘积上界时 with_budgets 拒绝创建");

  assert!(
    BufferPool::with_budgets(
      1 << 54,
      DEFAULT_SMALL_BUDGET_BYTES,
      DEFAULT_LARGE_BUDGET_BYTES
    )
    .is_err(),
    "最大 class 容量 × 扇区大小超出 i64 记账安全范围必须报错"
  );

  OK
}

/// 负数预算必须在创建期即被拒绝，杜绝池静默退化为全直配
#[test]
fn with_budgets_rejects_negative_budget() -> Void {
  info!("验证负数 small/large 预算创建报 InvalidBudget");

  assert!(
    BufferPool::with_budgets(DEFAULT_SECTOR_SIZE, -1, DEFAULT_LARGE_BUDGET_BYTES).is_err(),
    "负数小预算必须报错"
  );
  assert!(
    BufferPool::with_budgets(DEFAULT_SECTOR_SIZE, DEFAULT_SMALL_BUDGET_BYTES, -8).is_err(),
    "负数大预算必须报错"
  );
  // 零预算合法：等价于禁用缓存，全部走显式直配
  assert!(BufferPool::with_budgets(DEFAULT_SECTOR_SIZE, 0, 0).is_ok());

  OK
}
