//! LimitedFixedBufferPool 分级队列借还语义测试
//!
//! 对标 C#：libs/common/Memory/LimitedFixedBufferPool.cs 的
//! 多级池契约（Get/Return 经 Position 匹配 2^i * minAllocationSize 层级、
//! 超最高层级越界分配、Purge 全层清空、GetStats 各层分布）。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs（容量上限/超限分配）

use aok::{OK, Void};
use log::info;
use wbase::pool::{DEFAULT_BUFFER_SIZE, DEFAULT_NUM_LEVELS, LimitedFixedBufferPool};

/// 阶梯各级缓冲归还后回入所属层级并二次无损复用（缺陷核心复现位：
/// 修订前 128KB/256KB/512KB 归还端严格等值判定恒伪，缓冲被就地丢弃、池空转）
#[test]
fn ladder_levels_repool_and_reuse_without_reallocation() -> Void {
  info!("以默认 4 级阶梯（64KB~512KB）逐级借出-写脏-归还-复借，验证按层级回池且零重分配");

  let pool = LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, 8);
  for size in [
    DEFAULT_BUFFER_SIZE,
    DEFAULT_BUFFER_SIZE << 1,
    DEFAULT_BUFFER_SIZE << 2,
    DEFAULT_BUFFER_SIZE << (DEFAULT_NUM_LEVELS - 1),
  ] {
    let free_before = pool.free_count();
    {
      let mut b = pool.get_ref(size);
      assert_eq!(b.capacity(), size, "{size} 请求应得精确层级规格");
      b.resize(size, 0xA5);
    }
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(
      pool.free_count(),
      free_before + 1,
      "{size} 缓冲归还必须回入其所属层级队列"
    );

    let allocated_before = pool.allocated_count();
    {
      let b = pool.get_ref(size);
      assert_eq!(
        pool.allocated_count(),
        allocated_before,
        "{size} 二次借出必须命中层级队列复用，不得再走系统堆"
      );
      assert_eq!(b.capacity(), size, "复用块规格无损（无 realloc 缩容）");
      assert!(b.is_empty(), "归还时已清零长度，复用为空白缓冲");
    }
  }

  OK
}

/// 借出低阶缓冲、倍增扩容后归还，应无损回入高阶阶梯队列供大请求复用
/// （对标 C# 网络接收缓冲 grow 场景：扩容收敛期内存块跨层级池化）
#[test]
fn grown_buffer_repools_into_higher_level() -> Void {
  info!("64KB 借出经 reserve 倍增扩容至 128KB 后归还，验证回入 1 级队列且被 128KB 请求复用");

  let pool = LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, 8);
  let mut b = pool.get_ref(DEFAULT_BUFFER_SIZE);
  b.resize(DEFAULT_BUFFER_SIZE, 0x5A);
  // 模拟读泵扩容：倍增后恰为下一层级规格（2 的幂），回池守卫按容量精确匹配
  b.reserve(DEFAULT_BUFFER_SIZE);
  assert_eq!(b.capacity(), DEFAULT_BUFFER_SIZE << 1);
  drop(b);
  assert_eq!(pool.free_count(), 1, "扩容块必须回入高一级阶梯队列");

  let allocated_before = pool.allocated_count();
  let big = pool.get_ref(DEFAULT_BUFFER_SIZE << 1);
  assert_eq!(
    pool.allocated_count(),
    allocated_before,
    "128KB 请求必须复用阶梯扩容块"
  );
  assert_eq!(big.capacity(), DEFAULT_BUFFER_SIZE << 1);

  OK
}

/// 超出最高层级与非法规格（非 2 的幂次容量）借出正常、归还就地系统释放，
/// 计数不泄漏（对标 C# Position 对越界/非幂次返回 -1 的裁断）
#[test]
fn out_of_range_and_unaligned_buffers_are_discarded() -> Void {
  info!("验证越界大缓冲与非幂次容量缓冲归还丢弃、borrowed 归零、out_of_bound 计数准确");

  let pool = LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, 8);
  let top = DEFAULT_BUFFER_SIZE << (DEFAULT_NUM_LEVELS - 1);

  {
    let b = pool.get_ref(top << 1);
    assert!(b.capacity() >= top << 1, "越界请求仍须获得足量系统缓冲");
    assert_eq!(pool.out_of_bound_allocations(), 1);
  }
  assert_eq!(pool.free_count(), 0, "越界缓冲不得挤占任何阶梯层级");
  assert_eq!(pool.borrowed_count(), 0);

  let mut b = pool.get_ref(DEFAULT_BUFFER_SIZE);
  // 手工换入非 2 的幂次容量块（3 倍层级规格）：归还端复配失败即弃
  let mut odd = Vec::with_capacity(DEFAULT_BUFFER_SIZE * 3);
  odd.resize(DEFAULT_BUFFER_SIZE * 3, 0x33);
  assert!(!odd.capacity().is_power_of_two(), "夹具须为非幂次容量");
  b.set_buffer(odd);
  drop(b);
  assert_eq!(pool.borrowed_count(), 0);
  assert_eq!(pool.free_count(), 0, "非层级规格缓冲归还必须就地释放不入池");

  OK
}

/// purge 清空全部分级队列；get_stats 输出各层级分布指标
#[test]
fn purge_clears_all_levels_and_stats_report_distribution() -> Void {
  info!("各级挂入闲置缓冲后验证 purge 全清与 get_stats 层级分布字段");

  let pool = LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, 8);
  let sizes = [
    DEFAULT_BUFFER_SIZE,
    DEFAULT_BUFFER_SIZE << 1,
    DEFAULT_BUFFER_SIZE << 2,
    DEFAULT_BUFFER_SIZE << (DEFAULT_NUM_LEVELS - 1),
  ];
  let held: Vec<_> = sizes.iter().map(|&s| pool.get_ref(s)).collect();
  assert_eq!(pool.borrowed_count(), sizes.len());
  drop(held);
  assert_eq!(pool.free_count(), sizes.len(), "四级各回一块");

  let stats = pool.get_stats();
  assert!(
    stats.contains("num_levels=4") && stats.contains("max_entries_per_level=8"),
    "统计串须含阶梯参数: {stats}"
  );
  for size in sizes {
    let arm = format!(" free_at_{}KB=1", size >> 10);
    assert!(
      stats.contains(&arm),
      "统计串须含 {size} 层级闲置分布: {stats}"
    );
  }

  pool.purge();
  assert_eq!(pool.free_count(), 0, "purge 必须清空全部分级队列");

  OK
}
