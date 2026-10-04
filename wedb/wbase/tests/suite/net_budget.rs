//! 网络缓冲预算纯语义与池集成回归
//!
//! 对标 garnet/test/standalone/Garnet.test/NetworkBufferBudgetTests.cs（PR #2157）
//! 的无套接字用例族：目标只降不升（以配置规格为顶）、按 2 的幂阶梯下行、
//! 地板绑定、迟滞带（增长须达已发布目标两倍）、计数排空后回顶、借出归还
//! 计数守恒、无预算池零参与、压力期超目标条目按自身方向对照后弃置。

use std::sync::Arc;

use wbase::pool::{
  BufferKind, LimitedFixedBufferPool, NetworkBufferBudget, NetworkBufferSettings, PooledRefBuffer,
};

/// 预算基线：顶 64K、接收地板 16K、send 地板 32K（全 2 的幂）
fn budget(budget_bytes: i64) -> NetworkBufferBudget {
  NetworkBufferBudget::new(budget_bytes, 1 << 16, 1 << 14, 1 << 15)
}

/// 持有 n 个借出句柄驱动活跃计数（对标 C# 池 Get 驱动计数路径）
fn hold(pool: &LimitedFixedBufferPool, n: usize) -> Vec<PooledRefBuffer<'_>> {
  (0..n).map(|_| pool.get_ref(1 << 16)).collect()
}

/// TargetPinsAtCeilingWhileBudgetIsSlack：连接少时预算宽松，目标恒为配置规格
#[test]
fn target_pins_at_ceiling_while_budget_slack() {
  let b = budget(1 << 30);
  assert_eq!(b.target_buffer_size(), 1 << 16);
  assert!(!b.is_under_pressure());
  // 1GB / 16 = 64MB >> 顶：计数增长仍在带外
  for _ in 0..16 {
    b.on_buffer_acquired();
  }
  assert_eq!(b.target_buffer_size(), 1 << 16);
  for _ in 0..16 {
    b.on_buffer_released();
  }
}

/// TargetStepsDownThroughPowersOfTwo：预算 ÷ 活跃数逐级压低目标（2 的幂阶梯）
#[test]
fn target_steps_down_through_powers_of_two() {
  let b = budget(256 * 1024); // 顶 64K：256K/4 = 64K 仍贴顶
  for _ in 0..4 {
    b.on_buffer_acquired();
  }
  assert_eq!(b.target_buffer_size(), 1 << 16);
  // 256K/5 = 51.2K < 64K → 下行至 32K
  b.on_buffer_acquired();
  assert_eq!(b.target_buffer_size(), 1 << 15);
  // 256K/11 = 23.3K → 16K
  for _ in 0..5 {
    b.on_buffer_acquired();
  }
  assert_eq!(b.target_buffer_size(), 1 << 14);
}

/// IncidentShapeLandsOneStepBelowConfiguredSize：商恰落顶下相邻级
#[test]
fn incident_shape_lands_one_step_below_configured_size() {
  let b = budget(96 * 1024); // 顶 64K
  for _ in 0..2 {
    b.on_buffer_acquired();
  }
  // 96K/2 = 48K → prev_pow2 = 32K（一级之下）
  assert_eq!(b.target_buffer_size(), 1 << 15);
  assert!(b.is_under_pressure());
}

/// TargetNeverFallsBelowTheFloor / SendAndReceiveFloorsAreSeparate：
/// 地板兜底且 send 地板更高；地板永不超顶
#[test]
fn floors_bind_and_never_exceed_ceiling() {
  let b = budget(1); // 极小预算：直接压到地板
  b.on_buffer_acquired();
  assert_eq!(b.target_buffer_size(), 1 << 14);
  assert_eq!(b.target_receive_buffer_size(), 1 << 14);
  assert_eq!(b.target_send_buffer_size(), 1 << 15);
  // 地板超顶时被压回顶（抬升基准是自适应绝不允许做的事）
  let clamped = NetworkBufferBudget::new(1 << 20, 1 << 14, 1 << 16, 1 << 20);
  assert_eq!(clamped.target_send_buffer_size(), 1 << 14);
  assert_eq!(clamped.target_receive_buffer_size(), 1 << 14);
}

/// DisabledBudgetAlwaysReportsTheConfiguredSize：0 字节预算全惰性、钳制恒等
#[test]
fn disabled_budget_is_inert() {
  let b = NetworkBufferBudget::new(0, 1 << 16, 1 << 14, 1 << 15);
  assert!(!b.is_enabled());
  assert!(!b.is_under_pressure());
  for _ in 0..100 {
    b.on_buffer_acquired();
  }
  assert_eq!(b.target_buffer_size(), 1 << 16);
  assert_eq!(b.clamp_receive_buffer_size(1 << 16), 1 << 16);
  assert_eq!(b.clamp_send_buffer_size(1 << 16), 1 << 16);
  // 禁用形态不计数收缩
  b.record_pressure_shrink();
  b.record_idle_shrink();
  assert_eq!(b.pressure_shrinks(), 0);
  assert_eq!(b.idle_shrinks(), 0);
}

/// GrowthRequiresTwiceThePublishedTarget：迟滞带——商低于目标即收缩，
/// 增长须达已发布目标两倍（更窄的带会在相邻规格间无限振荡）
#[test]
fn growth_requires_twice_the_published_target() {
  let b = budget(256 * 1024);
  for _ in 0..5 {
    b.on_buffer_acquired();
  }
  assert_eq!(b.target_buffer_size(), 1 << 15);
  // 排空至商 = 64K（= 目标 2 倍阈值之上）：256K/4 = 64K ≥ 2×32K → 回顶
  b.on_buffer_released();
  assert_eq!(b.target_buffer_size(), 1 << 16);
}

/// TargetReturnsToTheCeilingWhenConnectionsDrain：峰值后排空即恢复顶
///（对标「Recompute 在计数自身的增减内重算」修正——只挂 allocate-miss 路径
/// 时池化复用永不触达，峰值后排空目标卡死地板）
#[test]
fn target_returns_to_ceiling_when_connections_drain() {
  let b = budget(256 * 1024);
  // 驱入压力：256K/16 = 16K → 目标踩到接收地板
  for _ in 0..16 {
    b.on_buffer_acquired();
  }
  assert_eq!(b.target_buffer_size(), 1 << 14);
  assert!(b.is_under_pressure());
  // 排空：商回到全额预算 → 目标回升至顶、压力解除
  for _ in 0..16 {
    b.on_buffer_released();
  }
  assert_eq!(b.target_buffer_size(), 1 << 16);
  assert!(!b.is_under_pressure());
}

/// EveryCountChangeRepublishesBeforeTheCallerCanObserveIt：
/// 每次借出/归还后目标与计数一致（对任意计数 target_for_count 同式可验）
#[test]
fn every_count_change_republishes() {
  let b = budget(1 << 20);
  for n in 1..=40i64 {
    b.on_buffer_acquired();
    assert_eq!(b.target_buffer_size(), b.target_for_count(n), "计数 {n}");
  }
  for n in (1..=40i64).rev() {
    b.on_buffer_released();
    assert_eq!(b.target_buffer_size(), b.target_for_count(n - 1));
  }
}

/// TargetForCount 纯函数（C# TargetForCount：配置的纯函数，无共享态）
#[test]
fn target_for_count_is_pure_config_function() {
  let b = budget(1 << 22);
  assert_eq!(b.target_for_count(0), 1 << 16); // 除 1 兜底
  assert_eq!(b.target_for_count(1), 1 << 16);
  assert_eq!(b.target_for_count(63), 1 << 16); // 4M/63≈66K → 顶
  assert_eq!(b.target_for_count(65), 1 << 15); // 4M/65≈64.1K → prev_pow2=32K
  assert_eq!(b.target_for_count(300), 1 << 14); // 4M/300≈13.9K → 16K 地板
}

/// PoolGetAndReturnConserveTheLiveBufferCount：池借出/归还守恒活跃计数；
/// PoolsWithoutABudgetDoNotAffectIt：无预算池零参与
#[test]
fn pool_get_return_conserve_live_count() {
  let b = Arc::new(budget(1 << 30));
  let settings = NetworkBufferSettings {
    min_allocation_size: 1 << 14,
    ..NetworkBufferSettings::default()
  };
  let pool = settings.create_buffer_pool(0, 0, Some(Arc::clone(&b)));
  let guards = hold(&pool, 7);
  assert_eq!(b.live_buffer_count(), 7);
  drop(guards);
  assert_eq!(b.live_buffer_count(), 0);

  // 无预算池：借还零参与（None 即 Disabled 的惰性形态）
  let plain = LimitedFixedBufferPool::new(1 << 16, 0);
  let g = plain.get_ref(1 << 16);
  assert_eq!(b.live_buffer_count(), 0);
  drop(g);
  assert_eq!(b.live_buffer_count(), 0);
}

/// OverTargetBuffersAreNotPooledWhileTheBudgetIsBinding +
/// UnderPressureAnEntryIsComparedAgainstTheTargetForItsOwnDirection：
/// 压力期超目标条目弃置不回池；send 条目对照 send 目标（规格正确的 send
/// 块不被误判超目标而不可回池）
#[test]
fn under_pressure_over_target_return_uses_own_direction() {
  let b = Arc::new(budget(1)); // 极小预算：必达压力、目标=地板
  let settings = NetworkBufferSettings {
    send_buffer_size: 1 << 15,
    initial_receive_buffer_size: 1 << 15,
    min_allocation_size: 1 << 14,
    ..NetworkBufferSettings::default()
  };
  let pool = settings.create_buffer_pool(0, 0, Some(Arc::clone(&b)));

  // 驱动入压（保持若干借出令目标落到地板）
  let keep = hold(&pool, 2);
  assert!(b.is_under_pressure());

  // 超目标 receive 块（64K > 16K 地板）：归还即弃置，不占空闲链
  {
    let big = pool.get_ref(1 << 16);
    drop(big);
    assert_eq!(pool.free_count(), 0, "压力期 64K receive 块须弃置");
  }
  // 规格恰为 send 地板的 send 块（32K == send 目标）：照常回池
  {
    let send = pool.get_ref_kind(1 << 15, BufferKind::Send);
    drop(send);
    assert!(
      pool.free_count() >= 1,
      "send 块对照 send 目标，恰规格块须可回池"
    );
  }
  // 非压力形态（预算宽松）：超规格 grown 块照常回池复用（Unpressured-
  // ConnectionChurnKeepsRecyclingGrownBuffers 的池面臂）
  let slack = Arc::new(budget(1 << 30));
  let settings2 = NetworkBufferSettings {
    send_buffer_size: 1 << 15,
    initial_receive_buffer_size: 1 << 15,
    min_allocation_size: 1 << 14,
    ..NetworkBufferSettings::default()
  };
  let pool2 = settings2.create_buffer_pool(0, 0, Some(Arc::clone(&slack)));
  assert!(!slack.is_under_pressure());
  {
    let big = pool2.get_ref(1 << 16);
    drop(big);
    assert!(pool2.free_count() >= 1, "非压力期 grown 块须回池复用");
  }
  drop(keep);
}

/// 字节记账基础：live/peak/pooled 对称（借出活跃、归还入空闲或弃置）
#[test]
fn byte_accounting_tracks_live_and_pooled() {
  let pool = LimitedFixedBufferPool::new(1 << 16, 0);
  {
    let buf = pool.get_ref(1 << 16);
    assert_eq!(pool.live_bytes(), buf.capacity() as i64);
    assert!(pool.peak_live_bytes() >= pool.live_bytes());
  }
  // 归还即入空闲链：live 归零、pooled 记层级规格
  assert_eq!(pool.live_bytes(), 0);
  assert_eq!(pool.pooled_bytes(), (1 << 16) as i64);
  assert!(pool.free_count() == 1);
  // Purge 清空闲并回收记账
  pool.purge();
  assert_eq!(pool.pooled_bytes(), 0);
  assert_eq!(pool.free_count(), 0);
}

/// 闲置字节上限收紧：超限块不入池且无记账残留（pooled_bytes 以加后新值
/// 判定，C# LimitedFixedBufferPool.Return 的 Add-后-查语义）
#[test]
fn pooled_byte_ceiling_rejects_oversized_idle_entries() {
  let settings = NetworkBufferSettings {
    min_allocation_size: 1 << 14,
    ..NetworkBufferSettings::default()
  };
  // 上限 24K：首块 16K 入池、次块 16K 超限弃置
  let pool = settings.create_buffer_pool(0, 24 * 1024, None);
  drop(pool.get_ref(1 << 14));
  assert_eq!(pool.pooled_bytes(), 1 << 14);
  assert_eq!(pool.free_count(), 1);
  drop(pool.get_ref(1 << 15));
  assert_eq!(
    pool.free_count(),
    1,
    "32K 块超 24K 闲置上限须弃置（新增后 48K > 24K）"
  );
  assert_eq!(
    pool.pooled_bytes(),
    1 << 14,
    "弃置臂不得残留字节记账（曾以加前旧值误判并回滚出负）"
  );
}

/// INFO 统计片段含全字段（BPSTATS 消费面）
#[test]
fn budget_stats_renders_all_fields() {
  let b = budget(1 << 20);
  let stats = b.get_stats();
  for key in [
    "budget_bytes=",
    "target_buffer_size=",
    "target_send_buffer_size=",
    "live_buffer_count=",
    "pressure_shrinks=",
    "idle_shrinks=",
  ] {
    assert!(stats.contains(key), "缺统计字段 {key}: {stats}");
  }
}
