//! Token 版本地板单调性集成测试（墙钟回拨与跨进程重启时钟滞后的签发钳制面）
//!
//! wnode 以 `token >> 64` 投影检查点/AOF 版本号（`checkpoint_version` 高位分支），
//! 对标 C# `VersionChangeSM.NextState` 的 `nextState.Version = start.Version + 1`
//! 严格代际推进。本套件经公开签发口 [`wcpr::next_token_above`] 构造「目录内历史
//! 检查点高位远超当前墙钟读数」的跨进程时钟滞后场景（候选形态与墙钟回拨同构：
//! 候选高位落在本进程历史与目录下界之下），断言连续签发的高位逐次严格递增——
//! 版本号跨代停滞即恢复基线与前代相等、AOF 版本闸（IsOldVersionRecord）对已固化
//! 旧代条目恒判 false、非幂等命令重复重放的根因。
//!
//! 断言全部取相对序：签发闸门为进程级全局态（`LAST_TOKEN`），与同二进制内其他
//! 用例并行运行时不假设任何绝对值。

use wcpr::{candidate_token, issue_token_after, next_token_above, reset_last_token_for_test};

/// 跨进程重启时钟滞后：目录现存最大 Token（floor）高位高于当前墙钟读数时，
/// 签发值必须严格大于 floor，且高 64 位（版本投影域）必须超越 floor 高位
#[test]
fn token_version_floor_clamped_above_stale_clock_dir_floor() {
  // 模拟历史目录 floor：高位取远超当前墙钟纳秒量级（~1.8e18 @2026）、仍留充足
  // 递增余地的值（时钟回拨/滞后后的目录形态；刻意不取 u64 边界，钳制续发不挤占
  // 同进程后续用例）
  let floor_hi: u64 = 8_000_000_000_000_000_000;
  let floor = ((floor_hi as u128) << 64) | 42;

  let t1 = next_token_above(floor);
  assert!(
    t1 > floor,
    "签发值必须严格大于目录下界: floor {floor:#x} -> {t1:#x}"
  );
  assert!(
    t1 >> 64 > floor >> 64,
    "签发高位必须超越 floor 高位（版本投影域跨代推进）: {:#x} -> {:#x}",
    floor >> 64,
    t1 >> 64
  );
}

/// 连续两次签发（连续两代检查点）的版本投影域必须逐次严格递增：
/// 停滞即恢复基线与前代相等，AOF 旧代条目无法被版本闸跳过而重复重放
#[test]
fn consecutive_tokens_project_strictly_increasing_versions() {
  let floor_hi: u64 = 8_000_000_000_000_000_000;
  let floor = ((floor_hi as u128) << 64) | 42;

  let t1 = next_token_above(floor);
  let t2 = next_token_above(floor);
  assert!(t2 > t1, "连续签发必须严格递增: {t1:#x} -> {t2:#x}");
  assert!(
    t2 >> 64 > t1 >> 64,
    "连续签发的高位必须逐次严格递增（checkpoint_version 跨代单调）: {:#x} -> {:#x}",
    t1 >> 64,
    t2 >> 64
  );
}

/// Token 签发闸门（自 src/manager/mod.rs 迁出）：墙钟回拨续发、目录下界钳制与两者叠加均保持签发值严格递增，
/// 且高 64 位（wnode `checkpoint_version` 的版本投影域）逐次严格递增
///
/// 单测进程独占全局闸门状态，断言仅依赖自身签发值的相对序，对并发测试无时序敏感
#[test]
fn token_gate_monotonic_under_rollback_and_floor() {
  reset_last_token_for_test();
  let a = issue_token_after(candidate_token(), 0);
  let b = issue_token_after(a.saturating_sub(1), 0);
  assert!(b > a, "回拨候选必须续发: {a} -> {b}");
  assert!(
    b >> 64 > a >> 64,
    "回拨续发必须抬升高位版本投影域: {a} -> {b}"
  );
  let c = issue_token_after(0, b);
  assert!(c > b, "目录下界必须抬升签发值: {b} -> {c}");
  assert!(c >> 64 > b >> 64, "目录下界钳制必须抬升高位: {b} -> {c}");
  let d = issue_token_after(b, c);
  assert!(d > c, "叠加路径必须续发: {c} -> {d}");
  assert!(d >> 64 > c >> 64, "叠加路径必须抬升高位: {c} -> {d}");
  let e = issue_token_after(candidate_token(), 0);
  assert!(e > d, "常规签发必须严格递增: {d} -> {e}");
  assert!(e >> 64 > d >> 64, "常规签发高位必须严格递增: {d} -> {e}");
  let f = issue_token_after(u128::MAX - 5, 0);
  let g = issue_token_after(candidate_token(), 0);
  assert!(f > e && g > f, "高位候选后仍须递增: {e} -> {f} -> {g}");
  reset_last_token_for_test();
}
