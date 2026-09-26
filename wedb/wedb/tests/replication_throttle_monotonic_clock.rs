//! 复制节流门与主从同步时间戳的时间域回归测试
//!
//! 对标 C# `ReplicationManager.cs`：`UpdateLastPrimarySyncTime`/`LastPrimarySyncSeconds`
//! 走 `Stopwatch`（:38-40），`EnsureReplication` 间隔门与消费推进走
//! `Environment.TickCount64`（:194/:251-256），全部落在单调时钟域，NTP/手动调时的
//! 墙钟回拨不得使其冻结或钳 0。
//!
//! 判别原理与取舍：本仓 `wbase::time` 分两域——`now_ms()` 取 SystemTime 实时墙钟
//! （纪元毫秒，量级 ~1.7e12），`now_ms_i64()` 取 Instant 单调域（进程锚点毫秒，
//! 测试内极小）。真实 NTP 回拨无法在单测内触发（不可控改系统钟），故以「四处时间源
//! 的读/写值是否与 `now_ms_i64()` 同域」作为回拨免疫性的等价判据：只要 `last` 记录恒
//! 落在单调域、`ensure_replication_due` 的 `now` 也取单调域，则墙钟回拨永不使
//! `now < last`，间隔门恒按真实经过时长推进、`last_primary_sync_seconds` 恒反映真实
//! 静默秒数，不会被 `saturating_sub` 钳 0 冻结。误用墙钟时，纪元量级读数与单调量级
//! 记录值相差万亿级，下述断言即红。

use std::sync::atomic::Ordering;

use wbase::time::{now_ms, now_ms_i64};
use wedb::server::replication::replication_manager::ReplicationManager;

/// 两域量级差距下界（纪元毫秒 − 测试进程内单调毫秒）：证明 `stored <= now_ms_i64()`
/// 这类同域判据有效，且反向注入（换回 `now_ms()`）必跨越此界。
const DOMAIN_GAP_FLOOR_MS: i64 = 1_000_000_000_000;

/// 相邻两次单调读数的最大容差（微秒级执行，留 60s 宽裕以适配高负载 CI）
const SAME_DOMAIN_SLACK_MS: i64 = 60_000;

#[test]
fn two_time_domains_differ_by_epoch_magnitude() {
  // 前提自证：墙钟纪元毫秒远大于进程内单调毫秒，域判据才有鉴别力
  let delta = now_ms() as i64 - now_ms_i64();
  assert!(
    delta > DOMAIN_GAP_FLOOR_MS,
    "墙钟 now_ms 应处纪元量级、显著大于单调 now_ms_i64，实差 {delta}ms"
  );
}

#[test]
fn primary_sync_timestamp_is_monotonic_domain() {
  let rm = ReplicationManager::new();
  rm.update_last_primary_sync_time();
  let stored = rm.primary_sync_last_timestamp.load(Ordering::Acquire);
  let mono = now_ms_i64();
  // 0 哨兵语义保持：更新后恒 > 0
  assert!(
    stored > 0,
    "update_last_primary_sync_time 后记录值应 > 0，实得 {stored}"
  );
  // 域判别：记录值必与当下单调读数同域。误用墙钟时 stored ≈ 纪元毫秒 >> mono，
  // 使 stored <= mono 直接失败。
  assert!(
    stored <= mono,
    "记录值 {stored} 须落在单调域（<= now_ms_i64 {mono}），墙钟污染则远超"
  );
  assert!(
    mono - stored < SAME_DOMAIN_SLACK_MS,
    "记录值与单调当下差 {}ms，应处同域毫秒级",
    mono - stored
  );
}

#[test]
fn last_primary_sync_seconds_reports_true_elapsed_under_monotonic_domain() {
  use wedb::server::replication::recovery_status::RecoveryStatus;

  let rm = ReplicationManager::new();
  // 1. 稳定态（!is_recovering）：即使时间戳非零，也恒回 0
  rm.primary_sync_last_timestamp
    .store(now_ms_i64() - 5_000, Ordering::Release);
  assert_eq!(
    rm.last_primary_sync_seconds(),
    0,
    "非恢复态（稳定态）恒回 0"
  );

  // 2. 恢复态（is_recovering）：反映单调域真实流逝秒数（约 5 秒）
  assert!(rm.begin_recovery(RecoveryStatus::InitializeRecover, false));
  let secs = rm.last_primary_sync_seconds();
  assert!(
    (4..=6).contains(&secs),
    "恢复态静默秒数须反映单调域真实经过（约 5s），实得 {secs}s；墙钟污染则畸大"
  );

  // 3. 恢复结束（NoRecovery）：切回恒 0
  rm.end_recovery(RecoveryStatus::NoRecovery, false);
  assert_eq!(rm.last_primary_sync_seconds(), 0, "恢复结束后恒回 0");
}

#[test]
fn ensure_replication_window_cas_writes_monotonic_domain() {
  let rm = ReplicationManager::new();
  let observed = rm
    .last_ensure_replication_attempt_ms
    .load(Ordering::Acquire);
  assert_eq!(observed, 0, "初始哨兵应为 0");
  assert!(
    rm.try_consume_ensure_replication_window(observed),
    "首次 CAS 应以 0 为基准成功推进"
  );
  let advanced = rm
    .last_ensure_replication_attempt_ms
    .load(Ordering::Acquire);
  let mono = now_ms_i64();
  assert!(
    advanced > 0 && advanced <= mono,
    "CAS 写入 {advanced} 须落在单调域 (0, now_ms_i64 {mono}]"
  );
  assert!(
    mono - advanced < SAME_DOMAIN_SLACK_MS,
    "CAS 写入与单调当下差 {}ms，应处同域毫秒级",
    mono - advanced
  );
}

#[test]
fn ensure_replication_gate_reads_monotonic_domain() {
  let rm = ReplicationManager::new();
  let interval_secs: i64 = 3600;
  // 在单调域标记「刚刚尝试过」：修复态下间隔门取的 now 亦为单调域，与 last 同域，
  // 1h 窗口内判未到期（None）。误用墙钟时 now 携带纪元量级 >> last（单调小值），
  // 差值远超窗口，被错误判为到期（Some），反向注入下此断言红。
  rm.last_ensure_replication_attempt_ms
    .store(now_ms_i64(), Ordering::Release);
  assert_eq!(
    rm.ensure_replication_due(interval_secs),
    None,
    "刚尝试过则 1h 窗口内不应到期；墙钟污染会使纪元级 now 远大于单调 last 而误判到期"
  );

  // 正对照：把 last 回填到窗口之外（单调域过去值），到期应放行返回 Some。
  rm.last_ensure_replication_attempt_ms.store(
    now_ms_i64() - (interval_secs * 1000 + 1_000),
    Ordering::Release,
  );
  assert!(
    rm.ensure_replication_due(interval_secs).is_some(),
    "窗口已过应按单调差值到期放行"
  );
}
