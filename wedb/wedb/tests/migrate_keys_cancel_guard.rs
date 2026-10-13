#![recursion_limit = "256"]
//! KEYS 迁移驱动取消安全守卫回归锁面（KeysDriverGuard 双臂）
//!
//! 背景：KEYS 同步形态挂慢路径（cluster_session/migrate.rs pending_slow），
//! 发起客户端断连/CLIENT KILL/停机广播即被网络泵 RaceEnd::Disposed 丢弃
//! future（future drop 即取消），裸顺序收尾（keys.rs finally
//! TryRemoveMigrationTask）不可达——sketch 滞留 Transmitting/Deleting 源端
//! 键级写门关闭（默认 cluster_node_timeout 后转 ASK 写失败）、任务表槽位
//! 泄漏（同槽再迁移恒 IOERR）、远端自动下发的 IMPORTING 无 recover 回滚
//! （目标端该槽持续 CLUSTERDOWN）、session.status 恒 Pending。
//!
//! C# 基线：BlockingWait 阻塞网络线程，断连不取消迁移；finally
//! TryRemoveMigrationTask + recover 必达。守卫即该必达面的 rust 投影。
//!
//! 锁面（revert-proof：卸下守卫或摘空 abandon_migration_session 任一清理件
//! 即对应用例转红）：
//! - 取消臂同步面：直 drop 守卫对象（泵 Disposed 丢弃 future 的同构收口）
//!   → 任务表清零、终态 Fail、键级写门放行（can_access_key 假转真）；
//! - 取消臂异步面：drop 时 spawn 的独立回滚任务向假目标端补发
//!   SETSLOTSRANGE STABLE（block_on 内 drop 使能 spawn——生产泵 poll 同
//!   runtime 上下文；异步面以「帧到达假目标端」端到端锁定，不 mock spawn
//!   本身）；
//! - 正常臂：disarm 后 drop 零操作（任务表照常在册、零回滚帧——清理交还
//!   既有显式单点，与失败 recover 幂等共存）。

#[path = "common/migrate_fixture.rs"]
mod migrate_fixture;
use migrate_fixture::{migrate_spec, port_of};

#[path = "common/scripted_migrate_target.rs"]
mod scripted_migrate_target_core;

use std::{sync::Arc, time::Duration};

use aok::Void;
use compio::{runtime::Runtime, time::sleep};
use parking_lot::Mutex;
use scripted_migrate_target_core::{ScriptedTargetOptions, scripted_migrate_target};
use wbase::{hash_slot::slot_of, map::HashSet};
use wedb::server::{
  cluster_provider::ClusterProvider,
  migration::{
    migrate_driver::{KeysDriverGuard, try_add_slots_migration_task},
    migrate_state::MigrateState,
    sketch_status::SketchStatus,
  },
};

/// 默认会话 (0,0) 库槽位（库级定槽：迁移域唯一可承接槽位）
const SLOT0: u16 = slot_of(0, 0);
/// 假目标端行为面（全册统一：RESERVE 合成基数 9_000_000，对标同名件）
static TARGET_OPTS: ScriptedTargetOptions = ScriptedTargetOptions {
  reserve_ctx_base: Some(9_000_000),
  seen_with_payload_len: false,
  frame_archive: None,
};

/// 会话槽位集（库级定槽：KEYS 形态单槽）
fn slot_set() -> HashSet<i32> {
  [i32::from(SLOT0)].into_iter().collect()
}

/// 取消臂：直 drop 守卫对象——同步面清理（任务表/终态/写门）与异步面回滚
/// （STABLE 帧到达假目标端）双双必达
#[test]
fn keys_cancel_guard_drop_cleans_task_and_reopens_key_gate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let cp = ClusterProvider::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本段仅供回滚连接弹 +OK（首连接即回滚新建连；驱动本体不参战）
    let addr =
      scripted_migrate_target(vec![vec![b"+OK\r\n"; 4]], Arc::clone(&seen), TARGET_OPTS).await;

    let spec = migrate_spec(port_of(&addr), 5000);
    let session = try_add_slots_migration_task(&cp, spec, &slot_set()).unwrap();
    let mgr = cp.migration_manager().unwrap();

    // 复现丢弃点现场：sketch 收录 + Transmitting（键级写门关闭）
    session.sketch.hash_and_store(b"kcg1");
    session.sketch.set_status(SketchStatus::Transmitting);
    assert!(
      !mgr.can_access_key(b"kcg1", i32::from(SLOT0), false),
      "前置：Transmitting 态写门须已关闭"
    );
    assert_eq!(mgr.get_migration_task_count(), 1, "前置：任务在册");

    // 取消臂：future 被丢弃时守卫随栈展开退场的同构收口（block_on 内 drop
    // 即生产泵 poll 同 runtime 上下文）
    let guard = KeysDriverGuard::new(Arc::clone(&mgr), Arc::clone(&session));
    drop(guard);
    // 异步面收敛窗：spawn 的回滚任务全新建连 + STABLE 停等
    sleep(Duration::from_millis(300)).await;

    // 同步面三件
    assert_eq!(
      mgr.get_migration_task_count(),
      0,
      "任务表须清零（槽位泄漏即同槽再迁移恒 IOERR）"
    );
    assert_eq!(
      *session.status.read(),
      MigrateState::Fail,
      "会话终态须 Fail"
    );
    assert!(
      mgr.can_access_key(b"kcg1", i32::from(SLOT0), false),
      "键级写门须已放行（sketch 复位 + 任务摘除双保险）"
    );
    // 异步面：远端 IMPORTING 回滚补跑（帧到达假目标端端到端直证）
    assert!(
      seen
        .lock()
        .iter()
        .any(|f| f.contains("SETSLOTSRANGE STABLE")),
      "回滚任务须向目标端补发 STABLE: {:?}",
      seen.lock()
    );
  });
  aok::OK
}

/// 正常臂：disarm 后 drop 零操作——任务表照常在册、零回滚帧（清理交还
/// 既有显式单点，与失败 recover 幂等共存）
#[test]
fn keys_cancel_guard_disarm_is_noop() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let cp = ClusterProvider::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr =
      scripted_migrate_target(vec![vec![b"+OK\r\n"; 4]], Arc::clone(&seen), TARGET_OPTS).await;

    let spec = migrate_spec(port_of(&addr), 5000);
    let session = try_add_slots_migration_task(&cp, spec, &slot_set()).unwrap();
    let mgr = cp.migration_manager().unwrap();
    assert_eq!(mgr.get_migration_task_count(), 1, "前置：任务在册");

    let mut guard = KeysDriverGuard::new(Arc::clone(&mgr), Arc::clone(&session));
    guard.disarm();
    drop(guard);
    sleep(Duration::from_millis(150)).await;

    assert_eq!(
      mgr.get_migration_task_count(),
      1,
      "正常臂清理交还显式单点（finally TryRemoveMigrationTask），守卫退场零操作"
    );
    assert_eq!(
      *session.status.read(),
      MigrateState::Pending,
      "终态不被守卫覆写"
    );
    assert!(
      !seen
        .lock()
        .iter()
        .any(|f| f.contains("SETSLOTSRANGE STABLE")),
      "disarm 后不得补发回滚帧: {:?}",
      seen.lock()
    );
  });
  aok::OK
}
