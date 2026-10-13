//! 集群键迁移发送驱动 (MigrateDriver)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs
//! （任务启动/恢复编排）与 libs/cluster/Server/Migration/MigrateSessionKeys.cs:
//! MigrateKeysFromStoreAsync/TransmitKeysAsync、libs/cluster/Server/Migration/MigrateSessionSlots.cs
//! （槽级迁移驱动循环）
//!
//! 迁移资格（对标 MigrateOperation.TransmitKeysAsync 经 UnifiedInput 统一
//! 读写主存对象）：string 记录与 Hash/Set/List/ZSet 对象信封记录可迁移；
//! wbftree 页存储集合键（RangeIndex 与升阶分层集合，rust 分层扩展面）与
//! 向量集走带外通道——页存储树经同一分块流式快照通道
//!（migrate_session_range_index.rs，流元携载判别类型与 TTL），向量集经
//!「上下文预留 → 索引/元素帧 → 源端删除」专用编排
//!（migrate_session_vector_set.rs）。未知信封内层类型（仅数据损坏可达）
//! 显式「暂不支持」：KEYS 入口对含此类键的请求整体拒绝并列明清单，SLOTS
//! 与快照链上抛判败收口，键一律保留源端，绝不静默跳键。
//!
//! 孤儿键投影（安全声明，仅 SLOTS 链）：槽位移交（远端 NODE +
//! RelinquishOwnership + gossip 传播）与源端物理删除存在交错窗口，窗口内
//! 按槽路由的读可能在源端命中尚未删除的旧键投影（孤儿键）。键不丢、最终
//! 一致；C# 的 DELETING 分相与 gossip 传播交错存在同形窗口，属双方共有的
//! 安全投影而非缺陷。
//!
//! 槽位属主语义（KEYS/SLOTS 链差异登记）：C# MigrateKeysAsync 全程不动
//! 槽位（MigrationDriver.cs:TryStartMigrationTaskAsync 的 KEYS 臂仅调
//! MigrateKeysAsync，无任何 SETSLOT/NODE/Relinquish 编排），rust 同口径
//! ——KEYS 链收尾仅发完成哨兵帧，绝不移交槽属主，同槽未迁移键持续可源
//! 端访问，本端 MIGRATING 与远端收口归运维 CLUSTER SETSLOT。与 C# 的显式
//! 差异：KEYS 链远端 IMPORTING 由驱动自动下发（C# 由运维预先 SETSLOT
//! IMPORTING），保证目标端 is_importing_slot 接收门放行；对偶地，失败恢
//! 复仅回滚该自动下发的 IMPORTING→STABLE，成功收尾不作任何槽位复位。

pub mod keys;
pub mod live_value;
pub mod phase;
pub mod slots;

/// KEYS 驱动执行体（任务注册后的键迁移主状态机），私有子模块，经
/// [`keys`] 的 run_keys_migration_driver 驱动
mod keys_execute;

use std::sync::Arc;

pub use keys::{KeysDriverGuard, MigrateTransmitEnv, run_keys_migration_driver, transmit_keys};
#[doc(hidden)]
pub use live_value::TEST_LIVE_VALUE_READ_HOOK;
pub use live_value::{
  LiveKeyKind, LiveValue, UnsupportedKey, collect_vector_set_keys, migratable_object_type,
  probe_live_key_kind, probe_unsupported_keys, read_live_value, unsupported_label,
};
pub use phase::{connect_migrate_client, send_payload_and_wait};
pub use slots::{RevivPauseGuard, run_slots_migration_task, try_add_slots_migration_task};

use crate::server::migration::{
  migrate_session::MigrateSession, migrate_state::MigrateState,
  migration_manager::MigrationManager, sketch_status::SketchStatus,
};

/// 遗弃迁移会话同步面清理单点（KEYS 守卫取消臂 Drop 与 SLOTS 后台驱动监督
/// panic 臂共用）：sketch 复位放行键级写门（Transmitting/Deleting 滞留即源端
/// 键级写门关闭，默认 cluster_node_timeout 后转 ASK 写失败）+ 会话终态 Fail +
/// 任务表摘除（槽位泄漏即同槽再迁移恒 IOERR）。
///
/// 全为同步方法（sketch/status 锁 + 册子锁），Drop/panic 臂内安全；幂等共存：
/// recover/正常收口已先达时 sketch 复位与摘除天然零操作，status 与 recover
/// 置 Fail 同值不产生观察窗（SyncBatchGuard 终态判据同口径）
pub(crate) fn abandon_migration_session(mgr: &MigrationManager, session: &Arc<MigrateSession>) {
  session.sketch.set_status(SketchStatus::Initializing);
  *session.status.write() = MigrateState::Fail;
  mgr.try_remove_migration_task_session(Arc::clone(session));
}

/// 迁移失败统一收口：recover 后直接 return Err（err 臂带 poisoned 判定，
/// why 臂固定 false + InvalidArgument）
macro_rules! recover_and_fail {
  ($c:expr, $s:expr, $r:expr, $d:expr, $t:expr, $msg:expr) => {{
    try_recover_from_failure($c, $s, $r, $d, $msg, false, $t).await;
    return Err(crate::error::Error::InvalidArgument($msg.into()));
  }};
  ($c:expr, $s:expr, $r:expr, $d:expr, $t:expr, $p:expr, $w:expr, $e:expr) => {{
    // 先落绑 poisoned 判定，保证 $w 的方法解析时 err 类型已定型
    let poisoned = $p;
    try_recover_from_failure($c, $s, $r, $d, $w, poisoned, $t).await;
    return Err($e);
  }};
}
pub(crate) use recover_and_fail;
