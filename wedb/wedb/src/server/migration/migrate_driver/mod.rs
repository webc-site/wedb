//! 集群键迁移发送驱动 (MigrateDriver)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs
//! （任务启动/恢复编排）与 libs/cluster/Server/Migration/MigrateSessionKeys.cs:
//! MigrateKeysFromStoreAsync/TransmitKeysAsync、libs/cluster/Server/Migration/MigrateSessionSlots.cs
//! （槽级迁移驱动循环）
//!
//! 迁移资格（对标 MigrateOperation.TransmitKeysAsync 经 UnifiedInput 统一
//! 读写主存对象）：string 记录与 Hash/Set/List/ZSet 对象信封记录可迁移；
//! RangeIndex 树与向量集走带外通道——RangeIndex 经分块流式快照
//!（migrate_session_range_index.rs），向量集经「上下文预留 → 索引/元素帧
//! → 源端删除」专用编排（migrate_session_vector_set.rs）。未知信封类型
//! 显式「暂不支持」：KEYS 入口对含此类键的请求整体拒绝并列明清单，SLOTS
//! 游标登记清单留痕，键一律保留源端，绝不静默跳键。
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
pub mod slots;

pub use keys::{
  MigrateTransmitEnv, connect_migrate_client, run_keys_migration_driver, send_payload_and_wait,
  transmit_keys,
};
pub use live_value::{
  LiveKeyKind, LiveValue, UnsupportedKey, collect_vector_set_keys, migratable_object_type,
  probe_live_key_kind, probe_unsupported_keys, read_live_value, unsupported_label,
};
pub use slots::{RevivPauseGuard, run_slots_migration_task, try_add_slots_migration_task};
