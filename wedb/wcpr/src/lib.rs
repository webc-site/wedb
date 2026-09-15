#![cfg_attr(docsrs, feature(doc_cfg))]

//! wcpr：CPR 检查点内核 crate（Checkpoint / Recovery）
//!
//! 公共暴露面按五类入口分组（对标 C# Tsavorite
//! `DeviceLogCommitCheckpointManager` 的 Purge/PurgeAll/List 公共面 +
//! `Tsavorite` 的 TakeFullCheckpointAsync/RecoverAsync 入口）：
//!
//! 1. **create 族**——快照创建（自动签发 / 指定 Token / Token 预知签发）
//! 2. **recover 族**——崩溃恢复（指定 Token / 自动回退扫描最新）
//! 3. **purge 族**——快照清理（单 Token / 全量残留清扫 / 保留最新 N 个）
//! 4. **list 族**——目录枚举（升序列表 / 最新值）
//! 5. **宿主契约端口**——`CprStore`/`CprRecover` 由宿主引擎（wkv::WedbStore）
//!    实现的恢复契约
//!
//! 其余保留导出：元数据构成类型（`CheckpointMeta` 公共字段的必需类型）、
//! 底层索引快照 I/O 与文件命名方案（跨 crate 故障注入测试直查文件格式用）。
//! Token 派生（`next_token`）、组件级恢复（`recover_checkpoint_components`）、
//! 状态机（`CprPhase`）、文件格式头（`IndexCkptHeader`）等均为 crate 内部实现细节，
//! 不对外导出。

mod error;
mod index_ckpt;
mod manager;
mod meta;

// ===================== 错误类型 =====================
pub use error::{Error, Result};
// ===================== 底层快照 I/O 与文件命名方案 =====================
// 恢复语义一律要求 tail 一致性截断：不带钳制的裸恢复（tail = None）会放行超出
// 一致性点的索引条目，已收敛为 [`read_index_checkpoint_truncated`] 的显式参数，
// 杜绝「图省事传 None」的误用面（模糊截断测试显式传 None 属例外）
pub use index_ckpt::{read_index_checkpoint_truncated, write_index_checkpoint};
// ===================== 宿主契约端口（五类入口之一）=====================
// CprStore/CprRecover 由宿主引擎实现；RecoveredCheckpoint 为 from_recovered
// 的组件载体（meta + index + hlog + epoch）
pub use manager::{CprRecover, CprStore, RecoveredCheckpoint};
// ===================== create 族 =====================
// create_checkpoint 自动签发 Token；create_checkpoint_with_token 指定 Token；
// next_token_above 供宿主在版本切换回调需预知 Token 时签发
// （对标 C# GarnetClusterCheckpointManager.checkpointVersionShiftStart 携 newVersion）
pub use manager::{create_checkpoint, create_checkpoint_with_token, next_token_above};
// ===================== list 族 =====================
// list_checkpoints 升序 Token 列表；find_latest_checkpoint 最新值
pub use manager::{find_latest_checkpoint, list_checkpoints};
// ===================== purge 族 =====================
// purge_checkpoint 清理单 Token 文件集；purge_all 全量含残留清扫；
// purge_outdated 保留最新 keep 个（对标 C# CheckpointManager Purge/PurgeAll）
pub use manager::{purge_all, purge_checkpoint, purge_outdated};
// ===================== recover 族 =====================
// recover 按指定 Token 恢复；recover_latest 由新到旧自动回退扫描
pub use manager::{recover, recover_latest};
// ===================== 元数据类型 =====================
// CheckpointMeta 公共字段的构成类型：IndexMeta/HlogMeta/StoreMeta 随元数据
// 结构对外可见（宿主 from_recovered 与测试构造必需）；FORMAT_VERSION 供
// 诊断超前版本拒绝
pub use meta::{CheckpointMeta, CheckpointType, FORMAT_VERSION, HlogMeta, IndexMeta, StoreMeta};
// 文件名构造族对标 C# ICheckpointNamingScheme 公共命名方案
pub use meta::{
  index_filename, index_tmp_filename, meta_filename, meta_tmp_filename, token_to_base32,
};
