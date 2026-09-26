use std::{io, result};

use thiserror::Error;

use crate::types::BfTreeInsertResult;

/// 重复创建索引的错误文案单点（C# 在
/// libs/server/Storage/Session/MainStore/RangeIndexOps.cs 就地书写一条，
/// rust 收敛到本常量，wkv RangeIndexError::AlreadyExists 转调，不复制字面量）
pub const ERR_INDEX_ALREADY_EXISTS: &str = "ERR index already exists";

/// Memory 后端树迁移拒绝文案单点（C# 在
/// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:308 就地书写一条，
/// rust 收敛到本常量：CLUSTER MIGRATE 快照门禁与 RENAME 迁移门禁共用，不复制字面量）
pub const ERR_MEMORY_TREE_MIGRATION: &str =
  "SnapshotForMigration: memory-only trees cannot be migrated";

/// BfTree 统一错误枚举
#[derive(Error, Debug)]
pub enum Error {
  /// 底层 I/O 错误
  #[error(transparent)]
  Io(#[from] io::Error),

  /// 参数非法
  #[error("参数非法: {0}")]
  InvalidArgument(String),

  /// 索引已存在 (重复创建同名 RangeIndex)
  #[error("{ERR_INDEX_ALREADY_EXISTS}")]
  IndexExists,

  /// 惰性恢复目标工件缺失 (data.bftree 不在盘)。并发删空的墓碑与延迟 unlink
  /// 可先于在途慢路径落地，属合法时序而非不变量破坏——1:1 对标 C#
  /// RestoreTree 的 `File.Exists(workingPath)` 为假臂 (Release 模式 LogWarning +
  /// return false，上层答 NOTFOUND)，绝不向客户端上抛致命恢复错误
  #[error("目标不存在")]
  NotFound,

  /// 无效配置
  #[error("配置非法: {0}")]
  InvalidConfig(String),

  /// CPR 快照生成失败
  #[error("快照失败: {0}")]
  Snapshot(String),

  /// CPR 快照恢复失败
  #[error("恢复失败: {0}")]
  Recovery(String),

  /// 实例已被释放 (Disposed)
  #[error("BfTree 实例已被释放")]
  Disposed,

  /// 范围扫描失败
  #[error("扫描失败: {0}")]
  Scan(String),

  /// 数据损坏
  #[error("数据损坏: {0}")]
  Corrupted(String),

  /// 批量装载被引擎拒绝（键值违反长度契约 / 引擎参数非法），携原始状态码
  /// 供宿主分流 RESP 错误文案（见 [`crate::RangeIndexManager::build_collection_tree_snapshot`]）
  #[error("批量装载被拒: {0:?}")]
  LoadRejected(BfTreeInsertResult),

  /// 常驻页缓存总预算耗尽（升阶 scratch 建树闸 / 新树登记被总闸拒绝）。
  /// 本仓分层防 OOM 自定义面，C# 无对应变体——C# 树仅 RI.CREATE 显式创建、
  /// 数量用户可控，无自动升阶面即无总闸义务
  #[error("树页缓存预算耗尽")]
  CacheBudgetExhausted,
}

/// 模块全局 Result 别名
pub type Result<T> = result::Result<T, Error>;
