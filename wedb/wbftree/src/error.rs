use std::{io, result};

use thiserror::Error;

use crate::types::BfTreeInsertResult;

/// 重复创建索引的错误文案单点（C# 在
/// libs/server/Storage/Session/MainStore/RangeIndexOps.cs 就地书写一条，
/// rust 收敛到本常量，wkv RangeIndexError::AlreadyExists 转调，不复制字面量）
pub const ERR_INDEX_ALREADY_EXISTS: &str = "ERR index already exists";

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

  /// 屏障等待超时 (排空在途写者 / 等待快照完成超过上限，持有者疑似卡死)
  #[error("屏障等待超时")]
  Timeout,

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
}

/// 模块全局 Result 别名
pub type Result<T> = result::Result<T, Error>;
