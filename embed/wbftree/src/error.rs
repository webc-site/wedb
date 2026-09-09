use std::{io, result};

use thiserror::Error;

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
  #[error("ERR index already exists")]
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

  /// 扫描已被终止
  #[error("扫描终止")]
  ScanAborted,

  /// 数据损坏
  #[error("数据损坏: {0}")]
  Corrupted(String),
}

/// 模块全局 Result 别名
pub type Result<T> = result::Result<T, Error>;
