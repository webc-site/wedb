use core::{result::Result as StdResult, str::Utf8Error};
use std::{io::Error as IoError, num::ParseIntError};

use thiserror::Error;

pub type Result<T> = StdResult<T, Error>;

/// libs/client/ExceptionTypes.cs
#[derive(Debug, Error)]
pub enum Error {
  /// GarnetClient disposed exception
  /// libs/client/ExceptionTypes.cs:GarnetClientDisposedException
  #[error("GarnetClient is disposed")]
  Disposed,

  /// GarnetClient timeout exception
  /// libs/client/ExceptionTypes.cs:GarnetClientTimeoutException
  #[error("GarnetClient timeout")]
  Timeout,

  /// GarnetClient socket disposed exception
  /// libs/client/ExceptionTypes.cs:GarnetClientSocketDisposedException
  #[error("GarnetClient socket is disposed")]
  SocketDisposed,

  /// 客户端尚未连接或连接已断开
  #[error("Not connected")]
  NotConnected,

  /// 读泵意外退出
  #[error("Read pump exited")]
  ReadPumpExited,

  /// 应答通道已关闭
  #[error("Response channel closed")]
  ResponseChannelClosed,

  /// 发送通道已满/饱和
  #[error("Send channel saturated")]
  SendChannelSaturated,

  /// 连接到达 EOF
  #[error("EOF")]
  Eof,

  /// 服务端返回错误应答 (-ERR ...)
  #[error("{0}")]
  Server(String),

  /// 非预期协议标记
  #[error("Unexpected token {0}")]
  UnexpectedToken(char),

  /// 无效整数
  #[error("Invalid integer")]
  InvalidInteger,

  /// 在途命令上限形参非法（非 2 的幂或超 1<<20）
  ///
  /// 在 garnet 中的相对路径: libs/client/GarnetClient.cs:GarnetClient 构造重载
  ///（maxOutstandingTasks 双校验的 ThrowException 对位，文案同档）
  #[error("max outstanding tasks {0} must be a power of two, up to 1048576")]
  InvalidOutstandingTasks(usize),

  /// 分块记录帧载荷非法（长度头截断 / 不支持的记录类型）。
  /// Display 带 `invalid argument: ` 前缀，与消费方服务端错误应答的历史文案逐字等价
  #[error("invalid argument: {0}")]
  InvalidRecord(String),

  /// UTF-8 编码错误透明转发
  #[error(transparent)]
  Utf8(#[from] Utf8Error),

  /// 整数解析错误透明转发
  #[error(transparent)]
  ParseInt(#[from] ParseIntError),

  /// RESP 协议解析错误透明转发
  #[error(transparent)]
  Resp(#[from] wresp::Error),

  /// IO 错误透明转发
  #[error(transparent)]
  Io(#[from] IoError),
}
