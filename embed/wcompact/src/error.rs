use std::{error, result};

use thiserror::Error;

/// wedb_compact 模块错误类型
///
/// 错误面 = 紧缩器自身校验错误 + 底层引擎类型化故障的透明转发（对照宿主 wkv
/// error.rs 的 `Compact(#[from] wcompact::Error)` 透明组合关系）：宿主经穷尽映射
/// 注入底层错误，全程无字符串化降级。
#[derive(Error, Debug)]
pub enum Error {
  /// 紧缩目标地址超出只读区边界
  #[error("紧缩目标地址 {until_address:#x} 超出只读区边界 {read_only_address:#x}")]
  UntilAddressOutOfRange {
    until_address: u64,
    read_only_address: u64,
  },

  /// 紧缩会话纪元参与者注册失败（宿主纪元参与者表耗尽）
  #[error(transparent)]
  Epoch(#[from] wepoch::Error),

  /// 混合日志故障（尾部追加、起始地址推进补刷、磁盘记录回读）
  #[error(transparent)]
  Hlog(#[from] whlog::Error),

  /// 记录编解码故障
  #[error(transparent)]
  Record(#[from] wrecord::Error),

  /// 值层编解码故障（集合元数据 / 子键命名空间解析）
  #[error(transparent)]
  Value(#[from] wval::Error),

  /// 宿主引擎紧缩契约外故障
  ///
  /// 紧缩契约操作（会话创建、尾部追加、TTL 探测、起始地址推进）的错误面由纪元、
  /// 混合日志与记录编解码构成，正常情况下不可达此变体；穷尽映射将其承接为哨兵——
  /// 一旦触发即宿主错误面与紧缩契约失配（宿主新增错误通道未同步紧缩映射），
  /// 保留完整类型化错误链供诊断，绝不静默吞没
  #[error("紧缩路径不可达的宿主引擎错误")]
  Host(Box<dyn error::Error + Send + Sync>),
}

pub type Result<T> = result::Result<T, Error>;
