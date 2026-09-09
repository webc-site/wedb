//! wcluster 集中层集中错误定义
//!
//! 所有集群层错误集中于此（thiserror），依赖库错误用
//! `#[error(transparent)]` 透明转发；会话层负责按需附加 `ERR ` 前缀
//! 转 RESP，错误本体不携带协议痕迹

use std::result;

use thiserror::Error;

/// 集中层错误
#[derive(Debug, Error)]
pub enum Error {
  /// 集群配置 bitcode 编解码错误
  #[error(transparent)]
  Codec(#[from] bitcode::Error),
  /// 配置载荷为空（不足以容纳版本字节）
  #[error("cluster config payload too short to contain a version")]
  PayloadTooShort,
  /// 配置格式版本不兼容
  #[error("incompatible cluster config version: got {got}, expect {expect}")]
  Version { got: u8, expect: u8 },
  /// RLE 槽位段长度越界（累计覆盖超过 16384 槽）
  #[error("cluster config slot segments overflow 16384 slots")]
  SlotOverflow,
  /// 槽位状态字节非法
  #[error("invalid slot state byte: {0}")]
  SlotState(u8),
  /// 集群 worker 尚未初始化（无本地节点）
  #[error("workers not initialized")]
  NoWorkers,
  /// config epoch 设置被拒：仅允许从 0 初始化且新值必须更大
  #[error("config epoch not set: current epoch is non-zero or value not greater")]
  EpochNotSet,
}

pub type Result<T> = result::Result<T, Error>;
