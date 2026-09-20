//! wvector 中心错误定义（跨模块传播的错误枚举单点收口）
//!
//! 传播链：StoreError → FsmError → WedbProviderError，QuantizerError →
//! WedbProviderError；单模块内使用的错误（如 element_data 的
//! [`crate::element_data::PrepareError`]）留原地，不上收。

use diskann_quantization::{algorithms::transforms::NewTransformError, alloc::AllocatorError};
use thiserror::Error;

/// 存储回调失败（garnet.rs GarnetError 的等价承接）。
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone, Copy)]
pub enum StoreError {
  #[error("store read failed")]
  Read,
  #[error("store write failed")]
  Write,
  #[error("store delete failed")]
  Delete,
}

/// 空闲空间映射操作失败。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FsmError {
  #[error(transparent)]
  Store(#[from] StoreError),
  #[error("requested ID is out of range {0}")]
  IdOutOfRange(u32),
}

/// 量化器错误。
#[derive(Debug, Error)]
pub enum QuantizerError {
  #[error("Quantization training error: {0}")]
  Training(String),
  #[error("Quantization alloc error: {0}")]
  Alloc(#[from] AllocatorError),
  #[error("Query computer error: {0}")]
  QueryComputer(String),
  #[error("Binary quantization error: {0}")]
  Compression(String),
  #[error("No quantizer found")]
  NoQuantizer,
  #[error("Got zero dimension")]
  ZeroDim,
  #[error("Transform error: {0}")]
  BadTransform(#[from] NewTransformError),
  #[error("Unsupported serialization/deserialization")]
  UnsupportedSerialization,
  #[error("Quantizer deserialization error: {0}")]
  Deserialization(String),
}

/// WedbProvider 内部与桥接错误。
#[derive(Debug, thiserror::Error)]
pub enum WedbProviderError {
  #[error("Wedb store operation failed")]
  Store(#[from] StoreError),
  #[error("FSM error")]
  Fsm(#[from] FsmError),
  #[error("Start point invalid")]
  StartPoint,
  #[error("Allocation failed")]
  AllocFailed(#[from] AllocatorError),
  #[error("Invalid quantizer for vector data")]
  InvalidQuantizer,
  #[error("Quantizer error: {0}")]
  Quantizer(#[from] QuantizerError),
  #[error("Post processing error: {0}")]
  PostProcessing(String),
}

diskann::convert_error!(WedbProviderError);
diskann::always_escalate!(WedbProviderError);

impl From<StoreError> for diskann::ANNError {
  #[inline]
  fn from(err: StoreError) -> Self {
    WedbProviderError::Store(err).into()
  }
}
