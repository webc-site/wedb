use std::{io, result};

use thiserror::Error;

/// HybridLog 模块错误类型
#[derive(Error, Debug)]
pub enum Error {
  /// 设备 I/O 错误
  #[error(transparent)]
  Device(#[from] wdev::Error),

  /// 记录编码或解码错误
  #[error(transparent)]
  Record(#[from] wrecord::Error),

  /// 内存对齐或分配错误
  #[error(transparent)]
  Mem(#[from] wbase::Error),

  /// 纪元并发保护错误
  #[error(transparent)]
  Epoch(#[from] wepoch::Error),

  /// 标准 I/O 错误
  #[error(transparent)]
  Io(#[from] io::Error),

  /// 配置参数无效
  #[error("无效配置: {0}")]
  InvalidConfig(String),

  /// 逻辑地址非法或超出范围
  #[error("无效逻辑地址: {0}")]
  InvalidAddress(u64),

  /// 逻辑地址超出范围
  #[error("地址越界: 地址 {addr} 不在有效区间 [{begin}, {tail}) 内")]
  AddressOutOfRange { addr: u64, begin: u64, tail: u64 },

  /// 记录大小超过单页可用容量
  #[error("记录过大: 记录大小 {size} 超过页面容量 {page_size}")]
  RecordTooLarge { size: usize, page_size: usize },

  /// 访问到了换页填充标记（Padding 记录）
  #[error("访问到换页填充标记: 地址 {0:#x}")]
  PadRecord(u64),

  /// 数据损坏或校验失败
  #[error("记录数据损坏: 地址 {addr:#x}, 详情: {detail}")]
  RecordCorrupted { addr: u64, detail: String },

  /// 页面未就绪
  #[error("页面未就绪: 页号 {0}")]
  PageNotReady(u64),

  /// 页面刷盘失败
  #[error("页面刷盘失败: 页号 {page_id}, 详情: {detail}")]
  FlushFailed { page_id: u64, detail: String },

  /// 状态机状态非法或恢复失败
  #[error("状态机状态非法: {0}")]
  InvalidState(String),
}

pub type Result<T> = result::Result<T, Error>;
