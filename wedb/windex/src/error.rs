use std::result;

use thiserror::Error;
use wbase::error;

/// 哈希索引错误枚举
#[derive(Error, Debug)]
pub enum Error {
  /// 桶数量非法（必须为 2 的幂且大于 0）
  #[error("桶数量必须是 2 的幂且大于 0: {0}")]
  InvalidBucketCount(usize),

  /// 逻辑地址非法（0 为保留的无效地址）
  #[error("逻辑地址非法 (0 为保留的无效地址): {0:#x}")]
  InvalidAddress(u64),

  /// 逻辑地址超过 48 位上限（最大 256TB）
  #[error("逻辑地址超过 48 位限制 (最大 256TB): {0:#x}")]
  AddressOverflow(u64),

  /// 溢出桶内存池耗尽
  #[error("溢出桶内存池已耗尽")]
  OverflowPoolExhausted,

  /// 溢出桶链回环检测异常
  #[error("检测到哈希桶溢出链存在环或深度超过上限")]
  OverflowCycleDetected,

  /// 单键桶闩取闩失败（索引层不设超时：`HashIndex::try_lock_key_exclusive` 为一次
  /// 尝试的 ephemeral 语义，本变体由取闩方（如 wkv 键级 TTL 读改写窗口）按 C#
  /// `RETRY_LATER` 口径在调用侧承接并向外上浮）
  #[error("键所在桶闩占用中，取闩失败（请重试）")]
  LockTimeout,

  /// 操作系统直接虚拟内存分配异常
  #[error(transparent)]
  Mem(#[from] error::Error),
}

/// 哈希索引结果类型
pub type Result<T> = result::Result<T, Error>;
