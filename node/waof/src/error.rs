use std::result;

use thiserror::Error;

/// WAL 模块错误枚举
#[derive(Error, Debug)]
pub enum Error {
  /// 底层存储设备错误（含被包装的 I/O 错误）
  #[error(transparent)]
  Device(#[from] wdev::Error),

  /// 内存分配或对齐错误
  #[error(transparent)]
  Mem(#[from] wram::Error),

  /// 记录校验和不匹配
  #[error("记录校验和不匹配: 期望 0x{expected:08X}，实际 0x{actual:08X}")]
  ChecksumMismatch {
    /// 期望的 CRC32 校验和
    expected: u32,
    /// 实际计算得到的 CRC32 校验和
    actual: u32,
  },

  /// 环形写缓冲区已满
  #[error("环形写缓冲区已满: 可用 {available} 字节，请求 {requested} 字节")]
  BufferFull {
    /// 当前可用的字节数
    available: u64,
    /// 本次请求写入的字节数（u64 口径，与记录总长一致）
    requested: u64,
  },

  /// 写入单条记录过大（记录总长超出缓冲区单条最大限制或记录头编码上限）
  #[error("写入记录过大: 总长 {len} 字节超出单条上限 {limit} 字节")]
  RecordTooLarge {
    /// 本次请求写入的记录总长（含记录头）
    len: u64,
    /// 单条记录的长度上限
    limit: u64,
  },

  /// 刷盘短写入（设备未完整写入对齐块，禁止推进提交位点）
  #[error("刷盘短写入: 期望写入 {expected} 字节，实际仅写入 {written} 字节")]
  ShortWrite {
    /// 期望写入的字节数
    expected: usize,
    /// 实际写入的字节数
    written: usize,
  },

  /// 无效的记录头格式
  #[error("无效记录头: 数据损坏或未完全写入")]
  InvalidRecordHeader,
}

/// WAL 操作结果类型别名
pub type Result<T> = result::Result<T, Error>;
