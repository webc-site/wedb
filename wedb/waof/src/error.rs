use std::result;

use thiserror::Error;
use wbase::{error, group_commit::Broken};

/// WAL 模块错误枚举
#[derive(Error, Debug)]
pub enum Error {
  /// 底层存储设备错误（含被包装的 I/O 错误）
  #[error(transparent)]
  Device(#[from] wdev::Error),

  /// 内存分配或对齐错误
  #[error(transparent)]
  Mem(#[from] error::Error),

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

  /// 回放头类型不受支持（对标 C# AofProcessor.cs:CanReplay 的
  /// `default: throw new GarnetException($"Replay header type ... not supported!")`）
  #[error("不支持的回放头类型: {0}")]
  UnsupportedReplayHeaderType(u8),

  /// 未知 AOF 操作类型判别值（头内 opType 字节不可解释为 AofEntryType）
  #[error("未知 AOF 操作类型: {0}")]
  UnknownEntryType(u8),

  /// 刷盘失败致环形缓冲区无法腾窗，入队背压终止（对标 C# TsavoriteLog 中
  /// `cannedException` 非空即从 AllocateBlock 挂起循环抛出终止：磁盘故障等
  /// 致命 I/O 下绝不无限挂起入队者，显式上抛交由命令层失败）。区别于瞬时的
  /// [`Error::BufferFull`]——后者经背压等待刷盘水位推进后必然可解，本变体仅在
  /// 常驻提交协程记录到底层刷盘失败后抵达，代表无法通过重试恢复的终态
  #[error("刷盘失败致环形缓冲区无法腾窗，入队终止")]
  FlushFailed,

  /// 提交流水线中断（Follower 等待侧统一哨兵，类型单点在
  /// wbase::group_commit::Broken）
  #[error(transparent)]
  PipelineBroken(#[from] Broken),
}

/// WAL 操作结果类型别名
pub type Result<T> = result::Result<T, Error>;
