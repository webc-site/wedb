use std::{error::Error as StdError, io, path::PathBuf, result};

use thiserror::Error;

/// wedb_checkpoint 模块自定义错误类型
#[derive(Error, Debug)]
pub enum Error {
  /// I/O 错误
  #[error(transparent)]
  Io(#[from] io::Error),

  /// 元数据序列化/反序列化 JSON 错误
  #[error(transparent)]
  Json(#[from] sonic_rs::Error),

  /// 元数据 bitcode 序列化/反序列化错误
  #[error("Checkpoint 元数据 bitcode 编解码失败: {0}")]
  Bitcode(#[from] bitcode::Error),

  /// 块设备错误
  #[error(transparent)]
  Device(#[from] wdev::Error),

  /// 索引错误
  #[error(transparent)]
  Index(#[from] windex::Error),

  /// 混合日志错误
  #[error(transparent)]
  Hlog(#[from] whlog::Error),

  /// 纪元系统错误（宿主恢复路径注册纪元参与者等）
  #[error(transparent)]
  Epoch(#[from] wepoch::Error),

  /// 宿主存储引擎端口错误（宿主端口模式透明转发）
  ///
  /// wcpr 仅依赖 wdev/windex/whlog/wepoch 基础设施 crate，不依赖 wbftree 与宿主
  /// 引擎 crate（依赖无环约束）；宿主实现 [`CprStore`](crate::CprStore)/
  /// [`CprRecover`](crate::CprRecover) 时，BfTree 快照、RangeIndex 恢复、配置重构
  /// 等宿主侧类型化错误经本变体透明转发——Display 与 `source()` 错误链完整保留，
  /// 调用方可继续向下解引用具体错误类型，不做任何字符串化有损降级
  #[error(transparent)]
  Host(#[from] Box<dyn StdError + Send + Sync>),

  /// 快照元数据文件不存在
  #[error("未找到 Checkpoint 元数据文件: {0}")]
  MetaNotFound(PathBuf),

  /// 目录中不存在任何可用的有效 Checkpoint
  #[error("目录中不存在有效的 Checkpoint: {0}")]
  NoValidCheckpoint(PathBuf),

  /// 索引快照文件不存在
  #[error("未找到 Index 快照文件: {0}")]
  IndexCkptNotFound(PathBuf),

  /// 索引快照文件损坏或格式无效
  #[error("Index 快照文件损坏或格式无效: {0}")]
  InvalidIndexCkpt(String),

  /// 快照 Token 不匹配
  #[error("Token 不匹配: 期望 {expected:#x}，实际 {actual:#x}")]
  TokenMismatch { expected: u128, actual: u128 },

  /// 快照文件校验和不匹配
  #[error("快照校验和不匹配: 期望 {expected:#x}，实际 {actual:#x}")]
  ChecksumMismatch { expected: u32, actual: u32 },

  /// 快照元数据完整性封签不匹配（元数据被篡改或损坏）
  #[error("Checkpoint 元数据完整性校验失败: 期望 {expected:#x}，实际 {actual:#x}")]
  MetaChecksumMismatch { expected: u32, actual: u32 },

  /// 恢复逻辑地址校验异常
  #[error("恢复地址校验异常: {0}")]
  InvalidRecoveryAddress(String),

  /// 元数据格式版本高于当前引擎支持的版本（由更新版本的引擎写入）
  #[error("不支持的 Checkpoint 元数据格式版本: {actual}（当前支持最高版本 {supported}）")]
  UnsupportedMetaVersion { actual: u32, supported: u32 },

  /// 调用方处于纪元保护区时发起 Checkpoint（违反调用契约：检查点必须在保护区外驱动）
  ///
  /// C# Tsavorite/Garnet 中检查点由外部串行驱动（CHECKPOINT 命令与周期紧缩任务均在
  /// 会话作用域外调用），「不在保护区内发起」仅靠调用约定成立；wedb 将契约升级为
  /// fail-fast 类型化错误：调用方自持纪元会使排空屏障永远无法完成，静默跳过将打开
  /// 「数据页已刷盘但索引插入尚未提交」的丢失更新窗口
  #[error(
    "调用方处于纪元保护区，禁止发起 Checkpoint：自持纪元使排空屏障永远无法完成（丢失更新窗口），请退出会话/纪元作用域后再驱动检查点"
  )]
  CheckpointWhileEpochProtected,
}

pub type Result<T> = result::Result<T, Error>;
