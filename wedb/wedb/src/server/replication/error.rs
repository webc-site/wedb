//! 复制域集中错误定义（diskbased 检查点同步链 + diskless 无盘同步链 + 副本
//! 重放/恢复链共用单一错误类型）
//!
//! 对应依赖库错误经 `#[error(transparent)]` 透明转发；Display 文案逐字节
//! 保持既有字符串错误原文——错误最终去向是日志、会话中止与 RESP -ERR
//! 应答，部分文案被测试断言（见各变体注释的消费点），不可归一改写。
//! 判定类分支（重试/中止/重同步收敛）一律按变体 match，不再字符串匹配。

use std::{error, fmt, io, path::PathBuf};

use thiserror::Error;
use waof::AofAddress;

/// 纪元静止未达成的共享尾缀文案（发起臂与无盘扫描门两处同源）
///
/// 测试面消费点登记（文案即判据，改动前逐点核对）：
/// wedb/tests/diskless_epoch_drain_failclose.rs:216/:227、
/// wedb/tests/failover_epoch_drain_failclose.rs:143（RESP 帧面）、
/// wedb/tests/replicate_sync_epoch_drain_failclose.rs:178（全量字面量镜像）
pub(crate) const EPOCH_DRAIN_UNSETTLED: &str =
  "epoch drain not settled within cluster-node-timeout";

/// 副本建连失败阶段（Display 即该阶段的既有完整文案前缀；两支文案大小写
/// 与措辞各异——diskbased `aofSync` / diskless `AOF stream`——不可归一，
/// 有测试逐字断言，见 tests/replica_sync_pin_release.rs 与
/// tests/replication_assembly_e2e.rs）
#[derive(Debug, Clone)]
pub enum ConnectStage {
  /// 检查点下发专用客户端建连（SendCheckpointAsync 的 gcs 构造形态）
  CheckpointSend,
  /// BEGIN_REPLICA_RECOVER 停等往返专用客户端建连
  RecoverRoundtrip,
  /// AOF 推流通道建连（diskbased 支 startAofSync 文案）
  AofSync,
  /// AOF 推流通道建连（diskless 支 BeginAofSync 文案）
  AofStream,
}

impl fmt::Display for ConnectStage {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let text = match self {
      Self::CheckpointSend => "Failed connecting to replica for checkpoint send",
      Self::RecoverRoundtrip => "Failed connecting to replica for recover roundtrip",
      Self::AofSync => "Failed connecting to replica for aofSync",
      Self::AofStream => "failed connecting to replica for AOF stream",
    };
    f.write_str(text)
  }
}

/// 复制域错误
#[derive(Debug, Error)]
pub enum ReplicationError {
  /// 按需检查点重拍混尽且未允许丢数据：拒绝 attach（C# AcquireCheckpoint-
  /// EntryAsync 的 possible data loss 拒绝臂；测试断言原 contains("possible
  /// data loss") 已改按本变体匹配）
  #[error(
    "Failed to acquire checkpoint after {attempts} on-demand checkpoint attempts: possible data loss"
  )]
  CheckpointAcquire {
    /// 已消耗的重拍次数（MAX_ODC_ATTEMPTS 混尽时点值）
    attempts: usize,
  },

  /// 副本建连失败（`detail` 为底层因由文本，Display 前缀由阶段决定）
  #[error("{stage}: {detail}")]
  Connect {
    /// 建连阶段（决定文案前缀，见 [`ConnectStage`]）
    stage: ConnectStage,
    /// 底层建连错误文本
    detail: String,
  },

  /// 专用客户端已构造但未达已连态（原 `is_connected()` 检查臂，文案尾缀
  /// `(not connected)`）
  #[error("{0} (not connected)")]
  ConnectNotReady(ConnectStage),

  /// 编目在册的检查点文件磁盘缺席：中止下发（副本侧半截文件集由 meta 缺席
  /// 拒绝导入；原 contains("checkpoint file missing") 消费已改按本变体匹配）
  #[error("IOERR checkpoint file missing: {}", path.display())]
  CheckpointFileMissing {
    /// 缺席文件的路径
    path: PathBuf,
  },

  /// DataLossCheck 判败：副本请求位点低于主端活体日志起点，推流无法连续
  /// 衔接，默认拒绝建流（C# appendOnlyFile.DataLossCheck 同位语义）
  #[error(
    "Failed syncing because replica requested truncated AOF address: {sync_from:?} < beginAofAddress: {begin:?}"
  )]
  DataLoss {
    /// 副本请求的同步位点
    sync_from: AofAddress,
    /// 主端活体日志起点（比对基线）
    begin: AofAddress,
  },

  /// 历史发散/残留段被物理删段越过类判败（重同步收敛通路的确定性信号：
  /// recover 授予位点落后应用位点、钳制补应用窗无源可补）
  #[error("{0}")]
  HistoryGap(String),

  /// 带语境的底层操作失败（IOERR 家族；`context` 承接原文案语境段，底层库
  /// 错误统一收敛为 [`io::Error`] 承载——非 io 来源经 `io::Error::other`
  /// 包装后 Display 逐字节等于原 `{e}` 文本）
  #[error("IOERR {context}: {source}")]
  Io {
    /// 操作语境（如 "device read at {offset}"）
    context: String,
    /// 底层错误
    source: io::Error,
  },

  /// 客户端错误透明转发（GarnetClient 网络面 wedb::Error，Display 等于
  /// 原 `e.to_string()` 文本）
  #[error(transparent)]
  Client(#[from] crate::Error),

  /// 主端单帧 SNAPSHOT_DATA 收到非 OK 应答（C# ExecuteClusterSnapshotData
  /// 的 TransmitAsync 错误形态）
  #[error("Primary error at TransmitAsync {0}")]
  PrimaryResp(String),

  /// 帧级/往返级限时（文案即完整原文，各限时点措辞不一，不可归一）
  #[error("{0}")]
  Timeout(&'static str),

  /// 副本回传的复制位点无法解析（C# AofAddress.FromString 失败臂）
  #[error("invalid replication offset from replica: {0}")]
  InvalidReplicaOffset(String),

  /// 依赖资产未接线（store / 检查点目录等装配期注入缺席）
  #[error("{0} not wired")]
  NotWired(&'static str),

  /// 管理面未初始化（replication manager / cluster manager 缺席）
  #[error("{0} not initialized")]
  NotInitialized(&'static str),

  /// 协议违约/非法帧/非法参数（检查点接收面与恢复面的确定性拒绝臂）
  #[error("{0}")]
  Protocol(String),

  /// 同步编排描述性失败（一次性文案兜底：文案即原文，最终去向为日志与
  /// RESP -ERR，无变体级判定消费；收敛于此避免为单点文案立变体）
  #[error("{0}")]
  Sync(String),

  /// 监督任务 panic 转错（wbase supervise 的 panic 臂：保证 finally 收尾
  /// 必达的错误化承载，`stage` 为任务段名）
  #[error("replica {stage} panic: {text}")]
  Panic {
    /// 任务段名（attach / sync）
    stage: &'static str,
    /// panic 载荷文本
    text: String,
  },
}

impl ReplicationError {
  /// IOERR 家族构造单点：非 io 来源的库错误经 `io::Error::other` 包装
  /// （Display 透传底层库错误文本），与原字符串拼接逐字节一致
  pub(crate) fn io(
    context: impl fmt::Display,
    source: impl error::Error + Send + Sync + 'static,
  ) -> Self {
    Self::Io {
      context: context.to_string(),
      source: io::Error::other(source),
    }
  }
}
