/// 延迟指标类别
///（对标 libs/common/Metrics/LatencyMetricsType.cs:LatencyMetricsType）。
///
/// 判别值与 C# 一致；改动须同步 SessionParseStateExtensions.TryGetLatencyMetricsType。
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum LatencyMetricsType {
  /// 每次网络接收调用的处理延迟（服务端）：仅含至少处理一个请求的调用，
  /// 自包内首请求开始处理起，至末请求处理完成（含响应发送）止。
  NetRsLat = 0,
  /// 待处理请求完成延迟。
  PendingLat = 1,
  /// 事务处理延迟。
  TxProcLat = 2,
  /// 每次网络接收调用的字节数。
  NetRsBytes = 3,
  /// 每次网络接收调用的操作数。
  NetRsOps = 4,
  /// 每次网络接收调用的处理延迟（服务端）：仅含至少一个非管理请求的批次。
  NetRsLatAdmin = 5,
}

impl LatencyMetricsType {
  /// 全部成员，按 C# 声明顺序（`Enum.GetValues` 顺序：0,1,2,3,4,5）。
  pub const ALL: [LatencyMetricsType; 6] = [
    Self::NetRsLat,
    Self::PendingLat,
    Self::TxProcLat,
    Self::NetRsBytes,
    Self::NetRsOps,
    Self::NetRsLatAdmin,
  ];

  /// 该类别是否以 Stopwatch tick 计（否则为直接上报值）。
  ///（对齐 GarnetLatencyMetrics.defaultLatencyTypesTicks = [true, true, true, false, false, true]）
  pub const IS_TICKS: [bool; 6] = [true, true, true, false, false, true];

  /// 类别下标（数组索引便捷方法）。
  #[inline]
  pub fn idx(self) -> usize {
    self as usize
  }

  /// C# 枚举成员名（LATENCY 线上事件类别写法），如 "NET_RS_LAT"。
  pub fn cs_name(self) -> &'static str {
    match self {
      Self::NetRsLat => "NET_RS_LAT",
      Self::PendingLat => "PENDING_LAT",
      Self::TxProcLat => "TX_PROC_LAT",
      Self::NetRsBytes => "NET_RS_BYTES",
      Self::NetRsOps => "NET_RS_OPS",
      Self::NetRsLatAdmin => "NET_RS_LAT_ADMIN",
    }
  }
}
