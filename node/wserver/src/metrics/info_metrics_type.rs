/// Garnet 服务器暴露的信息段类别
///（对标 libs/common/Metrics/InfoMetricsType.cs:InfoMetricsType）。
///
/// 判别值与 C# 一致；改动须同步 SessionParseStateExtensions.TryGetInfoMetricsType。
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum InfoMetricsType {
  /// Server info
  Server = 0,
  /// Memory info
  Memory = 1,
  /// Cluster info
  Cluster = 2,
  /// Replication info
  Replication = 3,
  /// Stats info
  Stats = 4,
  /// Store info
  Store = 5,
  /// Store hash table info
  StoreHashtable = 6,
  /// Store revivification info
  StoreReviv = 7,
  /// Persistence information
  Persistence = 8,
  /// Clients connections stats
  Clients = 9,
  /// Database related stats
  Keyspace = 10,
  /// Modules info
  Modules = 11,
  /// Shared buffer pool stats
  BpStats = 12,
  /// Checkpoint information used for cluster
  CInfo = 13,
  /// Scan and return distribution of in-memory portion of hybrid logs
  HlogScan = 14,
  /// Per-command usage statistics (calls, failures, rejections)
  CommandStats = 15,
}

impl InfoMetricsType {
  /// 全部已声明成员，按判别值升序（对齐 Enum.GetValues）。
  pub const ALL: [InfoMetricsType; 16] = [
    Self::Server,
    Self::Memory,
    Self::Cluster,
    Self::Replication,
    Self::Stats,
    Self::Store,
    Self::StoreHashtable,
    Self::StoreReviv,
    Self::Persistence,
    Self::Clients,
    Self::Keyspace,
    Self::Modules,
    Self::BpStats,
    Self::CInfo,
    Self::HlogScan,
    Self::CommandStats,
  ];

  /// 类别下标（数组索引便捷方法）。
  #[inline]
  pub fn idx(self) -> usize {
    self as usize
  }

  /// C# 枚举成员名（INFO 段的线上写法）。
  pub fn as_cs_name(self) -> &'static str {
    match self {
      Self::Server => "SERVER",
      Self::Memory => "MEMORY",
      Self::Cluster => "CLUSTER",
      Self::Replication => "REPLICATION",
      Self::Stats => "STATS",
      Self::Store => "STORE",
      Self::StoreHashtable => "STOREHASHTABLE",
      Self::StoreReviv => "STOREREVIV",
      Self::Persistence => "PERSISTENCE",
      Self::Clients => "CLIENTS",
      Self::Keyspace => "KEYSPACE",
      Self::Modules => "MODULES",
      Self::BpStats => "BPSTATS",
      Self::CInfo => "CINFO",
      Self::HlogScan => "HLOGSCAN",
      Self::CommandStats => "COMMANDSTATS",
    }
  }
}

/// INFO 命令工具
///（对标 libs/common/Metrics/InfoMetricsType.cs:InfoCommandUtils）。
pub struct InfoCommandUtils;

impl InfoCommandUtils {
  /// libs/common/Metrics/InfoMetricsType.cs:GetRespFormattedInfoSection
  ///
  /// 返回 RESP 预格式化的段名（`$len\r\n<NAME>\r\n`）；
  /// Default（判别值 0，即 SERVER）返回 None（对齐 C# 返回 null）。
  pub fn get_resp_formatted_info_section(
    info_metrics_type: InfoMetricsType,
  ) -> Option<&'static str> {
    // C# 以 infoMetricsType == default 判定，判别值 0 即 SERVER。
    const FORMATTED: [(InfoMetricsType, &str); 15] = [
      (InfoMetricsType::Memory, "$6\r\nMEMORY\r\n"),
      (InfoMetricsType::Cluster, "$7\r\nCLUSTER\r\n"),
      (InfoMetricsType::Replication, "$11\r\nREPLICATION\r\n"),
      (InfoMetricsType::Stats, "$5\r\nSTATS\r\n"),
      (InfoMetricsType::Store, "$5\r\nSTORE\r\n"),
      (InfoMetricsType::StoreHashtable, "$14\r\nSTOREHASHTABLE\r\n"),
      (InfoMetricsType::StoreReviv, "$10\r\nSTOREREVIV\r\n"),
      (InfoMetricsType::Persistence, "$11\r\nPERSISTENCE\r\n"),
      (InfoMetricsType::Clients, "$7\r\nCLIENTS\r\n"),
      (InfoMetricsType::Keyspace, "$8\r\nKEYSPACE\r\n"),
      (InfoMetricsType::Modules, "$7\r\nMODULES\r\n"),
      (InfoMetricsType::BpStats, "$7\r\nBPSTATS\r\n"),
      (InfoMetricsType::CInfo, "$5\r\nCINFO\r\n"),
      (InfoMetricsType::HlogScan, "$8\r\nHLOGSCAN\r\n"),
      (InfoMetricsType::CommandStats, "$12\r\nCOMMANDSTATS\r\n"),
    ];
    if info_metrics_type == InfoMetricsType::Server {
      return None;
    }
    FORMATTED
      .iter()
      .find(|(t, _)| *t == info_metrics_type)
      .map(|(_, s)| *s)
  }
}
