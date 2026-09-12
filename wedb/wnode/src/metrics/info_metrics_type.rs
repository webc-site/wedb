//! Garnet 服务器暴露的信息段类别（对标 libs/common/Metrics/InfoMetricsType.cs）

/// Garnet 服务器暴露的信息段类别
///（对标 libs/common/Metrics/InfoMetricsType.cs:InfoMetricsType）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl TryFrom<u8> for InfoMetricsType {
  type Error = u8;
  #[inline]
  fn try_from(v: u8) -> Result<Self, Self::Error> {
    if (v as usize) < Self::ALL.len() {
      Ok(Self::ALL[v as usize])
    } else {
      Err(v)
    }
  }
}

impl From<InfoMetricsType> for u8 {
  #[inline]
  fn from(t: InfoMetricsType) -> Self {
    t as u8
  }
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

  /// 兼任 num_enum::TryFromPrimitive 语义
  #[inline]
  pub fn try_from_primitive(v: u8) -> Result<Self, u8> {
    Self::try_from(v)
  }

  /// 类别下标（数组索引便捷方法）。
  #[inline]
  #[must_use]
  pub const fn idx(self) -> usize {
    self as usize
  }

  /// C# 枚举成员名（INFO 段的线上写法）。
  #[inline]
  #[must_use]
  pub const fn as_cs_name(self) -> &'static str {
    const CS_NAMES: [&str; 16] = [
      "SERVER",
      "MEMORY",
      "CLUSTER",
      "REPLICATION",
      "STATS",
      "STORE",
      "STOREHASHTABLE",
      "STOREREVIV",
      "PERSISTENCE",
      "CLIENTS",
      "KEYSPACE",
      "MODULES",
      "BPSTATS",
      "CINFO",
      "HLOGSCAN",
      "COMMANDSTATS",
    ];
    CS_NAMES[self as usize]
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
  #[inline]
  #[must_use]
  pub const fn get_resp_formatted_info_section(
    info_metrics_type: InfoMetricsType,
  ) -> Option<&'static str> {
    const RESP_FORMATTED: [Option<&str>; 16] = [
      None, // Server = 0 (Default returns None)
      Some("$6\r\nMEMORY\r\n"),
      Some("$7\r\nCLUSTER\r\n"),
      Some("$11\r\nREPLICATION\r\n"),
      Some("$5\r\nSTATS\r\n"),
      Some("$5\r\nSTORE\r\n"),
      Some("$14\r\nSTOREHASHTABLE\r\n"),
      Some("$10\r\nSTOREREVIV\r\n"),
      Some("$11\r\nPERSISTENCE\r\n"),
      Some("$7\r\nCLIENTS\r\n"),
      Some("$8\r\nKEYSPACE\r\n"),
      Some("$7\r\nMODULES\r\n"),
      Some("$7\r\nBPSTATS\r\n"),
      Some("$5\r\nCINFO\r\n"),
      Some("$8\r\nHLOGSCAN\r\n"),
      Some("$12\r\nCOMMANDSTATS\r\n"),
    ];
    RESP_FORMATTED[info_metrics_type as usize]
  }
}
