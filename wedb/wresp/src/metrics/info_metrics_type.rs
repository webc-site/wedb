//! INFO 段类别（对标 libs/common/Metrics/InfoMetricsType.cs）
//!
//! C# 侧同文件还有 `InfoCommandUtils.GetRespFormattedInfoSection`：把段名预格式化成
//! RESP bulk string（`$len\r\n<NAME>\r\n`）后交给客户端直接拼帧——C# 的
//! GarnetClient 命令面以「预格式化字节」为入参形态。rust 客户端命令面以裸 token
//! 为入参（wconn 的 execute_for_string_result_async 统一出帧），预格式化副本会
//! 二次成帧，故该助手在 rust 无对应形态，整面删除并在
//! js/check/ignore/garnet/libs/common/Metrics/InfoMetricsType.yml 登记偏离；
//! 线上口径由 `InfoMetricsType::as_cs_name` 与 C# 逐字节对齐（同为大写段名）。

/// Garnet 服务器暴露的信息段类别
///（对标 libs/common/Metrics/InfoMetricsType.cs:InfoMetricsType）。
///
/// 变体值改动需同步其解析器
///（libs/server/SessionParseStateExtensions.cs:TryGetInfoMetricsType →
/// rust `InfoMetricsType::from_name`，单点即本文件）。
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

  /// 类别下标（数组索引便捷方法）。
  #[inline]
  #[must_use]
  pub const fn idx(self) -> usize {
    self as usize
  }

  /// C# 枚举成员名（INFO 段的线上写法，与 C#
  /// `InfoCommandUtils.GetRespFormattedInfoSection` 内 `$len\r\n<NAME>\r\n`
  /// 的 `<NAME>` 同串）。
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

  /// 根据段名（ASCII 大小写不敏感）匹配类别。
  #[inline]
  pub fn from_name(name: &[u8]) -> Option<Self> {
    if name.eq_ignore_ascii_case(b"STATISTICS") {
      return Some(Self::Stats);
    }
    Self::ALL
      .iter()
      .copied()
      .find(|t| t.as_cs_name().as_bytes().eq_ignore_ascii_case(name))
  }
}

#[cfg(test)]
mod tests {
  use super::InfoMetricsType;

  /// 段名解析（大小写不敏感 + STATISTICS 别名 + 未知段回 None）
  #[test]
  fn from_name_matches_cs_section_names() {
    assert_eq!(
      InfoMetricsType::from_name(b"server"),
      Some(InfoMetricsType::Server)
    );
    assert_eq!(
      InfoMetricsType::from_name(b"KEYSPACE"),
      Some(InfoMetricsType::Keyspace)
    );
    assert_eq!(
      InfoMetricsType::from_name(b"statistics"),
      Some(InfoMetricsType::Stats)
    );
    for t in InfoMetricsType::ALL {
      assert_eq!(
        InfoMetricsType::from_name(t.as_cs_name().as_bytes()),
        Some(t)
      );
    }
    assert_eq!(InfoMetricsType::from_name(b"nope"), None);
  }
}
