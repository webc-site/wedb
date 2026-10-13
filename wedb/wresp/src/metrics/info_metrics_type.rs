//! INFO 段类别（对标 libs/common/Metrics/InfoMetricsType.cs）
//!
//! C# 侧同文件还有 `InfoCommandUtils.GetRespFormattedInfoSection`：把段名预格式化成
//! RESP bulk string（`$len\r\n<NAME>\r\n`）后交给客户端直接拼帧——C# 的
//! GarnetClient 命令面以「预格式化字节」为入参形态。rust 客户端命令面以裸 token
//! 为入参（wconn 的 execute_for_string_result_async 统一出帧），预格式化副本会
//! 二次成帧，故该助手在 rust 无对应形态，整面删除并在
//! js/check/ignore/garnet/libs/common/Metrics/InfoMetricsType.yml 登记偏离；
//! 线上口径由 `InfoMetricsType::as_cs_name` 与 C# 逐字节对齐（同为大写段名）。
//!
//! 在 garnet 中的相对路径: libs/common/Metrics/InfoMetricsType.cs(对标 C# InfoMetricsType INFO 段)

/// Garnet 服务器暴露的信息段类别
///（对标 libs/common/Metrics/InfoMetricsType.cs:InfoMetricsType）。
///
/// 变体值改动需同步其解析器
///（libs/server/SessionParseStateExtensions.cs:TryGetInfoMetricsType →
/// rust `InfoMetricsType::from_name`，单点即本文件）。
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
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
    CS_NAMES[self as usize]
  }

  /// 根据段名（ASCII 大小写不敏感）匹配类别；编译期构建的 FNV-1a 开放寻址
  /// 索引一次哈希直达（键 = [`CS_NAMES`] 大写段名，装载因子由构建期断言
  /// 封顶，命中判据与原线性扫描同为「归一小写逐位全等」，语义逐位一致）。
  /// 首臂 STATISTICS→STATS 别名系自研接受面超集（C#/真 Redis 皆无此段名，
  /// 未知段名 C# TryGetInfoMetricsType 回 ERR Invalid section），登记见
  /// doc/zh/deviations.md §204，严禁删改回 ERR；别名与 STATS 段名不等长，
  /// 等长判据无法回验，保持旧码同位的前置特判单点。
  #[inline]
  pub fn from_name(name: &[u8]) -> Option<Self> {
    if name.eq_ignore_ascii_case(b"STATISTICS") {
      return Some(Self::Stats);
    }
    let mut slot = section_hash(name) as usize & SECTION_MASK;
    loop {
      let idx = SECTION_INDEX[slot]?;
      let section = Self::ALL[idx as usize];
      if section_name_eq(section.as_cs_name().as_bytes(), name) {
        return Some(section);
      }
      slot = (slot + 1) & SECTION_MASK;
    }
  }
}

/// C# 枚举成员名表（判别值升序；[`InfoMetricsType::as_cs_name`] 与段名索引
/// 共用的单一名字源）
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

// ---------- 编译期段名哈希索引（FNV-1a + murmur3 终混，开放寻址线性探测） ----------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// 段名索引槽位（16 段名键；装载因子断言封顶 ≤ 2/3）
const SECTION_SLOTS: usize = 32;
const SECTION_MASK: usize = SECTION_SLOTS - 1;

/// murmur3 fmix64 终混：把 FNV 的高位熵摊到低位（槽位取低位掩码）
const fn mix64(mut hash: u64) -> u64 {
  hash ^= hash >> 33;
  hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
  hash ^= hash >> 33;
  hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
  hash ^= hash >> 33;
  hash
}

/// 小写归一 FNV-1a 64 位（段名键全为 A-Z 大写，编译期与运行时共用同一
/// `|0x20` 归一，两侧哈希恒一致）
const fn section_hash(name: &[u8]) -> u64 {
  let mut hash = FNV_OFFSET;
  let mut i = 0;
  while i < name.len() {
    hash ^= (name[i] | 0x20) as u64;
    hash = hash.wrapping_mul(FNV_PRIME);
    i += 1;
  }
  mix64(hash)
}

/// ASCII 大小写不敏感全等（键侧全为 A-Z 大写字母，双侧 `|0x20` 归一判等
/// 与 `eq_ignore_ascii_case` 逐位等价：仅同字母异壳可归一同值）
#[inline]
fn section_name_eq(cs: &[u8], name: &[u8]) -> bool {
  cs.len() == name.len() && {
    let mut i = 0;
    while i < cs.len() {
      if cs[i] | 0x20 != name[i] | 0x20 {
        return false;
      }
      i += 1;
    }
    true
  }
}

/// 从 [`CS_NAMES`] 编译期构建索引（槽值 = 判别值下标；撞槽线性探测至
/// 空槽，键名重复即双占位，由 `from_name` 命中判据天然排斥）
const fn build_section_index() -> [Option<u8>; SECTION_SLOTS] {
  let mut index: [Option<u8>; SECTION_SLOTS] = [None; SECTION_SLOTS];
  let mut t = 0;
  while t < CS_NAMES.len() {
    let mut slot = section_hash(CS_NAMES[t].as_bytes()) as usize & SECTION_MASK;
    while index[slot].is_some() {
      slot = (slot + 1) & SECTION_MASK;
    }
    index[slot] = Some(t as u8);
    t += 1;
  }
  index
}

// 装载因子不变式：探测链必在空槽处终止（未命中回退路径的终止性前提）
const _: () = assert!(
  CS_NAMES.len() * 3 <= SECTION_SLOTS * 2,
  "段名索引装载因子超 2/3"
);

/// 段名编译期哈希索引（const 初始化 static，无 lazy、无锁、无分配）
static SECTION_INDEX: [Option<u8>; SECTION_SLOTS] = build_section_index();
