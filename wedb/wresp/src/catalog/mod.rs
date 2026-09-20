//! RESP 协议命令目录单点：内嵌 JSON 的一次反序列化 + 全部静态索引
//!
//! 对标 libs/server/Resp/RespCommandsInfo.cs（静态构造单次反序列化，全进程
//! 一份）：数据单一真值为 wresp 内嵌 RespCommandsInfo.json，由本模块
//! [`try_initialize`] 经 [`data_provider`] 校验导入并一次性反序列化，产物
//! 同时喂两张消费面——[`commands_info`] 的完整元数据索引（按名 / 按枚举 /
//! ACL 类别 / KeySpec）与本模块的 ACL 目录（356 条 [`CmdEntry`] 扁平表，
//! 对标 libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories 的消费
//! 面）。域类型字符串字段为 `&'static str`（解析期一次定型），初始化失败
//! （JSON 损坏 / 枚举名不可解析）即空表 + 查询 None，对标 C# TryInitialize
//! 的 false 语义。

use std::sync::OnceLock;

use bitflags::bitflags;

use crate::command::RespCommand;

/// 命令元数据（C# Garnet.resources:RespCommandsInfo.json）
pub const RESP_COMMANDS_INFO_JSON: &str = include_str!("../../RespCommandsInfo.json");

/// 命令文档（C# Garnet.resources:RespCommandsDocs.json）
pub const RESP_COMMANDS_DOCS_JSON: &str = include_str!("../../RespCommandsDocs.json");

pub mod commands_info;
pub mod data_provider;
pub mod simplified;

pub use commands_info::{
  RespCommandFlags, RespCommandsInfo, RespCommandsTables, get_resp_command_name,
  try_fast_get_resp_command_info, try_get_commandsfor_acl_category,
  try_get_resp_command_info_by_cmd, try_get_resp_command_info_by_name, try_get_resp_command_names,
  try_get_resp_commands_info, try_get_resp_commands_info_count, try_get_resp_sub_commands_info,
  try_get_simple_resp_command_info,
};
use commands_info::{RespCommandsInfoImport, build_tables};
pub use data_provider::{
  DefaultRespCommandsDataProvider, IRespCommandData, get_resp_commands_data_provider,
  try_import_resp_commands_data,
};
pub use simplified::{
  INLINE_KEYS, SimpleRespCommandInfo, SimpleRespKeySpec, SimpleRespKeySpecBeginSearch,
  SimpleRespKeySpecFindKeys, extract_keys_and_flags_from_slice, extract_keys_from_slice,
  populate_simple_command_info, try_get_simple_key_spec,
};

/// 单条命令目录（RespCommandsInfo 中 ACL 消费的最小面）
pub struct CmdEntry {
  /// C# 枚举成员名（Enum.TryParse 对照，含下划线）
  pub cs: &'static str,
  /// 展示名（info.Name 小写；子命令为 parent|sub 形式）
  pub name: &'static str,
  /// 对应 RespCommand
  pub cmd: RespCommand,
  /// ACL 分类位集
  pub cats: RespAclCategories,
  /// 父命令（仅子命令有）
  pub parent: Option<RespCommand>,
}

bitflags! {
  /// RESP ACL 分类位集
  ///
  /// libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct RespAclCategories: u32 {
    /// 管理
    const ADMIN = 1;
    /// 位图
    const BITMAP = 1 << 1;
    /// 阻塞
    const BLOCKING = 1 << 2;
    /// 连接
    const CONNECTION = 1 << 3;
    /// 危险
    const DANGEROUS = 1 << 4;
    /// 地理
    const GEO = 1 << 5;
    /// 哈希
    const HASH = 1 << 6;
    /// HyperLogLog
    const HYPERLOGLOG = 1 << 7;
    /// 快速
    const FAST = 1 << 8;
    /// 键空间
    const KEYSPACE = 1 << 9;
    /// 列表
    const LIST = 1 << 10;
    /// 发布订阅
    const PUBSUB = 1 << 11;
    /// 读
    const READ = 1 << 12;
    /// 脚本
    const SCRIPTING = 1 << 13;
    /// 集合
    const SET = 1 << 14;
    /// 有序集合
    const SORTEDSET = 1 << 15;
    /// 慢速
    const SLOW = 1 << 16;
    /// 流
    const STREAM = 1 << 17;
    /// 字符串
    const STRING = 1 << 18;
    /// 事务
    const TRANSACTION = 1 << 19;
    /// 写
    const WRITE = 1 << 20;
    /// Garnet 扩展
    const GARNET = 1 << 21;
    /// 自定义命令
    const CUSTOM = 1 << 22;
    /// 向量集
    const VECTOR = 1 << 23;
    /// 全部（C# All = (Vector << 1) - 1，覆盖全部单类别位）
    const ALL = (1 << 24) - 1;
  }
}

impl RespAclCategories {
  /// 成员名串解析（"Fast, String, Write"；大小写不敏感，未知名即失败）
  ///
  /// libs/server/Resp/RespCommandsInfo.cs:AclCategories 反序列化
  /// （C# JsonStringEnumConverter 语义）
  pub fn from_member_names(names: &str) -> Option<Self> {
    // C# 枚举成员名 → 位（声明序）
    const ALL: [(u32, &str); 24] = [
      (RespAclCategories::ADMIN.bits(), "ADMIN"),
      (RespAclCategories::BITMAP.bits(), "BITMAP"),
      (RespAclCategories::BLOCKING.bits(), "BLOCKING"),
      (RespAclCategories::CONNECTION.bits(), "CONNECTION"),
      (RespAclCategories::DANGEROUS.bits(), "DANGEROUS"),
      (RespAclCategories::GEO.bits(), "GEO"),
      (RespAclCategories::HASH.bits(), "HASH"),
      (RespAclCategories::HYPERLOGLOG.bits(), "HYPERLOGLOG"),
      (RespAclCategories::FAST.bits(), "FAST"),
      (RespAclCategories::KEYSPACE.bits(), "KEYSPACE"),
      (RespAclCategories::LIST.bits(), "LIST"),
      (RespAclCategories::PUBSUB.bits(), "PUBSUB"),
      (RespAclCategories::READ.bits(), "READ"),
      (RespAclCategories::SCRIPTING.bits(), "SCRIPTING"),
      (RespAclCategories::SET.bits(), "SET"),
      (RespAclCategories::SORTEDSET.bits(), "SORTEDSET"),
      (RespAclCategories::SLOW.bits(), "SLOW"),
      (RespAclCategories::STREAM.bits(), "STREAM"),
      (RespAclCategories::STRING.bits(), "STRING"),
      (RespAclCategories::TRANSACTION.bits(), "TRANSACTION"),
      (RespAclCategories::WRITE.bits(), "WRITE"),
      (RespAclCategories::GARNET.bits(), "GARNET"),
      (RespAclCategories::CUSTOM.bits(), "CUSTOM"),
      (RespAclCategories::VECTOR.bits(), "VECTOR"),
    ];
    let mut bits = 0u32;
    for name in names.split(',') {
      let trimmed = name.trim().to_ascii_uppercase();
      let bit = ALL
        .iter()
        .find(|(_, member)| *member == trimmed)
        .map(|(bit, _)| *bit)?;
      bits |= bit;
    }
    Some(Self::from_bits_retain(bits))
  }
}

// —— 单点编排（一次导入 → 完整表 + ACL 目录） ——

/// 单点初始化产物（完整元数据索引 + ACL 目录投影）
struct Catalog {
  tables: RespCommandsTables,
  entries: Vec<CmdEntry>,
}

static CATALOG: OnceLock<Option<Catalog>> = OnceLock::new();

/// 导入 + 校验 + 域转换（JSON 文本 → 先序域类型树）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryInitializeRespCommandsInfo
fn try_initialize_resp_commands_info() -> Option<Vec<RespCommandsInfo>> {
  let imported = try_import_resp_commands_data::<RespCommandsInfoImport>(RESP_COMMANDS_INFO_JSON)?;
  RespCommandsInfoImport::convert_all(imported)
}

/// 一次导入 + 反序列化 + 双面构建（全进程仅此一处解析 RespCommandsInfo.json）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryInitialize
fn init_catalog() -> Option<Catalog> {
  let roots = try_initialize_resp_commands_info()?;
  Some(Catalog {
    tables: build_tables(&roots),
    entries: project_entries(&roots),
  })
}

/// 目录初始化（幂等；对标 C# 静态构造的首次访问触发）
pub(crate) fn try_initialize() -> bool {
  CATALOG.get_or_init(init_catalog).is_some()
}

/// 已初始化的完整元数据索引
pub(crate) fn tables() -> Option<&'static RespCommandsTables> {
  try_initialize();
  CATALOG.get().and_then(|c| c.as_ref()).map(|c| &c.tables)
}

/// 全量命令目录（根 + 子命令扁平表，序即 JSON 声明序先序）
fn entries() -> &'static [CmdEntry] {
  try_initialize();
  CATALOG
    .get()
    .and_then(|c| c.as_ref())
    .map(|c| c.entries.as_slice())
    .unwrap_or(&[])
}

/// ACL 目录投影：完整树先序遍历（根 → 子命令），全量收录含 IsInternal 与
/// Name=SLAVEOF 历史别名条目，对标 C# AclCommandInfo 的全收构建（SLAVEOF
/// 去重仅存在于 INFO 表扁平枚举索引，因枚举键冲突而起，ACL 目录无此约束）
fn project_entries(roots: &[RespCommandsInfo]) -> Vec<CmdEntry> {
  fn push(out: &mut Vec<CmdEntry>, entry: &RespCommandsInfo, parent: Option<RespCommand>) {
    let cs: &'static str = entry.command.into();
    out.push(CmdEntry {
      cs,
      name: commands_info::static_str(entry.name.to_lowercase()),
      cmd: entry.command,
      cats: entry.acl_categories,
      parent,
    });
    for sub in &entry.sub_commands {
      push(out, sub, Some(entry.command));
    }
  }
  let mut out = Vec::new();
  for root in roots {
    push(&mut out, root, None);
  }
  out
}

/// SET 的 ACL 展开集（对标 C# ExpandedSET）
const EXPANDED_SET: [RespCommand; 4] = [
  RespCommand::Setexnx,
  RespCommand::Setexxx,
  RespCommand::Setkeepttl,
  RespCommand::Setkeepttlxx,
];

/// BITOP 的 ACL 展开集（对标 C# ExpandedBITOP）
const EXPANDED_BITOP: [RespCommand; 5] = [
  RespCommand::BitopAnd,
  RespCommand::BitopNot,
  RespCommand::BitopOr,
  RespCommand::BitopXor,
  RespCommand::BitopDiff,
];

/// 把"并非真实命令"的枚举值归一到 ACL 等价命令
///
/// libs/server/Resp/Parser/RespCommand.cs:NormalizeForACLs
#[inline]
pub const fn normalize_for_acls(cmd: RespCommand) -> RespCommand {
  match cmd {
    RespCommand::Setexnx
    | RespCommand::Setexxx
    | RespCommand::Setkeepttl
    | RespCommand::Setkeepttlxx => RespCommand::Set,
    RespCommand::BitopAnd
    | RespCommand::BitopNot
    | RespCommand::BitopOr
    | RespCommand::BitopXor
    | RespCommand::BitopDiff => RespCommand::Bitop,
    _ => cmd,
  }
}

/// 反向于 [`normalize_for_acls`]：给出 `cmd` 覆盖的全部等价命令
///
/// libs/server/Resp/Parser/RespCommand.cs:ExpandForACLs
#[inline]
pub fn expand_for_acls(cmd: RespCommand) -> &'static [RespCommand] {
  match cmd {
    RespCommand::Set => &EXPANDED_SET,
    RespCommand::Bitop => &EXPANDED_BITOP,
    _ => &[],
  }
}

/// 未认证也可执行的命令（AUTH..=QUIT 连续区间）
///
/// libs/server/Resp/Parser/RespCommand.cs:IsNoAuth
#[inline]
pub const fn is_no_auth(cmd: RespCommand) -> bool {
  // 无符号化做区间判断：低于 AUTH 的值下溢成大数，天然出界
  let v = (cmd as u16).wrapping_sub(RespCommand::Auth as u16);
  v <= (RespCommand::Quit as u16).wrapping_sub(RespCommand::Auth as u16)
}

/// 按 RespCommand 取 ACL 目录条目（含子命令条目）
#[inline]
pub fn try_get_resp_command_info(cmd: RespCommand) -> Option<&'static CmdEntry> {
  entries().iter().find(|e| e.cmd == cmd)
}

/// 按命令名（大小写不敏感）取目录条目
///
/// 覆盖 Enum.TryParse(ignoreCase) 的对照（`cs` 即 C# 枚举成员名）
#[inline]
pub fn try_get_by_cs_name(name: &str) -> Option<&'static CmdEntry> {
  entries().iter().find(|e| e.cs.eq_ignore_ascii_case(name))
}

/// 根命令的全部子命令条目
#[inline]
pub fn children_of(cmd: RespCommand) -> impl Iterator<Item = &'static CmdEntry> {
  entries().iter().filter(move |e| e.parent == Some(cmd))
}

/// 分类成员条目（对标 C# AclCommandInfo 字典查询；复合分类按单类别位并集）
#[inline]
pub fn commands_for_category(acl: RespAclCategories) -> impl Iterator<Item = &'static CmdEntry> {
  entries().iter().filter(move |e| e.cats.intersects(acl))
}

#[cfg(test)]
mod tests {
  use super::{
    CmdEntry, RespAclCategories, children_of, commands_for_category, entries, try_get_by_cs_name,
    try_get_resp_command_info, try_initialize,
  };
  use crate::command::RespCommand;

  /// 目录全量锚定：259 根 + 94 子 = 353 条（含 1 条内部根命令与两条
  /// SECONDARYOF 历史别名条目，逐条对照内嵌 JSON；相对 C# 356 条：删
  /// module/module|loadcs/registercs 三条模块加载命令、删 customtxn/
  /// customrawstringcmd/customprocedure 三条占位内部命令、增
  /// cluster|flushall_ns 与 ri.count 与 sunsubscribe 三条 rust 扩展）
  #[test]
  fn catalog_size_and_aliases() {
    assert!(try_initialize());
    assert_eq!(entries().len(), 353);
    assert_eq!(entries().iter().filter(|e| e.parent.is_none()).count(), 259);
    assert_eq!(entries().iter().filter(|e| e.parent.is_some()).count(), 94);

    // 历史别名：SECONDARYOF 枚举承载 SECONDARYOF 与 SLAVEOF 两个展示名
    let aliases: Vec<&CmdEntry> = entries()
      .iter()
      .filter(|e| e.cmd == RespCommand::Secondaryof)
      .collect();
    assert_eq!(aliases.len(), 2);
    assert!(aliases.iter().any(|e| e.name == "secondaryof"));
    assert!(aliases.iter().any(|e| e.name == "slaveof"));

    // 扁平序即 JSON 声明序（先序：根紧随其子命令）
    let head: Vec<&str> = entries().iter().take(3).map(|e| e.name).collect();
    assert_eq!(head, ["acl", "acl|cat", "acl|deluser"]);
  }

  /// ACL 消费面抽查（分类 / 父子关系 / 大小写不敏感枚举名对照）
  #[test]
  fn acl_lookup() {
    let acl = try_get_resp_command_info(RespCommand::Acl).unwrap();
    assert_eq!(acl.cs, "ACL");
    assert_eq!(acl.name, "acl");
    assert_eq!(acl.cats, RespAclCategories::SLOW);
    assert_eq!(acl.parent, None);

    // Enum.TryParse(ignoreCase) 对照
    let cat = try_get_by_cs_name("acl_cat").unwrap();
    assert_eq!(cat.cmd, RespCommand::AclCat);
    assert_eq!(cat.parent, Some(RespCommand::Acl));
    assert_eq!(cat.cats, RespAclCategories::SLOW);

    // DELUSER = Admin + Dangerous + Slow
    let deluser = try_get_by_cs_name("ACL_DELUSER").unwrap();
    assert_eq!(
      deluser.cats,
      RespAclCategories::ADMIN | RespAclCategories::DANGEROUS | RespAclCategories::SLOW
    );

    // 根命令的全部子命令（10 个）
    assert_eq!(children_of(RespCommand::Acl).count(), 10);

    // rust 自增命令 SUNSUBSCRIBE（位 370，C# 目录无对位）：入册后 ACL 侧
    // 才有按名与按 +@pubsub 类别的置位入口
    let sunsub = try_get_by_cs_name("SUNSUBSCRIBE").unwrap();
    assert_eq!(sunsub.cs, "SUNSUBSCRIBE");
    assert_eq!(sunsub.name, "sunsubscribe");
    assert_eq!(sunsub.cmd, RespCommand::Sunsubscribe);
    assert_eq!(sunsub.parent, None);
    assert!(sunsub.cats.contains(RespAclCategories::PUBSUB));

    // 未知名
    assert!(try_get_by_cs_name("NO_SUCH_COMMAND").is_none());
  }

  /// 分类 → 命令（C# TryGetCommandsforAclCategory 单类别位语义）
  #[test]
  fn category_members() {
    let bitmap: Vec<&CmdEntry> = commands_for_category(RespAclCategories::BITMAP).collect();
    assert!(bitmap.iter().any(|e| e.name == "setbit"));
    // 全类别覆盖全部条目
    assert_eq!(
      commands_for_category(RespAclCategories::ALL).count(),
      entries().len()
    );
  }

  /// 成员名串解析（C# JsonStringEnumConverter 语义）
  #[test]
  fn member_names_parse() {
    let cats = RespAclCategories::from_member_names("Fast, String, Write").unwrap();
    assert_eq!(
      cats,
      RespAclCategories::FAST | RespAclCategories::STRING | RespAclCategories::WRITE
    );
    assert!(RespAclCategories::from_member_names("Nope").is_none());
  }
}
