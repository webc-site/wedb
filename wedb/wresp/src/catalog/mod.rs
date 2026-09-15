//! RESP 协议元数据：命令目录 + ACL 分类位集
//!
//! 对标 libs/server/Resp/RespCommandsInfo.cs 中 ACL 消费的面与
//! libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories。C# 侧 ACL 经
//! 静态 `RespCommandsInfo` 取命令元数据（名称 / 父子关系 / 分类归属），数据
//! 单一真值为 Garnet.resources 内嵌 RespCommandsInfo.json；rust 侧以
//! [`entries`] OnceLock 运行时解析 wresources 内嵌的同一份 JSON 承接（对标
//! C# 静态初始化），全部查询为无锁单遍扫描——目录仅 353 条且 ACL 规则修改
//! 是管理频度操作。

use std::{str::FromStr, sync::OnceLock};

use bitflags::bitflags;
use sonic_rs::Deserialize;
use wresources::RESP_COMMANDS_INFO_JSON;

use crate::command::RespCommand;

/// 单条命令目录（RespCommandsInfo 中 ACL 消费的最小面）
pub struct CmdEntry {
  /// C# 枚举成员名（Enum.TryParse 对照，含下划线）
  pub cs: String,
  /// 展示名（info.Name 小写；子命令为 parent|sub 形式）
  pub name: String,
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

/// JSON 命令条目导入面（目录消费的最小字段）
#[derive(Deserialize)]
struct EntryImport {
  #[serde(rename = "Command")]
  command: String,
  #[serde(rename = "Name")]
  name: String,
  #[serde(rename = "AclCategories")]
  acl_categories: Option<String>,
  #[serde(rename = "SubCommands")]
  sub_commands: Option<Vec<EntryImport>>,
}

impl EntryImport {
  /// 先序压入（根 → 子命令，序即 JSON 声明序）
  ///
  /// 全量收录含 IsInternal 与 Name=SLAVEOF 历史别名条目，对标 C#
  /// AclCommandInfo 的全收构建（SLAVEOF 去重仅存在于 INFO 表扁平枚举索引，
  /// 因枚举键冲突而起，ACL 目录无此约束）
  fn push_entry(out: &mut Vec<CmdEntry>, entry: &Self, parent: Option<RespCommand>) {
    let cmd = RespCommand::from_str(&entry.command)
      .unwrap_or_else(|_| panic!("RespCommandsInfo.json 未知命令枚举名 {}", entry.command));
    let cats = match entry.acl_categories.as_deref() {
      Some(names) => RespAclCategories::from_member_names(names)
        .unwrap_or_else(|| panic!("RespCommandsInfo.json 未知 ACL 分类名 {}", entry.name)),
      None => RespAclCategories::empty(),
    };
    out.push(CmdEntry {
      cs: entry.command.clone(),
      name: entry.name.to_lowercase(),
      cmd,
      cats,
      parent,
    });
    for sub in entry.sub_commands.iter().flatten() {
      Self::push_entry(out, sub, Some(cmd));
    }
  }
}

/// 全量命令目录（根 + 子命令扁平表，序即 JSON 声明序）
///
/// C# 侧目录初始化单点为 RespCommandsInfo.cs:TryInitialize（完整对标见
/// wnode 的 try_initialize），此处为其 ACL 消费面的分层子集，不再声明映射
fn entries() -> &'static [CmdEntry] {
  static ENTRIES: OnceLock<Vec<CmdEntry>> = OnceLock::new();
  ENTRIES.get_or_init(|| {
    let roots: Vec<EntryImport> =
      sonic_rs::from_str(RESP_COMMANDS_INFO_JSON).expect("RespCommandsInfo.json 反序列化失败");
    let mut out = Vec::new();
    for root in &roots {
      EntryImport::push_entry(&mut out, root, None);
    }
    out
  })
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

/// 最后一个有效命令（除 INVALID 外的最大值，对标 C# LastValidCommand；
/// 单处定义于 [`crate::command`]，此处转导出）
///
/// 在 garnet 中的相对路径:libs/server/Resp/Parser/RespCommand.cs:LastValidCommand
pub use crate::command::LAST_VALID_COMMAND;

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
    CmdEntry, RespAclCategories, RespCommand, children_of, commands_for_category, entries,
    try_get_by_cs_name, try_get_resp_command_info,
  };

  /// 目录全量锚定：260 根 + 93 子 = 353 条（含 4 条内部根命令与两条
  /// SECONDARYOF 历史别名条目，逐条对照内嵌 JSON）
  #[test]
  fn catalog_size_and_aliases() {
    assert_eq!(entries().len(), 353);
    assert_eq!(entries().iter().filter(|e| e.parent.is_none()).count(), 260);
    assert_eq!(entries().iter().filter(|e| e.parent.is_some()).count(), 93);

    // 历史别名：SECONDARYOF 枚举承载 SECONDARYOF 与 SLAVEOF 两个展示名
    let aliases: Vec<&CmdEntry> = entries()
      .iter()
      .filter(|e| e.cmd == RespCommand::Secondaryof)
      .collect();
    assert_eq!(aliases.len(), 2);
    assert!(aliases.iter().any(|e| e.name == "secondaryof"));
    assert!(aliases.iter().any(|e| e.name == "slaveof"));

    // 扁平序即 JSON 声明序（先序：根紧随其子命令）
    let head: Vec<&str> = entries().iter().take(3).map(|e| e.name.as_str()).collect();
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
