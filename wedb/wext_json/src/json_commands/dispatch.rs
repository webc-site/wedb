use wcustom::{CommandType, CustomObjectFns, KeyScope};
use wval::CustomObjectType;

use super::JsonCommands;

/// JSON 命令枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonCommand {
  Set,
  Get,
  Del,
  Forget,
  Type,
  MGet,
  NumIncrBy,
  NumMultBy,
  Toggle,
  StrAppend,
  StrLen,
  ArrLen,
  ArrAppend,
  ArrPop,
  ArrIndex,
  ArrInsert,
  ArrTrim,
  ObjKeys,
  ObjLen,
  Clear,
  Resp,
}

/// 命令静态清单表项
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonCommandInfo {
  pub name: &'static str,
  pub arity: i32,
  pub acl_categories: &'static [&'static str],
  pub summary: &'static str,
}

/// 编译期命令清单：本表全 21 条 = 2 对位 + 19 扩展。对位的 2 条（SET / GET）
/// 对标 modules/GarnetJSON/JsonModule.cs 的 OnLoad 注册面（C# 仅 RegisterCommand
/// JSON.SET / JSON.GET 两条）；余下 19 条为 RedisJSON 兼容扩展面，garnet 无对位。
pub const COMMAND_INFOS: &[JsonCommandInfo] = &[
  JsonCommandInfo {
    name: JsonCommand::Set.name(),
    arity: JsonCommand::Set.arity(),
    acl_categories: &["write", "json"],
    summary: "Set the JSON value at path in key",
  },
  JsonCommandInfo {
    name: JsonCommand::Get.name(),
    arity: JsonCommand::Get.arity(),
    acl_categories: &["read", "json"],
    summary: "Get the JSON value at path in key",
  },
  JsonCommandInfo {
    name: JsonCommand::Del.name(),
    arity: JsonCommand::Del.arity(),
    acl_categories: &["write", "json"],
    summary: "Delete a value",
  },
  JsonCommandInfo {
    name: JsonCommand::Forget.name(),
    arity: JsonCommand::Forget.arity(),
    acl_categories: &["write", "json"],
    summary: "Delete a value (alias for JSON.DEL)",
  },
  JsonCommandInfo {
    name: JsonCommand::Type.name(),
    arity: JsonCommand::Type.arity(),
    acl_categories: &["read", "json"],
    summary: "Report the type of JSON value at path",
  },
  JsonCommandInfo {
    name: JsonCommand::MGet.name(),
    arity: JsonCommand::MGet.arity(),
    acl_categories: &["read", "json"],
    summary: "Get the JSON values at path from multiple keys",
  },
  JsonCommandInfo {
    name: JsonCommand::NumIncrBy.name(),
    arity: JsonCommand::NumIncrBy.arity(),
    acl_categories: &["write", "json"],
    summary: "Increment the numeric value at path by a number",
  },
  JsonCommandInfo {
    name: JsonCommand::NumMultBy.name(),
    arity: JsonCommand::NumMultBy.arity(),
    acl_categories: &["write", "json"],
    summary: "Multiply the numeric value at path by a number",
  },
  JsonCommandInfo {
    name: JsonCommand::Toggle.name(),
    arity: JsonCommand::Toggle.arity(),
    acl_categories: &["write", "json"],
    summary: "Toggle a boolean value at path",
  },
  JsonCommandInfo {
    name: JsonCommand::StrAppend.name(),
    arity: JsonCommand::StrAppend.arity(),
    acl_categories: &["write", "json"],
    summary: "Append a string to a JSON string value at path",
  },
  JsonCommandInfo {
    name: JsonCommand::StrLen.name(),
    arity: JsonCommand::StrLen.arity(),
    acl_categories: &["read", "json"],
    summary: "Report the length of the JSON string at path in key",
  },
  JsonCommandInfo {
    name: JsonCommand::ArrLen.name(),
    arity: JsonCommand::ArrLen.arity(),
    acl_categories: &["read", "json"],
    summary: "Report the length of the JSON array at path in key",
  },
  JsonCommandInfo {
    name: JsonCommand::ArrAppend.name(),
    arity: JsonCommand::ArrAppend.arity(),
    acl_categories: &["write", "json"],
    summary: "Append one or more values to the JSON array at path",
  },
  JsonCommandInfo {
    name: JsonCommand::ArrPop.name(),
    arity: JsonCommand::ArrPop.arity(),
    acl_categories: &["write", "json"],
    summary: "Remove and return an element from the JSON array at path",
  },
  JsonCommandInfo {
    name: JsonCommand::ArrIndex.name(),
    arity: JsonCommand::ArrIndex.arity(),
    acl_categories: &["read", "json"],
    summary: "Search for the first occurrence of a scalar JSON value in an array",
  },
  JsonCommandInfo {
    name: JsonCommand::ArrInsert.name(),
    arity: JsonCommand::ArrInsert.arity(),
    acl_categories: &["write", "json"],
    summary: "Insert one or more values into the JSON array at path",
  },
  JsonCommandInfo {
    name: JsonCommand::ArrTrim.name(),
    arity: JsonCommand::ArrTrim.arity(),
    acl_categories: &["write", "json"],
    summary: "Trim an array so that it contains only the specified inclusive range of elements",
  },
  JsonCommandInfo {
    name: JsonCommand::ObjKeys.name(),
    arity: JsonCommand::ObjKeys.arity(),
    acl_categories: &["read", "json"],
    summary: "Return the keys in the object that's referenced by the path",
  },
  JsonCommandInfo {
    name: JsonCommand::ObjLen.name(),
    arity: JsonCommand::ObjLen.arity(),
    acl_categories: &["read", "json"],
    summary: "Report the number of keys in the JSON object at path in key",
  },
  JsonCommandInfo {
    name: JsonCommand::Clear.name(),
    arity: JsonCommand::Clear.arity(),
    acl_categories: &["write", "json"],
    summary: "Clear container values (arrays/objects) and set numeric values to 0",
  },
  JsonCommandInfo {
    name: JsonCommand::Resp.name(),
    arity: JsonCommand::Resp.arity(),
    acl_categories: &["read", "json"],
    summary: "Return the JSON value at path in Redis Serialization Protocol (RESP) format",
  },
];

/// 注册名查询面：本函数是 [`JsonCommand::match_command`] 的布尔投影（命令名唯一
/// 权威为枚举 [`JsonCommand::name`]）。C# 的 modules/GarnetJSON/JsonModule.cs:OnLoad
/// 只注册 SET / GET 两条，此处覆盖 rust 侧全部 21 条编译期静态名单（2 对位 + 19 扩展）；
/// server 层 ACL 门已改走 wnode 编译期清单单点，此处只承扩展 crate 自身的注册名查询面
pub fn is_command_registered(name: &str) -> bool {
  JsonCommand::match_command(name.as_bytes()).is_some()
}

impl JsonCommand {
  /// 命令全集（枚举单源权威表：按名解析与 [`COMMAND_INFOS`] 目录共用的
  /// 唯一名单，新增一条命令只在此加一项）
  pub const ALL: &[Self] = &[
    Self::Set,
    Self::Get,
    Self::Del,
    Self::Forget,
    Self::Type,
    Self::MGet,
    Self::NumIncrBy,
    Self::NumMultBy,
    Self::Toggle,
    Self::StrAppend,
    Self::StrLen,
    Self::ArrLen,
    Self::ArrAppend,
    Self::ArrPop,
    Self::ArrIndex,
    Self::ArrInsert,
    Self::ArrTrim,
    Self::ObjKeys,
    Self::ObjLen,
    Self::Clear,
    Self::Resp,
  ];

  /// Redis TYPE 应答类型串（C# modules 注册名，对标
  /// JsonModule.cs:20 `context.Initialize("GarnetJSON", 1)`；C# HandleType
  /// 对 custom object 无 default 臂输出零字节 quirk，rust 修复为回注册名，
  /// TYPE 与 EXISTS 存活口径一致）
  pub const OBJECT_TYPE_NAME: &str = "GarnetJSON";

  /// 扩展对象静态描述清单项（wnode 编译期清单单条；标签、TYPE 注册名、
  /// 按名解析入口、堆估算入口一处描述，对标 CustomObjectFactory 工厂集中
  /// 持有形态与 GarnetJsonObject 的 IHeapObject.HeapMemorySize 恒定常数）
  pub const OBJECT_ENTRY: wcustom::CustomObjectEntry = wcustom::CustomObjectEntry {
    tag: CustomObjectType::Json,
    type_name: Self::OBJECT_TYPE_NAME,
    match_command: Self::match_command_meta,
    heap_estimate: crate::heap_estimate,
  };

  /// 按名解析 → 描述清单命令元数据（标签由清单项 [`Self::OBJECT_ENTRY`]
  /// 单点附带，命令元数据只承载执行面）
  fn match_command_meta(name: &[u8]) -> Option<wcustom::CustomCommandMeta> {
    Self::match_command(name).map(|cmd| wcustom::CustomCommandMeta {
      name: cmd.name(),
      command_type: cmd.command_type(),
      key_scope: cmd.key_scope(),
      arity: cmd.arity(),
      fns: cmd.fns(),
    })
  }

  /// 按名匹配（大小写不敏感；命令名唯一权威为 [`Self::name`]）。对位 C#
  /// modules/GarnetJSON/JsonModule.cs 的 OnLoad RegisterCommand 名（C# 仅 SET / GET），
  /// 本枚举另含 19 条 RedisJSON 扩展名）
  pub fn match_command(name: &[u8]) -> Option<Self> {
    Self::ALL
      .iter()
      .copied()
      .find(|cmd| cmd.name().as_bytes().eq_ignore_ascii_case(name))
  }

  pub const fn name(self) -> &'static str {
    match self {
      Self::Set => "JSON.SET",
      Self::Get => "JSON.GET",
      Self::Del => "JSON.DEL",
      Self::Forget => "JSON.FORGET",
      Self::Type => "JSON.TYPE",
      Self::MGet => "JSON.MGET",
      Self::NumIncrBy => "JSON.NUMINCRBY",
      Self::NumMultBy => "JSON.NUMMULTBY",
      Self::Toggle => "JSON.TOGGLE",
      Self::StrAppend => "JSON.STRAPPEND",
      Self::StrLen => "JSON.STRLEN",
      Self::ArrLen => "JSON.ARRLEN",
      Self::ArrAppend => "JSON.ARRAPPEND",
      Self::ArrPop => "JSON.ARRPOP",
      Self::ArrIndex => "JSON.ARRINDEX",
      Self::ArrInsert => "JSON.ARRINSERT",
      Self::ArrTrim => "JSON.ARRTRIM",
      Self::ObjKeys => "JSON.OBJKEYS",
      Self::ObjLen => "JSON.OBJLEN",
      Self::Clear => "JSON.CLEAR",
      Self::Resp => "JSON.RESP",
    }
  }

  pub const fn arity(self) -> i32 {
    match self {
      Self::Set => -4,
      Self::Get => -2,
      Self::Del | Self::Forget => -2,
      Self::Type => -2,
      Self::MGet => -3,
      Self::NumIncrBy | Self::NumMultBy => 4,
      Self::Toggle => -2,
      Self::StrAppend => -3,
      Self::StrLen | Self::ArrLen => -2,
      Self::ArrAppend => -4,
      Self::ArrPop => -2,
      Self::ArrIndex => -4,
      Self::ArrInsert => -5,
      Self::ArrTrim => 5,
      Self::ObjKeys | Self::ObjLen | Self::Clear | Self::Resp => -2,
    }
  }

  pub const fn command_type(self) -> CommandType {
    match self {
      Self::Set
      | Self::Del
      | Self::Forget
      | Self::NumIncrBy
      | Self::NumMultBy
      | Self::Toggle
      | Self::StrAppend
      | Self::ArrAppend
      | Self::ArrPop
      | Self::ArrInsert
      | Self::ArrTrim
      | Self::Clear => CommandType::ReadModifyWrite,
      Self::Get
      | Self::Type
      | Self::MGet
      | Self::StrLen
      | Self::ArrLen
      | Self::ArrIndex
      | Self::ObjKeys
      | Self::ObjLen
      | Self::Resp => CommandType::Read,
    }
  }

  /// 键作用域（多键读的分型知识入静态清单；JSON.MGET 之外全为单键。
  /// C# 无对位：`modules/GarnetJSON` 只注册 JSON.SET / JSON.GET，
  /// 多键读为 rust 侧 RedisJSON 兼容扩展面）
  pub const fn key_scope(self) -> KeyScope {
    match self {
      Self::MGet => KeyScope::MultiRead { tail: 1 },
      _ => KeyScope::Single,
    }
  }

  pub const fn fns(self) -> CustomObjectFns {
    match self {
      Self::Set => JsonCommands::JSON_SET,
      Self::Get => JsonCommands::JSON_GET,
      Self::Del | Self::Forget => JsonCommands::JSON_DEL,
      Self::Type => JsonCommands::JSON_TYPE,
      // 单值读法与 JSON.GET 同一执行体：多键循环由 [`JsonCommand::key_scope`]
      // 的静态形态位驱动，逐键各喂一次 reader（执行期不再按命令名特判）
      Self::MGet => JsonCommands::JSON_GET,
      Self::NumIncrBy => JsonCommands::JSON_NUMINCRBY,
      Self::NumMultBy => JsonCommands::JSON_NUMMULTBY,
      Self::Toggle => JsonCommands::JSON_TOGGLE,
      Self::StrAppend => JsonCommands::JSON_STRAPPEND,
      Self::StrLen => JsonCommands::JSON_STRLEN,
      Self::ArrLen => JsonCommands::JSON_ARRLEN,
      Self::ArrAppend => JsonCommands::JSON_ARRAPPEND,
      Self::ArrPop => JsonCommands::JSON_ARRPOP,
      Self::ArrIndex => JsonCommands::JSON_ARRINDEX,
      Self::ArrInsert => JsonCommands::JSON_ARRINSERT,
      Self::ArrTrim => JsonCommands::JSON_ARRTRIM,
      Self::ObjKeys => JsonCommands::JSON_OBJKEYS,
      Self::ObjLen => JsonCommands::JSON_OBJLEN,
      Self::Clear => JsonCommands::JSON_CLEAR,
      Self::Resp => JsonCommands::JSON_RESP,
    }
  }
}
