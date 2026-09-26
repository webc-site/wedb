use wcustom::{CommandType, CustomObjectFns, KeyScope};
use wval::CustomObjectType;

use super::JsonCommands;
use crate::json_object::scan_members;

/// 命令静态清单表项
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonCommandInfo {
  pub name: &'static str,
  pub arity: i32,
  pub acl_categories: &'static [&'static str],
  pub summary: &'static str,
}

macro_rules! define_json_commands {
  ($(
    $variant:ident => {
      name: $name:literal,
      suffix: $suffix:literal,
      arity: $arity:expr,
      cmd_type: $cmd_type:ident,
      acl: $acl:expr,
      summary: $summary:literal,
      fns: $fns:expr $(,
      key_scope: $key_scope:expr)? $(,)?
    },
  )*) => {
    /// JSON 命令枚举
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum JsonCommand {
      $($variant,)*
    }

    /// 编译期命令清单：本表全 21 条 = 2 对位 + 19 扩展。对位的 2 条（SET / GET）
    /// 对标 modules/GarnetJSON/JsonModule.cs 的 OnLoad 注册面（C# 仅 RegisterCommand
    /// JSON.SET / JSON.GET 两条）；余下 19 条为 RedisJSON 兼容扩展面，garnet 无对位。
    pub const COMMAND_INFOS: &[JsonCommandInfo] = &[
      $(
        JsonCommandInfo {
          name: $name,
          arity: $arity,
          acl_categories: $acl,
          summary: $summary,
        },
      )*
    ];

    impl JsonCommand {
      /// 命令全集（枚举单源权威表：按名解析与 [`COMMAND_INFOS`] 目录共用的
      /// 唯一名单，新增一条命令只在此加一项）
      pub const ALL: &[Self] = &[$(Self::$variant,)*];

      pub const fn name(self) -> &'static str {
        match self {
          $(Self::$variant => $name,)*
        }
      }

      pub const fn arity(self) -> i32 {
        match self {
          $(Self::$variant => $arity,)*
        }
      }

      pub const fn command_type(self) -> CommandType {
        match self {
          $(Self::$variant => CommandType::$cmd_type,)*
        }
      }

      pub const fn key_scope(self) -> KeyScope {
        match self {
          $($(Self::$variant => $key_scope,)?)*
          _ => KeyScope::Single,
        }
      }

      pub const fn fns(self) -> CustomObjectFns {
        match self {
          $(Self::$variant => $fns,)*
        }
      }

      /// 按名匹配（大小写不敏感；前缀 O(1) 预筛 + 后缀比对）
      pub fn match_command(name: &[u8]) -> Option<Self> {
        let suffix = match name {
          [b'j' | b'J', b's' | b'S', b'o' | b'O', b'n' | b'N', b'.', rest @ ..] => rest,
          _ => return None,
        };
        $(
          if suffix.eq_ignore_ascii_case($suffix) {
            return Some(Self::$variant);
          }
        )*
        None
      }
    }
  };
}

define_json_commands! {
  Set => {
    name: "JSON.SET",
    suffix: b"SET",
    arity: -4,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Set the JSON value at path in key",
    fns: JsonCommands::JSON_SET,
  },
  Get => {
    name: "JSON.GET",
    suffix: b"GET",
    arity: -2,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Get the JSON value at path in key",
    fns: JsonCommands::JSON_GET,
  },
  Del => {
    name: "JSON.DEL",
    suffix: b"DEL",
    arity: -2,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Delete a value",
    fns: JsonCommands::JSON_DEL,
  },
  Forget => {
    name: "JSON.FORGET",
    suffix: b"FORGET",
    arity: -2,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Delete a value (alias for JSON.DEL)",
    fns: JsonCommands::JSON_DEL,
  },
  Type => {
    name: "JSON.TYPE",
    suffix: b"TYPE",
    arity: -2,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Report the type of JSON value at path",
    fns: JsonCommands::JSON_TYPE,
  },
  MGet => {
    name: "JSON.MGET",
    suffix: b"MGET",
    arity: -3,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Get the JSON values at path from multiple keys",
    fns: JsonCommands::JSON_GET,
    key_scope: KeyScope::MultiRead { tail: 1 },
  },
  NumIncrBy => {
    name: "JSON.NUMINCRBY",
    suffix: b"NUMINCRBY",
    arity: 4,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Increment the numeric value at path by a number",
    fns: JsonCommands::JSON_NUMINCRBY,
  },
  NumMultBy => {
    name: "JSON.NUMMULTBY",
    suffix: b"NUMMULTBY",
    arity: 4,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Multiply the numeric value at path by a number",
    fns: JsonCommands::JSON_NUMMULTBY,
  },
  Toggle => {
    name: "JSON.TOGGLE",
    suffix: b"TOGGLE",
    arity: -2,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Toggle a boolean value at path",
    fns: JsonCommands::JSON_TOGGLE,
  },
  StrAppend => {
    name: "JSON.STRAPPEND",
    suffix: b"STRAPPEND",
    arity: -3,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Append a string to a JSON string value at path",
    fns: JsonCommands::JSON_STRAPPEND,
  },
  StrLen => {
    name: "JSON.STRLEN",
    suffix: b"STRLEN",
    arity: -2,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Report the length of the JSON string at path in key",
    fns: JsonCommands::JSON_STRLEN,
  },
  ArrLen => {
    name: "JSON.ARRLEN",
    suffix: b"ARRLEN",
    arity: -2,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Report the length of the JSON array at path in key",
    fns: JsonCommands::JSON_ARRLEN,
  },
  ArrAppend => {
    name: "JSON.ARRAPPEND",
    suffix: b"ARRAPPEND",
    arity: -4,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Append one or more values to the JSON array at path",
    fns: JsonCommands::JSON_ARRAPPEND,
  },
  ArrPop => {
    name: "JSON.ARRPOP",
    suffix: b"ARRPOP",
    arity: -2,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Remove and return an element from the JSON array at path",
    fns: JsonCommands::JSON_ARRPOP,
  },
  ArrIndex => {
    name: "JSON.ARRINDEX",
    suffix: b"ARRINDEX",
    arity: -4,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Search for the first occurrence of a scalar JSON value in an array",
    fns: JsonCommands::JSON_ARRINDEX,
  },
  ArrInsert => {
    name: "JSON.ARRINSERT",
    suffix: b"ARRINSERT",
    arity: -5,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Insert one or more values into the JSON array at path",
    fns: JsonCommands::JSON_ARRINSERT,
  },
  ArrTrim => {
    name: "JSON.ARRTRIM",
    suffix: b"ARRTRIM",
    arity: 5,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Trim an array so that it contains only the specified inclusive range of elements",
    fns: JsonCommands::JSON_ARRTRIM,
  },
  ObjKeys => {
    name: "JSON.OBJKEYS",
    suffix: b"OBJKEYS",
    arity: -2,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Return the keys in the object that's referenced by the path",
    fns: JsonCommands::JSON_OBJKEYS,
  },
  ObjLen => {
    name: "JSON.OBJLEN",
    suffix: b"OBJLEN",
    arity: -2,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Report the number of keys in the JSON object at path in key",
    fns: JsonCommands::JSON_OBJLEN,
  },
  Clear => {
    name: "JSON.CLEAR",
    suffix: b"CLEAR",
    arity: -2,
    cmd_type: ReadModifyWrite,
    acl: &["write", "json"],
    summary: "Clear container values (arrays/objects) and set numeric values to 0",
    fns: JsonCommands::JSON_CLEAR,
  },
  Resp => {
    name: "JSON.RESP",
    suffix: b"RESP",
    arity: -2,
    cmd_type: Read,
    acl: &["read", "json"],
    summary: "Return the JSON value at path in Redis Serialization Protocol (RESP) format",
    fns: JsonCommands::JSON_RESP,
  },
}

/// 注册名查询面：本函数是 [`JsonCommand::match_command`] 的布尔投影（命令名唯一
/// 权威为枚举 [`JsonCommand::name`]）。C# 的 modules/GarnetJSON/JsonModule.cs:OnLoad
/// 只注册 SET / GET 两条，此处覆盖 rust 侧全部 21 条编译期静态名单（2 对位 + 19 扩展）；
/// server 层 ACL 门已改走 wnode 编译期清单单点，此处只承扩展 crate 自身的注册名查询面
pub fn is_command_registered(name: &str) -> bool {
  JsonCommand::match_command(name.as_bytes()).is_some()
}

impl JsonCommand {
  /// Redis TYPE 应答类型串（C# modules 注册名，对标
  /// JsonModule.cs:20 `context.Initialize("GarnetJSON", 1)`；C# HandleType
  /// 对 custom object 无 default 臂输出零字节 quirk，rust 修复为回注册名，
  /// TYPE 与 EXISTS 存活口径一致）
  pub const OBJECT_TYPE_NAME: &str = "GarnetJSON";

  /// 扩展对象静态描述清单项（wnode 编译期清单单条；标签、TYPE 注册名、
  /// 按名解析入口、堆估算入口、COSCAN 成员扫描执行体一处描述，对标
  /// CustomObjectFactory 工厂集中持有形态与 GarnetJsonObject 的
  /// IHeapObject.HeapMemorySize 恒定常数）
  pub const OBJECT_ENTRY: wcustom::CustomObjectEntry = wcustom::CustomObjectEntry {
    tag: CustomObjectType::Json,
    type_name: Self::OBJECT_TYPE_NAME,
    match_command: Self::match_command_meta,
    heap_estimate: crate::heap_estimate,
    scan_members,
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
}
