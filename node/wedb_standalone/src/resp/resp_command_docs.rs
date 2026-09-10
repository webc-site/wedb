//! 命令文档元数据表（对标 libs/server/Resp/RespCommandDocs.cs）
//!
//! C# 内嵌 `RespCommandsDocs.json`（Garnet.resources），初始化时反序列化并
//! 依 [`super::resp_commands_info`] 的外部命令面过滤；Rust 侧同源 JSON +
//! OnceLock 静态表。

use std::sync::OnceLock;

use gxhash::{GxBuildHasher, HashMap, HashSet};
use sonic_rs::Deserialize;

use super::{
  resp_command_argument::{
    ArgumentBase, RespCommandArgument, RespCommandArgumentFlags, RespCommandArgumentType,
  },
  resp_command_data_common::try_import_resp_commands_data,
  resp_command_data_provider::IRespCommandData,
  resp_commands_info::{try_get_resp_command_info_by_cmd, try_get_resp_command_names},
  resp_memory_writer::RespMemoryWriter,
};
use crate::types::RespCommand;

/// 内嵌命令文档（C# Garnet.resources:RespCommandsDocs.json）
const RESP_COMMANDS_DOCS_JSON: &str = include_str!("RespCommandsDocs.json");

/// 命令功能组（C# RespCommandGroup；声明序即 wire 顺序）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RespCommandGroup {
  /// 无
  #[default]
  None,
  /// bitmap
  Bitmap,
  /// cluster
  Cluster,
  /// connection
  Connection,
  /// generic
  Generic,
  /// geo
  Geo,
  /// hash
  Hash,
  /// hyperloglog
  HyperLogLog,
  /// list
  List,
  /// module
  Module,
  /// pubsub
  PubSub,
  /// scripting
  Scripting,
  /// sentinel
  Sentinel,
  /// server
  Server,
  /// set
  Set,
  /// sorted-set
  SortedSet,
  /// stream
  Stream,
  /// string
  String,
  /// transactions
  Transactions,
  /// vector
  Vector,
}

impl RespCommandGroup {
  /// wire 描述（C# Description 特性；GetEnumDescriptions()[0]）
  pub fn description(&self) -> &'static str {
    match self {
      Self::None => "None",
      Self::Bitmap => "bitmap",
      Self::Cluster => "cluster",
      Self::Connection => "connection",
      Self::Generic => "generic",
      Self::Geo => "geo",
      Self::Hash => "hash",
      Self::HyperLogLog => "hyperloglog",
      Self::List => "list",
      Self::Module => "module",
      Self::PubSub => "pubsub",
      Self::Scripting => "scripting",
      Self::Sentinel => "sentinel",
      Self::Server => "server",
      Self::Set => "set",
      Self::SortedSet => "sorted-set",
      Self::Stream => "stream",
      Self::String => "string",
      Self::Transactions => "transactions",
      Self::Vector => "vector",
    }
  }

  /// 按成员名解析（大小写不敏感；C# JsonStringEnumConverter 语义）
  pub fn from_member_name(name: &str) -> Option<Self> {
    Some(match name.to_ascii_uppercase().as_str() {
      "NONE" => Self::None,
      "BITMAP" => Self::Bitmap,
      "CLUSTER" => Self::Cluster,
      "CONNECTION" => Self::Connection,
      "GENERIC" => Self::Generic,
      "GEO" => Self::Geo,
      "HASH" => Self::Hash,
      "HYPERLOGLOG" => Self::HyperLogLog,
      "LIST" => Self::List,
      "MODULE" => Self::Module,
      "PUBSUB" => Self::PubSub,
      "SCRIPTING" => Self::Scripting,
      "SENTINEL" => Self::Sentinel,
      "SERVER" => Self::Server,
      "SET" => Self::Set,
      "SORTEDSET" => Self::SortedSet,
      "STREAM" => Self::Stream,
      "STRING" => Self::String,
      "TRANSACTIONS" => Self::Transactions,
      "VECTOR" => Self::Vector,
      _ => return None,
    })
  }
}

/// 文档标记（C# RespCommandDocFlags）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RespCommandDocFlags(u8);

impl RespCommandDocFlags {
  /// 无
  pub const NONE: Self = Self(0);
  /// 已弃用（C# Deprecated）
  pub const DEPRECATED: Self = Self(1);
  /// 系统命令（C# SysCmd）
  pub const SYS_CMD: Self = Self(1 << 1);

  /// 是否无标记
  #[inline]
  pub fn is_none(&self) -> bool {
    self.0 == 0
  }

  /// wire 描述（C# EnumUtils.GetEnumDescriptions）
  pub fn descriptions(&self) -> Vec<&'static str> {
    [
      (Self::DEPRECATED.0, "deprecated"),
      (Self::SYS_CMD.0, "syscmd"),
    ]
    .iter()
    .filter(|(bit, _)| self.0 & bit != 0)
    .map(|(_, desc)| *desc)
    .collect()
  }

  /// 按成员名串解析（大小写不敏感）
  pub fn from_member_names(names: &str) -> Option<Self> {
    let mut out = Self::NONE;
    for name in names.split(',') {
      let bit = match name.trim().to_ascii_uppercase().as_str() {
        "NONE" => Self::NONE,
        "DEPRECATED" => Self::DEPRECATED,
        "SYSCMD" => Self::SYS_CMD,
        _ => return None,
      };
      out.0 |= bit.0;
    }
    Some(out)
  }
}

/// 一条命令的文档（C# RespCommandDocs）
#[derive(Debug, Clone)]
pub struct RespCommandDocs {
  /// 命令枚举（C# Command）
  pub command: RespCommand,
  /// 命令名
  pub name: String,
  /// 简述（C# Summary）
  pub summary: Option<String>,
  /// 功能组（C# Group）
  pub group: RespCommandGroup,
  /// 时间复杂度说明（C# Complexity）
  pub complexity: Option<String>,
  /// 文档标记（C# DocFlags）
  pub doc_flags: RespCommandDocFlags,
  /// 弃用替代（C# ReplacedBy）
  pub replaced_by: Option<String>,
  /// 参数（C# Arguments）
  pub arguments: Option<Vec<RespCommandArgument>>,
  /// 子命令文档（C# SubCommands）
  pub sub_commands: Vec<RespCommandDocs>,
  /// 是否为子命令（C# Parent != null 投影）
  pub is_sub_command: bool,
}

impl IRespCommandData for RespCommandDocs {
  fn name(&self) -> &str {
    &self.name
  }
}

impl IRespCommandData for RespCommandDocsImport {
  fn name(&self) -> &str {
    &self.name
  }
}

impl RespCommandDocs {
  /// 序列化为 RESP 格式
  ///
  /// libs/server/Resp/RespCommandDocs.cs:ToRespFormat
  pub fn to_resp_format(&self, writer: &mut RespMemoryWriter) {
    let mut arg_count = 1; // group

    if self.summary.is_some() {
      arg_count += 1;
    }

    if self.complexity.is_some() {
      arg_count += 1;
    }

    if !self.doc_flags.is_none() {
      arg_count += 1;
    }

    if self.replaced_by.is_some() {
      arg_count += 1;
    }

    if self.arguments.is_some() {
      arg_count += 1;
    }

    if !self.sub_commands.is_empty() {
      arg_count += 1;
    }

    writer.write_ascii_bulk_string(&self.name);
    writer.write_map_length(arg_count);

    if let Some(summary) = &self.summary {
      writer.write_bulk_string(b"summary");
      writer.write_ascii_bulk_string(summary);
    }

    writer.write_bulk_string(b"group");
    writer.write_ascii_bulk_string(self.group.description());

    if let Some(complexity) = &self.complexity {
      writer.write_bulk_string(b"complexity");
      writer.write_ascii_bulk_string(complexity);
    }

    if !self.doc_flags.is_none() {
      let resp_format_doc_flags = self.doc_flags.descriptions();
      writer.write_bulk_string(b"doc_flags");
      writer.write_set_length(resp_format_doc_flags.len());
      for resp_doc_flag in resp_format_doc_flags {
        writer.write_simple_string(resp_doc_flag);
      }
    }

    if let Some(replaced_by) = &self.replaced_by {
      writer.write_bulk_string(b"replaced_by");
      writer.write_ascii_bulk_string(replaced_by);
    }

    if let Some(arguments) = &self.arguments {
      writer.write_bulk_string(b"arguments");
      writer.write_array_length(arguments.len());
      for argument in arguments {
        argument.to_resp_format(writer);
      }
    }

    if !self.sub_commands.is_empty() {
      writer.write_bulk_string(b"subcommands");
      writer.write_map_length(self.sub_commands.len());
      for sub_command in &self.sub_commands {
        sub_command.to_resp_format(writer);
      }
    }
  }
}

// —— JSON 导入结构 ——

/// 参数导入（C# RespCommandArgumentConverter 读出的多态字段平铺）
#[derive(Deserialize, Clone, Default)]
struct ArgumentImport {
  #[serde(rename = "TypeDiscriminator")]
  discriminator: String,
  #[serde(rename = "Name")]
  name: Option<String>,
  #[serde(rename = "DisplayText")]
  display_text: Option<String>,
  #[serde(rename = "Type")]
  argument_type: Option<String>,
  #[serde(rename = "Token")]
  token: Option<String>,
  #[serde(rename = "Summary")]
  summary: Option<String>,
  #[serde(rename = "ArgumentFlags")]
  argument_flags: Option<String>,
  #[serde(rename = "Value")]
  value: Option<String>,
  #[serde(rename = "KeySpecIndex")]
  key_spec_index: Option<i32>,
  #[serde(rename = "Arguments")]
  arguments: Option<Vec<ArgumentImport>>,
}

impl ArgumentImport {
  fn convert(self) -> Option<RespCommandArgument> {
    if !RespCommandArgument::can_convert(&self.discriminator) {
      return None;
    }
    let base = ArgumentBase {
      name: self.name.unwrap_or_default(),
      display_text: self.display_text,
      argument_type: match &self.argument_type {
        Some(t) => RespCommandArgumentType::from_member_name(t)?,
        None => RespCommandArgumentType::None,
      },
      token: self.token,
      summary: self.summary,
      argument_flags: match &self.argument_flags {
        Some(f) => RespCommandArgumentFlags::from_member_name(f)?,
        None => RespCommandArgumentFlags::NONE,
      },
    };

    Some(match self.discriminator.as_str() {
      "RespCommandKeyArgument" => RespCommandArgument::Key {
        base,
        value: self.value,
        key_spec_index: self.key_spec_index.unwrap_or(-1),
      },
      "RespCommandBasicArgument" => RespCommandArgument::Basic {
        base,
        value: self.value,
      },
      _ => {
        let arguments = match self.arguments {
          Some(args) => {
            let mut converted = Vec::with_capacity(args.len());
            for arg in args {
              converted.push(arg.convert()?);
            }
            Some(converted)
          }
          None => None,
        };
        RespCommandArgument::Container { base, arguments }
      }
    })
  }
}

/// 文档导入（C# RespCommandDocs JSON 面）
#[derive(Deserialize, Clone, Default)]
struct RespCommandDocsImport {
  #[serde(rename = "Command")]
  command: String,
  #[serde(rename = "Name")]
  name: String,
  #[serde(rename = "Summary")]
  summary: Option<String>,
  #[serde(rename = "Group")]
  group: Option<String>,
  #[serde(rename = "Complexity")]
  complexity: Option<String>,
  #[serde(rename = "DocFlags")]
  doc_flags: Option<String>,
  #[serde(rename = "ReplacedBy")]
  replaced_by: Option<String>,
  #[serde(rename = "Arguments")]
  arguments: Option<Vec<ArgumentImport>>,
  #[serde(rename = "SubCommands")]
  sub_commands: Option<Vec<RespCommandDocsImport>>,
}

/// docs.json 中无 Command 键的条目以空串兜底（C# JsonStringEnumConverter
/// 遇未知名抛异常；空串条目在实际数据中不存在，此处 None 兜底）
fn convert_import(import: RespCommandDocsImport, parent_is_sub: bool) -> Option<RespCommandDocs> {
  let command = if import.command.is_empty() {
    RespCommand::None
  } else {
    super::resp_commands_info_data::resp_command_from_cs_name(&import.command)?
  };
  let group = match &import.group {
    Some(g) => RespCommandGroup::from_member_name(g)?,
    None => RespCommandGroup::None,
  };
  let doc_flags = match &import.doc_flags {
    Some(f) => RespCommandDocFlags::from_member_names(f)?,
    None => RespCommandDocFlags::NONE,
  };
  let arguments = match import.arguments {
    Some(args) => {
      let mut converted = Vec::with_capacity(args.len());
      for arg in args {
        converted.push(arg.convert()?);
      }
      Some(converted)
    }
    None => None,
  };

  let mut sub_commands = Vec::new();
  for sc in import.sub_commands.unwrap_or_default() {
    sub_commands.push(convert_import(sc, true)?);
  }

  Some(RespCommandDocs {
    command,
    name: import.name,
    summary: import.summary,
    group,
    complexity: import.complexity,
    doc_flags,
    replaced_by: import.replaced_by,
    arguments,
    sub_commands,
    is_sub_command: parent_is_sub,
  })
}

// —— 静态表 ——

/// 全量文档索引（C# 静态字段族）
pub struct RespCommandDocsTables {
  /// 全部文档（键 = 小写名；C# AllRespCommandsDocs）
  pub all: HashMap<String, RespCommandDocs>,
  /// 全部子命令文档（C# AllRespSubCommandsDocs）
  pub all_sub: HashMap<String, RespCommandDocs>,
  /// 外部文档（C# ExternalRespCommandsDocs）
  pub external: HashMap<String, RespCommandDocs>,
  /// 外部子命令文档（C# ExternalRespSubCommandsDocs）
  pub external_sub: HashMap<String, RespCommandDocs>,
}

static TABLES: OnceLock<Option<RespCommandDocsTables>> = OnceLock::new();

/// libs/server/Resp/RespCommandDocs.cs:TryInitialize
fn try_initialize() -> bool {
  TABLES
    .get_or_init(|| {
      if try_initialize_resp_commands_docs() {
        Some(build_tables())
      } else {
        None
      }
    })
    .is_some()
}

/// 导入暂存（构建期中间结构）
static IMPORTED: OnceLock<Vec<RespCommandDocs>> = OnceLock::new();

/// libs/server/Resp/RespCommandDocs.cs:TryInitializeRespCommandsDocs
///
/// 依赖命令信息表（外部命令名过滤）；C# 以命令枚举回查子命令与父命令的
/// internal 面，Rust 侧在导入结构上已带 `is_sub_command` 投影。
fn try_initialize_resp_commands_docs() -> bool {
  // 命令信息表须先可用（C# TryGetRespCommandNames 前置）
  if super::resp_commands_info::try_get_resp_command_names(false).is_none() {
    return false;
  }

  let imported = try_import_resp_commands_data::<RespCommandDocsImport>(RESP_COMMANDS_DOCS_JSON);
  let Some(imported) = imported else {
    return false;
  };

  let mut converted = Vec::with_capacity(imported.len());
  for entry in imported {
    let Some(docs) = convert_import(entry, false) else {
      return false;
    };
    converted.push(docs);
  }
  IMPORTED.set(converted).is_ok()
}

/// 构建文档索引
fn build_tables() -> RespCommandDocsTables {
  let imported = IMPORTED.get().expect("导入已完成");
  let external_command_names: Option<&'static HashSet<String>> = try_get_resp_command_names(true);

  let mut all: HashMap<String, RespCommandDocs> = HashMap::with_hasher(GxBuildHasher::default());
  let mut all_sub: HashMap<String, RespCommandDocs> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut external: HashMap<String, RespCommandDocs> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut external_sub: HashMap<String, RespCommandDocs> =
    HashMap::with_hasher(GxBuildHasher::default());

  for entry in imported {
    all.insert(entry.name.to_lowercase(), entry.clone());

    // 外部文档：仅收录命令信息表标记为外部的命令
    let is_external =
      external_command_names.is_some_and(|names| names.contains(&entry.name.to_lowercase()));
    if is_external {
      external.insert(entry.name.to_lowercase(), entry.clone());
    }

    for sc in &entry.sub_commands {
      all_sub.insert(sc.name.to_lowercase(), sc.clone());
      // 父命令或子命令为内部命令则不入外部子命令表
      let sub_cmd_info = try_get_resp_command_info_by_cmd(sc.command, false);
      let internal = match sub_cmd_info {
        Some(info) => info.is_internal || info.parent_is_internal,
        None => true,
      };
      if internal {
        continue;
      }
      external_sub.insert(sc.name.to_lowercase(), sc.clone());
    }
  }

  RespCommandDocsTables {
    all,
    all_sub,
    external,
    external_sub,
  }
}

/// 已初始化表引用
fn tables() -> Option<&'static RespCommandDocsTables> {
  if !try_initialize() {
    return None;
  }
  TABLES.get().and_then(|t| t.as_ref())
}

/// 取全部命令文档（键为小写命令名）
///
/// libs/server/Resp/RespCommandDocs.cs:TryGetRespCommandsDocs
pub fn try_get_resp_commands_docs(
  external_only: bool,
) -> Option<&'static HashMap<String, RespCommandDocs>> {
  let tables = tables()?;
  Some(if external_only {
    &tables.external
  } else {
    &tables.all
  })
}

/// 按命令名取文档（`include_sub_commands` 同时查子命令表）
///
/// libs/server/Resp/RespCommandDocs.cs:TryGetRespCommandDocs
pub fn try_get_resp_command_docs(
  cmd_name: &str,
  external_only: bool,
  include_sub_commands: bool,
) -> Option<&'static RespCommandDocs> {
  let tables = tables()?;
  let key = cmd_name.to_lowercase();
  let primary = if external_only {
    &tables.external
  } else {
    &tables.all
  };
  primary.get(&key).or_else(|| {
    if include_sub_commands {
      let sub = if external_only {
        &tables.external_sub
      } else {
        &tables.all_sub
      };
      sub.get(&key)
    } else {
      None
    }
  })
}

/// 取全部子命令文档（键为小写名）
///
/// libs/server/Resp/RespCommandDocs.cs:TryGetRespSubCommandsDocs
pub fn try_get_resp_sub_commands_docs(
  external_only: bool,
) -> Option<&'static HashMap<String, RespCommandDocs>> {
  let tables = tables()?;
  Some(if external_only {
    &tables.external_sub
  } else {
    &tables.all_sub
  })
}

#[cfg(test)]
mod tests {
  use super::{
    RespCommandDocFlags, RespCommandGroup, try_get_resp_command_docs, try_get_resp_commands_docs,
    try_get_resp_sub_commands_docs,
  };
  use crate::resp::resp_memory_writer::RespMemoryWriter;

  /// 表初始化 + 检索（COMMAND DOCS 表快照入口）
  #[test]
  fn tables_initialize_and_lookup() {
    assert!(super::try_initialize());

    let all = try_get_resp_commands_docs(false).unwrap();
    let external = try_get_resp_commands_docs(true).unwrap();
    assert_eq!(all.len(), 258, "文档根命令数快照");
    // 内部命令不设文档，外部面与全量面等大
    assert_eq!(external.len(), 258, "外部文档根命令数快照");

    // GET 文档字段面
    let get = try_get_resp_command_docs("get", false, false).unwrap();
    assert_eq!(get.group, RespCommandGroup::String);
    assert!(
      get
        .summary
        .as_deref()
        .unwrap()
        .contains("Returns the string value")
    );

    // 子命令文档（CONFIG|GET）
    let config_get = try_get_resp_command_docs("CONFIG|GET", false, true).unwrap();
    assert!(config_get.is_sub_command);

    // 外部表不含内部命令文档
    assert!(try_get_resp_command_docs("CUSTOMOBJCMD", false, false).is_none());
  }

  #[test]
  fn doc_flags_and_to_resp_format() {
    // 标记解析
    let flags = RespCommandDocFlags::from_member_names("SysCmd").unwrap();
    assert_eq!(flags.descriptions(), vec!["syscmd"]);

    // GET 文档 RESP 快照（RESP3 map 面）
    let get = try_get_resp_command_docs("get", false, false).unwrap();
    let mut w = RespMemoryWriter::new(true);
    get.to_resp_format(&mut w);
    let text = String::from_utf8(w.out).unwrap();
    assert!(text.starts_with("$3\r\nGET\r\n"), "{text}");
    assert!(text.contains("$7\r\nsummary\r\n"), "{text}");
    assert!(text.contains("$5\r\ngroup\r\n$6\r\nstring\r\n"), "{text}");
    assert!(text.contains("$9\r\narguments\r\n"), "{text}");

    // 子命令文档以 map 收纳（RESP3）
    let config = try_get_resp_command_docs("CONFIG", false, false).unwrap();
    assert!(!config.sub_commands.is_empty());
    let mut w = RespMemoryWriter::new(true);
    config.to_resp_format(&mut w);
    let text = String::from_utf8(w.out).unwrap();
    assert!(text.contains("$11\r\nsubcommands\r\n%"), "{text}");

    // 子命令表
    let subs = try_get_resp_sub_commands_docs(false).unwrap();
    assert!(subs.contains_key("config|get"));
  }
}
