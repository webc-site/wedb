#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! RESP 命令文档（COMMAND DOCS）初始化与格式化集成测试
//! （对应 libs/server/Resp/RespCommandDocs.cs）

use wnode::resp::{
  RespCommandDocFlags, RespCommandGroup, resp_command_docs::try_initialize,
  try_get_resp_command_docs, try_get_resp_commands_docs, try_get_resp_sub_commands_docs,
};
use wresp::resp_memory_writer::{Resp3, RespMemoryWriter};

/// 表初始化 + 检索（COMMAND DOCS 表快照入口）
#[test]
fn tables_initialize_and_lookup() {
  assert!(try_initialize());

  let all = try_get_resp_commands_docs(false).unwrap();
  let external = try_get_resp_commands_docs(true).unwrap();
  assert_eq!(all.len(), 257, "文档根命令数快照");
  // 内部命令不设文档，外部面与全量面等大
  assert_eq!(external.len(), 257, "外部文档根命令数快照");

  // rust 自增命令 SUNSUBSCRIBE 的 COMMAND DOCS 面（入目录后方可见）
  let sunsub = try_get_resp_command_docs("SUNSUBSCRIBE", false, false).unwrap();
  assert_eq!(sunsub.group, RespCommandGroup::PubSub);

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
  let mut w = RespMemoryWriter::<Resp3>::new();
  get.to_resp_format(&mut w);
  let text = String::from_utf8(w.into_inner()).unwrap();
  assert!(text.starts_with("$3\r\nGET\r\n"), "{text}");
  assert!(text.contains("$7\r\nsummary\r\n"), "{text}");
  assert!(text.contains("$5\r\ngroup\r\n$6\r\nstring\r\n"), "{text}");
  assert!(text.contains("$9\r\narguments\r\n"), "{text}");

  // 子命令文档以 map 收纳（RESP3）
  let config = try_get_resp_command_docs("CONFIG", false, false).unwrap();
  assert!(
    config
      .sub_commands
      .as_ref()
      .is_some_and(|scs| !scs.is_empty())
  );
  let mut w = RespMemoryWriter::<Resp3>::new();
  config.to_resp_format(&mut w);
  let text = String::from_utf8(w.into_inner()).unwrap();
  assert!(text.contains("$11\r\nsubcommands\r\n%"), "{text}");

  // 子命令表
  let subs = try_get_resp_sub_commands_docs(false).unwrap();
  assert!(subs.contains_key("config|get"));
}
