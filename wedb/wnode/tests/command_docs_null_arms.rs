//! COMMAND DOCS 三臂回归锁（zcode-r123c-cmddocs1）
//!
//! 对拍 C# 数据源形态（garnet/libs/server/Resp/RespCommandDocs.cs:ToRespFormat
//! 与 RespCommandDataProvider.cs:TryImportRespCommandsData）：
//! 臂1 `SubCommands` 发射判据为 `!= null`——空数组也发射键并写零长 map
//! （RESP3 `%0` / RESP2 降级 `*0`），JSON 无键才不发射；逐字节锁 null 与 []
//! 两形态帧形不等。
//! 臂2 `Command` 键缺失落 STJ 值缺省语义（RespCommand::None、条目保留可用），
//! 未知枚举名才整表熔断（C# JsonException catch 同臂）。
//! 臂3 跨父（及同父）子命令重名整表熔断——rust 口径为文档面永久空表，
//! C# 口径为 Add 抛 ArgumentException 命令层上抛，两口径非等形、分写登记。
//!
//! 全部合成 JSON 经 [`try_build_resp_commands_docs_tables`] 纯函数面导入，
//! 不触 OnceLock 全局表，无 mock。

use wnode::resp::{RespCommandDocs, RespCommandDocsTables, try_build_resp_commands_docs_tables};
use wresp::{
  command::RespCommand,
  resp_memory_writer::{Resp2, Resp3, RespMemoryWriter, RespProtocol},
};

/// 单条文档序列化为线上字节（协议形态由类型参数指定）
fn to_bytes<P: RespProtocol>(doc: &RespCommandDocs) -> Vec<u8> {
  let mut writer = RespMemoryWriter::<P>::new();
  doc.to_resp_format(&mut writer);
  writer.into_inner()
}

/// 取表内根文档（键 = 小写名）
fn root<'a>(tables: &'a RespCommandDocsTables, name: &str) -> &'a RespCommandDocs {
  tables.all.get(name).expect("条目应保留在根表")
}

// —— 臂1：SubCommands null / [] / 非空 三形态逐字节锁 ——

/// `"SubCommands": []` 发射 subcommands 键 + 零长 map；无键不发射；两帧不等
#[test]
fn empty_array_emits_zero_map_and_missing_key_omits_it() {
  const WITH_EMPTY: &str =
    r#"[{"Command":"PING","Name":"SUBEMPTY","Group":"Server","SubCommands":[]}]"#;
  const WITHOUT: &str = r#"[{"Command":"PING","Name":"SUBMISSING","Group":"Server"}]"#;

  let with = try_build_resp_commands_docs_tables(WITH_EMPTY).expect("[] 形态导入应成功");
  let without = try_build_resp_commands_docs_tables(WITHOUT).expect("无键形态导入应成功");

  // 导入面：Some([]) 与 None 可区分（坍缩即回归）
  assert!(
    root(&with, "subempty")
      .sub_commands
      .as_ref()
      .is_some_and(|scs| scs.is_empty())
  );
  assert!(root(&without, "submissing").sub_commands.is_none());

  // RESP3 帧逐字节：[] → %2 含 subcommands 键 + %0；无键 → %1 仅 group
  assert_eq!(
    to_bytes::<Resp3>(root(&with, "subempty")),
    b"$8\r\nSUBEMPTY\r\n%2\r\n$5\r\ngroup\r\n$6\r\nserver\r\n$11\r\nsubcommands\r\n%0\r\n"
  );
  assert_eq!(
    to_bytes::<Resp3>(root(&without, "submissing")),
    b"$10\r\nSUBMISSING\r\n%1\r\n$5\r\ngroup\r\n$6\r\nserver\r\n"
  );

  // RESP2 降级：map → *2n；%0 → *0
  assert_eq!(
    to_bytes::<Resp2>(root(&with, "subempty")),
    b"$8\r\nSUBEMPTY\r\n*4\r\n$5\r\ngroup\r\n$6\r\nserver\r\n$11\r\nsubcommands\r\n*0\r\n"
  );
  assert_eq!(
    to_bytes::<Resp2>(root(&without, "submissing")),
    b"$10\r\nSUBMISSING\r\n*2\r\n$5\r\ngroup\r\n$6\r\nserver\r\n"
  );

  // null 与 [] 帧形必须相异（发射判据 != null 的字节级体现）
  assert_ne!(
    to_bytes::<Resp3>(root(&with, "subempty")),
    to_bytes::<Resp3>(root(&without, "submissing"))
  );
}

/// 非空 SubCommands 嵌套 map 帧形对照（[] / 无键的对照组，锁定 %n 正常臂）
#[test]
fn non_empty_subcommands_frame_lock() {
  const JSON: &str = concat!(
    r#"[{"Command":"PING","Name":"SUBOK","Group":"Server","SubCommands":["#,
    r#"{"Command":"GET","Name":"SUBOK|A","Group":"String"}]}]"#,
  );
  let tables = try_build_resp_commands_docs_tables(JSON).expect("非空形态导入应成功");
  let doc = root(&tables, "subok");
  assert_eq!(
    to_bytes::<Resp3>(doc),
    concat!(
      "$5\r\nSUBOK\r\n%2\r\n$5\r\ngroup\r\n$6\r\nserver\r\n$11\r\nsubcommands\r\n%1\r\n",
      "$7\r\nSUBOK|A\r\n%1\r\n$5\r\ngroup\r\n$6\r\nstring\r\n",
    )
    .as_bytes()
  );
  // 子命令表收录并带 is_sub_command 投影
  let sub = tables
    .all_sub
    .get("subok|a")
    .expect("子命令应入 all_sub 表");
  assert!(sub.is_sub_command);
  assert!(sub.sub_commands.is_none());
}

// —— 臂2：Command 键缺失 = 值缺省保留；未知枚举名 = 整表熔断 ——

/// 缺 Command 键条目保留、command 落 RespCommand::None、回显面完整可用
/// （C# STJ 缺成员 = 枚举默认值 NONE；修复前 rust 为整表反序列化失败永久 %0）
#[test]
fn missing_command_key_keeps_entry_with_none_command() {
  const JSON: &str = concat!(
    r#"[{"Name":"NOCMD","Group":"Server","SubCommands":["#,
    r#"{"Name":"NOCMD|S","Group":"Server"}]}]"#,
  );
  let tables = try_build_resp_commands_docs_tables(JSON).expect("缺 Command 键应保留条目");
  assert_eq!(root(&tables, "nocmd").command, RespCommand::None);
  assert!(tables.all.contains_key("nocmd"));
  let sub = tables.all_sub.get("nocmd|s").expect("子命令条目应保留");
  assert_eq!(sub.command, RespCommand::None);
  // 回显面：缺 Command 不影响 subcommands 键发射（Some 非空）
  assert!(
    root(&tables, "nocmd")
      .sub_commands
      .as_ref()
      .is_some_and(|scs| scs.len() == 1)
  );
}

/// 值为未知枚举名 → 整表失败（C# JsonException catch 才炸整表同臂）；
/// 根与子命令两处触发点均锁
#[test]
fn unknown_command_name_fails_whole_table() {
  assert!(
    try_build_resp_commands_docs_tables(
      r#"[{"Command":"NOT_A_REAL_COMMAND","Name":"X","Group":"Server"}]"#
    )
    .is_none()
  );
  const SUB_UNKNOWN: &str = concat!(
    r#"[{"Command":"PING","Name":"OK","Group":"Server","SubCommands":["#,
    r#"{"Command":"NOT_A_REAL_COMMAND","Name":"OK|S","Group":"Server"}]}]"#,
  );
  assert!(try_build_resp_commands_docs_tables(SUB_UNKNOWN).is_none());
  // 对照：合法枚举名成功
  assert!(
    try_build_resp_commands_docs_tables(r#"[{"Command":"PING","Name":"OK","Group":"Server"}]"#)
      .is_some()
  );
}

// —— 臂3：跨父重名整表熔断（判重 return false 口径） ——

/// 跨父重名子命令（大小写不敏感）→ 整表熔断；对照非重名成功且 all_sub 完整
#[test]
fn duplicate_sub_command_names_across_parents_fail_table() {
  const CROSS_PARENT_DUP: &str = concat!(
    r#"[{"Command":"ACL","Name":"FOO","Group":"Server","SubCommands":["#,
    r#"{"Command":"PING","Name":"DUPSUB","Group":"Server"}]},"#,
    r#"{"Command":"SET","Name":"BAR","Group":"Server","SubCommands":["#,
    r#"{"Command":"GET","Name":"dupsub","Group":"Server"}]}]"#,
  );
  assert!(
    try_build_resp_commands_docs_tables(CROSS_PARENT_DUP).is_none(),
    "跨父重名（OrdinalIgnoreCase）须整表熔断"
  );

  // 同父重名同样触发 all_sub 判重
  const SAME_PARENT_DUP: &str = concat!(
    r#"[{"Command":"ACL","Name":"FOO","Group":"Server","SubCommands":["#,
    r#"{"Command":"PING","Name":"DUP","Group":"Server"},"#,
    r#"{"Command":"GET","Name":"DUP","Group":"Server"}]}]"#,
  );
  assert!(try_build_resp_commands_docs_tables(SAME_PARENT_DUP).is_none());

  // 对照：无重名成功且两子命令均入 all_sub
  const NO_DUP: &str = concat!(
    r#"[{"Command":"ACL","Name":"FOO","Group":"Server","SubCommands":["#,
    r#"{"Command":"PING","Name":"DUP1","Group":"Server"}]},"#,
    r#"{"Command":"SET","Name":"BAR","Group":"Server","SubCommands":["#,
    r#"{"Command":"GET","Name":"DUP2","Group":"Server"}]}]"#,
  );
  let tables = try_build_resp_commands_docs_tables(NO_DUP).expect("无重名对照应成功");
  assert_eq!(tables.all_sub.len(), 2);
}
