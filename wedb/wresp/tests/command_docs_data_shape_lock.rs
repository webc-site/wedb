//! COMMAND 数据形态复跑审计 + 元数据面（info）null/缺键/重名回归锁
//! （zcode-r123c-cmddocs1）
//!
//! 复跑判据：随包 RespCommandsDocs.json / RespCommandsInfo.json 全条目
//! 零 `"SubCommands":[]`、零缺 Command 键、零跨父重名（登记级三臂在现网
//! 数据上不触发，帧面与修复前逐字节等值）；并以合成 JSON 经纯函数面对拍
//! C# 数据源形态锁 info 面导入语义（缺 Command 键条目保留、未知枚举名与
//! 跨父重名整表熔断）。

use std::{collections::BTreeMap, slice::from_ref};

use sonic_rs::Deserialize;
use wresp::{
  catalog::{
    RESP_COMMANDS_DOCS_JSON, RESP_COMMANDS_INFO_JSON, try_build_resp_commands_info_tables,
  },
  command::RespCommand,
};

/// 数据普查最小面：Command 键存在性（Option 区分缺失/有值）与 SubCommands
/// 键存在性（Option 区分 null·缺失/数组）
#[derive(Deserialize, Default)]
struct Entry {
  #[serde(rename = "Command")]
  command: Option<String>,
  #[serde(rename = "Name")]
  name: String,
  #[serde(rename = "SubCommands")]
  sub_commands: Option<Vec<Entry>>,
}

/// 递归普查计数
#[derive(Default)]
struct Audit {
  total: usize,
  missing_command: Vec<String>,
  empty_sub_commands: Vec<String>,
  duplicate_sub_names: Vec<(String, String)>,
  seen_sub: BTreeMap<String, String>,
}

impl Audit {
  fn walk(&mut self, entries: &[Entry]) {
    for e in entries {
      self.total += 1;
      if e.command.is_none() {
        self.missing_command.push(e.name.clone());
      }
      if let Some(scs) = &e.sub_commands {
        if scs.is_empty() {
          self.empty_sub_commands.push(e.name.clone());
        }
        for sc in scs {
          let key = sc.name.to_lowercase();
          if let Some(prev) = self.seen_sub.insert(key, e.name.clone()) {
            self.duplicate_sub_names.push((prev, sc.name.clone()));
          }
          self.walk(from_ref(sc));
        }
      }
    }
  }
}

fn audit(json: &str) -> Audit {
  let roots: Vec<Entry> = sonic_rs::from_str(json).expect("随包数据必须可解析");
  let mut a = Audit::default();
  a.walk(&roots);
  a
}

/// 复跑判据：两份随包数据全条目零 `"SubCommands":[]`、零缺 Command 键、
/// 零跨父重名；条目数快照锁（docs 257 根 / 328 全条目，info 259 根 / 353 全条目）
#[test]
fn bundled_data_triggers_none_of_the_three_arms() {
  for (json, roots_expected, total_expected) in [
    (RESP_COMMANDS_DOCS_JSON, 257, 328),
    (RESP_COMMANDS_INFO_JSON, 259, 353),
  ] {
    let a = audit(json);
    assert!(
      a.missing_command.is_empty(),
      "缺 Command 键条目: {:?}",
      a.missing_command
    );
    assert!(
      a.empty_sub_commands.is_empty(),
      "空 SubCommands 数组条目: {:?}",
      a.empty_sub_commands
    );
    assert!(
      a.duplicate_sub_names.is_empty(),
      "跨父重名: {:?}",
      a.duplicate_sub_names
    );
    let roots: Vec<Entry> = sonic_rs::from_str(json).unwrap();
    assert_eq!(roots.len(), roots_expected, "根条目数快照");
    assert_eq!(a.total, total_expected, "含子命令全条目数快照");
  }
}

/// info 面随包数据端到端构建成功（判重熔断不误伤、表数与普查一致）
#[test]
fn info_tables_build_from_bundled_data() {
  let tables =
    try_build_resp_commands_info_tables(RESP_COMMANDS_INFO_JSON).expect("随包数据构建必须成功");
  assert_eq!(tables.all.len(), 259, "根命令表数快照");
  assert_eq!(
    tables.all_sub.len(),
    353 - 259,
    "子命令表数快照（零重名即全收录）"
  );
  assert!(
    !tables.flattened.contains_key(&(RespCommand::None as u16)),
    "NONE 判别值条目不入扁平索引（C# 同臂）"
  );
}

/// info 面臂2：缺 Command 键条目保留、command 落 NONE、不入扁平表
/// （C# STJ 缺成员 = 默认值 NONE；修复前 rust 为整表反序列化失败）
#[test]
fn info_missing_command_key_keeps_entry() {
  const JSON: &str = r#"[{"Name":"NOCMDI","Arity":-1,"Flags":"ReadOnly","AclCategories":"Read"}]"#;
  let tables = try_build_resp_commands_info_tables(JSON).expect("缺 Command 键应保留条目");
  let entry = tables.all.get("nocmdi").expect("条目应保留在根表");
  assert_eq!(entry.command, RespCommand::None);
  assert!(!tables.flattened.contains_key(&(RespCommand::None as u16)));

  // 子命令缺 Command 键同样保留落 NONE；C# 的 NONE 跳过守卫仅拦根条目
  // （RespCommandsInfo.cs:146），子命令 NONE 照常入扁平表（:159），双侧同形
  const SUB_JSON: &str = r#"[{"Command":"ACL","Name":"ACL","SubCommands":[{"Name":"ACL|X"}]}]"#;
  let tables = try_build_resp_commands_info_tables(SUB_JSON).expect("子命令缺键应保留");
  assert_eq!(
    tables.all_sub.get("acl|x").expect("子命令保留").command,
    RespCommand::None
  );
  assert!(tables.flattened.contains_key(&(RespCommand::None as u16)));
}

/// info 面臂2 反向：未知枚举名整表失败（C# JsonException catch 才炸整表同臂）
#[test]
fn info_unknown_command_name_fails_whole_table() {
  assert!(
    try_build_resp_commands_info_tables(r#"[{"Command":"NOT_A_REAL_COMMAND","Name":"X"}]"#)
      .is_none()
  );
}

/// info 面臂3：跨父重名子命令整表熔断（rust 口径空表 = 全查询 None；
/// C# 口径 Add 抛 ArgumentException 命令层上抛——两口径非等形，分写登记）
#[test]
fn info_duplicate_sub_command_names_fail_whole_table() {
  const DUP: &str = concat!(
    r#"[{"Command":"ACL","Name":"FOO","SubCommands":[{"Command":"PING","Name":"DUPSUB"}]},"#,
    r#"{"Command":"SET","Name":"BAR","SubCommands":[{"Command":"GET","Name":"dupsub"}]}]"#
  );
  assert!(
    try_build_resp_commands_info_tables(DUP).is_none(),
    "跨父重名须整表熔断"
  );

  const OK: &str = concat!(
    r#"[{"Command":"ACL","Name":"FOO","SubCommands":[{"Command":"PING","Name":"DUP1"}]},"#,
    r#"{"Command":"SET","Name":"BAR","SubCommands":[{"Command":"GET","Name":"DUP2"}]}]"#
  );
  let tables = try_build_resp_commands_info_tables(OK).expect("无重名对照应成功");
  assert_eq!(tables.all_sub.len(), 2);
}
