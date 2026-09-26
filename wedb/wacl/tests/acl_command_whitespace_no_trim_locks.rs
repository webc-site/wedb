//! ACL +命令名 携首尾空白词形拒收锁测（deviations §105，零行为改动纯增量锁）
//!
//! 在 garnet 中的相对路径: libs/server/ACL/ACLParser.cs:TryParseCommandForAcl（空白怪癖面对拍锁）
//!
//! C# 侧 `Enum.TryParse(ignoreCase)` 沿 Parse 语义静默修剪首尾空白，
//! `ACL SETUSER u "+ get"` 授权 GET 回 +OK；rust 精确查表不修剪，必落
//! `CommandDoesNotExist`。本锁锁 rust 现状拒收面，严禁按 C# 怪癖补 trim。

use wacl::{AclError, AclParser, User};
use wresp::command::RespCommand;

/// 命令名解析臂：携带首尾空白的词形一律 None（不修剪、不镜像 C# 怪癖）
#[test]
fn try_parse_command_for_acl_whitespace_forms_none() {
  // 前导 / 尾随 / 双侧空白，及子命令折名与去点重试臂同形
  assert_eq!(AclParser::try_parse_command_for_acl(" get"), None);
  assert_eq!(AclParser::try_parse_command_for_acl("get "), None);
  assert_eq!(AclParser::try_parse_command_for_acl(" get "), None);
  assert_eq!(AclParser::try_parse_command_for_acl("\tget"), None);
  assert_eq!(AclParser::try_parse_command_for_acl(" client|list"), None);
  assert_eq!(AclParser::try_parse_command_for_acl(" ri.create "), None);
  // 正对照：无空白词形照常解析（证拒收仅因空白，非名字本身）
  assert_eq!(
    AclParser::try_parse_command_for_acl("get"),
    Some(RespCommand::Get)
  );
  assert_eq!(
    AclParser::try_parse_command_for_acl(" client|list".trim()),
    Some(RespCommand::ClientList)
  );
}

/// SETUSER 活链臂：`+ 命令名` 携空白回 CommandDoesNotExist 精确文案且零授权
#[test]
fn apply_acl_op_plus_whitespace_command_rejected_no_grant() {
  let mut user = User::new("u".to_string());
  for (op, name) in [("+ get", " get"), ("+get ", "get "), ("+ get ", " get ")] {
    match AclParser::apply_acl_op_to_user(&mut user, op) {
      Err(AclError::CommandDoesNotExist(got)) => {
        // 原样入错、未修剪（补 trim 即引入空白名静默授权，deviations §105 严禁面）
        assert_eq!(got, name);
        assert_eq!(
          AclError::CommandDoesNotExist(name.to_string()).to_string(),
          format!("Command '{name}' does not exist")
        );
      }
      other => panic!("op '{op}' 应落 CommandDoesNotExist，实得 {other:?}"),
    }
  }
  // 空白词形零授权：GET 权限未被静默授予
  assert!(!user.can_access_command(RespCommand::Get));
  // 正对照：无空白形照常授权
  AclParser::apply_acl_op_to_user(&mut user, "+get").unwrap();
  assert!(user.can_access_command(RespCommand::Get));
}
