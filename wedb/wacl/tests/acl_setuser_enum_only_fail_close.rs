#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 枚举在场、目录缺席名的 SETUSER 失败关闭锁测（票 wacl-acl-setuser-enum-only-name-custom-fallback）
//!
//! 在 garnet 中的相对路径: libs/server/ACL/ACLParser.cs:TryParseCommandForAcl
//! （`Enum.TryParse` 全枚举成员判定，:281/:285）+ libs/server/ACL/User.cs:
//! AddCommand/:189、RemoveCommand/:318（查 RespCommandsInfo 目录失配即抛
//! "Unable to obtain ACL information, this shouldn't be possible"，被
//! libs/server/Resp/ACLCommands.cs:228 catch 回 -ERR）——失败关闭。
//!
//! C# `RespCommand.cs:40 DELIFEXPIM=9 / :94 RIPROMOTE=63 / :95 RIRESTORE=64`
//! 枚举在场而 RespCommandsInfo.json 目录零条目（亲验 grep 计 0）。rust 判定集
//! 为 `RespCommand` 枚举成员名集的 strum 编译期单源派生
//! （`RespCommand::from_cs_name`，禁第二份手写名单），此三名解析命中后由
//! `User::apply_command` 目录查询失败关闭，绝不落自定义命令回落臂。

use wacl::{AclError, AclParser, User};
use wresp::command::RespCommand;

/// C# User.cs AddCommand/RemoveCommand 抛形消息（本侧错误面同文案）
const ACL_INFO_ERR: &str = "Unable to obtain ACL information, this shouldn't be possible";

/// 解析臂命中形：枚举在场目录缺席名照常 Some（C# Enum.TryParse 全成员对位），
/// 大小写折叠与去点形同判
#[test]
fn enum_only_names_parse_to_commands() {
  assert_eq!(
    AclParser::try_parse_command_for_acl("DELIFEXPIM").unwrap(),
    Some(RespCommand::Delifexpim)
  );
  assert_eq!(
    AclParser::try_parse_command_for_acl("ripromote").unwrap(),
    Some(RespCommand::Ripromote)
  );
  assert_eq!(
    AclParser::try_parse_command_for_acl("RiReStOrE").unwrap(),
    Some(RespCommand::Rirestore)
  );
  // 去点形：RI.PROMOTE → RIPROMOTE（对标 C# :285 dotless 重试臂）
  assert_eq!(
    AclParser::try_parse_command_for_acl("RI.PROMOTE").unwrap(),
    Some(RespCommand::Ripromote)
  );
  assert_eq!(
    AclParser::try_parse_command_for_acl("RI.RESTORE").unwrap(),
    Some(RespCommand::Rirestore)
  );
  assert_eq!(
    AclParser::try_parse_command_for_acl("DElIF.EXPIM").unwrap(),
    Some(RespCommand::Delifexpim)
  );
}

/// 加减双臂失败关闭：三名 × 大小写形 × 去点形均回 C# 同文案错误，
/// 既有权限零变化、自定义轨零收录（两档 feature 共用本 wacl 路径，
/// 判定先于 wnode 侧扩展命令注册门，档位无关）
#[test]
fn enum_only_names_apply_fail_closed_zero_change() {
  let mut user = User::new("u".to_string());
  AclParser::apply_acl_op_to_user(&mut user, "on").unwrap();
  AclParser::apply_acl_op_to_user(&mut user, "+set").unwrap();

  for op in [
    "+DELIFEXPIM",
    "-DELIFEXPIM",
    "+RIPROMOTE",
    "-ripromote",
    "+RIRESTORE",
    "-RiReStOrE",
    "+RI.PROMOTE",
    "-RI.RESTORE",
    "+DElIF.EXPIM",
  ] {
    let err = AclParser::apply_acl_op_to_user(&mut user, op)
      .err()
      .unwrap_or_else(|| panic!("{op} 应失败关闭却成功"));
    assert!(
      matches!(&err, AclError::Acl(m) if m == ACL_INFO_ERR),
      "{op} 错误形或文案偏离 C# User.AddCommand 抛形: {err:?}"
    );
    assert_eq!(err.to_string(), ACL_INFO_ERR, "{op} Display 文案偏离");
  }

  // 零权限变化：基线 +set 仍在、三名零授予、自定义轨零收录
  assert!(user.can_access_command(RespCommand::Set));
  assert!(!user.can_access_command(RespCommand::Delifexpim));
  assert!(!user.can_access_command(RespCommand::Ripromote));
  assert!(!user.can_access_command(RespCommand::Rirestore));
  assert!(user.custom_commands_allowed().is_empty());
  assert!(user.custom_commands_denied().is_empty());
}

/// 正对照：枚举亦无的纯字母未知名仍走自定义回落轨（形不变）；
/// 归一化实现细节名（IsInvalidCommandToAcl 拒形，如 SETEXXX）同落自定义轨
#[test]
fn unknown_and_internal_detail_names_still_take_custom_rail() {
  let mut user = User::new("u".to_string());
  AclParser::apply_acl_op_to_user(&mut user, "+FOOBAR").unwrap();
  AclParser::apply_acl_op_to_user(&mut user, "-QUUXCMD").unwrap();
  // 枚举在场但归一化（normalize_for_acls(Setexxx)=Set）→ C# IsInvalidCommandToAcl
  // 拒解析 → 自定义轨
  AclParser::apply_acl_op_to_user(&mut user, "+SETEXXX").unwrap();

  let allowed = user.custom_commands_allowed();
  for name in ["FOOBAR", "SETEXXX"] {
    assert!(allowed.contains(name), "{name} 应入自定义允许轨");
  }
  assert!(user.custom_commands_denied().contains("QUUXCMD"));
}

/// 子命令折名目录缺席（BITOP|AND → BITOP_AND，RespCommandsInfo.json 零条目）：
/// C# ACLParser.cs if(isSubCommand) 块 throw "Couldn't load information for
/// BITOP_AND, shouldn't be possible"（先于 IsInvalidCommandToAcl，绝不落
/// 自定义回落轨），rust 同位 fail-closed 上抛、文案逐字对位
#[test]
fn subcommand_fold_catalog_absent_fails_closed() {
  let mut user = User::new("u".to_string());
  let err = AclParser::apply_acl_op_to_user(&mut user, "+BITOP|AND")
    .expect_err("目录缺席子命令折名须 fail-closed");
  assert!(
    err
      .to_string()
      .contains("Couldn't load information for BITOP_AND"),
    "文案须逐字对位 C# ACLException: {err}"
  );
  assert!(user.custom_commands_allowed().is_empty(), "绝不入自定义轨");
}
