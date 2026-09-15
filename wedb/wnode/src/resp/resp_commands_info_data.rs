//! 命令枚举名 ↔ [`RespCommand`] 静态对照（基于 strum
//! 派生，对标 C# `Enum.TryParse`/`ToString` 的大小写不敏感双向解析；
//! resp_commands_info 域导入 / 导出 JSON 时使用）
//!
//! 与 wresp::catalog 的目录表（RespCommandsInfo.json 的 ACL 消费面，
//! 353 命令）同源自 wresources 内嵌 JSON 但非同一份元数据：本表覆盖全枚举
//! （含 BITOP_\* / SETEXNX 等非真实命令与 NONE / INVALID / RESET 等 16 个
//! 目录表缺口），被 JSON 导入导出 / 分派命名 / 脚本 ACL 路径广泛消费。

use std::str::FromStr;

use wresp::RespCommand;

/// 首个数据命令（libs/server/Resp/Parser/RespCommand.cs:FirstDataCommand = APPEND）
pub const FIRST_DATA_COMMAND: RespCommand = RespCommand::Append;
/// 末个数据命令（libs/server/Resp/Parser/RespCommand.cs:LastDataCommand = EVALSHA）
pub const LAST_DATA_COMMAND: RespCommand = RespCommand::Evalsha;

/// C# 枚举成员名（如 `ACL_CAT`）→ [`RespCommand`]（大小写不敏感）
///
/// libs/server/Resp/Parser/RespCommand.cs:Enum.TryParse(ignoreCase)
#[inline]
pub fn resp_command_from_cs_name(name: &str) -> Option<RespCommand> {
  RespCommand::from_str(name).ok()
}

/// [`RespCommand`] → C# 枚举成员名（C# ToString()）
#[inline]
pub fn resp_command_to_cs_name(cmd: RespCommand) -> &'static str {
  cmd.into()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_resp_command_from_and_to_cs_name() {
    assert_eq!(
      resp_command_from_cs_name("ACL_CAT"),
      Some(RespCommand::AclCat)
    );
    assert_eq!(
      resp_command_from_cs_name("acl_cat"),
      Some(RespCommand::AclCat)
    );
    assert_eq!(
      resp_command_from_cs_name("APPEND"),
      Some(RespCommand::Append)
    );
    assert_eq!(
      resp_command_from_cs_name("bitop_and"),
      Some(RespCommand::BitopAnd)
    );
    assert_eq!(
      resp_command_from_cs_name("CLUSTER_SEND_CKPT_FILE_SEGMENT"),
      Some(RespCommand::ClusterSendCkptFileSegment)
    );
    assert_eq!(
      resp_command_from_cs_name("INVALID"),
      Some(RespCommand::Invalid)
    );
    assert_eq!(resp_command_from_cs_name("unknown_command"), None);

    assert_eq!(resp_command_to_cs_name(RespCommand::AclCat), "ACL_CAT");
    assert_eq!(resp_command_to_cs_name(RespCommand::Append), "APPEND");
    assert_eq!(resp_command_to_cs_name(RespCommand::BitopAnd), "BITOP_AND");
    assert_eq!(
      resp_command_to_cs_name(RespCommand::ClusterSendCkptFileSegment),
      "CLUSTER_SEND_CKPT_FILE_SEGMENT"
    );
    assert_eq!(resp_command_to_cs_name(RespCommand::Invalid), "INVALID");
  }
}
