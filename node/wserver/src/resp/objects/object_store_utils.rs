//! 对象存储命令中止工具（对标 libs/server/Resp/Objects/ObjectStoreUtils.cs，
//! C# 为 RespServerSession partial；Rust 侧错误帧写入 `output` 缓冲）
//!
//! `AbortWithWrongNumberOfArgumentsOrUnknownSubcommand` 已由 resp::admin_commands
//! 在 RespServerSession 上实现（同映射注释），此处不再重复定义；
//! 自由函数形态的 AbortWithWrongNumberOfArguments/AbortWithErrorMessage
//! 另见 resp::cmd_strings。

use crate::resp::{cmd_strings::write_error_raw, resp_server_session::RespServerSession};

/// ERR wrong number of arguments for '{0}' command
const GENERIC_ERR_WRONG_NUM_ARGS: &str = "ERR wrong number of arguments for '{name}' command";

impl RespServerSession {
  /// 参数数量错误中止（始终消费完整命令，返回 true）
  ///
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithWrongNumberOfArguments
  pub fn abort_with_wrong_number_of_arguments(
    &mut self,
    cmd_name: &str,
    output: &mut Vec<u8>,
  ) -> bool {
    write_error_raw(
      output,
      &GENERIC_ERR_WRONG_NUM_ARGS.replace("{name}", cmd_name),
    );
    true
  }

  /// 以给定错误信息中止
  ///
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithErrorMessage
  /// （C# 置 commandErrorWritten 后经 RespWriteUtils 写错误帧）
  pub fn abort_with_error_message(&mut self, error_message: &[u8], output: &mut Vec<u8>) -> bool {
    output.push(b'-');
    output.extend_from_slice(error_message);
    output.extend_from_slice(b"\r\n");
    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn abort_frames_match_csharp_text() {
    let mut sess = RespServerSession;
    let mut out = Vec::new();
    assert!(sess.abort_with_wrong_number_of_arguments("ZADD", &mut out));
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZADD' command\r\n"
    );

    out.clear();
    assert!(sess.abort_with_error_message(b"ERR custom", &mut out));
    assert_eq!(out, b"-ERR custom\r\n");
  }
}
