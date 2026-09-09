//! 共享对象命令（对标 libs/server/Resp/Objects/SharedObjectCommands.cs）
//!
//! ObjectScan 是四类集合对象共用的 ZSCAN/HSCAN/SSCAN/COSCAN 入口：
//! 参数校验与光标解析在本层完成，对象遍历经 ObjectInput 走各对象的
//! scan 分片（有序集合侧见 objects::sortedset::sorted_set_object_impl::scan_operate）。

use std::str;

use crate::{
  arg_slice::ArgSlice,
  input_header::RespInputHeader,
  inputs::ObjectInput,
  objects::types::object_output::ObjectOutput,
  resp::resp_server_session::RespServerSession,
  session_parse_state::SessionParseState,
  types::{GarnetObjectType, RespInputFlags},
};

impl RespServerSession {
  /// HSCAN / SSCAN / ZSCAN / COSCAN 共享入口
  ///
  /// libs/server/Resp/Objects/SharedObjectCommands.cs:ObjectScan
  ///
  /// `scan_count_limit` 对应 OBJECT_SCAN_COUNT_LIMIT 运行时配置（经 arg2 下发钳制 COUNT）；
  /// `operate` 为装载对象后的操作回调（由调用方按对象类型注入），返回 RESP 负载
  pub fn object_scan(
    &mut self,
    parse_state: &[&[u8]],
    object_type: GarnetObjectType,
    scan_count_limit: i32,
    output: &mut Vec<u8>,
    operate: impl FnOnce(&ObjectInput, &mut ObjectOutput) -> bool,
  ) -> bool {
    // 命令名仅用于错误文本
    let cmd_name = match object_type {
      GarnetObjectType::Hash => "HSCAN",
      GarnetObjectType::Set => "SSCAN",
      GarnetObjectType::SortedSet => "ZSCAN",
      GarnetObjectType::All => "COSCAN",
      _ => "NONE",
    };

    if parse_state.len() < 2 {
      return self.abort_with_wrong_number_of_arguments(cmd_name, output);
    }

    // 光标须为非负整数
    let Some(_cursor_value) = str::from_utf8(parse_state[1])
      .unwrap_or("")
      .parse::<i64>()
      .ok()
      .filter(|v| *v >= 0)
    else {
      return self.abort_with_error_message(b"ERR invalid cursor", output);
    };

    // ObjectInput：startIdx = 1（跳过键），arg2 = 单轮 COUNT 上限
    let backing: Vec<Vec<u8>> = parse_state[1..].iter().map(|a| a.to_vec()).collect();
    let slices: Vec<ArgSlice> = backing
      .iter()
      .map(|b| ArgSlice::new(b.as_ptr(), b.len()))
      .collect();
    let mut session_parse_state = SessionParseState::new();
    session_parse_state.initialize_with_args(&slices);

    let header = RespInputHeader::new_with_type(object_type, RespInputFlags::empty());
    let input = ObjectInput::new_with_state(header, &mut session_parse_state, 0, scan_count_limit);

    let mut obj_out = ObjectOutput::new();
    operate(&input, &mut obj_out);
    output.extend_from_slice(&obj_out.payload);
    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn object_scan_validates_args() {
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    // 参数不足：中止返回 true（命令已完整消费，对齐 C# Abort 语义）
    assert!(sess.object_scan(
      &[b"key"],
      GarnetObjectType::SortedSet,
      10,
      &mut out,
      |_, _| true,
    ));
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZSCAN' command\r\n"
    );

    // 非法光标
    out.clear();
    assert!(sess.object_scan(
      &[b"key", b"-1"],
      GarnetObjectType::Hash,
      10,
      &mut out,
      |_, _| true,
    ));
    assert_eq!(out, b"-ERR invalid cursor\r\n");

    // 合法输入透传至操作回调
    out.clear();
    assert!(sess.object_scan(
      &[b"key", b"0", b"COUNT", b"5"],
      GarnetObjectType::SortedSet,
      10,
      &mut out,
      |input, out| {
        assert_eq!(input.arg2, 10);
        assert_eq!(input.parse_state.count, 3); // 键已在底层剥离
        out.payload.extend_from_slice(b"*2\r\n$1\r\n0\r\n*0\r\n");
        true
      },
    ));
    assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");
  }
}
