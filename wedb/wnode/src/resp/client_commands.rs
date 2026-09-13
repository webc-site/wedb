//! CLIENT 族命令（对标 libs/server/Resp/ClientCommands.cs）
//!
//! C# ClientCommands.cs 为 `RespServerSession` 的 partial 分片，本文件同构
//! 映射为 `impl RespServerSession` 扩展块；CLIENT INFO/GETNAME/SETNAME/
//! SETINFO 直读会话真实状态（clientName / clientLib* / 端点 / Id）。

use std::str::from_utf8;

use wresp::{
  RespVecExt, cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments, write_raw},
  strict_i64,
};

use super::resp_server_session::RespServerSession;
use crate::session_parse_state_extensions::try_get_client_name_bytes;

impl RespServerSession {
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTLIST
  ///
  /// C# 首行即 `Server is GarnetServerBase` 判定，非 Garnet 服务器场景直接回
  /// "cannot be listed"（在参数校验之前）；rust 会话层无服务器活跃消费者
  /// 注册表（对应 C# GarnetServerBase.ActiveConsumers 链），与该分支语义一致。
  /// 缺口：wserver 需补服务器级消费者枚举后方可实现 TYPE/ID 过滤
  pub fn network_clientlist(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    abort_with_error_message(output, cs::RESP_ERR_CANNOT_LIST_CLIENTS);
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTINFO
  pub fn network_clientinfo(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "client|info");
      return Ok(true);
    }

    // C# 拼行后 WriteLargeVerbatimString（RESP3 verbatim / RESP2 bulk）；rust
    // 会话恒默认 RESP2，按 bulk string 应答
    let mut result = String::new();
    self.write_client_info_state(&mut result);
    result.push('\n');
    output.write_resp_bulk_string(result.as_bytes());
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTKILL
  ///
  /// 同 NetworkCLIENTLIST：C# 先行 `Server is GarnetServerBase` 判定，无消费者
  /// 注册表时参数校验不可达，直接回 "cannot be listed"
  pub fn network_clientkill(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    abort_with_error_message(output, cs::RESP_ERR_CANNOT_LIST_CLIENTS);
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTGETNAME
  pub fn network_clientgetname(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "CLIENT|GETNAME");
      return Ok(true);
    }
    // C# IsNullOrEmpty → nil（空名视同未设置）
    match self.client_name.as_deref() {
      Some(name) if !name.is_empty() => output.write_resp_bulk_string(name.as_bytes()),
      _ => output.write_resp_null(),
    }
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTSETNAME
  pub fn network_clientsetname(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "CLIENT|SETNAME");
      return Ok(true);
    }

    // 对标 C# TryGetClientName：33..=126 可打印字符，空串允许（清名语义）
    match try_get_client_name_bytes(parse_state[0]) {
      Some(name) => {
        self.set_client_name(Some(name));
        write_raw(output, cs::RESP_OK);
      }
      None => abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_NAME),
    }
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTSETINFO
  pub fn network_clientsetinfo(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "CLIENT|SETINFO");
      return Ok(true);
    }

    let option = parse_state[0];
    let value = parse_state[1];
    // C# 对 "LIB-NAME"/"lib-name" 与 "LIB-VER"/"lib-ver" 做两组精确匹配
    //（`-` 无法经 ASCII 大小写折叠，故非大小写不敏感）
    let Ok(value) = from_utf8(value) else {
      // C# GetString 为 ASCII 解码不失败；非法 UTF-8 按空值承接
      write_raw(output, cs::RESP_OK);
      return Ok(true);
    };
    if option == b"LIB-NAME" || option == b"lib-name" {
      self.client_lib_name = Some(value.to_string());
      write_raw(output, cs::RESP_OK);
    } else if option == b"LIB-VER" || option == b"lib-ver" {
      self.client_lib_version = Some(value.to_string());
      write_raw(output, cs::RESP_OK);
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    }
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTUNBLOCK
  ///
  /// 经纪注入时按客户端 ID 查观察者并强制解除（TIMEOUT → 空结果，
  /// ERROR → UNBLOCKED 错误应答给被解除方）；无经纪/未阻塞回 0
  pub fn network_clientunblock(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 && parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "client|unblock");
      return Ok(true);
    }

    // 解析目标客户端 ID（C# TryGetLong 严格口径）
    let Some(client_id) = strict_i64(parse_state[0]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    let mut throw_error = false;
    if parse_state.len() == 2 {
      let option = parse_state[1];
      if option.eq_ignore_ascii_case(b"TIMEOUT") {
        throw_error = false;
      } else if option.eq_ignore_ascii_case(b"ERROR") {
        throw_error = true;
      } else {
        abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON);
        return Ok(true);
      }
    }

    // C# ActiveConsumers 找会话 → itemBroker.TryGetObserver → TryForceUnblock；
    // rust 观察者以会话 ID 直键经纪（会话 ID 即客户端 ID），未阻塞即回 0
    let unblocked = self
      .item_broker
      .as_ref()
      .and_then(|broker| broker.try_get_observer(client_id as usize))
      .is_some_and(|observer| observer.try_force_unblock(throw_error));
    output.write_resp_int(i64::from(unblocked));
    Ok(true)
  }
}
