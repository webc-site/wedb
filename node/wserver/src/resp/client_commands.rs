use super::{
  basic_commands::try_get_client_name,
  cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments, write_raw},
  parser::resp_ext::{RespSliceExt, RespVecExt},
  resp_server_session::RespServerSession,
};

pub struct ClientCommands;

impl ClientCommands {
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTLIST
  ///
  /// C# 首行即 `Server is GarnetServerBase` 判定，非 Garnet 服务器场景直接回
  /// "cannot be listed"（在参数校验之前）；rust 会话层无服务器活跃消费者
  /// 注册表（对应 C# GarnetServerBase.ActiveConsumers 链），与该分支语义一致。
  /// 缺口：wserver 需补服务器级消费者枚举后方可实现 TYPE/ID 过滤
  pub fn network_clientlist<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    abort_with_error_message(output, cs::RESP_ERR_CANNOT_LIST_CLIENTS);
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTINFO
  pub fn network_clientinfo<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "client|info");
      return Ok(true);
    }

    // C# 拼行后 WriteLargeVerbatimString（RESP3 verbatim / RESP2 bulk）；rust
    // 会话恒默认 RESP2，按 bulk string 应答
    let mut result = String::new();
    RespServerSession::write_client_info(&mut result);
    result.push('\n');
    output.write_resp_bulk_string(result.as_bytes());
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTKILL
  ///
  /// 同 NetworkCLIENTLIST：C# 先行 `Server is GarnetServerBase` 判定，无消费者
  /// 注册表时参数校验不可达，直接回 "cannot be listed"
  pub fn network_clientkill<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    abort_with_error_message(output, cs::RESP_ERR_CANNOT_LIST_CLIENTS);
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTGETNAME
  pub fn network_clientgetname<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "CLIENT|GETNAME");
      return Ok(true);
    }
    // rust 会话无 clientName 字段（CLIENT SETNAME 校验后无法持久化），等价
    // C# "名字未设置"路径：nil
    output.write_resp_null();
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTSETNAME
  pub fn network_clientsetname<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "CLIENT|SETNAME");
      return Ok(true);
    }

    // 对标 TryGetClientName：33..=126 可打印字符，空串允许（清名语义）；
    // rust 会话无 clientName 字段可存（待会话状态域补齐），校验后即回 OK
    match try_get_client_name(parse_state[0]) {
      Some(_) => write_raw(output, cs::RESP_OK),
      None => abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_NAME),
    }
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTSETINFO
  pub fn network_clientsetinfo<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "CLIENT|SETINFO");
      return Ok(true);
    }

    let option = parse_state[0];
    // C# 对 "LIB-NAME"/"lib-name" 与 "LIB-VER"/"lib-ver" 做两组精确匹配
    //（`-` 无法经 ASCII 大小写折叠，故非大小写不敏感）；rust 会话无
    // clientLibName/clientLibVersion 字段可存，校验后即回 OK
    if option == b"LIB-NAME"
      || option == b"lib-name"
      || option == b"LIB-VER"
      || option == b"lib-ver"
    {
      write_raw(output, cs::RESP_OK);
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    }
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTUNBLOCK
  pub fn network_clientunblock<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 && parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "client|unblock");
      return Ok(true);
    }

    // 解析目标客户端 ID（严格整数）
    let Some(_client_id) = parse_state[0].try_parse_i64() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    if parse_state.len() == 2 {
      let option = parse_state[1];
      if !option.eq_ignore_ascii_case(b"TIMEOUT") && !option.eq_ignore_ascii_case(b"ERROR") {
        abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON);
        return Ok(true);
      }
    }

    // C# 服务器无 itemBroker / 会话未阻塞时回 0；rust 无阻塞观察者域，
    // "解除 0 个客户端"是语义一致的常量结果
    output.write_resp_int(0);
    Ok(true)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::*;

  type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

  fn with_batch(f: impl FnOnce(&mut ClientCommands, &Batch)) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("client.db")).unwrap());
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = ClientCommands;
      f(&mut s, &batch);
    });
  }

  #[test]
  fn list_and_kill_report_unavailable() {
    with_batch(|c, batch| {
      // C# 非 GarnetServerBase 分支：参数校验之前即回 cannot be listed
      let mut out = Vec::new();
      let _ = c
        .network_clientlist(&[b"GARBAGE"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Clients cannot be listed.\r\n");

      let mut out = Vec::new();
      let _ = c.network_clientkill(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR Clients cannot be listed.\r\n");
    });
  }

  #[test]
  fn clientinfo_frame() {
    with_batch(|c, batch| {
      let mut out = Vec::new();
      let _ = c.network_clientinfo(&[], batch, &mut out).unwrap();
      let expected = "id=0 addr= laddr= age=0 flags=N db=0 resp=2 lib-name= lib-ver=\n";
      let frame = format!("${}\r\n{expected}\r\n", expected.len());
      assert_eq!(out, frame.as_bytes());

      let mut out = Vec::new();
      let _ = c.network_clientinfo(&[b"x"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'client|info' command\r\n"
      );
    });
  }

  #[test]
  fn getname_returns_null() {
    with_batch(|c, batch| {
      let mut out = Vec::new();
      let _ = c.network_clientgetname(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");

      let mut out = Vec::new();
      let _ = c.network_clientgetname(&[b"x"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'CLIENT|GETNAME' command\r\n"
      );
    });
  }

  #[test]
  fn setname_validation() {
    with_batch(|c, batch| {
      let mut out = Vec::new();
      let _ = c
        .network_clientsetname(&[b"client-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");

      // 空串允许（清名语义）
      let mut out = Vec::new();
      let _ = c.network_clientsetname(&[b""], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");

      // 含空格
      let mut out = Vec::new();
      let _ = c.network_clientsetname(&[b"a b"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n"
      );

      // 参数个数
      let mut out = Vec::new();
      let _ = c.network_clientsetname(&[], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'CLIENT|SETNAME' command\r\n"
      );
    });
  }

  #[test]
  fn setinfo_exact_case_match() {
    with_batch(|c, batch| {
      for option in [b"LIB-NAME".as_slice(), b"lib-name".as_slice()] {
        let mut out = Vec::new();
        let _ = c
          .network_clientsetinfo(&[option, b"phpredis"], batch, &mut out)
          .unwrap();
        assert_eq!(out, b"+OK\r\n");
      }

      for option in [
        b"lib-nameX".as_slice(),
        b"Lib-Name".as_slice(),
        b"BAD".as_slice(),
      ] {
        let mut out = Vec::new();
        let _ = c
          .network_clientsetinfo(&[option, b"v"], batch, &mut out)
          .unwrap();
        assert_eq!(out, b"-ERR syntax error\r\n");
      }

      let mut out = Vec::new();
      let _ = c
        .network_clientsetinfo(&[b"lib-ver"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'CLIENT|SETINFO' command\r\n"
      );
    });
  }

  #[test]
  fn unblock_validation() {
    with_batch(|c, batch| {
      let mut out = Vec::new();
      let _ = c.network_clientunblock(&[b"123"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = c
        .network_clientunblock(&[b"123", b"error"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      // 非法 ID（严格整数，含尾随句点校验）
      let mut out = Vec::new();
      let _ = c.network_clientunblock(&[b"1x"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // 非法原因
      let mut out = Vec::new();
      let _ = c
        .network_clientunblock(&[b"1", b"WHATEVER"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR\r\n"
      );

      // 参数个数
      let mut out = Vec::new();
      let _ = c.network_clientunblock(&[], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'client|unblock' command\r\n"
      );

      let mut out = Vec::new();
      let _ = c
        .network_clientunblock(&[b"1", b"TIMEOUT", b"x"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'client|unblock' command\r\n"
      );
    });
  }
}
