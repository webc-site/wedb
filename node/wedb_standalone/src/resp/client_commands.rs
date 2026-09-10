//! CLIENT 族命令（对标 libs/server/Resp/ClientCommands.cs）
//!
//! C# ClientCommands.cs 为 `RespServerSession` 的 partial 分片，本文件同构
//! 映射为 `impl RespServerSession` 扩展块；CLIENT INFO/GETNAME/SETNAME/
//! SETINFO 直读会话真实状态（clientName / clientLib* / 端点 / Id）。

use std::str::from_utf8;

use super::{
  basic_commands::try_get_client_name,
  cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments, write_raw},
  parser::{resp_ext::RespVecExt, session_parse_state::strict_i64},
  resp_server_session::RespServerSession,
};

impl RespServerSession {
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
    self.write_client_info_state(&mut result);
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
    // C# IsNullOrEmpty → nil（空名视同未设置）
    match self.client_name.as_deref() {
      Some(name) if !name.is_empty() => output.write_resp_bulk_string(name.as_bytes()),
      _ => output.write_resp_null(),
    }
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

    // 对标 C# TryGetClientName：33..=126 可打印字符，空串允许（清名语义）
    match try_get_client_name(parse_state[0]) {
      Some(name) => {
        self.set_client_name(Some(name));
        write_raw(output, cs::RESP_OK);
      }
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

    // 解析目标客户端 ID（C# TryGetLong 严格口径）
    let Some(_client_id) = strict_i64(parse_state[0]) else {
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

  fn with_batch(f: impl FnOnce(&mut RespServerSession, &Batch)) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("client.db")).unwrap());
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = RespServerSession::default();
      f(&mut s, &batch);
    });
  }

  #[test]
  fn list_and_kill_report_unavailable() {
    with_batch(|s, batch| {
      // C# 非 GarnetServerBase 分支：参数校验之前即回 cannot be listed
      let mut out = Vec::new();
      let _ = s
        .network_clientlist(&[b"GARBAGE"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Clients cannot be listed.\r\n");

      let mut out = Vec::new();
      let _ = s.network_clientkill(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR Clients cannot be listed.\r\n");
    });
  }

  #[test]
  fn clientinfo_frame() {
    with_batch(|s, batch| {
      s.id = 42;
      s.remote_endpoint = "127.0.0.1:9999".to_string();
      s.set_client_lib_info(Some("redis-py"), Some("5.0.1"));
      let mut out = Vec::new();
      let _ = s.network_clientinfo(&[], batch, &mut out).unwrap();
      let expected = "id=42 addr=127.0.0.1:9999 laddr= age=0 flags=N db=0 resp=2 lib-name=redis-py lib-ver=5.0.1\n";
      let frame = format!("${}\r\n{expected}\r\n", expected.len());
      assert_eq!(out, frame.as_bytes());

      let mut out = Vec::new();
      let _ = s.network_clientinfo(&[b"x"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'client|info' command\r\n"
      );
    });
  }

  #[test]
  fn getname_returns_session_name() {
    with_batch(|s, batch| {
      // 未设置 → nil（C# IsNullOrEmpty 分支）
      let mut out = Vec::new();
      let _ = s.network_clientgetname(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");

      // SETNAME 后 GETNAME 回读真实会话状态
      let mut out = Vec::new();
      let _ = s
        .network_clientsetname(&[b"client-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");
      let mut out = Vec::new();
      let _ = s.network_clientgetname(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"$8\r\nclient-1\r\n");

      // 空串清名 → GETNAME 回 nil
      let mut out = Vec::new();
      let _ = s.network_clientsetname(&[b""], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");
      let mut out = Vec::new();
      let _ = s.network_clientgetname(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");

      let mut out = Vec::new();
      let _ = s.network_clientgetname(&[b"x"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'CLIENT|GETNAME' command\r\n"
      );
    });
  }

  #[test]
  fn setname_validation() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .network_clientsetname(&[b"client-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");

      // 空串允许（清名语义）
      let mut out = Vec::new();
      let _ = s.network_clientsetname(&[b""], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");

      // 含空格
      let mut out = Vec::new();
      let _ = s.network_clientsetname(&[b"a b"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n"
      );

      // 参数个数
      let mut out = Vec::new();
      let _ = s.network_clientsetname(&[], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'CLIENT|SETNAME' command\r\n"
      );
    });
  }

  #[test]
  fn setinfo_exact_case_match_and_state() {
    with_batch(|s, batch| {
      for option in [b"LIB-NAME".as_slice(), b"lib-name".as_slice()] {
        let mut out = Vec::new();
        let _ = s
          .network_clientsetinfo(&[option, b"phpredis"], batch, &mut out)
          .unwrap();
        assert_eq!(out, b"+OK\r\n");
      }
      // LIB-NAME 后接 LIB-VER：两槽分别落库
      assert_eq!(s.client_lib_name.as_deref(), Some("phpredis"));

      let mut out = Vec::new();
      let _ = s
        .network_clientsetinfo(&[b"lib-ver", b"6.0.2".as_slice()], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");
      assert_eq!(s.client_lib_version.as_deref(), Some("6.0.2"));

      for option in [
        b"lib-nameX".as_slice(),
        b"Lib-Name".as_slice(),
        b"BAD".as_slice(),
      ] {
        let mut out = Vec::new();
        let _ = s
          .network_clientsetinfo(&[option, b"v"], batch, &mut out)
          .unwrap();
        assert_eq!(out, b"-ERR syntax error\r\n");
      }

      let mut out = Vec::new();
      let _ = s
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
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_clientunblock(&[b"123"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_clientunblock(&[b"123", b"error"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      // 非法 ID（严格整数口径：前导零/尾随字符均拒绝）
      let mut out = Vec::new();
      let _ = s.network_clientunblock(&[b"1x"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_clientunblock(&[b"0123"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // 非法原因
      let mut out = Vec::new();
      let _ = s
        .network_clientunblock(&[b"1", b"WHATEVER"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR\r\n"
      );

      // 参数个数
      let mut out = Vec::new();
      let _ = s.network_clientunblock(&[], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'client|unblock' command\r\n"
      );

      let mut out = Vec::new();
      let _ = s
        .network_clientunblock(&[b"1", b"TIMEOUT", b"x"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'client|unblock' command\r\n"
      );
    });
  }
}
