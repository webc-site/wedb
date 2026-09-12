//! 存储型脚本命令面：ScriptingApi 的 wkv 存储实现
//! （对标 C# RespServerSession 上 basicGarnetApi + TryConsumeMessages 的
//! redis.call 落地面；resp 域就绪后可由其会话类型替换接线）。
//!
//! 同步约束：Lua 回调内不可 await，本实现走 BatchStoreSession 同步快路径；
//! 命中"需异步闭环"的降级形态（冷键/环形翻转）时上抛脚本错误而非返回
//! 错误数据。

use std::str;

use wdev::Device;
use wkv::ConsistentReadFunctions;
use wlua::{ScratchBufferNetworkSender, ScriptingApi};
use wresp::{
  cmd_strings::{RESP_ERR_GENERIC_UNK_CMD, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_NOPERM},
  strict_i64,
};

use crate::storage::session::storage_session::StorageSession;

/// 同步面无法闭环时的降级错误文案。
const ERR_ASYNC_REQUIRED: &str =
  "ERR Script attempted to access data requiring asynchronous completion";

/// RESP2 数组请求解析（`*N` + N 个 bulk 串；`$-1` 空参接受为空串，借用零拷贝）。
fn parse_request(request: &[u8]) -> Option<Vec<&[u8]>> {
  if request.first() != Some(&b'*') {
    return None;
  }
  let mut cursor = &request[1..];
  let line_end = cursor.iter().position(|&b| b == b'\r')?;
  let count = strict_i64(&cursor[..line_end])? as usize;
  cursor = &cursor[line_end + 2..];
  let mut args = Vec::with_capacity(count);
  for _ in 0..count {
    if cursor.first() != Some(&b'$') {
      return None;
    }
    let after_prefix = &cursor[1..];
    let len_end = after_prefix.iter().position(|&b| b == b'\r')?;
    let len_str = &after_prefix[..len_end];
    cursor = &after_prefix[len_end + 2..];
    if len_str == b"-1" {
      args.push(&[][..]);
      continue;
    }
    let arg_len = strict_i64(len_str)? as usize;
    if cursor.len() < arg_len + 2 {
      return None;
    }
    args.push(&cursor[..arg_len]);
    cursor = &cursor[arg_len + 2..];
  }
  Some(args)
}

/// 显式白名单（区分于 None = 全量放行）。
type AclAllowList = Option<Vec<String>>;

/// 存储型脚本命令面。
pub struct StorageScriptingApi<'s, D: Device, CR: ConsistentReadFunctions = ()> {
  /// 底层存储会话。
  storage: &'s StorageSession<'s, D, CR>,
  /// ACL 授权集。
  acl_allow: AclAllowList,
}

impl<'s, D: Device, CR: ConsistentReadFunctions> StorageScriptingApi<'s, D, CR> {
  /// 构造（`acl_allow` 为 None 时全放行）。
  pub fn new(storage: &'s StorageSession<'s, D, CR>, acl_allow: AclAllowList) -> Self {
    Self { storage, acl_allow }
  }

  /// ACL 权限判定（对标 CheckACLPermissions；支持 "BITOP_AND" 展开形态）。
  fn allowed(&self, command: &[u8]) -> bool {
    let Some(allow) = &self.acl_allow else {
      return true;
    };
    let name = command
      .split(|&b| b == b' ' || b == b'_')
      .next()
      .unwrap_or(command);
    allow
      .iter()
      .any(|a| a.as_bytes().eq_ignore_ascii_case(name))
  }

  /// 同步读（快路径闭环；降级上抛 Err）。
  fn read_sync(&self, key: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    match self.storage.batch.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(value)) => Ok(value),
      Ok(None) => Err(ERR_ASYNC_REQUIRED),
      Err(_) => Err(ERR_ASYNC_REQUIRED),
    }
  }

  /// 同步探测键存在性（零拷贝）。
  fn exists_sync(&self, key: &[u8]) -> Result<bool, &'static str> {
    match self.storage.batch.try_read_sync(key, |_| ()) {
      Ok(Some(Some(()))) => Ok(true),
      Ok(Some(None)) => Ok(false),
      Ok(None) => Err(ERR_ASYNC_REQUIRED),
      Err(_) => Err(ERR_ASYNC_REQUIRED),
    }
  }

  /// 同步读取字符串长度（零拷贝）。
  fn strlen_sync(&self, key: &[u8]) -> Result<Option<usize>, &'static str> {
    match self.storage.batch.try_read_sync(key, |v| v.len()) {
      Ok(Some(len)) => Ok(len),
      Ok(None) => Err(ERR_ASYNC_REQUIRED),
      Err(_) => Err(ERR_ASYNC_REQUIRED),
    }
  }

  /// 同步写（快路径闭环；降级上抛 Err）。
  fn upsert_sync(&self, key: &[u8], val: &[u8]) -> Result<(), &'static str> {
    match self.storage.batch.try_upsert_sync(key, val) {
      // 写入成功即推进 WATCH 版本（C# 脚本写经 storageSession 同挂点）
      Ok(Ok(_)) => {
        self.storage.bump_watch_version(key);
        Ok(())
      }
      Ok(Err(_)) => Err(ERR_ASYNC_REQUIRED),
      Err(_) => Err(ERR_ASYNC_REQUIRED),
    }
  }

  /// 同步删除。
  fn delete_sync(&self, key: &[u8]) -> Result<bool, &'static str> {
    // C# InitialDeleter 无条件推进版本（缺席键墓碑同样计入，保守方向）
    self.storage.bump_watch_version(key);
    match self.storage.batch.try_delete_sync(key) {
      Ok(Ok(deleted)) => Ok(deleted),
      Ok(Err(_)) => Err(ERR_ASYNC_REQUIRED),
      Err(_) => Err(ERR_ASYNC_REQUIRED),
    }
  }

  /// 写 RESP2 响应便捷面。
  fn write(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes);
  }

  fn write_bulk(out: &mut Vec<u8>, value: &[u8]) {
    out.push(b'$');
    let mut buffer = itoa::Buffer::new();
    out.extend_from_slice(buffer.format(value.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
  }

  fn write_int(out: &mut Vec<u8>, value: i64) {
    out.push(b':');
    let mut buffer = itoa::Buffer::new();
    out.extend_from_slice(buffer.format(value).as_bytes());
    out.extend_from_slice(b"\r\n");
  }

  fn write_error(out: &mut Vec<u8>, message: &str) {
    out.push(b'-');
    out.extend_from_slice(message.as_bytes());
    out.extend_from_slice(b"\r\n");
  }

  fn write_null(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1\r\n");
  }

  /// 命令分派（RESP2 请求 → RESP2 响应；ACL 校验内嵌对标 TryConsumeMessages）。
  fn dispatch(&self, request: &[u8], sender: &mut ScratchBufferNetworkSender) {
    let mut out = Vec::new();
    let Some(args) = parse_request(request) else {
      Self::write_error(&mut out, "ERR invalid request format");
      Self::emit(sender, &out);
      return;
    };

    let Some(command) = args.first() else {
      Self::write_error(&mut out, "ERR unknown command");
      Self::emit(sender, &out);
      return;
    };
    if !self.allowed(command) {
      Self::write_error(&mut out, RESP_ERR_NOPERM);
      Self::emit(sender, &out);
      return;
    }

    let cmd = *command;
    let n = args.len();
    if cmd.eq_ignore_ascii_case(b"PING") && n <= 2 {
      if n == 2 {
        Self::write_bulk(&mut out, args[1]);
      } else {
        Self::write(&mut out, b"+PONG\r\n");
      }
    } else if cmd.eq_ignore_ascii_case(b"ECHO") && n == 2 {
      Self::write_bulk(&mut out, args[1]);
    } else if cmd.eq_ignore_ascii_case(b"GET") && n == 2 {
      match self.read_sync(args[1]) {
        Ok(Some(value)) => Self::write_bulk(&mut out, &value),
        Ok(None) => Self::write_null(&mut out),
        Err(err) => Self::write_error(&mut out, err),
      }
    } else if cmd.eq_ignore_ascii_case(b"SET") && n == 3 {
      match self.upsert_sync(args[1], args[2]) {
        Ok(()) => Self::write(&mut out, b"+OK\r\n"),
        Err(err) => Self::write_error(&mut out, err),
      }
    } else if cmd.eq_ignore_ascii_case(b"DEL") && n >= 2 {
      let mut deleted = 0i64;
      for key in &args[1..] {
        match self.delete_sync(key) {
          Ok(true) => deleted += 1,
          Ok(false) => {}
          Err(err) => {
            Self::write_error(&mut out, err);
            Self::emit(sender, &out);
            return;
          }
        }
      }
      Self::write_int(&mut out, deleted);
    } else if cmd.eq_ignore_ascii_case(b"EXISTS") && n >= 2 {
      let mut found = 0i64;
      for key in &args[1..] {
        match self.exists_sync(key) {
          Ok(true) => found += 1,
          Ok(false) => {}
          Err(err) => {
            Self::write_error(&mut out, err);
            Self::emit(sender, &out);
            return;
          }
        }
      }
      Self::write_int(&mut out, found);
    } else if cmd.eq_ignore_ascii_case(b"TYPE") && n == 2 {
      match self.exists_sync(args[1]) {
        Ok(true) => Self::write(&mut out, b"+string\r\n"),
        Ok(false) => Self::write(&mut out, b"+none\r\n"),
        Err(err) => Self::write_error(&mut out, err),
      }
    } else if cmd.eq_ignore_ascii_case(b"STRLEN") && n == 2 {
      match self.strlen_sync(args[1]) {
        Ok(Some(len)) => Self::write_int(&mut out, len as i64),
        Ok(None) => Self::write_int(&mut out, 0),
        Err(err) => Self::write_error(&mut out, err),
      }
    } else if cmd.eq_ignore_ascii_case(b"INCR") && n == 2 {
      self.incr_by(args[1], 1, &mut out);
    } else if cmd.eq_ignore_ascii_case(b"DECR") && n == 2 {
      self.incr_by(args[1], -1, &mut out);
    } else if (cmd.eq_ignore_ascii_case(b"INCRBY") || cmd.eq_ignore_ascii_case(b"DECRBY")) && n == 3
    {
      let Some(delta) = strict_i64(args[2]) else {
        Self::write_error(&mut out, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        Self::emit(sender, &out);
        return;
      };
      self.incr_by(
        args[1],
        if cmd.eq_ignore_ascii_case(b"INCRBY") {
          delta
        } else {
          -delta
        },
        &mut out,
      );
    } else {
      Self::write_error(&mut out, RESP_ERR_GENERIC_UNK_CMD);
    }

    Self::emit(sender, &out);
  }

  /// INCR/DECR 族（同步读改写）。
  fn incr_by(&self, key: &[u8], delta: i64, out: &mut Vec<u8>) {
    let current: i64 = match self.read_sync(key) {
      Ok(Some(value)) => match str::from_utf8(&value).unwrap_or("").parse() {
        Ok(parsed) => parsed,
        Err(_) => {
          Self::write_error(out, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return;
        }
      },
      Ok(None) => 0,
      Err(err) => {
        Self::write_error(out, err);
        return;
      }
    };
    let next = match current.checked_add(delta) {
      Some(next) => next,
      None => {
        Self::write_error(out, "ERR increment or decrement would overflow");
        return;
      }
    };
    match self.upsert_sync(key, next.to_string().as_bytes()) {
      Ok(()) => Self::write_int(out, next),
      Err(err) => Self::write_error(out, err),
    }
  }

  /// 响应字节推进发送器有效区（对标 dcurr 写 + SendResponse）。
  fn emit(sender: &mut ScratchBufferNetworkSender, response: &[u8]) {
    if response.is_empty() {
      return;
    }
    let (view, _) = sender.enter_and_get_response_object();
    let len = response.len().min(view.len());
    view[..len].copy_from_slice(&response[..len]);
    _ = sender.send_response(0, len);
  }
}

impl<'s, D: Device, CR: ConsistentReadFunctions> ScriptingApi for StorageScriptingApi<'s, D, CR> {
  fn dispatch_resp(&mut self, request: &[u8], sender: &mut ScratchBufferNetworkSender) {
    self.dispatch(request, sender);
  }

  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    self.read_sync(key)
  }

  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), &'static str> {
    self.upsert_sync(key, value)
  }

  fn resp_protocol_version(&self) -> u8 {
    self.storage.resp_protocol_version()
  }

  fn update_resp_protocol_version(&mut self, version: u8) {
    self.storage.update_resp_protocol_version(version);
  }

  fn check_acl_permissions(&self, command: &str) -> bool {
    self.allowed(command.as_bytes())
  }
}

#[cfg(test)]
mod tests {
  use super::parse_request;

  /// RESP2 请求拼装（与 ScratchBufferBuilder 同形，测试便利面）。
  fn build_request(command: &[u8], args: &[&[u8]]) -> Vec<u8> {
    let mut out = vec![b'*'];
    out.extend_from_slice((args.len() + 1).to_string().as_bytes());
    out.extend_from_slice(b"\r\n$");
    out.extend_from_slice(command.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(command);
    out.extend_from_slice(b"\r\n");
    for arg in args {
      out.extend_from_slice(b"$");
      out.extend_from_slice(arg.len().to_string().as_bytes());
      out.extend_from_slice(b"\r\n");
      out.extend_from_slice(arg);
      out.extend_from_slice(b"\r\n");
    }
    out
  }

  #[test]
  fn parse_request_roundtrip() {
    let request = build_request(b"SET", &[b"k", b"v"]);
    assert_eq!(
      parse_request(&request).unwrap(),
      vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]
    );
  }
}
