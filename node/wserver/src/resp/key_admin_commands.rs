use wkv::TtlOpt;
use wresp::length::{try_read_length, try_write_length};

use super::{
  cmd_strings as cs,
  cmd_strings::{
    abort_with_error_message, abort_with_wrong_number_of_arguments, write_error_raw, write_raw,
  },
  parser::{
    resp_ext::{RespSliceExt, RespVecExt},
    session_parse_state::{strict_i32, strict_i64},
  },
  rdb_crc64,
  resp_server_session::RespServerSession,
  ttl_sync::{
    del_ttl_sync, now_unix_ms, probe_alive, put_ttl_sync, read_adjudicated_sync, ttl_of_sync,
  },
};

/// RDB 格式版本（libs/server/Resp/KeyAdminCommands.cs:RDB_VERSION）
const RDB_VERSION: u16 = 11;

/// EXPIRE 族命令形态（对标 libs/server/Resp/RespServerSession.cs 对
/// KeyAdminCommands.NetworkEXPIRE 的四路派发）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireCmd {
  Expire,
  Pexpire,
  Expireat,
  Pexpireat,
}

impl ExpireCmd {
  /// C# `command.ToString()` 的命令名（错误文案用）
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Expire => "EXPIRE",
      Self::Pexpire => "PEXPIRE",
      Self::Expireat => "EXPIREAT",
      Self::Pexpireat => "PEXPIREAT",
    }
  }

  /// 相对时长（EXPIRE/PEXPIRE）或绝对时间戳（EXPIREAT/PEXPIREAT）
  const fn is_relative(self) -> bool {
    matches!(self, Self::Expire | Self::Pexpire)
  }

  /// 毫秒口径（PEXPIRE/PEXPIREAT）
  const fn is_millis(self) -> bool {
    matches!(self, Self::Pexpire | Self::Pexpireat)
  }
}

/// TTL 族命令形态（TTL / PTTL）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlCmd {
  Ttl,
  Pttl,
}

/// EXPIRETIME 族命令形态（EXPIRETIME / PEXPIRETIME）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireTimeCmd {
  Expiretime,
  Pexpiretime,
}

/// libs/server/SessionParseStateExtensions.cs:TryGetExpireOption
///
/// 解析单个过期选项 NX/XX/GT/LT（大小写不敏感）
fn try_parse_expire_option(raw: &[u8]) -> bool {
  raw.eq_ignore_ascii_case(b"NX")
    || raw.eq_ignore_ascii_case(b"XX")
    || raw.eq_ignore_ascii_case(b"GT")
    || raw.eq_ignore_ascii_case(b"LT")
}

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRESTORE
  pub fn network_restore<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      abort_with_wrong_number_of_arguments(output, "RESTORE");
      return Ok(true);
    }

    let key = parse_state[0];
    // C# TryGetInt（index = Count - 2，arity 锁 3 即下标 1）
    let Some(expiry) = strict_i32(parse_state[1]) else {
      // C# 沿用 RESP_ERR_TIMEOUT_NOT_VALID_FLOAT（文案保留历史包袱）
      abort_with_error_message(output, cs::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
      return Ok(true);
    };
    let expiry = i64::from(expiry);
    let value = parse_state[2];

    // RESTORE 仅实现字符串类型（类型字节 0x00）
    if value.first() != Some(&0x00) {
      write_error_raw(output, "ERR RESTORE currently only supports string types");
      return Ok(true);
    }

    // C# 对空载荷直接 valueSpan[0] 越界（进程崩溃断连）；rust 无 panic 约束下
    // 按"载荷不足"同族错误降级，不复刻崩溃
    if value.len() < 10 {
      write_error_raw(output, "ERR DUMP payload version or checksum are wrong");
      return Ok(true);
    }

    // footer = 2 字节 rdb 版本 + 8 字节 crc64
    let footer = &value[value.len() - 10..];
    let rdb_version = u16::from_le_bytes([footer[0], footer[1]]);
    if rdb_version > RDB_VERSION {
      write_error_raw(output, "ERR DUMP payload version or checksum are wrong");
      return Ok(true);
    }

    // crc 覆盖除末 8 字节外的全部载荷
    let calculated_crc = rdb_crc64::hash(&value[..value.len() - 8]);
    if calculated_crc != footer[2..] {
      write_error_raw(output, "ERR DUMP payload version or checksum are wrong");
      return Ok(true);
    }

    let Some((length, payload_start)) = try_read_length(&value[1..]) else {
      write_error_raw(output, "ERR DUMP payload length format is invalid");
      return Ok(true);
    };
    let Some(val) = value
      .get(payload_start + 1..payload_start + 1 + length as usize)
      .filter(|_| payload_start as u64 + 1 + length as u64 <= value.len() as u64)
    else {
      // C# 此处 Slice 越界抛异常断连；rust 按长度格式非法同族错误降级
      write_error_raw(output, "ERR DUMP payload length format is invalid");
      return Ok(true);
    };

    // SET_Conditional(SETEXNX)：仅键不存在时写入（NX 语义）
    let exists = match probe_alive(store, key) {
      Ok(Some(alive)) => alive,
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };
    if exists {
      write_error_raw(output, cs::RESP_ERR_BUSSYKEY);
      return Ok(true);
    }

    match store.try_upsert_sync(key, val) {
      Ok(Ok(_)) => {}
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    if expiry > 0 {
      let expire_at_ms = now_unix_ms() as i64 + expiry.saturating_mul(1000);
      match put_ttl_sync(store, key, expire_at_ms.max(0) as u64) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkDUMP
  pub fn network_dump<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "DUMP");
      return Ok(true);
    }

    let key = parse_state[0];

    match read_adjudicated_sync(store, key, |v| v.to_vec()) {
      // C# 对非字符串值刻意按键不存在处理（nil）而非报错；
      // rust 字符串域读得的即字符串值
      Ok(Some(Some(value))) => {
        let mut encoded_len = [0u8; 5];
        let Some(bytes_written) = try_write_length(value.len() as u32, &mut encoded_len) else {
          write_error_raw(output, "ERR DUMP payload length is invalid");
          return Ok(true);
        };
        let encoded_len = &encoded_len[..bytes_written];

        // DUMP 长 = 类型 1 + 长度前缀 + 值 + rdb 版本 2 + crc64 8
        let payload_len = 1 + encoded_len.len() + value.len() + 2 + 8;
        output.push(b'$');
        let mut buf = itoa::Buffer::new();
        output.extend_from_slice(buf.format(payload_len).as_bytes());
        output.extend_from_slice(b"\r\n");
        // 类型字节 + 长度前缀 + 值 + rdb 版本（小端）
        output.push(0x00);
        output.extend_from_slice(encoded_len);
        output.extend_from_slice(&value);
        output.extend_from_slice(&RDB_VERSION.to_le_bytes());
        // crc64 覆盖类型字节起至版本字节止
        let framed = output.len() - (payload_len - 8);
        let crc = rdb_crc64::hash(&output[framed..]);
        output.extend_from_slice(&crc);
        output.extend_from_slice(b"\r\n");
      }
      Ok(Some(None)) => output.write_resp_null(),
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAME
  pub fn network_rename<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "RENAME");
      return Ok(true);
    }

    let old_key = parse_state[0];
    let new_key = parse_state[1];

    let old_val = match store.try_read_sync(old_key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => val,
      Ok(Some(None)) => {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
        return Ok(true);
      }
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    // 同键 RENAME：C# 存储层自改自即成功
    if old_key == new_key {
      write_raw(output, cs::RESP_OK);
      return Ok(true);
    }

    // 先写新键再删旧键：写新遇异步闭环时零变更可安全降级；删旧遇闭环时
    // 新键已落地，调用方重试整条命令（旧键仍在 → 幂等重放）
    match store.try_upsert_sync(new_key, old_val.as_slice()) {
      Ok(Ok(_)) => {}
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    match store.try_delete_sync(old_key) {
      Ok(Ok(_)) => write_raw(output, cs::RESP_OK),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAMENX
  pub fn network_renamenx<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "RENAMENX");
      return Ok(true);
    }

    let old_key = parse_state[0];
    let new_key = parse_state[1];

    let old_val = match store.try_read_sync(old_key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => val,
      Ok(Some(None)) => {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
        return Ok(true);
      }
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    // 新键已存在（含自改名）→ 0，不动旧键
    let new_exists = if old_key == new_key {
      true
    } else {
      match probe_alive(store, new_key) {
        Ok(Some(alive)) => alive,
        Ok(None) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    };
    if new_exists {
      output.write_resp_int(0);
      return Ok(true);
    }

    match store.try_upsert_sync(new_key, old_val.as_slice()) {
      Ok(Ok(_)) => {}
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    match store.try_delete_sync(old_key) {
      Ok(Ok(_)) => output.write_resp_int(1),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkGETDEL
  pub fn network_getdel<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'GETDEL' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        // 先删后答：删除遇异步闭环（环形页翻转/复合对象）时整体降级，
        // 避免已答出旧值而键未删成
        match store.try_delete_sync(key) {
          Ok(Ok(_)) => output.write_resp_bulk_string(&val),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
        }
      }
      Ok(Some(None)) => {
        output.write_resp_null();
      }
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXISTS
  pub fn network_exists<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'EXISTS' command\r\n");
      return Ok(true);
    }

    let mut exists_count = 0i64;
    for key in parse_state {
      let status = store.try_read_sync(key, |_| ());
      if let Ok(Some(Some(_))) = status {
        exists_count += 1;
      }
    }

    output.write_resp_int(exists_count);
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE
  ///
  /// EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 共同体：完整 C# 参数校验次序
  /// （个数 → 整数 → 非负 → NX/XX/GT/LT 选项组合），过期经
  /// [`super::ttl_sync`] 同步落 TTL 记录；磁盘候选/过期清除须异步时整体降级
  pub fn network_expire<'a, D: wdev::Device>(
    &mut self,
    command: ExpireCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if !(2..=4).contains(&count) {
      abort_with_wrong_number_of_arguments(output, command.as_str());
      return Ok(true);
    }

    let key = parse_state[0];
    let Some(expiration) = strict_i64(parse_state[1]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    if expiration < 0 {
      // C# 文案（与 Redis 的 "must be positive" 不同，逐字节保留）
      abort_with_error_message(output, cs::RESP_ERR_INVALID_EXPIRE_TIME);
      return Ok(true);
    }

    // NX/XX/GT/LT 选项与两参组合（XXGT/XXLT），兼容规则对标 C#
    let mut opt = TtlOpt::NONE;
    if count > 2 {
      if !try_parse_expire_option(parse_state[2]) {
        abort_with_error_message(
          output,
          &cs::GENERIC_ERR_UNSUPPORTED_OPTION.replace("{0}", parse_state[2].as_str_safe()),
        );
        return Ok(true);
      }
      opt = parse_expire_option(parse_state[2], TtlOpt::NONE);

      if count > 3 {
        if !try_parse_expire_option(parse_state[3]) {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_UNSUPPORTED_OPTION.replace("{0}", parse_state[3].as_str_safe()),
          );
          return Ok(true);
        }
        let first = parse_state[2];
        let compatible = (first.eq_ignore_ascii_case(b"XX")
          && (parse_state[3].eq_ignore_ascii_case(b"GT")
            || parse_state[3].eq_ignore_ascii_case(b"LT")))
          || ((first.eq_ignore_ascii_case(b"GT") || first.eq_ignore_ascii_case(b"LT"))
            && parse_state[3].eq_ignore_ascii_case(b"XX"));
        if !compatible {
          abort_with_error_message(
            output,
            "ERR NX and XX, GT or LT options at the same time are not compatible",
          );
          return Ok(true);
        }
        opt = merge_expire_options(first, parse_state[3]);
      }
    }

    // 换算绝对过期毫秒时间戳（对标 C# ticks 换算，rust TTL 记录为毫秒口径）
    let now = now_unix_ms() as i64;
    let expire_at_ms = if command.is_relative() {
      let span = if command.is_millis() {
        expiration
      } else {
        expiration.saturating_mul(1000)
      };
      now.saturating_add(span)
    } else if command.is_millis() {
      expiration
    } else {
      expiration.saturating_mul(1000)
    };

    match expire_apply_sync(store, key, expire_at_ms.max(0) as u64, opt) {
      Ok(Some(applied)) => {
        // C# status != OK（键缺失等）回 :0；成功由存储回 :1（含过去时间戳
        // 立即删除的 Redis 7.4 语义 :1）
        if applied != 0 {
          write_raw(output, cs::RESP_RETURN_VAL_1);
        } else {
          write_raw(output, cs::RESP_RETURN_VAL_0);
        }
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        Ok(true)
      }
    }
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkPERSIST
  pub fn network_persist<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'PERSIST' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];

    match persist_apply_sync(store, key) {
      Ok(Some(removed)) => {
        output.write_resp_int(i64::from(removed));
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        Ok(true)
      }
    }
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkTTL
  pub fn network_ttl<'a, D: wdev::Device>(
    &mut self,
    command: TtlCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, command_name_of_ttl(command));
      return Ok(true);
    }
    let key = parse_state[0];

    match ttl_read_sync(store, key) {
      Ok(Some(read)) => {
        let value = match read {
          // 键缺失 → -2（C# status != OK → RESP_RETURN_VAL_N2）
          ExpiryRead::Missing => -2,
          ExpiryRead::NoExpiry => -1,
          ExpiryRead::At(exp) => {
            let remaining = exp.saturating_sub(now_unix_ms());
            if command == TtlCmd::Pttl {
              remaining as i64
            } else {
              // 对标 ConvertUtils.SecondsFromDiffUtcNowTicks：半秒进位而非向上取整
              (remaining as i64 + 500) / 1000
            }
          }
        };
        output.write_resp_int(value);
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        Ok(true)
      }
    }
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRETIME
  pub fn network_expiretime<'a, D: wdev::Device>(
    &mut self,
    command: ExpireTimeCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "EXPIRETIME");
      return Ok(true);
    }
    let key = parse_state[0];

    match expiretime_read_sync(store, key) {
      Ok(Some(read)) => {
        let value = match read {
          ExpiryRead::Missing => -2,
          ExpiryRead::NoExpiry => -1,
          ExpiryRead::At(exp) => {
            if command == ExpireTimeCmd::Pexpiretime {
              exp as i64
            } else {
              // 对标 ConvertUtils.UnixTimeInSecondsFromTicks：整除截断
              exp as i64 / 1000
            }
          }
        };
        output.write_resp_int(value);
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        Ok(true)
      }
    }
  }
}

/// TTL/PTTL 命令名（错误文案用）
const fn command_name_of_ttl(command: TtlCmd) -> &'static str {
  match command {
    TtlCmd::Ttl => "TTL",
    TtlCmd::Pttl => "PTTL",
  }
}

/// 单个过期选项映射到 TtlOpt 标志
fn parse_expire_option(raw: &[u8], base: TtlOpt) -> TtlOpt {
  let mut opt = base;
  if raw.eq_ignore_ascii_case(b"NX") {
    opt.nx = true;
  } else if raw.eq_ignore_ascii_case(b"XX") {
    opt.xx = true;
  } else if raw.eq_ignore_ascii_case(b"GT") {
    opt.gt = true;
  } else if raw.eq_ignore_ascii_case(b"LT") {
    opt.lt = true;
  }
  opt
}

/// 两参组合（已由调用方校验兼容）：XX+GT / GT+XX → xx|gt，XX+LT / LT+XX → xx|lt
fn merge_expire_options(first: &[u8], second: &[u8]) -> TtlOpt {
  let opt = parse_expire_option(first, TtlOpt::NONE);
  parse_expire_option(second, opt)
}

/// EXPIRE 应用内核（同步镜像 wkv::StoreSession::expire_at 的判定表）
///
/// 返回 `Ok(None)` 须降级异步；`Ok(Some(0))` 条件不满足/键缺失；
/// `Ok(Some(1))` 已设置（或过去时间戳已物理删除）
fn expire_apply_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  expire_at_ms: u64,
  opt: TtlOpt,
) -> Result<Option<i32>, wkv::Error> {
  // 存活判定（含过期键视同缺失的裁决；过期清除须异步时降级）
  match probe_alive(store, key)? {
    None => Ok(None),
    // C# status != OK → 调用方回 :0
    Some(false) => Ok(Some(0)),
    Some(true) => {
      let current = match ttl_of_sync(store, key)? {
        None => return Ok(None),
        Some(cur) => cur,
      };
      // NX/XX/GT/LT 判定（镜像 wkv：多项同设须全部满足才放行）
      let denied = match current {
        None => opt.xx || opt.gt,
        Some(c) => opt.nx || (opt.gt && expire_at_ms <= c) || (opt.lt && expire_at_ms >= c),
      };
      if denied {
        return Ok(Some(0));
      }
      if expire_at_ms <= now_unix_ms() {
        // 过去时间戳：物理删除（先删 TTL 再删数据，镜像 purge_expired 顺序）
        if !del_ttl_sync(store, key)? {
          return Ok(None);
        }
        return store
          .try_delete_sync(key)
          .map(|done| if done.is_ok() { Some(1) } else { None });
      }
      put_ttl_sync(store, key, expire_at_ms).map(|done| if done { Some(1) } else { None })
    }
  }
}

/// PERSIST 应用内核（同步镜像 wkv::StoreSession::persist）
///
/// 返回 `Ok(None)` 须降级；`Ok(Some(1))` 已移除；`Ok(Some(0))` 无 TTL 或键缺失
fn persist_apply_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
) -> Result<Option<i32>, wkv::Error> {
  match probe_alive(store, key)? {
    None => Ok(None),
    Some(false) => Ok(Some(0)),
    Some(true) => match ttl_of_sync(store, key)? {
      None => Ok(None),
      // 从未设 TTL：无可移除
      Some(None) => Ok(Some(0)),
      Some(Some(_)) => del_ttl_sync(store, key).map(|done| if done { Some(1) } else { None }),
    },
  }
}

/// TTL 族读取结果三态
enum ExpiryRead {
  /// 键不存在（TTL → -2）
  Missing,
  /// 键存在但无 TTL（TTL → -1）
  NoExpiry,
  /// 有 TTL：绝对过期毫秒时间戳
  At(u64),
}

/// TTL/PTTL 读内核（同步镜像 wkv::StoreSession::pttl_ms）
///
/// `Ok(None)` 须降级（磁盘候选 / 过期清除待异步）
fn ttl_read_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
) -> Result<Option<ExpiryRead>, wkv::Error> {
  match probe_alive(store, key)? {
    None => Ok(None),
    // C# status != OK（键缺失）→ :n2
    Some(false) => Ok(Some(ExpiryRead::Missing)),
    Some(true) => match ttl_of_sync(store, key)? {
      None => Ok(None),
      Some(None) => Ok(Some(ExpiryRead::NoExpiry)),
      Some(Some(exp)) => Ok(Some(ExpiryRead::At(exp))),
    },
  }
}

/// EXPIRETIME/PEXPIRETIME 读内核（同步镜像 wkv::StoreSession::expiretime_ms）
///
/// `Ok(None)` 须降级
fn expiretime_read_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
) -> Result<Option<ExpiryRead>, wkv::Error> {
  ttl_read_sync(store, key)
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::{
    super::ttl_sync::{now_unix_ms, put_ttl_sync, ttl_of_sync},
    *,
  };

  type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

  fn with_batch(f: impl FnOnce(&mut RespServerSession, &Batch)) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("key_admin.db")).unwrap());
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = RespServerSession::default();
      f(&mut s, &batch);
    });
  }

  /// 构造合法 DUMP 载荷：0x00 + 长度前缀 + 值 + rdb 版本 + crc64
  fn dump_payload(val: &[u8]) -> Vec<u8> {
    let mut payload = vec![0x00];
    let mut len_buf = [0u8; 5];
    let n = try_write_length(val.len() as u32, &mut len_buf).unwrap();
    payload.extend_from_slice(&len_buf[..n]);
    payload.extend_from_slice(val);
    payload.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = rdb_crc64::hash(&payload);
    payload.extend_from_slice(&crc);
    payload
  }

  #[test]
  fn restore_writes_string_value() {
    with_batch(|s, batch| {
      let payload = dump_payload(b"hello");
      let mut out = Vec::new();
      let _ = s
        .network_restore(&[b"k", b"0", &payload], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");

      let mut out = Vec::new();
      let _ = s.network_getdel(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"$5\r\nhello\r\n");
    });
  }

  #[test]
  fn restore_busykey_and_validation_errors() {
    with_batch(|s, batch| {
      let payload = dump_payload(b"v");
      let _ = s
        .network_restore(&[b"k", b"0", &payload], batch, &mut Vec::new())
        .unwrap();

      // NX 语义：键已存在 → BUSYKEY
      let mut out = Vec::new();
      let _ = s
        .network_restore(&[b"k", b"0", &payload], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-BUSYKEY Target key name already exists.\r\n");

      // 参数个数
      let mut out = Vec::new();
      let _ = s.network_restore(&[b"k", b"0"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'RESTORE' command\r\n"
      );

      // 非整数过期
      let mut out = Vec::new();
      let _ = s
        .network_restore(&[b"k", b"abc", &payload], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR timeout is not a float or out of range\r\n");

      // 非字符串类型字节
      let mut bad_type = payload.clone();
      bad_type[0] = 0x0c;
      let mut out = Vec::new();
      let _ = s
        .network_restore(&[b"k2", b"0", &bad_type], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR RESTORE currently only supports string types\r\n"
      );

      // 载荷过短
      let mut out = Vec::new();
      let _ = s
        .network_restore(&[b"k2", b"0", &[0x00, 1, 2, 3]], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR DUMP payload version or checksum are wrong\r\n");

      // crc 损坏
      let mut corrupted = dump_payload(b"v");
      let last = corrupted.len() - 1;
      corrupted[last] ^= 0xff;
      let mut out = Vec::new();
      let _ = s
        .network_restore(&[b"k3", b"0", &corrupted], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR DUMP payload version or checksum are wrong\r\n");
    });
  }

  #[test]
  fn restore_with_expiry() {
    with_batch(|s, batch| {
      let payload = dump_payload(b"v");
      let mut out = Vec::new();
      let _ = s
        .network_restore(&[b"k", b"100", &payload], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");
      let ttl = ttl_of_sync(batch, b"k").unwrap().unwrap().unwrap();
      assert!(ttl > now_unix_ms() + 99_000);
    });
  }

  #[test]
  fn dump_frame_roundtrips_through_restore() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"k", b"hello"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s.network_dump(&[b"k"], batch, &mut out).unwrap();
      // 应答帧以 $ 前缀开头，类型字节 0x00 起为载荷
      assert!(out.starts_with(b"$"));
      let type_pos = out.iter().position(|b| *b == 0).unwrap();
      let payload = &out[type_pos..out.len() - 2];

      // DUMP 输出可直接 RESTORE 回去
      let mut out2 = Vec::new();
      let _ = s
        .network_restore(&[b"k2", b"0", payload], batch, &mut out2)
        .unwrap();
      assert_eq!(out2, b"+OK\r\n");
      let mut out3 = Vec::new();
      let _ = s.network_getdel(&[b"k2"], batch, &mut out3).unwrap();
      assert_eq!(out3, b"$5\r\nhello\r\n");

      // 键缺失 → nil
      let mut out4 = Vec::new();
      let _ = s.network_dump(&[b"missing"], batch, &mut out4).unwrap();
      assert_eq!(out4, b"$-1\r\n");

      // 参数个数
      let mut out5 = Vec::new();
      let _ = s.network_dump(&[], batch, &mut out5).unwrap();
      assert_eq!(
        out5,
        b"-ERR wrong number of arguments for 'DUMP' command\r\n"
      );
    });
  }

  #[test]
  fn rename_and_renamenx() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"a", b"va"], batch, &mut Vec::new())
        .unwrap();

      // RENAME a b
      let mut out = Vec::new();
      let _ = s.network_rename(&[b"a", b"b"], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");
      let mut out = Vec::new();
      let _ = s.network_exists(&[b"a"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      // 源键缺失
      let mut out = Vec::new();
      let _ = s.network_rename(&[b"a", b"c"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR no such key\r\n");

      // RENAMENX b c（c 不存在）→ 1
      let mut out = Vec::new();
      let _ = s.network_renamenx(&[b"b", b"c"], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");

      // RENAMENX 到已存在键 → 0
      let _ = s
        .network_set(&[b"x", b"vx"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s.network_renamenx(&[b"c", b"x"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      // 自改名：RENAME c c → OK；RENAMENX c c → 0
      let mut out = Vec::new();
      let _ = s.network_rename(&[b"c", b"c"], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");
      let mut out = Vec::new();
      let _ = s.network_renamenx(&[b"c", b"c"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");
    });
  }

  #[test]
  fn expire_validation_and_effects() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"k", b"v"], batch, &mut Vec::new())
        .unwrap();

      let mut out = Vec::new();
      let _ = s
        .network_expire(ExpireCmd::Expire, &[b"k", b"100"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
      let ttl = ttl_of_sync(batch, b"k").unwrap().unwrap().unwrap();
      assert!(ttl > now_unix_ms() + 99_000);

      // NX：已设 TTL → 0
      let mut out = Vec::new();
      let _ = s
        .network_expire(ExpireCmd::Expire, &[b"k", b"100", b"NX"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      // GT：更小的时间不生效
      let mut out = Vec::new();
      let _ = s
        .network_expire(ExpireCmd::Expire, &[b"k", b"1", b"GT"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      // XX：已设 TTL 时可改
      let mut out = Vec::new();
      let _ = s
        .network_expire(
          ExpireCmd::Pexpire,
          &[b"k", b"50000", b"XX"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b":1\r\n");

      // XX+GT 组合：当前 TTL 50s，更小的 1s 不生效
      let mut out = Vec::new();
      let _ = s
        .network_expire(
          ExpireCmd::Expire,
          &[b"k", b"1", b"XX", b"GT"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b":0\r\n");

      // NX+GT 不兼容
      let mut out = Vec::new();
      let _ = s
        .network_expire(
          ExpireCmd::Expire,
          &[b"k", b"1", b"NX", b"GT"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(
        out,
        b"-ERR NX and XX, GT or LT options at the same time are not compatible\r\n"
      );

      // 未知选项
      let mut out = Vec::new();
      let _ = s
        .network_expire(ExpireCmd::Expire, &[b"k", b"1", b"WHAT"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Unsupported option WHAT\r\n");

      // 负数过期
      let mut out = Vec::new();
      let _ = s
        .network_expire(ExpireCmd::Expire, &[b"k", b"-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR invalid expire time, must be >= 0\r\n");

      // 非整数过期
      let mut out = Vec::new();
      let _ = s
        .network_expire(ExpireCmd::Expire, &[b"k", b"abc"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // 键缺失 → 0
      let mut out = Vec::new();
      let _ = s
        .network_expire(ExpireCmd::Expire, &[b"nope", b"100"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      // EXPIREAT 绝对秒
      let mut out = Vec::new();
      let abs = (now_unix_ms() / 1000 + 100).to_string();
      let _ = s
        .network_expire(
          ExpireCmd::Expireat,
          &[b"k", abs.as_bytes()],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b":1\r\n");
    });
  }

  #[test]
  fn persist_ttl_expiretime_semantics() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"k", b"v"], batch, &mut Vec::new())
        .unwrap();

      // 无 TTL：PERSIST → 0；TTL → -1；EXPIRETIME → -1
      let mut out = Vec::new();
      let _ = s.network_persist(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_ttl(TtlCmd::Ttl, &[b"k"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":-1\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_expiretime(ExpireTimeCmd::Expiretime, &[b"k"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":-1\r\n");

      // 设 TTL 后：TTL/PTTL 为正；PEXPIRETIME 为绝对毫秒；PERSIST → 1
      let _ = put_ttl_sync(batch, b"k", now_unix_ms() + 60_000).unwrap();
      let mut out = Vec::new();
      let _ = s
        .network_ttl(TtlCmd::Ttl, &[b"k"], batch, &mut out)
        .unwrap();
      assert!(out != b":-1\r\n" && out != b":-2\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_ttl(TtlCmd::Pttl, &[b"k"], batch, &mut out)
        .unwrap();
      assert!(out != b":-1\r\n" && out != b":-2\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_expiretime(ExpireTimeCmd::Pexpiretime, &[b"k"], batch, &mut out)
        .unwrap();
      assert!(out != b":-1\r\n");

      let mut out = Vec::new();
      let _ = s.network_persist(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");
      assert_eq!(ttl_of_sync(batch, b"k").unwrap(), Some(None));

      // 键缺失：TTL → -2；PERSIST → 0
      let mut out = Vec::new();
      let _ = s
        .network_ttl(TtlCmd::Ttl, &[b"nope"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":-2\r\n");
      let mut out = Vec::new();
      let _ = s.network_persist(&[b"nope"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      // 参数校验
      let mut out = Vec::new();
      let _ = s
        .network_ttl(TtlCmd::Ttl, &[b"a", b"b"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR wrong number of arguments for 'TTL' command\r\n");
    });
  }

  #[test]
  fn getdel_and_exists() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"k", b"v"], batch, &mut Vec::new())
        .unwrap();

      let mut out = Vec::new();
      let _ = s.network_getdel(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"$1\r\nv\r\n");

      let mut out = Vec::new();
      let _ = s.network_getdel(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");

      let _ = s
        .network_set(&[b"x", b"vx"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s
        .network_exists(&[b"x", b"x", b"nope"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":2\r\n");
    });
  }
}
