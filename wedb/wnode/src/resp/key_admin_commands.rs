use itoa::Buffer;
use wbase::{
  convert::{
    expire_after_ms_to_ticks, expire_after_to_ticks, expire_at_milliseconds_to_ticks,
    expire_at_seconds_to_ticks, milliseconds_from_diff_utc_now_ticks,
    seconds_from_diff_utc_now_ticks, unix_time_in_milliseconds_from_ticks,
    unix_time_in_seconds_from_ticks,
  },
  crc64::hash as rdb_crc64_hash,
  num::{strict_i32, strict_i64},
  time::now_ticks,
};
use wkv::TtlOpt;
use wresp::{
  RespSliceExt, RespVecExt, check_arg_count, cmd_strings as cs,
  cmd_strings::{
    RESP_ERR_GENERIC, abort_with_error_message, abort_with_unsupported_option, write_error_raw,
    write_raw,
  },
  length::{try_read_length, try_write_length},
  unpack_args,
};
use wval::{KeyTag, NO_ETAG};

use super::resp_server_session::RespServerSession;
use crate::storage::session::common::{
  etag_sync::{del_etag_sync, etag_of_sync, put_etag_sync},
  ttl_sync::{
    del_ttl_sync, probe_alive, put_ttl_sync, read_adjudicated_tag_sync, read_adjudicated_user_sync,
    ttl_of_sync,
  },
};

/// DUMP 载荷版本/校验和非法文案（本域多处复用）。
const ERR_DUMP_VERSION_CHECKSUM: &str = "ERR DUMP payload version or checksum are wrong";
/// DUMP 载荷长度格式非法文案（本域两处复用）。
const ERR_DUMP_LENGTH_INVALID: &str = "ERR DUMP payload length format is invalid";

/// RDB 格式版本（libs/server/Resp/KeyAdminCommands.cs:RDB_VERSION）
const RDB_VERSION: u16 = 11;

/// libs/server/Resp/KeyAdminCommands.cs:ExpireCmd
///
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

  /// 换算绝对过期 .NET Ticks（对标 KeyAdminCommands.cs:421-427 的换算 switch）
  ///
  /// EXPIRE → UtcNow.AddSeconds(...).UtcTicks、PEXPIRE → AddMilliseconds、
  /// EXPIREAT → UnixTimestampInSecondsToTicks、PEXPIREAT →
  /// UnixTimestampInMillisecondsToTicks；乘加/钳制公式统一委托
  /// [`wbase::convert`] 单点（超界钳到最大可表示 ticks，杜绝 debug 构建
  /// 溢出 panic，C# unchecked 环绕对应的确定性降级），与 AOF 重放端同函数
  fn expire_at_ticks(self, expiration: i64) -> i64 {
    match self {
      Self::Expire => expire_after_to_ticks(now_ticks(), expiration),
      Self::Pexpire => expire_after_ms_to_ticks(now_ticks(), expiration),
      Self::Expireat => expire_at_seconds_to_ticks(expiration),
      Self::Pexpireat => expire_at_milliseconds_to_ticks(expiration),
    }
  }
}

/// libs/server/Resp/KeyAdminCommands.cs:TtlCmd
///
/// TTL 族命令形态（TTL / PTTL）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlCmd {
  Ttl,
  Pttl,
}

/// libs/server/Resp/KeyAdminCommands.cs:ExpireTimeCmd
///
/// EXPIRETIME 族命令形态（EXPIRETIME / PEXPIRETIME）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireTimeCmd {
  Expiretime,
  Pexpiretime,
}

/// 解析单个过期选项 NX/XX/GT/LT（大小写不敏感）
fn try_parse_expire_option(raw: &[u8]) -> bool {
  wresp::expire_option_from_token(raw).is_some()
}

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRESTORE
  pub fn network_restore<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unpack_args!(parse_state, output, "RESTORE", [key, expiry_raw, value]);

    // C# TryGetInt（index = Count - 2，arity 锁 3 即下标 1）
    let Some(expiry) = strict_i32(expiry_raw) else {
      // C# 沿用 RESP_ERR_TIMEOUT_NOT_VALID_FLOAT（文案保留历史包袱）
      abort_with_error_message(output, cs::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
      return Ok(true);
    };
    let expiry = i64::from(expiry);

    // RESTORE 仅实现字符串类型（类型字节 0x00）
    if value.first() != Some(&0x00) {
      write_error_raw(output, "ERR RESTORE currently only supports string types");
      return Ok(true);
    }

    // C# 对空载荷直接 valueSpan[0] 越界（进程崩溃断连）；rust 无 panic 约束下
    // 按"载荷不足"同族错误降级，不复刻崩溃
    if value.len() < 10 {
      write_error_raw(output, ERR_DUMP_VERSION_CHECKSUM);
      return Ok(true);
    }

    // footer = 2 字节 rdb 版本 + 8 字节 crc64
    let footer = &value[value.len() - 10..];
    let rdb_version = u16::from_le_bytes([footer[0], footer[1]]);
    if rdb_version > RDB_VERSION {
      write_error_raw(output, ERR_DUMP_VERSION_CHECKSUM);
      return Ok(true);
    }

    // crc 覆盖除末 8 字节外的全部载荷
    let calculated_crc = rdb_crc64_hash(&value[..value.len() - 8]);
    if calculated_crc != footer[2..] {
      write_error_raw(output, ERR_DUMP_VERSION_CHECKSUM);
      return Ok(true);
    }

    let Some((length, payload_start)) = try_read_length(&value[1..]) else {
      write_error_raw(output, ERR_DUMP_LENGTH_INVALID);
      return Ok(true);
    };
    let Some(val) = value
      .get(payload_start + 1..payload_start + 1 + length as usize)
      .filter(|_| payload_start as u64 + 1 + length as u64 <= value.len() as u64)
    else {
      // C# 此处 Slice 越界抛异常断连；rust 按长度格式非法同族错误降级
      write_error_raw(output, ERR_DUMP_LENGTH_INVALID);
      return Ok(true);
    };

    // SET_Conditional(SETEXNX)：仅键不存在时写入（NX 语义）
    let exists = match probe_alive(store, key) {
      Ok(Some(alive)) => alive,
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
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
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    if expiry > 0 {
      // C#：DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expiry).Ticks；
      // 换算单点与 EXPIRE 同源（expire_after_to_ticks）
      let expire_at_ticks = expire_after_to_ticks(now_ticks(), expiry);
      match put_ttl_sync(store, key, expire_at_ticks) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
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
    unpack_args!(parse_state, output, "DUMP", [key]);

    match read_adjudicated_user_sync(store, key, |v| v.to_vec()) {
      // C# 对非字符串值刻意按键不存在处理（nil）而非报错；rust 双域读下
      // String 域命中即字符串值，信封域命中（对象键）同 nil
      Ok(Some(Some(Ok(value)))) => {
        let mut encoded_len = [0u8; 5];
        let Some(bytes_written) = try_write_length(value.len() as u32, &mut encoded_len) else {
          write_error_raw(output, "ERR DUMP payload length is invalid");
          return Ok(true);
        };
        let encoded_len = &encoded_len[..bytes_written];

        // DUMP 长 = 类型 1 + 长度前缀 + 值 + rdb 版本 2 + crc64 8
        let payload_len = 1 + encoded_len.len() + value.len() + 2 + 8;
        output.push(b'$');
        let mut buf = Buffer::new();
        output.extend_from_slice(buf.format(payload_len).as_bytes());
        output.extend_from_slice(b"\r\n");
        // 类型字节 + 长度前缀 + 值 + rdb 版本（小端）
        output.push(0x00);
        output.extend_from_slice(encoded_len);
        output.extend_from_slice(&value);
        output.extend_from_slice(&RDB_VERSION.to_le_bytes());
        // crc64 覆盖类型字节起至版本字节止。刻意偏离 C#：C# DUMP 的 crc 从
        // 类型字节之后起算（KeyAdminCommands.cs:200 的 Slice 越过 0x00），
        // 而其 RESTORE 的 crc 校验含类型字节，C# 自身 DUMP→RESTORE 往返必被
        // "checksum wrong" 拒绝；rust 对齐 RESTORE 口径保证往返成立
        let framed = output.len() - (payload_len - 8);
        let crc = rdb_crc64_hash(&output[framed..]);
        output.extend_from_slice(&crc);
        output.extend_from_slice(b"\r\n");
      }
      // 对象键（信封域命中）与键缺失一致：C# WRONGTYPE → nil 同口径
      Ok(Some(Some(Err(())))) | Ok(Some(None)) => output.write_resp_null(),
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
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
    unpack_args!(parse_state, output, "RENAME", [old_key, new_key]);
    rename_sync(store, old_key, new_key, false, output)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAMENX
  pub fn network_renamenx<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unpack_args!(parse_state, output, "RENAMENX", [old_key, new_key]);
    rename_sync(store, old_key, new_key, true, output)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkGETDEL
  pub fn network_getdel<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unpack_args!(parse_state, output, "GETDEL", [key]);

    // 双域读：String 域命中取值删除；信封域命中（对象键）→ WRONGTYPE 且不删
    match read_adjudicated_user_sync(store, key, |v| v.to_vec()) {
      Ok(Some(Some(Ok(val)))) => {
        // 先删后答：删除遇异步闭环（环形页翻转/复合对象）时整体降级，
        // 避免已答出旧值而键未删成
        match store.try_delete_sync(key) {
          Ok(Ok(_)) => output.write_resp_bulk_string(&val),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
      Ok(Some(Some(Err(())))) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
      }
      Ok(Some(None)) => {
        output.write_resp_null();
      }
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXISTS
  ///
  /// 多键计数；任一键须异步裁决（磁盘候选/TTL 待裁决）时整体降级，
  /// 存储错误直接回错，避免计数口径失真
  pub fn network_exists<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, !empty, output, "EXISTS");

    let mut exists_count = 0i64;
    for key in parse_state {
      match probe_alive(store, key) {
        Ok(Some(true)) => exists_count += 1,
        Ok(Some(false)) => {}
        // 磁盘候选 / 异步裁决：整体降级
        Ok(None) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }

    output.write_resp_int(exists_count);
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE
  ///
  /// EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 共同体：完整 C# 参数校验次序
  /// （个数 → 整数 → 非负 → NX/XX/GT/LT 选项组合），过期经
  /// [`crate::storage::session::common::ttl_sync`] 同步落 TTL 记录；磁盘候选/过期清除须异步时整体降级
  pub fn network_expire<'a, D: wdev::Device>(
    &mut self,
    command: ExpireCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2..=4, output, command.as_str());
    let count = parse_state.len();

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
        abort_with_unsupported_option(output, parse_state[2].as_str_safe());
        return Ok(true);
      }
      opt = parse_expire_option(parse_state[2], TtlOpt::NONE);

      if count > 3 {
        if !try_parse_expire_option(parse_state[3]) {
          abort_with_unsupported_option(output, parse_state[3].as_str_safe());
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

    // 换算绝对过期 .NET Ticks（对标 KeyAdminCommands.cs:421-427 换算 switch，
    // rust TTL 记录即 ticks 口径，wkv/GC 同域解释）
    let expire_at_ticks = command.expire_at_ticks(expiration);

    match expire_apply_sync(store, key, expire_at_ticks, opt) {
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
        output.write_resp_error(RESP_ERR_GENERIC);
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
    unpack_args!(parse_state, output, "PERSIST", [key]);

    match persist_apply_sync(store, key) {
      Ok(Some(removed)) => {
        output.write_resp_int(i64::from(removed));
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
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
    unpack_args!(parse_state, output, command_name_of_ttl(command), [key]);

    match ttl_read_sync(store, key) {
      Ok(Some(read)) => {
        let value = match read {
          // 键缺失 → -2（C# status != OK → RESP_RETURN_VAL_N2）
          ExpiryRead::Missing => -2,
          ExpiryRead::NoExpiry => -1,
          // 对标 ConvertUtils：PTTL → MillisecondsFromDiffUtcNowTicks、
          // TTL → SecondsFromDiffUtcNowTicks（ReadMethods.cs:169-170 同源换算）
          ExpiryRead::At(exp) => {
            if command == TtlCmd::Pttl {
              milliseconds_from_diff_utc_now_ticks(exp)
            } else {
              seconds_from_diff_utc_now_ticks(exp)
            }
          }
        };
        output.write_resp_int(value);
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
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
    let cmd_name = if command == ExpireTimeCmd::Pexpiretime {
      "PEXPIRETIME"
    } else {
      "EXPIRETIME"
    };
    unpack_args!(parse_state, output, cmd_name, [key]);

    match expiretime_read_sync(store, key) {
      Ok(Some(read)) => {
        let value = match read {
          ExpiryRead::Missing => -2,
          ExpiryRead::NoExpiry => -1,
          // 对标 ConvertUtils：PEXPIRETIME → UnixTimeInMillisecondsFromTicks、
          // EXPIRETIME → UnixTimeInSecondsFromTicks（ReadMethods.cs:183-184）
          ExpiryRead::At(exp) => {
            if command == ExpireTimeCmd::Pexpiretime {
              unix_time_in_milliseconds_from_ticks(exp)
            } else {
              unix_time_in_seconds_from_ticks(exp)
            }
          }
        };
        output.write_resp_int(value);
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
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

/// RENAME/RENAMENX 共同内核（对标 libs/server/Storage/Session/UnifiedStore/
/// UnifiedStoreOps.cs:RENAME，C# 以 isNX 单实现双命令）
///
/// 序次对齐 C#：同键早退（首个检查，先于一切读取与 NX 判定）→ 双探旧键定
/// 物理域（String 命中 → 字符串；未命中探信封域，命中 → 对象键连同标签整体
/// 迁移；皆缺 → NOSUCHKEY）→ 旧键 TTL 记录 → [NX] 新键存活判定 → 写新键
///（同域写入，SET 语义自动清新键残留 TTL，对标 C# 全新记录拷贝）→ TTL 随键
/// 迁移（C# TryCopyFrom 连同 Expiration 拷入新记录）→ 清旧键 TTL → 删旧键。
///
/// 任一步遇异步闭环（磁盘候选/环形页翻转）即整体降级 `Ok(false)`：调用方
/// 重试整条命令，旧键未删时幂等重放。`Ok(true)` 已闭环（应答已写入 output）
fn rename_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  // C# 同键早退：RENAME → OK；RENAMENX → 1（result=1，先于 NX 存在性判定）
  if old_key == new_key {
    if nx {
      output.write_resp_int(1);
    } else {
      write_raw(output, cs::RESP_OK);
    }
    return Ok(true);
  }

  // 双探旧键定物理域：String 域命中 → 字符串迁移；信封域命中 → 对象键迁移
  //（值首字节起即信封载荷，原样搬移不嗅探内容）；两域皆缺 → NOSUCHKEY
  #[derive(Clone, Copy, PartialEq, Eq)]
  enum RenameDomain {
    Str,
    Obj,
  }
  let read_domain = |domain: RenameDomain| {
    let tag = match domain {
      RenameDomain::Str => KeyTag::String,
      RenameDomain::Obj => KeyTag::ObjectEnvelope,
    };
    read_adjudicated_tag_sync(store, old_key, tag, |v| v.to_vec())
  };
  let (old_val, domain) = match read_domain(RenameDomain::Str) {
    Ok(Some(Some(val))) => (val, RenameDomain::Str),
    Ok(Some(None)) => match read_domain(RenameDomain::Obj) {
      Ok(Some(Some(val))) => (val, RenameDomain::Obj),
      Ok(Some(None)) => {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
        return Ok(true);
      }
      // 旧键信封域有磁盘候选 / TTL 待裁决：降级
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    },
    Ok(None) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  };

  // 旧键 TTL 记录（Ok(None)：TTL 值在磁盘候选，降级）
  let old_ttl = match ttl_of_sync(store, old_key) {
    Ok(Some(ttl)) => ttl,
    Ok(None) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  };

  // 旧键 ETag 记录（对标 C# RENAME 搬迁记录连同可选 ETag 字段；
  // Ok(None)：磁盘候选降级）
  let old_etag = match etag_of_sync(store, old_key) {
    Ok(Some(etag)) => etag,
    Ok(None) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  };

  // RENAMENX：新键存活（含过期裁决，双域）→ 0，不动旧键
  if nx {
    match probe_alive(store, new_key) {
      Ok(Some(true)) => {
        output.write_resp_int(0);
        return Ok(true);
      }
      Ok(Some(false)) => {}
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }

  // 对象键迁移：覆写语义下先清退新键既有记录（含 String 域残留与随键 TTL，
  // 信封写入不自带跨域清退），再整体搬移信封载荷
  if domain == RenameDomain::Obj {
    match store.try_delete_sync(new_key) {
      Ok(Ok(_)) => {}
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }
  let upsert_done = match domain {
    RenameDomain::Str => store.try_upsert_sync(new_key, old_val.as_slice()),
    RenameDomain::Obj => {
      store.try_upsert_tag_sync(new_key, KeyTag::ObjectEnvelope, old_val.as_slice())
    }
  };
  match upsert_done {
    Ok(Ok(_)) => {}
    Ok(Err(_)) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  }
  // 对象域新键入账（对标 C# UnifiedStoreOps.RENAME 的 SET(newKey) →
  // WriteLogUpsert 全量条目：libs/server/Storage/Session/UnifiedStore/
  // UnifiedStoreOps.cs:RENAME）：resp 层直写信封域不经 StorageSession::upsert_tag
  // 的通知漏斗，此处显式补发 ObjectStoreUpsert——漏记则重放端只删旧键、
  // 新键无从建立，集合键丢失
  if domain == RenameDomain::Obj {
    let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, new_key);
    store
      .store
      .notify_envelope_upsert(raw_key.as_slice(), old_val.as_slice());
  }
  if let Some(exp) = old_ttl {
    match put_ttl_sync(store, new_key, exp) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }
  // 新键同步旧 etag：旧键有 etag 则回填，无 etag 则清退新键残留 etag
  //（旧键标签由尾部 try_delete_sync 级联清理）
  let etag_sync_res = if old_etag > NO_ETAG {
    put_etag_sync(store, new_key, old_etag)
  } else {
    del_etag_sync(store, new_key)
  };
  match etag_sync_res {
    Ok(true) => {}
    Ok(false) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  }
  // C# DELETE 将记录连同 Expiration 一并移除：先清旧键 TTL 记录再删数据，
  // 避免孤儿 TTL 记录令后续读取长期走异步裁决慢路径
  if old_ttl.is_some() {
    match del_ttl_sync(store, old_key) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }
  match store.try_delete_sync(old_key) {
    Ok(Ok(_)) => {}
    Ok(Err(_)) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  }

  if nx {
    output.write_resp_int(1);
  } else {
    write_raw(output, cs::RESP_OK);
  }
  Ok(true)
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

/// EXPIRE 应用内核（同步镜像 wkv::StoreSession::expire_at 的判定表，全链
/// .NET Ticks 同域比较）
///
/// 返回 `Ok(None)` 须降级异步；`Ok(Some(0))` 条件不满足/键缺失；
/// `Ok(Some(1))` 已设置（或过去时间戳已物理删除）
fn expire_apply_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  expire_at_ticks: i64,
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
      // NX/XX/GT/LT 判定（镜像 wkv：多项同设须全部满足才放行，ticks 同域比较）
      let denied = match current {
        None => opt.xx || opt.gt,
        Some(c) => opt.nx || (opt.gt && expire_at_ticks <= c) || (opt.lt && expire_at_ticks >= c),
      };
      if denied {
        return Ok(Some(0));
      }
      if expire_at_ticks <= now_ticks() {
        // 过去时间戳：物理删除（先删 TTL 再删数据，镜像 purge_expired 顺序）
        if !del_ttl_sync(store, key)? {
          return Ok(None);
        }
        return store
          .try_delete_sync(key)
          .map(|done| if done.is_ok() { Some(1) } else { None });
      }
      put_ttl_sync(store, key, expire_at_ticks).map(|done| if done { Some(1) } else { None })
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
  /// 有 TTL：绝对过期 .NET Ticks
  At(i64),
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
  use wbase::convert::{
    expire_after_ms_to_ticks, expire_after_to_ticks, expire_at_milliseconds_to_ticks,
    expire_at_seconds_to_ticks,
  };
  use wbase::time::now_ticks;

  use super::ExpireCmd;

  /// 命令端与 AOF 重放端的换算逐位一致（同输入同函数）：
  /// 绝对域（EXPIREAT/PEXPIREAT）命令端换算面与 wbase 单点恒等
  #[test]
  fn expire_at_matches_replay_conversion() {
    for seconds in [0, 1, 1_700_000_000, 4_102_444_799, i64::MAX, -1] {
      assert_eq!(
        ExpireCmd::Expireat.expire_at_ticks(seconds),
        expire_at_seconds_to_ticks(seconds)
      );
    }
    for millis in [0, 1, 1_700_000_000_000, i64::MAX, -1] {
      assert_eq!(
        ExpireCmd::Pexpireat.expire_at_ticks(millis),
        expire_at_milliseconds_to_ticks(millis)
      );
    }
  }

  /// 相对域（EXPIRE/PEXPIRE）命令端与重放端共享同一饱和乘加单点；
  /// 非饱和路径时钟在两次调用间推进，以 1 秒容差断言同源
  #[test]
  fn expire_after_matches_replay_conversion() {
    const DRIFT: i64 = wbase::convert::TICKS_PER_SECOND;
    let now = now_ticks();
    for seconds in [0, 1, 60, 86_400] {
      let cmd_ticks = ExpireCmd::Expire.expire_at_ticks(seconds);
      assert!(
        (cmd_ticks - expire_after_to_ticks(now, seconds)).abs() <= DRIFT,
        "EXPIRE {seconds}s: {cmd_ticks} vs {}",
        expire_after_to_ticks(now, seconds)
      );
    }
    for millis in [0, 1, 500, 86_400_000] {
      let cmd_ticks = ExpireCmd::Pexpire.expire_at_ticks(millis);
      assert!(
        (cmd_ticks - expire_after_ms_to_ticks(now, millis)).abs() <= DRIFT,
        "PEXPIRE {millis}ms: {cmd_ticks} vs {}",
        expire_after_ms_to_ticks(now, millis)
      );
    }
    // 饱和边界：与 now 无关，重放端与命令端逐位一致（同钉 i64::MAX）
    assert_eq!(
      ExpireCmd::Expire.expire_at_ticks(i64::MAX),
      expire_after_to_ticks(now_ticks(), i64::MAX)
    );
    assert_eq!(
      ExpireCmd::Pexpire.expire_at_ticks(i64::MAX),
      expire_after_ms_to_ticks(now_ticks(), i64::MAX)
    );
  }

  /// 命令形态枚举文本与 C# command.ToString() 对齐（错误文案面）
  #[test]
  fn cmd_as_str() {
    assert_eq!(ExpireCmd::Expire.as_str(), "EXPIRE");
    assert_eq!(ExpireCmd::Pexpireat.as_str(), "PEXPIREAT");
  }
}
