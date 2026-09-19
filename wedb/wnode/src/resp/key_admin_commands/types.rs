//! EXISTS / DUMP / RESTORE 类型判定与序列化载荷管理命令（对标 libs/server/Resp/KeyAdminCommands.cs）

use itoa::Buffer;
use wbase::{
  convert::expire_after_to_ticks, crc64::hash as rdb_crc64_hash, num::strict_i32, time::now_ticks,
};
use wresp::{
  check_args::{check_arg_count, unpack_args},
  cmd_strings as cs,
  cmd_strings::{RESP_ERR_GENERIC, abort_with_error_message, write_error_raw, write_raw},
  ext::RespVecExt,
  length::{try_read_length, try_write_length},
};

use super::super::{resp_server_session::RespServerSession, vector::vector_manager::VectorManager};
use crate::storage::session::common::{
  UserRead, read_user_sync,
  ttl_sync::{probe_alive, probe_alive_with_registry, put_ttl_sync},
};

/// DUMP 载荷版本/校验和非法文案（本域多处复用）。
const ERR_DUMP_VERSION_CHECKSUM: &str = "ERR DUMP payload version or checksum are wrong";
/// DUMP 载荷长度格式非法文案（本域两处复用）。
const ERR_DUMP_LENGTH_INVALID: &str = "ERR DUMP payload length format is invalid";

/// RDB 格式版本（libs/server/Resp/KeyAdminCommands.cs:RDB_VERSION）
pub(crate) const RDB_VERSION: u16 = 11;

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRESTORE
  ///
  /// 与 Redis 规范的已知差异（维持对标 Garnet，勿按 Redis 规范改）：
  /// - ttl 按秒解释：Redis 规范该参数是毫秒（缺省为相对空闲毫秒数，带 ABSTTL 时为绝对
  ///   Unix 毫秒时间戳），Garnet 与 C# 一样按秒换算，客户端若按 Redis 语义传 5000 表示
  ///   「5 秒」，这里会被解释成 5000 秒（TTL 放大 1000 倍）；
  /// - 只收三参 key、ttl、value：Redis 规范的 ABSTTL/IDLETIME/FREQ 修饰符与 REPLACE
  ///   均不支持，C# 同样在参数数不为 3 时直接回参数数错误，覆盖已存在键只能回 BUSYKEY、
  ///   无替换通路。
  ///
  /// 两点都是继承自 Garnet 的真实分叉而非 rust 回归，本实现 1:1 对标 Garnet，
  /// 禁自行改单位或扩参数面（transpile SKILL 的 1:1 对标原则）
  pub fn network_restore<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, expiry, val)) = parse_restore_args(parse_state, output) else {
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
      // 换算单点与 EXPIRE 同源（expire_after_to_ticks）。口径为秒，非 Redis 的毫秒，
      // 详见本函数头部与 Redis 规范的差异说明
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
    let Some([key]) = unpack_args(parse_state, output, "DUMP") else {
      return Ok(true);
    };

    let dump_res = read_user_sync(store, key, |value| {
      let mut encoded_len = [0u8; 5];
      let Some(bytes_written) = try_write_length(value.len() as u32, &mut encoded_len) else {
        return false;
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
      output.extend_from_slice(value);
      output.extend_from_slice(&RDB_VERSION.to_le_bytes());
      // crc64 覆盖类型字节起至版本字节止。刻意偏离 C#：C# DUMP 的 crc 从
      // 类型字节之后起算（KeyAdminCommands.cs:200 的 Slice 越过 0x00），
      // 而其 RESTORE 的 crc 校验含类型字节，C# 自身 DUMP→RESTORE 往返必被
      // "checksum wrong" 拒绝；rust 对齐 RESTORE 口径保证往返成立
      let framed = output.len() - (payload_len - 8);
      let crc = rdb_crc64_hash(&output[framed..]);
      output.extend_from_slice(&crc);
      output.extend_from_slice(b"\r\n");
      true
    });

    match dump_res {
      Ok(UserRead::Hit(true)) => {}
      Ok(UserRead::Hit(false)) => {
        write_error_raw(output, "ERR DUMP payload length is invalid");
      }
      // 对象键（信封域命中）与键缺失一致：C# WRONGTYPE → nil 同口径
      Ok(UserRead::WrongType | UserRead::Missing) => {
        output.write_resp_null_ver(self.resp_protocol_version)
      }
      Ok(UserRead::Deferred) => return Ok(false),
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
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "EXISTS");

    let mut exists_count = 0i64;
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    for key in parse_state {
      // 三域探针 + 向量登记表第四态（存活观测单点，对标 C# Reader 无类型门）
      match probe_alive_with_registry(store, prefix_slice, key, vector) {
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
}

/// NetworkRESTORE 的参数与载荷推导单源（快慢路径共用；解析失败时已写出错误
/// 应答并返回 None，返回 `(key, expiry 秒, 载荷内值切片)`）
///
/// 校验序列对标 C# KeyAdminCommands.cs:NetworkRESTORE：ttl 整数（沿用
/// RESP_ERR_TIMEOUT_NOT_VALID_FLOAT 历史文案）→ 类型字节 0x00 → footer
/// （2 字节 rdb 版本 + 8 字节 crc64）→ 长度前缀。C# 对空载荷直接 valueSpan[0]
/// 越界（进程崩溃断连）、Slice 越界抛异常断连；rust 无 panic 约束下按同族
/// 错误降级应答，不复刻崩溃
pub(crate) fn parse_restore_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], i64, &'p [u8])> {
  let Some([key, expiry_raw, value]) = unpack_args(parse_state, output, "RESTORE") else {
    return None;
  };
  // C# TryGetInt（index = Count - 2，arity 锁 3 即下标 1）
  let Some(expiry) = strict_i32(expiry_raw) else {
    abort_with_error_message(output, cs::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
    return None;
  };
  // RESTORE 仅实现字符串类型（类型字节 0x00）
  if value.first() != Some(&0x00) {
    write_error_raw(output, "ERR RESTORE currently only supports string types");
    return None;
  }
  if value.len() < 10 {
    write_error_raw(output, ERR_DUMP_VERSION_CHECKSUM);
    return None;
  }
  // footer = 2 字节 rdb 版本 + 8 字节 crc64
  let footer = &value[value.len() - 10..];
  let rdb_version = u16::from_le_bytes([footer[0], footer[1]]);
  if rdb_version > RDB_VERSION {
    write_error_raw(output, ERR_DUMP_VERSION_CHECKSUM);
    return None;
  }
  // crc 覆盖除末 8 字节外的全部载荷
  let calculated_crc = rdb_crc64_hash(&value[..value.len() - 8]);
  if calculated_crc != footer[2..] {
    write_error_raw(output, ERR_DUMP_VERSION_CHECKSUM);
    return None;
  }
  let Some((length, payload_start)) = try_read_length(&value[1..]) else {
    write_error_raw(output, ERR_DUMP_LENGTH_INVALID);
    return None;
  };
  let start = payload_start + 1;
  let val = start
    .checked_add(length as usize)
    .and_then(|end| value.get(start..end))?;
  Some((key, i64::from(expiry), val))
}
