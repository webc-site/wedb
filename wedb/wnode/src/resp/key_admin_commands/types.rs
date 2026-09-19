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
const RDB_VERSION: u16 = 11;

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRESTORE
  pub fn network_restore<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key, expiry_raw, value]) = unpack_args(parse_state, output, "RESTORE") else {
      return Ok(true);
    };

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
    let start = payload_start + 1;
    let Some(val) = start
      .checked_add(length as usize)
      .and_then(|end| value.get(start..end))
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
