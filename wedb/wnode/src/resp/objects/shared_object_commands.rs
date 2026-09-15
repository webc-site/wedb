//! 共享对象命令（对标 libs/server/Resp/Objects/SharedObjectCommands.cs）
//!
//! ObjectScan 是四类集合对象共用的 ZSCAN/HSCAN/SSCAN/COSCAN 入口：
//! 参数校验与光标解析在本层完成，对象遍历经 ObjectInput 走各对象的
//! scan 分片（有序集合侧见 wcol::zset::sorted_set_object_impl::scan_operate）。

use wcol::{
  ObjectInput, hash::hash_object::HashOperation, set::set_object::SetOperation,
  types::object_output::ObjectOutput, zset::sorted_set_object::SortedSetOperation,
};
use wconf::ServerConfigType;
use wresp::{RespSliceExt, RespVecExt, cmd_strings as cs};
use wval::GarnetObjectType;

use crate::resp::{
  objects::{
    hash_commands::hash_load_sync,
    object_store_utils::{ObjLoad, make_object_input},
    set_commands::set_load_sync,
    sorted_set_commands::zset_load_sync,
  },
  resp_server_session::RespServerSession,
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
    operate: impl FnOnce(&ObjectInput, &mut ObjectOutput),
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

    // 光标须为非负整数（parseState.GetLong 严格语义）
    if !parse_state[1].try_parse_i64().is_some_and(|v| v >= 0) {
      return self.abort_with_error_message(cs::RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes(), output);
    }

    // C# switch(objectType)：HashOp=HSCAN / SSetOp=SSCAN / SortedSetOp=ZSCAN；
    // COSCAN 经 header.type == All 分派（不覆盖子命令码）
    let sub_id = match object_type {
      GarnetObjectType::Hash => HashOperation::Hscan as u8,
      GarnetObjectType::Set => SetOperation::Sscan as u8,
      GarnetObjectType::SortedSet => SortedSetOperation::Zscan as u8,
      GarnetObjectType::All => 0,
      _ => 0,
    };
    // ObjectInput：startIdx = 1（跳过键），arg2 = 单轮 COUNT 上限
    let input = make_object_input(object_type, sub_id, &parse_state[1..], 0, scan_count_limit);

    let mut obj_out = ObjectOutput::new();
    operate(&input, &mut obj_out);
    output.extend_from_slice(&obj_out.payload);
    true
  }

  /// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
  ///
  /// HSCAN 分派承接（C# RespServerSession.cs switch 中 HSCAN => ObjectScan
  /// 的 Hash 形态存储接线：装载哈希信封后经 [`Self::object_scan`] 求值）
  pub fn network_hscan<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let resp_version = self.resp_protocol_version;
    self.scan_object_typed(
      parse_state,
      GarnetObjectType::Hash,
      hash_load_sync,
      |obj, input, obj_out| {
        obj.operate(input, obj_out, resp_version);
      },
      store,
      output,
    )
  }

  /// SSCAN key cursor [MATCH pattern] [COUNT count]
  ///
  /// SSCAN 分派承接（C# RespServerSession.cs switch 中 SSCAN => ObjectScan
  /// 的 Set 形态存储接线：装载集合信封后经 [`Self::object_scan`] 求值）
  pub fn network_sscan<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let resp_version = self.resp_protocol_version;
    self.scan_object_typed(
      parse_state,
      GarnetObjectType::Set,
      set_load_sync,
      |obj, input, obj_out| {
        obj.operate(input, obj_out, resp_version);
      },
      store,
      output,
    )
  }

  /// ZSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
  ///
  /// ZSCAN 分派承接（C# RespServerSession.cs switch 中 ZSCAN => ObjectScan
  /// 的 SortedSet 形态存储接线：装载有序集信封后经 [`Self::object_scan`] 求值）
  pub fn network_zscan<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let resp_version = self.resp_protocol_version;
    self.scan_object_typed(
      parse_state,
      GarnetObjectType::SortedSet,
      zset_load_sync,
      |obj, input, obj_out| {
        obj.operate(input, obj_out, resp_version);
      },
      store,
      output,
    )
  }

  /// COSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
  ///
  /// COSCAN 分派承接（对标 C# RespServerSession.cs: COSCAN => ObjectScan(GarnetObjectType.All)）
  pub fn network_coscan<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    use crate::storage::session::common::ttl_sync::{
      read_adjudicated_envelope_sync, read_adjudicated_user_sync,
    };
    if parse_state.len() < 2 {
      return Ok(self.abort_with_wrong_number_of_arguments("COSCAN", output));
    }
    let key = parse_state[0];
    // 双域判定：String 域命中即用户字符串键 → WRONGTYPE（COSCAN 仅作用于
    // 集合对象）；信封域命中按值首字节内层标签（信封载荷自身类型字段）分派
    let tag = match read_adjudicated_user_sync(store, key, |_| ()) {
      Ok(Some(Some(Ok(())))) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(Some(Err(())))) => {
        match read_adjudicated_envelope_sync(store, key, |v| v.first().copied()) {
          Ok(Some(Some(tag))) => tag,
          Ok(Some(None)) => None,
          // 信封域命中已由双探确认，磁盘候选/TTL 待裁决降级；存储错误照旧报错
          Ok(None) => return Ok(false),
          Err(_) => {
            output.write_resp_error(cs::RESP_ERR_GENERIC);
            return Ok(true);
          }
        }
      }
      Ok(Some(None)) => None,
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error(cs::RESP_ERR_GENERIC);
        return Ok(true);
      }
    };
    let Some(tag) = tag else {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(b"0");
      output.write_resp_array_len(0);
      return Ok(true);
    };
    match tag {
      3 => self.network_hscan(parse_state, store, output),
      4 => self.network_sscan(parse_state, store, output),
      1 => self.network_zscan(parse_state, store, output),
      _ => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        Ok(true)
      }
    }
  }

  /// 对象扫描的存储接线公共体：校验 → 装载 → scan operate → 负载
  ///
  /// C# ObjectScan 经 storageApi.ObjectScan 在存储层装载求值；rust 同步
  /// 执行域在命令层装载（信封解码与 storage 会话域同一格式），键缺失回
  /// `[0, 空数组]`（C# NOTFOUND 分支），磁盘候选降级 `Ok(false)`；
  /// COUNT 钳制上限取 runtimeConfig OBJECT_SCAN_COUNT_LIMIT
  /// （C# storeWrapper.runtimeConfig.GetInt，CONFIG SET 热更即时生效）
  fn scan_object_typed<T, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    object_type: GarnetObjectType,
    load: impl FnOnce(&wkv::BatchStoreSession<'_, D>, &[u8], &mut Vec<u8>) -> ObjLoad<T>,
    operate: impl FnOnce(&mut T, &ObjectInput, &mut ObjectOutput),
    store: &wkv::BatchStoreSession<'_, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let scan_count_limit = self
      .runtime_config()
      .get_int(ServerConfigType::ObjectScanCountLimit);
    // 参数形态预检（完整校验由 object_scan 承接，此处仅保证键位可读）
    if parse_state.len() < 2 || !parse_state[1].try_parse_i64().is_some_and(|v| v >= 0) {
      self.object_scan(
        parse_state,
        object_type,
        scan_count_limit,
        output,
        |_, _| {},
      );
      return Ok(true);
    }
    let key = parse_state[0];
    match load(store, key, output) {
      ObjLoad::Degrade => Ok(false),
      ObjLoad::Error => Ok(true),
      // C# NOTFOUND：游标 0 + 空数组
      ObjLoad::Missing => {
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(b"0");
        output.write_resp_array_len(0);
        Ok(true)
      }
      ObjLoad::Present(mut obj) => {
        self.object_scan(
          parse_state,
          object_type,
          scan_count_limit,
          output,
          |input, obj_out| operate(&mut obj, input, obj_out),
        );
        Ok(true)
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use wval::GarnetObjectType;

  use super::*;

  #[test]
  fn object_scan_validates_args() {
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    // 参数不足：中止返回 true（命令已完整消费，对齐 C# Abort 语义）
    assert!(sess.object_scan(
      &[b"key"],
      GarnetObjectType::SortedSet,
      10,
      &mut out,
      |_, _| {},
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
      |_, _| {},
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
      },
    ));
    assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");
  }
}
