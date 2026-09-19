//! 共享对象命令（对标 libs/server/Resp/Objects/SharedObjectCommands.cs）
//!
//! ObjectScan 是四类集合对象共用的 ZSCAN/HSCAN/SSCAN/COSCAN 入口：
//! 参数校验与光标解析在本层完成，对象遍历经 operate 直收切片走各对象的
//! scan 分片（有序集合侧见 wcol::zset::sorted_set_object_impl::scan_operate）。

use wcol::{
  ObjLoad, ObjectOutput, hash::hash_object::HashOperation, set::set_object::SetOperation,
  zset::sorted_set_object::SortedSetOperation,
};
use wconf::ServerConfigType;
use wresp::{
  cmd_strings as cs,
  ext::{RespSliceExt, RespVecExt},
};
use wval::{GarnetObjectType, KeyTag};

use crate::resp::{
  objects::{
    hash_commands::hash_load_sync, set_commands::set_load_sync, sorted_set_commands::zset_load_sync,
  },
  resp_server_session::RespServerSession,
};

/// 无 IO 共享校验结果：成功携带子命令码与跳键参数切片，失败仅携带失败类别
///
/// 同步段 `RespServerSession::object_scan` 与慢段 `slow::object_scan` 各按自身错误通道
/// 落帧，校验口径共用 `scan_validate` 一份，杜绝逐字双抄与 All 臂漂移
enum ScanOutcome<'a> {
  /// 参数计数不足（C# parseState.Count < 2 → AbortWithWrongNumberOfArguments(cmdName)）
  WrongNumArgs { cmd_name: &'static str },
  /// 光标非负校验失败（C# TryGetLong(1) 且 >= 0，否则 RESP_ERR_GENERIC_INVALIDCURSOR）
  InvalidCursor,
  /// 校验通过：子命令操作码 + 跳过键位的参数切片（C# startIdx = 1）
  Ready { sub_id: u8, args: &'a [&'a [u8]] },
}

/// HSCAN / SSCAN / ZSCAN / COSCAN 共享校验内核（无 IO，全仓一份）
///
/// 对位 C# ObjectScan 会话方法内的四段无 IO 校验（cmdName switch、参数计数门、光标非负门、
/// 子命令 switch）：同步执行与 pending 重放走同一内核，校验逻辑一处定义；
/// `All => COSCAN` 臂只此一份，消除慢段漂移
fn scan_validate<'a>(
  object_type: GarnetObjectType,
  parse_state: &'a [&'a [u8]],
) -> ScanOutcome<'a> {
  // 命令名仅用于错误文本（C# cmdName switch：Hash/Set/SortedSet/All => COSCAN/_ => NONE）
  let cmd_name = match object_type {
    GarnetObjectType::Hash => "HSCAN",
    GarnetObjectType::Set => "SSCAN",
    GarnetObjectType::SortedSet => "ZSCAN",
    GarnetObjectType::All => "COSCAN",
    _ => "NONE",
  };

  // 参数计数门（C# parseState.Count < 2）
  if parse_state.len() < 2 {
    return ScanOutcome::WrongNumArgs { cmd_name };
  }

  // 光标须为非负整数（C# parseState.TryGetLong(1) 且 >= 0）
  if !parse_state[1].try_parse_i64().is_some_and(|v| v >= 0) {
    return ScanOutcome::InvalidCursor;
  }

  // C# switch(objectType)：HashOp=HSCAN / SSetOp=SSCAN / SortedSetOp=ZSCAN；
  // COSCAN 经 header.type == All 分派（不覆盖子命令码），故 All/_ => 0
  let sub_id = match object_type {
    GarnetObjectType::Hash => HashOperation::Hscan as u8,
    GarnetObjectType::Set => SetOperation::Sscan as u8,
    GarnetObjectType::SortedSet => SortedSetOperation::Zscan as u8,
    _ => 0,
  };

  // startIdx = 1（跳过键直化为切片），arg2 = 单轮 COUNT 上限由调用方注入
  ScanOutcome::Ready {
    sub_id,
    args: &parse_state[1..],
  }
}

impl RespServerSession {
  /// HSCAN / SSCAN / ZSCAN / COSCAN 共享入口
  ///
  /// libs/server/Resp/Objects/SharedObjectCommands.cs:ObjectScan
  ///
  /// 校验委托无 IO 内核 `scan_validate`（与慢段 `slow::object_scan` 单源同口径），失败按
  /// 同步段错误通道 `abort_with_wrong_number_of_arguments` / `abort_with_error_message` 落帧；
  /// `scan_count_limit` 对应 OBJECT_SCAN_COUNT_LIMIT 运行时配置（经 arg2 下发钳制 COUNT）；
  /// `operate` 为装载对象后的操作回调（由调用方按对象类型注入），返回 RESP 负载
  pub fn object_scan(
    &mut self,
    parse_state: &[&[u8]],
    object_type: GarnetObjectType,
    scan_count_limit: i32,
    output: &mut Vec<u8>,
    operate: impl FnOnce(u8, &[&[u8]], i32, &mut ObjectOutput),
  ) -> bool {
    match scan_validate(object_type, parse_state) {
      ScanOutcome::WrongNumArgs { cmd_name } => {
        self.abort_with_wrong_number_of_arguments(cmd_name, output)
      }
      ScanOutcome::InvalidCursor => {
        self.abort_with_error_message(cs::RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes(), output)
      }
      ScanOutcome::Ready { sub_id, args } => {
        let mut obj_out = ObjectOutput::new();
        operate(sub_id, args, scan_count_limit, &mut obj_out);
        output.extend_from_slice(&obj_out.payload);
        true
      }
    }
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
      |obj, sub_id, args, arg2, obj_out| {
        obj.operate(sub_id, args, 0, arg2, obj_out, resp_version);
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
      |obj, sub_id, args, arg2, obj_out| {
        obj.operate(sub_id, args, 0, arg2, obj_out, resp_version);
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
      |obj, sub_id, args, arg2, obj_out| {
        obj.operate(sub_id, args, 0, arg2, obj_out, resp_version);
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
    use crate::storage::session::common::{
      TagRead, UserRead, read_envelope_sync, read_tag_sync, read_user_sync,
      ttl_sync::meta_collection_type_of,
    };
    if parse_state.len() < 2 {
      return Ok(self.abort_with_wrong_number_of_arguments("COSCAN", output));
    }
    let key = parse_state[0];
    // 三域判定：String 域命中即用户字符串键 → WRONGTYPE（COSCAN 仅作用于
    // 集合对象）；信封域命中按值首字节内层标签（信封载荷自身类型字段）分派；
    // Meta 域命中（升阶键）按 MetaValue.collection_type 分派
    let tag = match read_user_sync(store, key, |_| ()) {
      Ok(UserRead::Hit(())) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(UserRead::WrongType) => {
        match read_envelope_sync(store, key, |v| v.first().copied()) {
          Ok(TagRead::Hit(tag)) => tag,
          Ok(TagRead::Missing) => {
            // 升阶键：仅集合三型可扫描，其余（RangeIndex 等）WRONGTYPE
            match read_tag_sync(store, key, KeyTag::Meta, meta_collection_type_of) {
              Ok(TagRead::Hit(Some(t)))
                if matches!(
                  t,
                  GarnetObjectType::Hash | GarnetObjectType::Set | GarnetObjectType::SortedSet
                ) =>
              {
                Some(t as u8)
              }
              Ok(TagRead::Hit(_)) | Ok(TagRead::Missing) => {
                output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
                return Ok(true);
              }
              Ok(TagRead::Deferred) => return Ok(false),
              Err(_) => {
                output.write_resp_error(cs::RESP_ERR_GENERIC);
                return Ok(true);
              }
            }
          }
          // 信封域命中已由双探确认，磁盘候选/TTL 待裁决降级；存储错误照旧报错
          Ok(TagRead::Deferred) => return Ok(false),
          Err(_) => {
            output.write_resp_error(cs::RESP_ERR_GENERIC);
            return Ok(true);
          }
        }
      }
      Ok(UserRead::Missing) => None,
      Ok(UserRead::Deferred) => return Ok(false),
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
    match GarnetObjectType::from_u8(tag) {
      Some(GarnetObjectType::Hash) => self.network_hscan(parse_state, store, output),
      Some(GarnetObjectType::Set) => self.network_sscan(parse_state, store, output),
      Some(GarnetObjectType::SortedSet) => self.network_zscan(parse_state, store, output),
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
    operate: impl FnOnce(&mut T, u8, &[&[u8]], i32, &mut ObjectOutput),
    store: &wkv::BatchStoreSession<'_, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let scan_count_limit = self
      .runtime_config()
      .get_int(ServerConfigType::ObjectScanCountLimit);
    // 参数形态预检直接吃校验内核谓词（不再字面复抄 len/cursor 判定），未通过即由
    // object_scan 薄壳按同一口径落错误帧（此处仅保证键位可读，避免无谓装载）
    if !matches!(
      scan_validate(object_type, parse_state),
      ScanOutcome::Ready { .. }
    ) {
      self.object_scan(
        parse_state,
        object_type,
        scan_count_limit,
        output,
        |_, _, _, _| {},
      );
      return Ok(true);
    }
    let key = parse_state[0];
    match load(store, key, output) {
      ObjLoad::Degrade => Ok(false),
      ObjLoad::WrongType => Ok(true),
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
          |sub_id, args, arg2, obj_out| operate(&mut obj, sub_id, args, arg2, obj_out),
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
      |_, _, _, _| {},
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
      |_, _, _, _| {},
    ));
    assert_eq!(out, b"-ERR invalid cursor\r\n");

    // 合法输入透传至操作回调
    out.clear();
    assert!(sess.object_scan(
      &[b"key", b"0", b"COUNT", b"5"],
      GarnetObjectType::SortedSet,
      10,
      &mut out,
      |_sub_id, args, arg2, out| {
        assert_eq!(arg2, 10);
        assert_eq!(args.len(), 3); // 键已在底层剥离
        out.payload.extend_from_slice(b"*2\r\n$1\r\n0\r\n*0\r\n");
      },
    ));
    assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");
  }
}
/// 慢路径执行臂（exec_slow 冷键分派；嵌套模块保持对同步段零侵入）
///
/// 对标 libs/server/Resp/Objects/SharedObjectCommands.cs:ObjectScan 经
/// Tsavorite pending 读 CompletePending 后重放的异步形态：参数校验复用模块级
/// 无 IO 内核 scan_validate，与同步段 object_scan 走同一函数体（sub_id / 跳键
/// 切片单源、All => COSCAN 臂一份），慢段仅按自身错误通道落帧。`Err(())` 为存储
/// IO 失败，由 exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE
pub(crate) mod slow {
  use wcol::{
    ObjectOutput, hash::hash_object::HashObject, set::set_object::SetObject,
    zset::sorted_set_object::SortedSetObject,
  };
  use wresp::{cmd_strings as cs, ext::RespVecExt};
  use wval::GarnetObjectType;

  use crate::{
    resp::objects::object_store_utils::{GarnetObjectPayload, obj_load_typed_async},
    storage::session::{
      common::ttl_sync::meta_collection_type_of, storage_session::StorageSession,
    },
  };

  /// HSCAN / SSCAN / ZSCAN 慢路径承接（COSCAN 先经 [`coscan`] 解析内层
  /// 标签后转调本入口）
  pub(crate) async fn object_scan(
    storage: &StorageSession<'_, impl wdev::Device>,
    parse_state: &[&[u8]],
    object_type: GarnetObjectType,
    scan_count_limit: i32,
    resp_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    // 参数校验复用无 IO 内核 scan_validate（与同步段 object_scan 单源，含 All => COSCAN 臂），
    // 慢段按自身错误通道落帧；成功取子命令码与跳键切片
    let (sub_id, args) = match super::scan_validate(object_type, parse_state) {
      super::ScanOutcome::WrongNumArgs { cmd_name } => {
        cs::abort_with_wrong_number_of_arguments(output, cmd_name);
        return Ok(());
      }
      super::ScanOutcome::InvalidCursor => {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_INVALIDCURSOR);
        return Ok(());
      }
      super::ScanOutcome::Ready { sub_id, args } => (sub_id, args),
    };
    let key = parse_state[0];

    macro_rules! load_eval {
      ($ty:ty) => {{
        let loaded = obj_load_typed_async(storage, key, object_type, output, <$ty>::from_blob)
          .await
          .map_err(|_| ())?;
        match loaded {
          // 异步域 Degrade 唯一来源为分层 Meta 命中：树内游标扫描闭环
          //（C# ObjectScan 对任意规模对象恒可用；流式扫描不物化）
          wcol::ObjLoad::Degrade => {
            return if crate::resp::objects::tiered_collection_ops::exec_tiered_scan(
              &storage.batch,
              key,
              object_type,
              args,
              scan_count_limit,
              output,
              storage.resp_protocol_version(),
            )
            .await?
            {
              Ok(())
            } else {
              Err(())
            };
          }
          wcol::ObjLoad::WrongType => return Ok(()),
          // C# NOTFOUND：游标 0 + 空数组
          wcol::ObjLoad::Missing => {
            output.write_resp_array_len(2);
            output.write_resp_bulk_string(b"0");
            output.write_resp_array_len(0);
            return Ok(());
          }
          wcol::ObjLoad::Present(mut obj) => {
            let mut obj_out = ObjectOutput::new();
            obj.operate(
              sub_id,
              args,
              0,
              scan_count_limit,
              &mut obj_out,
              resp_version,
            );
            output.extend_from_slice(&obj_out.payload);
            return Ok(());
          }
        }
      }};
    }
    match object_type {
      GarnetObjectType::Hash => load_eval!(HashObject),
      GarnetObjectType::Set => load_eval!(SetObject),
      GarnetObjectType::SortedSet => load_eval!(SortedSetObject),
      _ => {
        cs::write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        Ok(())
      }
    }
  }

  /// COSCAN 慢路径承接：三域异步判定后按内层标签转调
  ///
  /// String 域命中 → WRONGTYPE（COSCAN 仅作用于集合对象）；信封域命中按
  /// 值首字节内层标签分派；Meta 域命中（升阶键）按 MetaValue.collection_type
  /// 分派；三域皆缺 → `[0, 空数组]`（对位同步段 network_coscan）
  pub(crate) async fn coscan(
    storage: &StorageSession<'_, impl wdev::Device>,
    parse_state: &[&[u8]],
    scan_count_limit: i32,
    resp_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    use wval::KeyTag;

    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "COSCAN");
      return Ok(());
    }
    let key = parse_state[0];
    // String 域命中即用户字符串键 → WRONGTYPE
    if matches!(
      storage.read_tag_with(key, KeyTag::String, |_| ()).await,
      Ok(Some(()))
    ) {
      output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
      return Ok(());
    }
    // 信封域命中按值首字节内层标签分派；三域皆缺 → [0, 空数组]
    let tag = match storage
      .read_tag_with(key, KeyTag::ObjectEnvelope, |v| v.first().copied())
      .await
    {
      Ok(Some(tag)) => tag,
      Ok(None) => {
        // 信封域缺失：探分层元记录域（升阶键）；非集合三型按缺失收口
        //（下方分派臂统一 WRONGTYPE 收口）
        match storage
          .read_tag_with(key, KeyTag::Meta, meta_collection_type_of)
          .await
        {
          Ok(Some(Some(t)))
            if matches!(
              t,
              GarnetObjectType::Hash | GarnetObjectType::Set | GarnetObjectType::SortedSet
            ) =>
          {
            Some(t as u8)
          }
          _ => None,
        }
      }
      Err(_) => return Err(()),
    };
    let Some(tag) = tag else {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(b"0");
      output.write_resp_array_len(0);
      return Ok(());
    };
    match GarnetObjectType::from_u8(tag) {
      Some(
        object_type
        @ (GarnetObjectType::Hash | GarnetObjectType::Set | GarnetObjectType::SortedSet),
      ) => {
        object_scan(
          storage,
          parse_state,
          object_type,
          scan_count_limit,
          resp_version,
          output,
        )
        .await
      }
      _ => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        Ok(())
      }
    }
  }
}
