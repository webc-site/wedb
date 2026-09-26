//! 共享对象命令（对标 libs/server/Resp/Objects/SharedObjectCommands.cs）
//!
//! ObjectScan 是四类集合对象共用的 ZSCAN/HSCAN/SSCAN/COSCAN 入口：
//! 参数校验与光标解析在本层完成，对象遍历经 operate 直收切片走各对象的
//! scan 分片（有序集合侧见 wcol::zset::sorted_set_object_impl::scan_operate）。

use wbase::num::strict_i32;
use wcol::{
  ObjLoad, ObjectOutput, hash::hash_object::HashOperation, set::set_object::SetOperation,
  types::custom_scan_operate, zset::sorted_set_object::SortedSetOperation,
};
use wconf::ServerConfigType;
use wdev::Device;
use wresp::{
  check_args::check_arg_count,
  cmd_strings as cs,
  ext::{RespSliceExt, RespVecExt},
};
use wval::GarnetObjectType;

use crate::resp::{
  custom_objects::custom_object_entry,
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
  let (cmd_name, sub_id) = match object_type {
    GarnetObjectType::Hash => ("HSCAN", HashOperation::Hscan as u8),
    GarnetObjectType::Set => ("SSCAN", SetOperation::Sscan as u8),
    GarnetObjectType::SortedSet => ("ZSCAN", SortedSetOperation::Zscan as u8),
    GarnetObjectType::All => ("COSCAN", 0),
    _ => ("NONE", 0),
  };

  // 参数计数门（C# parseState.Count < 2）
  if parse_state.len() < 2 {
    return ScanOutcome::WrongNumArgs { cmd_name };
  }

  // 光标须为非负整数（C# parseState.TryGetLong(1) 且 >= 0）
  if !parse_state[1].try_parse_i64().is_some_and(|v| v >= 0) {
    return ScanOutcome::InvalidCursor;
  }

  // startIdx = 1（跳过键直化为切片），arg2 = 单轮 COUNT 上限由调用方注入
  ScanOutcome::Ready {
    sub_id,
    args: &parse_state[1..],
  }
}

/// C# NOTFOUND 帧形：`[0, 空数组]`（对象扫描族缺键收口共用）
fn write_scan_not_found(output: &mut Vec<u8>) {
  output.write_resp_array_len(2);
  output.write_resp_bulk_string(b"0");
  output.write_resp_array_len(0);
}

/// COSCAN 信封载荷求值单点（同步臂 [`RespServerSession::network_coscan`] 与
/// 冷臂 `slow::coscan` 共用，杜绝双臂漂移）
///
/// 剥内层标签后按扩展标签域收口：命中静态清单即装载自定义对象走其 COSCAN
/// 扫描内核（C# 原域：header.type == All 仅 `CustomObjectBase.Operate` 接受
/// 转 Scan，游标/成对协议对齐 `GarnetObjectBase.Scan` 组帧帧形）；其余标签
///（内置 Hash/Set/SortedSet、List 与未知标签）→ WRONGTYPE——内置三型在 C#
/// 是严格类型检查挂 WrongType 旗，绝无扫描语义
fn coscan_eval(payload: &[u8], args: &[&[u8]], scan_count_limit: i32, output: &mut Vec<u8>) {
  let Some((tag, obj_payload)) = payload.split_first() else {
    // 信封记录至少含 1B 类型标签；残损载荷按类型不符收口
    output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
    return;
  };
  match custom_object_entry(*tag) {
    Some(entry) => {
      let mut obj_out = ObjectOutput::mount(output);
      custom_scan_operate(
        args,
        scan_count_limit,
        &mut obj_out,
        |start, count, pattern, is_no_value| {
          (entry.scan_members)(obj_payload, start, count, pattern, is_no_value)
        },
      );
    }
    None => output.write_resp_error(cs::RESP_ERR_WRONG_TYPE),
  }
}

pub(crate) struct MpopArgsConfig {
  pub cmd_name: &'static str,
  pub num_keys_non_blocking_err: &'static str,
  pub count_non_blocking_err: &'static str,
}

/// LMPOP / BLMPOP / ZMPOP / BZMPOP 通用参数解析单源
pub(crate) fn parse_mpop_args<'a, T>(
  parse_state: &'a [&'a [u8]],
  is_blocking: bool,
  cfg: MpopArgsConfig,
  parse_mid: impl FnOnce(&'a [u8]) -> Option<T>,
  output: &mut Vec<u8>,
) -> Option<(&'a [&'a [u8]], T, i32)> {
  let base = usize::from(is_blocking);
  check_arg_count!(parse_state, base + 3.., output, cfg.cmd_name, return None);

  let num_keys_err = if is_blocking {
    cs::RESP_ERR_PARAM_NUMKEYS_GREATER_THAN_ZERO
  } else {
    cfg.num_keys_non_blocking_err
  };
  let Some(num_keys) = strict_i32(parse_state[base]).filter(|&v| v >= 1) else {
    cs::abort_with_error_message(output, num_keys_err);
    return None;
  };

  let n = base + 1 + num_keys as usize;
  if parse_state.len() != n + 1 && parse_state.len() != n + 3 {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  }

  let keys = &parse_state[base + 1..n];
  let mid = match parse_state.get(n).copied().and_then(parse_mid) {
    Some(v) => v,
    None => {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return None;
    }
  };

  let mut count = 1_i32;
  if parse_state.len() == n + 3 {
    if !parse_state[n + 1].eq_ignore_ascii_case(cs::COUNT) {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return None;
    }
    let count_err = if is_blocking {
      cs::RESP_ERR_PARAM_COUNT_GREATER_THAN_ZERO
    } else {
      cfg.count_non_blocking_err
    };
    let Some(v) = strict_i32(parse_state[n + 2]).filter(|&v| v >= 1) else {
      cs::abort_with_error_message(output, count_err);
      return None;
    };
    count = v;
  }
  Some((keys, mid, count))
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
    operate: impl FnOnce(u8, &[&[u8]], i32, &mut ObjectOutput<'_>),
  ) -> bool {
    match scan_validate(object_type, parse_state) {
      ScanOutcome::WrongNumArgs { cmd_name } => {
        self.abort_with_wrong_number_of_arguments(cmd_name, output)
      }
      ScanOutcome::InvalidCursor => {
        self.abort_with_error_message(cs::RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes(), output)
      }
      ScanOutcome::Ready { sub_id, args } => {
        // 扫描负载直写会话输出尾段（构造 → 消费区间无旁路写入）
        let mut obj_out = ObjectOutput::mount(output);
        operate(sub_id, args, scan_count_limit, &mut obj_out);
        true
      }
    }
  }

  /// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
  ///
  /// HSCAN 分派承接（C# RespServerSession.cs switch 中 HSCAN => ObjectScan
  /// 的 Hash 形态存储接线：装载哈希信封后经 [`Self::object_scan`] 求值）
  pub fn network_hscan<'a, D: Device>(
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
  pub fn network_sscan<'a, D: Device>(
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
  pub fn network_zscan<'a, D: Device>(
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
  ///
  /// 域收口（C# 原域即自定义对象扫描，CUSTOMOBJECTSCAN 为其别名）：All 哨兵
  /// 仅 `CustomObjectBase.Operate` 接受转对象 Scan；内置 Hash/Set/SortedSet
  /// 严格类型检查对 All 一律 WrongType（HashObject.cs:225-231 / SetObject.cs:126-135）。
  /// String 域命中 → WRONGTYPE；信封域命中按内层标签走扩展标签域求值
  ///（[`coscan_eval`]）；升阶键（Meta 域，仅内置三型可产生）已在
  /// [`UserRead`] 折叠层归对象键口径，信封确认缺失即 WRONGTYPE；两域皆缺 →
  /// `[0, 空数组]`（C# NOTFOUND）
  pub fn network_coscan<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    use crate::storage::session::common::{TagRead, UserRead, read_envelope_sync, read_user_sync};
    // C# All 臂同走 cmdName switch、参数计数门、光标非负门
    //（SharedObjectCommands.cs:22-44），校验单源 scan_validate，先于域判定
    let args = match scan_validate(GarnetObjectType::All, parse_state) {
      ScanOutcome::WrongNumArgs { cmd_name } => {
        return Ok(self.abort_with_wrong_number_of_arguments(cmd_name, output));
      }
      ScanOutcome::InvalidCursor => {
        return Ok(
          self.abort_with_error_message(cs::RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes(), output),
        );
      }
      ScanOutcome::Ready { args, .. } => args,
    };
    let key = parse_state[0];
    let scan_count_limit = self
      .runtime_config()
      .get_int(ServerConfigType::ObjectScanCountLimit);
    // 三域折叠判定（String / ObjectEnvelope / Meta 已在 UserRead 单点收口）；
    // 信封命中在闭包内直喂扩展域求值（载荷零拷贝借用通道）
    let user = read_user_sync(store, key, None, |_| ());
    match user {
      Ok(UserRead::Hit(())) => output.write_resp_error(cs::RESP_ERR_WRONG_TYPE),
      Ok(UserRead::WrongType) => {
        let envelope = read_envelope_sync(store, key, |payload| {
          coscan_eval(payload, args, scan_count_limit, output);
        });
        match envelope {
          // 升阶键（Meta 域命中折叠为对象键口径）：信封确认缺失即 WRONGTYPE
          Ok(TagRead::Missing) => output.write_resp_error(cs::RESP_ERR_WRONG_TYPE),
          // 信封命中：应答帧已在闭包内由 coscan_eval 写毕
          Ok(TagRead::Hit(())) => {}
          Ok(TagRead::Deferred) => return Ok(false),
          Err(_) => output.write_resp_error(cs::RESP_ERR_GENERIC),
        }
      }
      Ok(UserRead::Missing) => write_scan_not_found(output),
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => output.write_resp_error(cs::RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// 对象扫描的存储接线公共体：校验 → 装载 → scan operate → 负载
  ///
  /// C# ObjectScan 经 storageApi.ObjectScan 在存储层装载求值；rust 同步
  /// 执行域在命令层装载（信封解码与 storage 会话域同一格式），键缺失回
  /// `[0, 空数组]`（C# NOTFOUND 分支），磁盘候选降级 `Ok(false)`；
  /// COUNT 钳制上限取 runtimeConfig OBJECT_SCAN_COUNT_LIMIT
  /// （C# storeWrapper.runtimeConfig.GetInt，CONFIG SET 热更即时生效）
  fn scan_object_typed<T, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    object_type: GarnetObjectType,
    load: impl FnOnce(&wkv::BatchStoreSession<'_, D>, &[u8], &mut Vec<u8>) -> ObjLoad<T>,
    operate: impl FnOnce(&mut T, u8, &[&[u8]], i32, &mut ObjectOutput<'_>),
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
        write_scan_not_found(output);
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
  use wdev::Device;
  use wresp::{cmd_strings as cs, ext::RespVecExt};
  use wval::GarnetObjectType;

  use super::{coscan_eval, write_scan_not_found};
  use crate::{
    resp::objects::object_store_utils::{GarnetObjectPayload, obj_load_typed},
    storage::session::storage_session::StorageSession,
  };

  /// HSCAN / SSCAN / ZSCAN 慢路径承接（COSCAN 域收口后走 [`coscan`]
  /// 自有扩展域求值，不再转调本入口）
  pub(crate) async fn object_scan(
    storage: &StorageSession<'_, impl Device>,
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
        let loaded = obj_load_typed(storage, key, object_type, output, <$ty>::from_blob)
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
              storage.resp_version,
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
            write_scan_not_found(output);
            return Ok(());
          }
          wcol::ObjLoad::Present(mut obj) => {
            obj.operate(
              sub_id,
              args,
              0,
              scan_count_limit,
              &mut ObjectOutput::mount(output),
              resp_version,
            );
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

  /// COSCAN 慢路径承接：三域异步判定后按扩展标签域收口（与同步臂
  /// [`super::coscan_eval`] 单源同口径）
  ///
  /// String 域命中 → WRONGTYPE（COSCAN 原域仅自定义对象）；信封域命中在
  /// 闭包内直喂扩展域求值；信封缺失探分层元记录域——升阶键仅内置三型可
  /// 产生，按新域直接 WRONGTYPE；两域皆缺 → `[0, 空数组]`（C# NOTFOUND）。
  /// `Err(())` 为存储 IO 失败，由 exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE
  pub(crate) async fn coscan(
    storage: &StorageSession<'_, impl Device>,
    parse_state: &[&[u8]],
    scan_count_limit: i32,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    use wval::KeyTag;

    // C# All 臂同走 cmdName switch、参数计数门、光标非负门
    //（SharedObjectCommands.cs:22-44），校验单源 scan_validate，
    // 先于任何域判定存储 IO
    let args = match super::scan_validate(GarnetObjectType::All, parse_state) {
      super::ScanOutcome::WrongNumArgs { cmd_name } => {
        cs::abort_with_wrong_number_of_arguments(output, cmd_name);
        return Ok(());
      }
      super::ScanOutcome::InvalidCursor => {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_INVALIDCURSOR);
        return Ok(());
      }
      super::ScanOutcome::Ready { args, .. } => args,
    };
    let key = parse_state[0];
    // String 域命中即用户字符串键 → WRONGTYPE
    if matches!(
      storage.read_tag_with(key, KeyTag::String, |_| ()).await,
      Ok(Some(()))
    ) {
      output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
      return Ok(());
    }
    // 信封域命中按内层标签走扩展域求值；缺失探分层元记录域（升阶键
    // WRONGTYPE）；两域皆缺 → [0, 空数组]
    let envelope = storage
      .read_tag_with(key, KeyTag::ObjectEnvelope, |payload| {
        coscan_eval(payload, args, scan_count_limit, output);
      })
      .await;
    match envelope {
      // 信封命中：应答帧已在闭包内由 coscan_eval 写毕
      Ok(Some(())) => {}
      Ok(None) => match storage.read_tag_with(key, KeyTag::Meta, |_| ()).await {
        // 升阶键（仅内置三型可产生）：按新域直接 WRONGTYPE
        Ok(Some(())) => output.write_resp_error(cs::RESP_ERR_WRONG_TYPE),
        Ok(None) => write_scan_not_found(output),
        Err(_) => return Err(()),
      },
      Err(_) => return Err(()),
    }
    Ok(())
  }
}
