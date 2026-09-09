//! 有序集合 RESP 命令（对标 libs/server/Resp/Objects/SortedSetCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`crate::objects::sortedset::sorted_set_object::SortedSetObject`] 的
//! operate/ObjectInput 通道（与 C# GarnetObjectBase.Operate 分层一致），
//! 存取经与 storage 会话域共享的 `[类型标签][载荷]` 信封
//! （见 [`crate::resp::objects::object_store_utils`]），载荷为 wobject
//! bitcode `(member, score)`；携带成员级过期时落 C# BinaryWriter 线格式。

use std::{collections::HashMap, io::Cursor, str};

use wobject::sorted_set::sorted_set_object::{
  SortedSetEntry as WoSortedSetEntry, SortedSetObject as WoSortedSetObject,
};

use crate::{
  arg_slice::ArgSlice,
  input_header::RespInputHeader,
  inputs::ObjectInput,
  objects::{
    parse_utils::{now_ticks, try_get_expire_option},
    sortedset::sorted_set_object::{
      ExpirationWithOption, ExpireOption, SortedSetEntry, SortedSetObject, SortedSetOperation,
      SortedSetRangeOpts,
    },
    types::object_output::ObjectOutput,
  },
  resp::{
    cmd_strings as cs,
    cmd_strings::write_error_raw,
    objects::object_store_utils::{
      OBJ_TAG_SORTED_SET, SyncObj, obj_load_sync, obj_save_or_gc_sync,
    },
    parser::resp_ext::{RespSliceExt, RespVecExt},
    resp_server_session::RespServerSession,
  },
  session_parse_state::SessionParseState,
  storage::session::objectstore::sorted_set_ops::ZSetAggregate,
  types::{GarnetObjectType, RespInputFlags},
};

/// 本命令面统一按 RESP2 协议输出（C# respProtocolVersion 由会话下发，
/// 会话层接线时替换为实际协商版本）
const RESP_VERSION: u8 = 2;

/// 从 wkv 信封载荷装载有序集合对象
///
/// 载荷双格式：默认 wobject bitcode（与 storage 会话域兼容）；携带成员级过期时
/// 落 C# BinaryWriter 线格式（bitcode 无过期槽位），读取先试 bitcode 再回退
pub(crate) fn zset_from_blob(raw: &[u8]) -> SortedSetObject {
  if let Ok(wo) = WoSortedSetObject::deserialize(&mut Cursor::new(raw)) {
    let pin = wo.dict.pin();
    let entries: Vec<(Vec<u8>, f64)> = pin.iter().map(|(k, v)| (k.clone(), *v)).collect();
    return SortedSetObject::from_entries(entries);
  }
  // C# 线格式回退（成员级过期）
  SortedSetObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化回 wkv 信封载荷（带成员过期时用 C# 线格式承载）
pub(crate) fn zset_to_blob(obj: &SortedSetObject) -> Vec<u8> {
  if obj.has_expirable_items() {
    let mut csharp_format = Vec::new();
    let _ = obj.clone().serialize(&mut csharp_format);
    return csharp_format;
  }

  let wo = WoSortedSetObject::new();
  {
    let pin = wo.dict.pin();
    let mut tree = wo.tree.lock();
    for (member, score) in obj.to_entries() {
      pin.insert(member.clone(), score);
      tree.insert(WoSortedSetEntry { score, member });
    }
  }
  let mut out = Vec::new();
  if wo.serialize(&mut out).is_err() {
    out = bitcode_encode_fallback(obj);
  }
  out
}

/// 序列化兜底（理论上不可达：entries 均为合法 (Vec<u8>, f64)）
fn bitcode_encode_fallback(obj: &SortedSetObject) -> Vec<u8> {
  let mut out = Vec::new();
  for (m, s) in obj.to_entries() {
    out.extend_from_slice(&(m.len() as u32).to_le_bytes());
    out.extend_from_slice(&m);
    out.extend_from_slice(&s.to_le_bytes());
  }
  out
}

/// GEO 命令域复用的 ObjectInput 构造入口
pub(crate) fn make_input_for_geo(
  op: SortedSetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> (ObjectInput, Vec<Vec<u8>>) {
  make_input(op, args, arg1, arg2)
}

/// 构造 ObjectInput（backing 与 input 同生命周期存活）
fn make_input(
  op: SortedSetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> (ObjectInput, Vec<Vec<u8>>) {
  let backing: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  let slices: Vec<ArgSlice> = backing
    .iter()
    .map(|b| ArgSlice::new(b.as_ptr(), b.len()))
    .collect();
  let mut parse_state = SessionParseState::new();
  parse_state.initialize_with_args(&slices);

  let mut header =
    RespInputHeader::new_with_type(GarnetObjectType::SortedSet, RespInputFlags::empty());
  header.set_sub_id(op as u8);
  (
    ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
    backing,
  )
}

/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate(
  obj: &mut SortedSetObject,
  op: SortedSetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> ObjectOutput {
  let (input, _backing) = make_input(op, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, RESP_VERSION);
  obj_out
}

/// zset 键同步装载结果
pub(crate) enum ZsetLoad {
  /// 磁盘候选：命令须降级异步重放（未写任何输出）
  Degrade,
  /// WrongType / 存储错误（错误行已写入输出）
  Error,
  /// 键缺失（可按空对象求值，但不得落库创建）
  Missing,
  /// 命中（信封载荷已解码）
  Present(SortedSetObject),
}

/// 同步装载有序集合（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
pub(crate) fn zset_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> ZsetLoad {
  match obj_load_sync(store, key, OBJ_TAG_SORTED_SET) {
    Ok(None) => ZsetLoad::Degrade,
    Ok(Some(SyncObj::Missing)) => ZsetLoad::Missing,
    Ok(Some(SyncObj::WrongType)) => {
      write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
      ZsetLoad::Error
    }
    Ok(Some(SyncObj::Present(p))) => ZsetLoad::Present(zset_from_blob(&p)),
    Err(_) => {
      output.write_resp_error("generic error");
      ZsetLoad::Error
    }
  }
}

/// 变更回写：空集合整键回收（对齐 storage 层 finalize_removal 与 set 命令域收尾）
///
/// 返回 `Ok(false)` 表示磁盘侧须降级异步重放；`Err(())` 为存储层错误（由调用方写错误行）
pub(crate) fn zset_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &SortedSetObject,
) -> Result<bool, ()> {
  let payload = zset_to_blob(obj);
  obj_save_or_gc_sync(
    store,
    key,
    OBJ_TAG_SORTED_SET,
    &payload,
    obj.sorted_set_dict.is_empty(),
  )
  .map_err(|_| ())
}

/// rmw 结果
enum Rmw {
  /// 磁盘候选降级（未写任何输出）
  Degrade,
  /// 错误行已写出，调用方不得追加回复
  Error,
  /// 已闭环：RESP 负载已随 rmw 写出；payload_written=false 时 result1 供调用方回执
  Done { result1: i64, payload_written: bool },
}

/// 读-改-写骨架：装载 → operate → 变更回写 → 负载输出
fn rmw(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  op: SortedSetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  output: &mut Vec<u8>,
) -> Rmw {
  let (mut obj, existed) = match zset_load_sync(store, key, output) {
    ZsetLoad::Degrade => return Rmw::Degrade,
    ZsetLoad::Error => return Rmw::Error,
    ZsetLoad::Missing => (SortedSetObject::new(), false),
    ZsetLoad::Present(o) => (o, true),
  };

  let obj_out = run_operate(&mut obj, op, args, arg1, arg2);
  let result1 = obj_out.result1;

  // 回写须先于回复输出：降级时保持输出零污染，交由异步重放整体重写
  if should_write_back(op, &obj_out, &obj, existed) {
    match zset_save_or_gc(store, key, &obj) {
      Ok(true) => {}
      Ok(false) => return Rmw::Degrade,
      Err(()) => {
        output.write_resp_error("generic error");
        return Rmw::Error;
      }
    }
  }
  output.extend_from_slice(&obj_out.payload);

  Rmw::Done {
    result1,
    payload_written: !obj_out.payload.is_empty(),
  }
}

/// rmw 回写判定
///
/// - 只读操作不落库；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的操作（ZREM/ZREMRANGEBYLEX）以移除计数为准。
fn should_write_back(
  op: SortedSetOperation,
  out: &ObjectOutput,
  obj: &SortedSetObject,
  existed: bool,
) -> bool {
  if is_read_only(op)
    || out.has_wrong_type()
    || out.payload.first() == Some(&b'-')
    || (!existed && obj.sorted_set_dict.is_empty())
  {
    return false;
  }
  match op {
    SortedSetOperation::Zrem | SortedSetOperation::Zremrangebylex => {
      out.result1 > 0 && out.result1 != i32::MAX as i64
    }
    _ => !out.payload.is_empty(),
  }
}

/// 只读操作（rmw 不落库）
fn is_read_only(op: SortedSetOperation) -> bool {
  matches!(
    op,
    SortedSetOperation::Zcard
      | SortedSetOperation::Zscore
      | SortedSetOperation::Zmscore
      | SortedSetOperation::Zcount
      | SortedSetOperation::Zrange
      | SortedSetOperation::Zrank
      | SortedSetOperation::Zrevrank
      | SortedSetOperation::Zlexcount
      | SortedSetOperation::Zrandmember
      | SortedSetOperation::Zttl
      | SortedSetOperation::Zscan
  )
}

impl RespServerSession {
  /// ZADD key [NX|XX|GT|LT|CH|INCR] score member [score member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd
  pub fn sorted_set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZADD' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(
      store,
      key,
      SortedSetOperation::Zadd,
      &parse_state[1..],
      0,
      0,
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZSCORE key member
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetScore
  pub fn sorted_set_score<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZSCORE' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(
      store,
      key,
      SortedSetOperation::Zscore,
      &parse_state[1..],
      0,
      0,
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZREM key member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRemove
  pub fn sorted_set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZREM' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(
      store,
      key,
      SortedSetOperation::Zrem,
      &parse_state[1..],
      0,
      0,
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::Error => {}
      // C# SortedSetRemove 仅回填 result1，整数回复由 RESP 层写出
      Rmw::Done {
        result1,
        payload_written,
      } => {
        if !payload_written {
          output.write_resp_int(result1);
        }
      }
    }
    Ok(true)
  }

  /// ZCARD key
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetLength
  pub fn sorted_set_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZCARD' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(store, key, SortedSetOperation::Zcard, &[], 0, 0, output) {
      Rmw::Degrade => return Ok(false),
      Rmw::Error => {}
      // C# SortedSetLength 仅回填 result1
      Rmw::Done {
        result1,
        payload_written,
      } => {
        if !payload_written {
          output.write_resp_int(result1);
        }
      }
    }
    Ok(true)
  }

  /// ZPOPMIN / ZPOPMAX key [count]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetPop
  pub fn sorted_set_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_min: bool,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() || parse_state.len() > 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    let op = if is_min {
      SortedSetOperation::Zpopmin
    } else {
      SortedSetOperation::Zpopmax
    };

    // count 缺省形态传 -1（无外层数组头）
    let arg1: i32 = match parse_state.get(1) {
      None => -1,
      Some(c) => match str::from_utf8(c).unwrap_or("").parse::<i32>() {
        Ok(v) if v >= 0 => v,
        _ => {
          output.extend_from_slice(b"-ERR value is out of range, must be >= 0\r\n");
          return Ok(true);
        }
      },
    };

    match rmw(store, key, op, &[], arg1, 0, output) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZRANGE / ZREVRANGE / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGEBYLEX
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRange
  pub fn sorted_set_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    range_opts: SortedSetRangeOpts,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZRANGE' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        output.extend_from_slice(b"*0\r\n");
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zrange,
      &parse_state[1..],
      0,
      range_opts.bits() as i32,
    );
    output.extend_from_slice(&obj_out.payload);
    Ok(true)
  }

  /// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRangeStore
  pub fn sorted_set_range_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 4 || parse_state.len() > 9 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZRANGESTORE' command\r\n");
      return Ok(true);
    }

    let dst_key = parse_state[0];
    let src_key = parse_state[1];

    // 源集合范围读取（Store 选项：强制 WITHSCORES + RESP2 成对负载）
    let mut src_obj = match zset_load_sync(store, src_key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        // 源缺失：目标回收（空结果删目标键，对齐 Redis ZRANGESTORE 语义）
        match zset_save_or_gc(store, dst_key, &SortedSetObject::new()) {
          Ok(true) => output.extend_from_slice(b":0\r\n"),
          Ok(false) => return Ok(false),
          Err(()) => output.write_resp_error("generic error"),
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut src_obj,
      SortedSetOperation::Zrange,
      &parse_state[2..],
      0,
      SortedSetRangeOpts::STORE.bits() as i32,
    );

    // result1 = -1 表示范围参数被拒（错误已写入负载）
    if obj_out.result1 == -1 {
      output.extend_from_slice(&obj_out.payload);
      return Ok(true);
    }

    // 成对负载 → 目标集合（ProcessRespArrayOutputAsPairs 语义）
    let pairs = parse_pairs_payload(&obj_out.payload);
    let mut dst = SortedSetObject::new();
    for (member, score) in pairs {
      dst.sorted_set_dict.insert(member.clone(), score);
      dst.sorted_set.insert(SortedSetEntry { score, member });
    }

    match zset_save_or_gc(store, dst_key, &dst) {
      Ok(true) => {
        let mut buf = itoa::Buffer::new();
        output.extend_from_slice(b":");
        output.extend_from_slice(buf.format(dst.sorted_set_dict.len()).as_bytes());
        output.extend_from_slice(b"\r\n");
      }
      Ok(false) => return Ok(false),
      Err(()) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// ZMSCORE key member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetScores
  pub fn sorted_set_scores<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZMSCORE' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        // 键缺失：全 null 数组
        output.write_resp_array_len(parse_state.len() - 1);
        for _ in 1..parse_state.len() {
          output.write_resp_null();
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zmscore,
      &parse_state[1..],
      0,
      0,
    );
    output.extend_from_slice(&obj_out.payload);
    Ok(true)
  }

  /// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetMPop
  pub fn sorted_set_m_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZMPOP' command\r\n");
      return Ok(true);
    }

    let Some(num_keys) = parse_state[0].try_parse_i64() else {
      output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
      return Ok(true);
    };
    if num_keys < 1 || parse_state.len() < num_keys as usize + 2 {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return Ok(true);
    }

    let order_arg = parse_state[num_keys as usize + 1];
    let low_scores_first = if order_arg.eq_ignore_ascii_case(b"MIN") {
      true
    } else if order_arg.eq_ignore_ascii_case(b"MAX") {
      false
    } else {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return Ok(true);
    };

    let mut count = 1_i64;
    if parse_state.len() > num_keys as usize + 2 {
      if parse_state.len() != num_keys as usize + 4
        || !parse_state[num_keys as usize + 2].eq_ignore_ascii_case(b"COUNT")
      {
        output.extend_from_slice(b"-ERR syntax error\r\n");
        return Ok(true);
      }
      match parse_state[num_keys as usize + 3].try_parse_i64() {
        Some(v) if v >= 1 => count = v,
        _ => {
          output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
          return Ok(true);
        }
      }
    }

    // 逐键尝试弹出第一个非空集合
    for key in &parse_state[1..=num_keys as usize] {
      let mut obj = match zset_load_sync(store, key, output) {
        ZsetLoad::Degrade => return Ok(false),
        ZsetLoad::Error => return Ok(true),
        ZsetLoad::Missing => continue,
        ZsetLoad::Present(o) => o,
      };
      if obj.count() == 0 {
        continue;
      }

      let popped: Vec<(f64, Vec<u8>)> = (0..count)
        .filter_map(|_| obj.pop_min_or_max(!low_scores_first))
        .collect();
      match zset_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(()) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }

      // 回复：[key, [[member, score], ...]]
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_array_len(popped.len());
      for (score, member) in &popped {
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(member);
        let s = ObjectOutput::format_double(*score);
        output.write_resp_bulk_string(s.as_bytes());
      }
      return Ok(true);
    }

    output.write_resp_null();
    Ok(true)
  }

  /// ZCOUNT key min max
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetCount
  pub fn sorted_set_count<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZCOUNT' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(
      store,
      key,
      SortedSetOperation::Zcount,
      &parse_state[1..],
      0,
      0,
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZLEXCOUNT key min max
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetLengthByValue
  pub fn sorted_set_length_by_value<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZLEXCOUNT' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        output.extend_from_slice(b":0\r\n");
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zlexcount,
      &parse_state[1..],
      0,
      0,
    );
    // 解析失败标记（int.MaxValue）→ 错误回复；否则以 result1 作整数回复
    // （C# SortedSetRemoveOrCountRangeByLex 仅回填 result1，RESP 层负责写整数）
    if obj_out.result1 == i32::MAX as i64 {
      output.clear();
      output.extend_from_slice(b"-ERR min or max not valid string range item\r\n");
    } else if obj_out.result1 != i32::MIN as i64 {
      output.clear();
      output.write_resp_int(obj_out.result1);
    }
    Ok(true)
  }

  /// ZINCRBY key increment member
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIncrement
  pub fn sorted_set_increment<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZINCRBY' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(
      store,
      key,
      SortedSetOperation::Zincrby,
      &parse_state[1..],
      0,
      0,
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZRANK / ZREVRANK key member [WITHSCORE]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRank
  pub fn sorted_set_rank<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    ascending: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZRANK' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    let with_score = parse_state.len() > 2 && parse_state[2].eq_ignore_ascii_case(b"WITHSCORE");

    let op = if ascending {
      SortedSetOperation::Zrank
    } else {
      SortedSetOperation::Zrevrank
    };

    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        output.write_resp_null();
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      op,
      &parse_state[1..2],
      if with_score { 1 } else { 0 },
      0,
    );
    output.extend_from_slice(&obj_out.payload);
    Ok(true)
  }

  /// ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREMRANGEBYLEX
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRemoveRange
  pub fn sorted_set_remove_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    range_kind: RemoveRangeKind,
  ) -> wresp::Result<bool> {
    let (name, op) = match range_kind {
      RemoveRangeKind::Rank => ("ZREMRANGEBYRANK", SortedSetOperation::Zremrangebyrank),
      RemoveRangeKind::Score => ("ZREMRANGEBYSCORE", SortedSetOperation::Zremrangebyscore),
      RemoveRangeKind::Lex => ("ZREMRANGEBYLEX", SortedSetOperation::Zremrangebylex),
    };
    if parse_state.len() != 3 {
      output.extend_from_slice(
        format!("-ERR wrong number of arguments for '{name}' command\r\n").as_bytes(),
      );
      return Ok(true);
    }

    let key = parse_state[0];
    let payload_start = output.len();
    match rmw(store, key, op, &parse_state[1..], 0, 0, output) {
      Rmw::Degrade => return Ok(false),
      Rmw::Error => return Ok(true),
      // ZREMRANGEBYLEX 仅回填 result1（int.MaxValue=参数错误、int.MinValue=部分执行）
      Rmw::Done {
        result1,
        payload_written,
      } => {
        if range_kind == RemoveRangeKind::Lex && !payload_written {
          if result1 == i32::MAX as i64 {
            output.truncate(payload_start);
            output.extend_from_slice(b"-ERR min or max not valid string range item\r\n");
          } else if output.len() == payload_start && result1 != i32::MIN as i64 {
            output.write_resp_int(result1);
          }
        }
      }
    }
    Ok(true)
  }

  /// ZRANDMEMBER key [count [WITHSCORES]]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRandomMember
  pub fn sorted_set_random_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() || parse_state.len() > 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZRANDMEMBER' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        if parse_state.len() > 1 {
          output.extend_from_slice(b"*0\r\n");
        } else {
          output.write_resp_null();
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    // 参数打包：arg1 = (count << 2) | (includedCount << 1) | withScores
    let mut param_count = 0_i64;
    let mut included_count = false;
    let mut with_scores = false;

    if let Some(c) = parse_state.get(1) {
      let Some(v) = c.try_parse_i64() else {
        output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
        return Ok(true);
      };
      param_count = v.min(i32::MAX as i64 >> 2);
      included_count = true;

      if let Some(ws) = parse_state.get(2) {
        if !ws.eq_ignore_ascii_case(b"WITHSCORES") {
          output.extend_from_slice(b"-ERR syntax error\r\n");
          return Ok(true);
        }
        with_scores = true;
      }
    }

    let arg1 = (((param_count << 1) | included_count as i64) << 1) | with_scores as i64;
    // count = 0 不触达后端（对齐 C#）
    if param_count == 0 {
      output.extend_from_slice(b"*0\r\n");
      return Ok(true);
    }

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zrandmember,
      &[],
      arg1 as i32,
      fastrand::i32(..),
    );
    output.extend_from_slice(&obj_out.payload);
    Ok(true)
  }

  /// ZDIFF numkeys key [key ...] [WITHSCORES]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetDifference
  pub fn sorted_set_difference<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let (keys, with_scores) = match parse_diff_args(parse_state, "ZDIFF", output) {
      Some(v) => v,
      None => return Ok(true),
    };

    // 第一集合 − 其余集合（CopyDiff 逐个收缩）
    let objs = match load_many(store, &keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let mut result: Option<SortedSetObject> = None;
    for (i, obj) in objs.iter().enumerate() {
      if i == 0 {
        result = Some(obj.clone());
      } else if let Some(current) = result.take() {
        result = Some(dict_to_zset(SortedSetObject::copy_diff(
          Some(&current),
          Some(obj),
        )));
      }
    }

    write_zset_entries(result.as_ref(), with_scores, output);
    Ok(true)
  }

  /// ZDIFFSTORE dst numkeys key [key ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetDifferenceStore
  pub fn sorted_set_difference_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZDIFFSTORE' command\r\n");
      return Ok(true);
    }

    let dst = parse_state[0];
    let Some((keys, _)) = parse_diff_args(&parse_state[1..], "ZDIFFSTORE", output) else {
      return Ok(true);
    };

    let objs = match load_many(store, &keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let result = diff_sets(&objs);
    let count = result.count();
    match zset_save_or_gc(store, dst, &result) {
      Ok(true) => output.write_resp_int(count as i64),
      Ok(false) => return Ok(false),
      Err(()) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// ZINTER numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersect
  pub fn sorted_set_intersect<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(args) = parse_combine_args(parse_state, "ZINTER", output, false) else {
      return Ok(true);
    };

    let objs = match load_many(store, &args.keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let result = combine_sets(&objs, &args.weights, args.aggregate, CombineKind::Intersect);
    write_zset_entries(Some(&result), args.with_scores, output);
    Ok(true)
  }

  /// ZINTERCARD numkeys key [key ...] [LIMIT limit]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersectLength
  pub fn sorted_set_intersect_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZINTERCARD' command\r\n");
      return Ok(true);
    }

    let Some(num_keys) = parse_state[0].try_parse_i64() else {
      output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
      return Ok(true);
    };
    if num_keys < 1 {
      output.extend_from_slice(b"-ERR numkeys should be greater than 0\r\n");
      return Ok(true);
    }

    let mut limit = 0_i64;
    let idx = num_keys as usize + 1;
    if parse_state.len() == idx + 2 {
      if !parse_state[idx].eq_ignore_ascii_case(b"LIMIT") {
        output.extend_from_slice(b"-ERR syntax error\r\n");
        return Ok(true);
      }
      match parse_state[idx + 1].try_parse_i64() {
        Some(v) if v >= 0 => limit = v,
        _ => {
          output.extend_from_slice(b"-ERR limit is negative\r\n");
          return Ok(true);
        }
      }
    } else if parse_state.len() != idx {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return Ok(true);
    }

    let keys = &parse_state[1..=num_keys as usize];
    let weights = vec![1.0; keys.len()];
    let objs = match load_many(store, keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let result = combine_sets(&objs, &weights, ZSetAggregate::Sum, CombineKind::Intersect);

    let card = result.count() as i64;
    output.write_resp_int(if limit > 0 { card.min(limit) } else { card });
    Ok(true)
  }

  /// ZINTERSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersectStore
  pub fn sorted_set_intersect_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    sorted_set_combine_store(self, parse_state, store, output, CombineKind::Intersect)
  }

  /// ZUNION numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetUnion
  pub fn sorted_set_union<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(args) = parse_combine_args(parse_state, "ZUNION", output, false) else {
      return Ok(true);
    };

    let objs = match load_many(store, &args.keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let result = combine_sets(&objs, &args.weights, args.aggregate, CombineKind::Union);
    write_zset_entries(Some(&result), args.with_scores, output);
    Ok(true)
  }

  /// ZUNIONSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetUnionStore
  pub fn sorted_set_union_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    sorted_set_combine_store(self, parse_state, store, output, CombineKind::Union)
  }

  /// BZPOPMIN / BZPOPMAX key [key ...] timeout
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetBlockingPop
  ///
  /// 刻意差异：本层仅实现"立即可取"路径（等价 timeout=0 立即返回）；
  /// 真正的阻塞等待由 CollectionItemBroker 承担（见 objects/itembroker 与汇报接线项）
  pub fn sorted_set_blocking_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_min: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'BZPOPMIN' command\r\n");
      return Ok(true);
    }
    if str::from_utf8(parse_state[parse_state.len() - 1])
      .unwrap_or("")
      .parse::<f64>()
      .is_err()
    {
      output.extend_from_slice(b"-ERR timeout is not a float or out of range\r\n");
      return Ok(true);
    }

    for key in &parse_state[..parse_state.len() - 1] {
      let mut obj = match zset_load_sync(store, key, output) {
        ZsetLoad::Degrade => return Ok(false),
        ZsetLoad::Error => return Ok(true),
        ZsetLoad::Missing => continue,
        ZsetLoad::Present(o) => o,
      };
      if let Some((score, member)) = obj.pop_min_or_max(!is_min) {
        match zset_save_or_gc(store, key, &obj) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(()) => {
            output.write_resp_error("generic error");
            return Ok(true);
          }
        }
        output.write_resp_array_len(3);
        output.write_resp_bulk_string(key);
        output.write_resp_bulk_string(&member);
        let s = ObjectOutput::format_double(score);
        output.write_resp_bulk_string(s.as_bytes());
        return Ok(true);
      }
    }

    output.write_resp_null();
    Ok(true)
  }

  /// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetBlockingMPop
  ///
  /// 刻意差异：同 BZPOPMIN——仅立即可取路径，阻塞等待由 Broker 承担
  pub fn sorted_set_blocking_m_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 4 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'BZMPOP' command\r\n");
      return Ok(true);
    }

    let Some(timeout) = str::from_utf8(parse_state[0])
      .unwrap_or("")
      .parse::<f64>()
      .ok()
    else {
      output.extend_from_slice(b"-ERR timeout is not a float or out of range\r\n");
      return Ok(true);
    };
    let _ = timeout;
    let Some(num_keys) = parse_state[1].try_parse_i64() else {
      output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
      return Ok(true);
    };
    if num_keys < 1 || parse_state.len() < num_keys as usize + 3 {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return Ok(true);
    }

    let order_arg = parse_state[num_keys as usize + 2];
    let low_scores_first = if order_arg.eq_ignore_ascii_case(b"MIN") {
      true
    } else if order_arg.eq_ignore_ascii_case(b"MAX") {
      false
    } else {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return Ok(true);
    };

    let mut count = 1_i64;
    if parse_state.len() > num_keys as usize + 3 {
      if parse_state.len() != num_keys as usize + 5
        || !parse_state[num_keys as usize + 3].eq_ignore_ascii_case(b"COUNT")
      {
        output.extend_from_slice(b"-ERR syntax error\r\n");
        return Ok(true);
      }
      match parse_state[num_keys as usize + 4].try_parse_i64() {
        Some(v) if v >= 1 => count = v,
        _ => {
          output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
          return Ok(true);
        }
      }
    }

    for key in &parse_state[2..=num_keys as usize + 1] {
      let mut obj = match zset_load_sync(store, key, output) {
        ZsetLoad::Degrade => return Ok(false),
        ZsetLoad::Error => return Ok(true),
        ZsetLoad::Missing => continue,
        ZsetLoad::Present(o) => o,
      };
      if obj.count() == 0 {
        continue;
      }

      let popped: Vec<(f64, Vec<u8>)> = (0..count)
        .filter_map(|_| obj.pop_min_or_max(!low_scores_first))
        .collect();
      match zset_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(()) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }

      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_array_len(popped.len());
      for (score, member) in &popped {
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(member);
        let s = ObjectOutput::format_double(*score);
        output.write_resp_bulk_string(s.as_bytes());
      }
      return Ok(true);
    }

    output.write_resp_null();
    Ok(true)
  }

  /// ZEXPIRE / ZEXPIREAT / ZPEXPIRE / ZPEXPIREAT key seconds [NX|XX|GT|LT] member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetExpire
  pub fn sorted_set_expire<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZEXPIRE' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    // 解析过期时长与选项词元（NX/XX/GT/LT），压缩进 ExpirationWithOption 字
    let expiration_base: i64 = match parse_state[1].try_parse_i64() {
      Some(v) => v,
      None => {
        output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
        return Ok(true);
      }
    };

    let mut curr_idx = 2;
    let mut expire_option = 0_u8;
    while curr_idx < parse_state.len() {
      let Some(opt) = try_get_expire_option(parse_state[curr_idx]) else {
        break;
      };
      expire_option |= opt.bits();
      curr_idx += 1;
    }

    // .NET Ticks 目标时刻
    const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
    let now_ticks = now_ticks();
    let expiration_ticks = if is_timestamp {
      UNIX_EPOCH_TICKS + expiration_base * if is_milliseconds { 10_000 } else { 10_000_000 }
    } else {
      now_ticks + expiration_base * if is_milliseconds { 10_000 } else { 10_000_000 }
    };

    let e = ExpirationWithOption::new(
      expiration_ticks,
      ExpireOption::from_bits_truncate(expire_option),
    );

    let args: Vec<&[u8]> = parse_state[curr_idx..].to_vec();
    match rmw(
      store,
      key,
      SortedSetOperation::Zexpire,
      &args,
      (e.word() >> 32) as i32,
      e.word() as i32,
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZTTL / ZPTTL / ZEXPIRETIME / ZPEXPIRETIME key member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetTimeToLive
  pub fn sorted_set_time_to_live<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZTTL' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(
      store,
      key,
      SortedSetOperation::Zttl,
      &parse_state[1..],
      if is_milliseconds { 1 } else { 0 },
      if is_timestamp { 1 } else { 0 },
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZPERSIST key member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetPersist
  pub fn sorted_set_persist<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZPERSIST' command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(
      store,
      key,
      SortedSetOperation::Zpersist,
      &parse_state[1..],
      0,
      0,
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }
}

/// ZREMRANGE 变体
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoveRangeKind {
  Rank,
  Score,
  Lex,
}

/// ZINTERSTORE / ZUNIONSTORE 公共入口
fn sorted_set_combine_store<'s, D: wdev::Device>(
  _session: &mut RespServerSession,
  parse_state: &[&[u8]],
  store: &wkv::BatchStoreSession<'s, D>,
  output: &mut Vec<u8>,
  kind: CombineKind,
) -> wresp::Result<bool> {
  let name = if kind == CombineKind::Intersect {
    "ZINTERSTORE"
  } else {
    "ZUNIONSTORE"
  };
  if parse_state.len() < 4 {
    output.extend_from_slice(
      format!("-ERR wrong number of arguments for '{name}' command\r\n").as_bytes(),
    );
    return Ok(true);
  }

  let dst = parse_state[0];
  let Some(args) = parse_combine_args(&parse_state[1..], name, output, true) else {
    return Ok(true);
  };

  let objs = match load_many(store, &args.keys, output) {
    Ok(Some(objs)) => objs,
    Ok(None) => return Ok(false),
    Err(()) => return Ok(true),
  };
  let result = combine_sets(&objs, &args.weights, args.aggregate, kind);
  let count = result.count();
  match zset_save_or_gc(store, dst, &result) {
    Ok(true) => output.write_resp_int(count as i64),
    Ok(false) => return Ok(false),
    Err(()) => output.write_resp_error("generic error"),
  }
  Ok(true)
}

/// 成对负载解析（ZRANGESTORE/GEOSEARCHSTORE 回读：member score member score ...）
///
/// 兼容两种形态：RESP2 扁平序列；GEOSEARCHSTORE 的每项前置 `*2` 嵌套数组头
pub(crate) fn parse_pairs_payload(payload: &[u8]) -> Vec<(Vec<u8>, f64)> {
  let mut pairs = Vec::new();
  let mut pos = 0;

  // 跳过外层数组头 *<n>

  if payload.first() == Some(&b'*')
    && let Some(line_end) = find_crlf(payload, 0)
  {
    pos = line_end + 2;
  }

  while pos < payload.len() {
    // 项间嵌套数组头跳过（*<n>\r\n）
    if payload[pos] == b'*'
      && let Some(end) = find_crlf(payload, pos)
    {
      pos = end + 2;
    }
    // $<len>\r\n<bytes>\r\n
    if pos >= payload.len() || payload[pos] != b'$' {
      break;
    }
    let Some(line_end) = find_crlf(payload, pos) else {
      break;
    };
    let Ok(len) = str::from_utf8(&payload[pos + 1..line_end])
      .unwrap_or("")
      .parse::<usize>()
    else {
      break;
    };
    let start = line_end + 2;
    let end = start + len;
    if end + 2 > payload.len() {
      break;
    }
    let member = payload[start..end].to_vec();
    pos = end + 2;

    // 第二项：bulk string 形式的分值（前置嵌套数组头同样跳过）
    if pos < payload.len()
      && payload[pos] == b'*'
      && let Some(end) = find_crlf(payload, pos)
    {
      pos = end + 2;
    }
    if pos >= payload.len() || payload[pos] != b'$' {
      break;
    }
    let Some(line_end) = find_crlf(payload, pos) else {
      break;
    };
    let Ok(len) = str::from_utf8(&payload[pos + 1..line_end])
      .unwrap_or("")
      .parse::<usize>()
    else {
      break;
    };
    let start = line_end + 2;
    let end = start + len;
    if end + 2 > payload.len() {
      break;
    }
    let score = str::from_utf8(&payload[start..end])
      .unwrap_or("")
      .parse::<f64>()
      .unwrap_or(0.0);
    pos = end + 2;

    pairs.push((member, score));
  }
  pairs
}

fn find_crlf(payload: &[u8], from: usize) -> Option<usize> {
  (from..payload.len().saturating_sub(1)).find(|&i| payload[i] == b'\r' && payload[i + 1] == b'\n')
}

/// ZDIFF 参数解析：numkeys key... [WITHSCORES]
fn parse_diff_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  output: &mut Vec<u8>,
) -> Option<(Vec<&'p [u8]>, bool)> {
  if parse_state.len() < 2 {
    output.extend_from_slice(
      format!("-ERR wrong number of arguments for '{name}' command\r\n").as_bytes(),
    );
    return None;
  }

  let Some(n_keys) = parse_state[0].try_parse_i64() else {
    output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
    return None;
  };

  if parse_state.len() - 1 != n_keys as usize && parse_state.len() - 1 != n_keys as usize + 1 {
    output.extend_from_slice(b"-ERR syntax error\r\n");
    return None;
  }

  let mut with_scores = false;
  if parse_state.len() - 1 > n_keys as usize {
    let last = parse_state[parse_state.len() - 1];
    if !last.eq_ignore_ascii_case(b"WITHSCORES") {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return None;
    }
    with_scores = true;
  }

  Some((parse_state[1..=n_keys as usize].to_vec(), with_scores))
}

/// 集合运算参数解析产物
pub struct CombineArgs<'p> {
  pub keys: Vec<&'p [u8]>,
  pub weights: Vec<f64>,
  pub aggregate: ZSetAggregate,
  pub with_scores: bool,
}

/// ZINTER/ZUNION 参数解析：numkeys key... [WEIGHTS w...] [AGGREGATE agg] [WITHSCORES]
fn parse_combine_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  output: &mut Vec<u8>,
  _store_form: bool,
) -> Option<CombineArgs<'p>> {
  if parse_state.len() < 2 {
    output.extend_from_slice(
      format!("-ERR wrong number of arguments for '{name}' command\r\n").as_bytes(),
    );
    return None;
  }

  let Some(n_keys) = parse_state[0].try_parse_i64() else {
    output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
    return None;
  };
  if n_keys < 1 || parse_state.len() < n_keys as usize + 1 {
    output.extend_from_slice(b"-ERR syntax error\r\n");
    return None;
  }

  let keys: Vec<&[u8]> = parse_state[1..=n_keys as usize].to_vec();
  let mut weights = vec![1.0_f64; keys.len()];
  let mut aggregate = ZSetAggregate::Sum;
  let mut with_scores = false;

  let mut idx = n_keys as usize + 1;
  while idx < parse_state.len() {
    let token = parse_state[idx];
    if token.eq_ignore_ascii_case(b"WEIGHTS") {
      idx += 1;
      let mut parsed = Vec::with_capacity(keys.len());
      while parsed.len() < keys.len() && idx < parse_state.len() {
        match str::from_utf8(parse_state[idx])
          .unwrap_or("")
          .parse::<f64>()
        {
          Ok(w) => parsed.push(w),
          Err(_) => {
            output.extend_from_slice(b"-ERR weight value is not a float\r\n");
            return None;
          }
        }
        idx += 1;
      }
      if parsed.len() != keys.len() {
        output.extend_from_slice(b"-ERR syntax error\r\n");
        return None;
      }
      weights = parsed;
    } else if token.eq_ignore_ascii_case(b"AGGREGATE") {
      idx += 1;
      let Some(agg) = parse_state.get(idx).and_then(|t| {
        if t.eq_ignore_ascii_case(b"SUM") {
          Some(ZSetAggregate::Sum)
        } else if t.eq_ignore_ascii_case(b"MIN") {
          Some(ZSetAggregate::Min)
        } else if t.eq_ignore_ascii_case(b"MAX") {
          Some(ZSetAggregate::Max)
        } else {
          None
        }
      }) else {
        output.extend_from_slice(b"-ERR syntax error\r\n");
        return None;
      };
      aggregate = agg;
      idx += 1;
    } else if token.eq_ignore_ascii_case(b"WITHSCORES") {
      with_scores = true;
      idx += 1;
    } else {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return None;
    }
  }

  Some(CombineArgs {
    keys,
    weights,
    aggregate,
    with_scores,
  })
}

/// 集合运算种类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineKind {
  Intersect,
  Union,
}

/// 多键装载（信封解码；缺失按空集合；WrongType 写错误行）
///
/// 返回 `Ok(None)` 表示磁盘候选须降级异步重放；`Err(())` 为错误行已写出
fn load_many(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  keys: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<Option<Vec<SortedSetObject>>, ()> {
  let mut objs = Vec::with_capacity(keys.len());
  for key in keys {
    match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(None),
      ZsetLoad::Error => return Err(()),
      ZsetLoad::Missing => objs.push(SortedSetObject::new()),
      ZsetLoad::Present(o) => objs.push(o),
    }
  }
  Ok(Some(objs))
}

/// 字典 → 有序集合（保持 (score, member) 双索引）
fn dict_to_zset(dict: HashMap<Vec<u8>, f64, gxhash::GxBuildHasher>) -> SortedSetObject {
  let entries: Vec<(Vec<u8>, f64)> = dict.iter().map(|(k, v)| (k.clone(), *v)).collect();
  SortedSetObject::from_entries(entries)
}

/// ZDIFF 语义计算
fn diff_sets(objs: &[SortedSetObject]) -> SortedSetObject {
  let mut result: Option<SortedSetObject> = None;
  for (i, obj) in objs.iter().enumerate() {
    if i == 0 {
      result = Some(obj.clone());
    } else if let Some(current) = result.take() {
      result = Some(dict_to_zset(SortedSetObject::copy_diff(
        Some(&current),
        Some(obj),
      )));
    }
  }
  result.unwrap_or_default()
}

/// ZINTER/ZUNION 语义计算（权重 + 聚合）
fn combine_sets(
  objs: &[SortedSetObject],
  weights: &[f64],
  aggregate: ZSetAggregate,
  kind: CombineKind,
) -> SortedSetObject {
  let mut combined: HashMap<Vec<u8>, f64, gxhash::GxBuildHasher> =
    HashMap::with_hasher(gxhash::GxBuildHasher::default());
  for (i, obj) in objs.iter().enumerate() {
    let weight = weights.get(i).copied().unwrap_or(1.0);
    for (member, score) in obj.to_entries() {
      let weighted = score * weight;
      combined
        .entry(member)
        .and_modify(|existing| {
          *existing = match aggregate {
            ZSetAggregate::Sum => *existing + weighted,
            ZSetAggregate::Min => (*existing).min(weighted),
            ZSetAggregate::Max => (*existing).max(weighted),
          };
        })
        .or_insert(weighted);
    }
  }

  if kind == CombineKind::Intersect {
    // 只保留出现在全部集合中的成员（字典键天然去重，直接计数）
    let mut counts: HashMap<Vec<u8>, usize, gxhash::GxBuildHasher> =
      HashMap::with_hasher(gxhash::GxBuildHasher::default());
    for obj in objs {
      for m in obj.sorted_set_dict.keys() {
        *counts.entry(m.clone()).or_insert(0) += 1;
      }
    }
    combined.retain(|m, _| counts.get(m).copied().unwrap_or(0) == objs.len());
  }

  dict_to_zset(combined)
}

/// 结果集合的 RESP 输出（WITHSCORES 双协议形态，RESP2 扁平）
fn write_zset_entries(obj: Option<&SortedSetObject>, with_scores: bool, output: &mut Vec<u8>) {
  let Some(obj) = obj else {
    output.extend_from_slice(b"*0\r\n");
    return;
  };

  let entries: Vec<(f64, Vec<u8>)> = obj
    .sorted_set
    .iter()
    .map(|e| (e.score, e.member.clone()))
    .collect();

  let n = entries.len();
  output.write_resp_array_len(if with_scores { n * 2 } else { n });
  for (score, member) in entries {
    output.write_resp_bulk_string(&member);
    if with_scores {
      let s = ObjectOutput::format_double(score);
      output.write_resp_bulk_string(s.as_bytes());
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use tempfile::{TempDir, tempdir};
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::*;

  type TestSession = wkv::StoreSession<SegmentedDevice>;

  fn fixture(tag: &str) -> (TempDir, Arc<WedbStore<SegmentedDevice>>, TestSession) {
    let dir = tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
    let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();
    (dir, store, session)
  }

  /// rmw 回写契约：ZREM/ZREMRANGEBYLEX 变更持久化、空集合整键回收、
  /// 错误回复不建幻键、WRONGTYPE 不覆盖异类键、信封与 storage 会话域互通
  #[test]
  fn rmw_writeback_contract() {
    let (_dir, _store, session) = fixture("zsetwb.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    // ZREM 变更必须落库（此前 result1-only 操作从不回写）
    sess
      .sorted_set_add(&[b"z", b"1", b"a", b"2", b"b"], &batch, &mut out)
      .unwrap();
    out.clear();
    sess
      .sorted_set_remove(&[b"z", b"a"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    sess.sorted_set_length(&[b"z"], &batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    sess
      .sorted_set_score(&[b"z", b"a"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // ZREMRANGEBYLEX 变更同样落库，清空后整键回收
    sess
      .sorted_set_add(&[b"lx", b"0", b"aa", b"0", b"bb"], &batch, &mut out)
      .unwrap();
    out.clear();
    sess
      .sorted_set_remove_range(
        &[b"lx", b"[a", b"[bb"],
        &batch,
        &mut out,
        RemoveRangeKind::Lex,
      )
      .unwrap();
    assert_eq!(out, b":2\r\n");
    assert!(
      batch
        .try_read_sync(b"lx", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );

    // ZPOPMIN 清空既有集合 → 键回收（不留空集合信封）
    sess
      .sorted_set_add(&[b"pop", b"1", b"one"], &batch, &mut out)
      .unwrap();
    out.clear();
    sess
      .sorted_set_pop(&[b"pop"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*2\r\n$3\r\none\r\n$1\r\n1\r\n");
    assert!(
      batch
        .try_read_sync(b"pop", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );

    // ZADD 非法参数 → 错误回复不创建幻键
    out.clear();
    sess
      .sorted_set_add(&[b"ghost", b"ZZ", b"1", b"x"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"-ERR"), "{out:?}");
    assert!(
      batch
        .try_read_sync(b"ghost", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );

    // ZADD 作用在字符串键上 → WRONGTYPE 且原值保留（此前裸载荷会静默覆盖）
    let _ = batch.try_upsert_sync(b"str", b"plain-value");
    out.clear();
    sess
      .sorted_set_add(&[b"str", b"1", b"m"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"-WRONGTYPE"), "{out:?}");
    assert_eq!(
      batch
        .try_read_sync(b"str", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten(),
      Some(b"plain-value".to_vec())
    );

    // 信封互通（r2 契约）：RESP 写入 = [OBJ_TAG_SORTED_SET][wobject bitcode]
    sess
      .sorted_set_add(&[b"env", b"3", b"m"], &batch, &mut out)
      .unwrap();
    let raw = batch
      .try_read_sync(b"env", |v| v.to_vec())
      .ok()
      .flatten()
      .flatten()
      .expect("envelope value");
    assert_eq!(raw[0], OBJ_TAG_SORTED_SET);
    let from_storage = WoSortedSetObject::deserialize(&mut Cursor::new(&raw[1..])).unwrap();
    assert_eq!(
      from_storage.dict.pin().get(b"m".as_slice()).copied(),
      Some(3.0)
    );

    // 反向：storage 会话域写入的同构信封 RESP 层可读
    let wo = WoSortedSetObject::new();
    {
      let pin = wo.dict.pin();
      pin.insert(b"s".to_vec(), 7.5);
      wo.tree.lock().insert(WoSortedSetEntry {
        score: 7.5,
        member: b"s".to_vec(),
      });
    }
    let mut payload = Vec::new();
    wo.serialize(&mut payload).unwrap();
    let mut envelope = vec![OBJ_TAG_SORTED_SET];
    envelope.extend_from_slice(&payload);
    let _ = batch.try_upsert_sync(b"fromstore", &envelope);
    out.clear();
    sess
      .sorted_set_score(&[b"fromstore", b"s"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$3\r\n7.5\r\n");
  }

  #[test]
  fn zadd_options_and_score_roundtrip() {
    let (_dir, _store, session) = fixture("zset.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    // ZADD 基础
    sess
      .sorted_set_add(
        &[b"z", b"1", b"a", b"2", b"b", b"3", b"c"],
        &batch,
        &mut out,
      )
      .unwrap();
    assert_eq!(out, b":3\r\n");

    // NX：不更新既有
    out.clear();
    sess
      .sorted_set_add(&[b"z", b"NX", b"99", b"a", b"4", b"d"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // CH + GT：a 1→99、b 2→5 均过 GT 门槛 → CH 计 2
    out.clear();
    sess
      .sorted_set_add(
        &[b"z", b"GT", b"CH", b"99", b"a", b"5", b"b"],
        &batch,
        &mut out,
      )
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // ZSCORE 读回
    out.clear();
    sess
      .sorted_set_score(&[b"z", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\n5\r\n");

    // ZINCRBY
    out.clear();
    sess
      .sorted_set_increment(&[b"z", b"2.5", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$3\r\n7.5\r\n");

    // ZCARD
    out.clear();
    sess.sorted_set_length(&[b"z"], &batch, &mut out).unwrap();
    assert_eq!(out, b":4\r\n");

    // ZREM
    out.clear();
    sess
      .sorted_set_remove(&[b"z", b"a", b"zz"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  }

  #[test]
  fn zrange_and_lex_variants() {
    let (_dir, _store, session) = fixture("zrange.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .sorted_set_add(
        &[b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d"],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();

    use crate::objects::sortedset::sorted_set_object::SortedSetRangeOpts as Opts;
    // ZRANGE 0 1
    sess
      .sorted_set_range(&[b"z", b"0", b"1"], &batch, &mut out, Opts::NONE)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");

    // ZREVRANGE 0 1 WITHSCORES
    out.clear();
    sess
      .sorted_set_range(
        &[b"z", b"0", b"1", b"WITHSCORES"],
        &batch,
        &mut out,
        Opts::REVERSE,
      )
      .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n3\r\n");

    // ZRANGESTORE
    out.clear();
    sess
      .sorted_set_range_store(&[b"dst", b"z", b"0", b"1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    sess
      .sorted_set_range(&[b"dst", b"0", b"-1"], &batch, &mut out, Opts::NONE)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");

    // ZCOUNT (1 3)
    out.clear();
    sess
      .sorted_set_count(&[b"z", b"(1", b"3"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // ZLEXCOUNT：同分字典序
    out.clear();
    sess
      .sorted_set_add(
        &[b"lx", b"0", b"aa", b"0", b"bb", b"0", b"cc"],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();
    sess
      .sorted_set_length_by_value(&[b"lx", b"[a", b"(c"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
  }

  #[test]
  fn rank_pop_and_random() {
    let (_dir, _store, session) = fixture("zrank.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .sorted_set_add(
        &[b"z", b"10", b"x", b"20", b"y", b"30", b"z"],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();

    sess
      .sorted_set_rank(&[b"z", b"y"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    sess
      .sorted_set_rank(&[b"z", b"y"], &batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    sess
      .sorted_set_rank(&[b"z", b"missing"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // ZPOPMIN 单元素
    out.clear();
    sess
      .sorted_set_pop(&[b"z"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nx\r\n$2\r\n10\r\n");

    // ZPOPMAX COUNT 2
    out.clear();
    sess
      .sorted_set_pop(&[b"z", b"2"], &batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\nz\r\n$2\r\n30\r\n$1\r\ny\r\n$2\r\n20\r\n");

    // ZRANDMEMBER
    out.clear();
    sess
      .sorted_set_add(
        &[b"r", b"1", b"m1", b"2", b"m2", b"3", b"m3"],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();
    sess
      .sorted_set_random_member(&[b"r", b"2"], &batch, &mut out)
      .unwrap();
    assert_eq!(&out[..4], b"*2\r\n");
  }

  #[test]
  fn diff_intersect_union() {
    let (_dir, _store, session) = fixture("zcombo.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .sorted_set_add(
        &[b"a", b"1", b"m1", b"2", b"m2", b"3", b"m3"],
        &batch,
        &mut out,
      )
      .unwrap();
    sess
      .sorted_set_add(&[b"b", b"2", b"m2", b"4", b"m4"], &batch, &mut out)
      .unwrap();
    out.clear();

    // ZDIFF 2 a b
    sess
      .sorted_set_difference(&[b"2", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$2\r\nm1\r\n$2\r\nm3\r\n");

    // ZDIFF WITHSCORES
    out.clear();
    sess
      .sorted_set_difference(&[b"2", b"a", b"b", b"WITHSCORES"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*4\r\n$2\r\nm1\r\n$1\r\n1\r\n$2\r\nm3\r\n$1\r\n3\r\n");

    // ZDIFFSTORE
    out.clear();
    sess
      .sorted_set_difference_store(&[b"dst", b"2", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // ZINTER：交集 m2（a:2, b:2 → SUM 4）
    out.clear();
    sess
      .sorted_set_intersect(&[b"2", b"a", b"b", b"WITHSCORES"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$2\r\nm2\r\n$1\r\n4\r\n");

    // ZINTERCARD
    out.clear();
    sess
      .sorted_set_intersect_length(&[b"2", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // ZINTERSTORE
    out.clear();
    sess
      .sorted_set_intersect_store(&[b"ids", b"2", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // ZUNION：m1(1) m2(4) m3(3) m4(4)
    out.clear();
    sess
      .sorted_set_union(&[b"2", b"a", b"b", b"WITHSCORES"], &batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"*8\r\n$2\r\nm1\r\n$1\r\n1\r\n$2\r\nm3\r\n$1\r\n3\r\n$2\r\nm2\r\n$1\r\n4\r\n$2\r\nm4\r\n$1\r\n4\r\n"
    );

    // ZUNIONSTORE
    out.clear();
    sess
      .sorted_set_union_store(&[b"uds", b"2", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");
  }

  #[test]
  fn expire_family_and_mpop() {
    let (_dir, _store, session) = fixture("zexp.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .sorted_set_add(&[b"z", b"1", b"a", b"2", b"b"], &batch, &mut out)
      .unwrap();
    out.clear();

    // ZEXPIRE 1h a（不存在的成员 zz）
    sess
      .sorted_set_expire(
        &[b"z", b"3600", b"a", b"zz"],
        &batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:-2\r\n");

    // ZTTL 剩余秒（3590..3600）
    out.clear();
    sess
      .sorted_set_time_to_live(&[b"z", b"a"], &batch, &mut out, false, false)
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    let ttl: i64 = payload
      .lines()
      .nth(1)
      .unwrap()
      .trim_start_matches(':')
      .parse()
      .unwrap();
    assert!((3590..=3600).contains(&ttl), "ttl = {payload}");

    // ZPERSIST
    out.clear();
    sess
      .sorted_set_persist(&[b"z", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:-1\r\n");

    // ZMPOP MAX COUNT 1
    out.clear();
    sess
      .sorted_set_m_pop(&[b"1", b"z", b"MAX", b"COUNT", b"1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n");

    // 集合取空 → null
    out.clear();
    sess
      .sorted_set_m_pop(&[b"1", b"z", b"MAX"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n");
    out.clear();
    sess
      .sorted_set_m_pop(&[b"1", b"z", b"MAX"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");
  }

  #[test]
  fn blocking_pop_immediate_path() {
    let (_dir, _store, session) = fixture("zblock.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .sorted_set_add(&[b"z", b"5", b"e"], &batch, &mut out)
      .unwrap();
    out.clear();

    // BZPOPMIN 立即可取
    sess
      .sorted_set_blocking_pop(&[b"z", b"0.1"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*3\r\n$1\r\nz\r\n$1\r\ne\r\n$1\r\n5\r\n");

    // 空集合 → null
    out.clear();
    sess
      .sorted_set_blocking_pop(&[b"z", b"0.1"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // 非法 timeout
    out.clear();
    sess
      .sorted_set_blocking_pop(&[b"zz", b"abc"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR timeout is not a float or out of range\r\n");
  }
}
