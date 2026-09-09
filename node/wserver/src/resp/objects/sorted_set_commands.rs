//! 有序集合 RESP 命令（对标 libs/server/Resp/Objects/SortedSetCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`crate::objects::sortedset::sorted_set_object::SortedSetObject`] 的
//! operate/ObjectInput 通道（与 C# GarnetObjectBase.Operate 分层一致），
//! 载荷经 wobject 兼容的 bitcode `(member, score)` 编码与 wkv 存储互转。

use std::{io::Cursor, str};

use wobject::sorted_set::sorted_set_object::{
  SortedSetEntry as WoSortedSetEntry, SortedSetObject as WoSortedSetObject,
};

use crate::{
  arg_slice::ArgSlice,
  inputs::ObjectInput,
  objects::sortedset::sorted_set_object::{
    SortedSetObject, SortedSetOperation, SortedSetRangeOpts,
  },
  resp::{parser::resp_ext::RespSliceExt, resp_server_session::RespServerSession},
  session_parse_state::SessionParseState,
  input_header::RespInputHeader,
  types::GarnetObjectType,
  objects::types::object_output::ObjectOutput,
};

/// 本命令面统一按 RESP2 协议输出（C# respProtocolVersion 由会话下发，
/// 会话层接线时替换为实际协商版本）
const RESP_VERSION: u8 = 2;

/// 从 wkv 载荷装载有序集合对象
///
/// 载荷双格式：默认 wobject bitcode（与 storage 会话域兼容）；携带成员级过期时
/// 落 C# BinaryWriter 线格式（bitcode 无过期槽位），读取先试 bitcode 再回退
fn zset_from_blob(raw: &[u8]) -> SortedSetObject {
  if let Ok(wo) = WoSortedSetObject::deserialize(&mut Cursor::new(raw)) {
    let pin = wo.dict.pin();
    let entries: Vec<(Vec<u8>, f64)> = pin.iter().map(|(k, v)| (k.clone(), *v)).collect();
    return SortedSetObject::from_entries(entries);
  }
  // C# 线格式回退（成员级过期）
  SortedSetObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化回 wkv 兼容载荷（带成员过期时用 C# 线格式承载）
fn zset_to_blob(obj: &SortedSetObject) -> Vec<u8> {
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
    RespInputHeader::new_with_type(GarnetObjectType::SortedSet, crate::types::RespInputFlags::empty());
  header.set_sub_id(op as u8);
  (
    ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
    backing,
  )
}

/// 经对象层 operate 通道执行操作并回写 RESP 负载
fn operate(
  obj: &mut SortedSetObject,
  op: SortedSetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  output: &mut Vec<u8>,
) -> i64 {
  let (input, _backing) = make_input(op, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, RESP_VERSION);
  output.extend_from_slice(&obj_out.payload);
  obj_out.result1
}

/// 读-改-写：装载 → 操作 → 回写（变更时）
fn rmw(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  op: SortedSetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  output: &mut Vec<u8>,
) -> (Option<SortedSetObject>, i64) {
  let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
    Ok(Some(Some(raw))) => zset_from_blob(&raw),
    _ => SortedSetObject::new(),
  };

  let payload_start = output.len();
  let result1 = operate(&mut obj, op, args, arg1, arg2, output);

  // 回写条件：有 RESP 负载产出（只读操作不落库）或 result1 变更信号
  let wrote_payload = output.len() > payload_start;
  let read_only = matches!(
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
  );
  if wrote_payload && !read_only {
    let _ = store.try_upsert_sync(key, &zset_to_blob(&obj));
  }
  (Some(obj), result1)
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
    rmw(
      store,
      key,
      SortedSetOperation::Zadd,
      &parse_state[1..],
      0,
      0,
      output,
    );
    Ok(true)
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
    rmw(
      store,
      key,
      SortedSetOperation::Zscore,
      &parse_state[1..],
      0,
      0,
      output,
    );
    Ok(true)
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
    let (_, result1) = rmw(
      store,
      key,
      SortedSetOperation::Zrem,
      &parse_state[1..],
      0,
      0,
      output,
    );
    // C# SortedSetRemove 仅回填 result1，整数回复由 RESP 层写出
    output.write_resp_int(result1);
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
    let (_, result1) = rmw(store, key, SortedSetOperation::Zcard, &[], 0, 0, output);
    // C# SortedSetLength 仅回填 result1
    output.write_resp_int(result1);
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

    rmw(store, key, op, &[], arg1, 0, output);
    Ok(true)
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
    let obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => {
        output.extend_from_slice(b"*0\r\n");
        return Ok(true);
      }
    };

    operate(
      &mut obj.clone(),
      SortedSetOperation::Zrange,
      &parse_state[1..],
      0,
      range_opts.bits() as i32,
      output,
    );
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
    let mut src_obj = match store.try_read_sync(src_key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => {
        let _ = store.try_upsert_sync(dst_key, &zset_to_blob(&SortedSetObject::new()));
        output.extend_from_slice(b":0\r\n");
        return Ok(true);
      }
    };

    let mut obj_out = ObjectOutput::new();
    let (input, _backing) = make_input(
      SortedSetOperation::Zrange,
      &parse_state[2..],
      0,
      SortedSetRangeOpts::STORE.bits() as i32,
    );
    src_obj.operate(&input, &mut obj_out, 2);

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
      dst.sorted_set.insert(crate::objects::sortedset::sorted_set_object::SortedSetEntry { score, member });
    }

    let _ = store.try_upsert_sync(dst_key, &zset_to_blob(&dst));
    output.extend_from_slice(format!(":{}\r\n", dst.sorted_set_dict.len()).as_bytes());
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
    let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => {
        // 键缺失：全 null 数组
        output.write_resp_array_len(parse_state.len() - 1);
        for _ in 1..parse_state.len() {
          output.write_resp_null();
        }
        return Ok(true);
      }
    };

    operate(
      &mut obj,
      SortedSetOperation::Zmscore,
      &parse_state[1..],
      0,
      0,
      output,
    );
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
      let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
        Ok(Some(Some(raw))) => zset_from_blob(&raw),
        _ => continue,
      };
      if obj.count() == 0 {
        continue;
      }

      let popped: Vec<(f64, Vec<u8>)> = (0..count)
        .filter_map(|_| obj.pop_min_or_max(!low_scores_first))
        .collect();
      let _ = store.try_upsert_sync(key, &zset_to_blob(&obj));

      // 回复：[key, [[member, score], ...]]
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_array_len(popped.len());
      for (score, member) in &popped {
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(member);
        let s = format_double_text(*score);
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
    rmw(
      store,
      key,
      SortedSetOperation::Zcount,
      &parse_state[1..],
      0,
      0,
      output,
    );
    Ok(true)
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
    let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => {
        output.extend_from_slice(b":0\r\n");
        return Ok(true);
      }
    };

    let result1 = operate(
      &mut obj,
      SortedSetOperation::Zlexcount,
      &parse_state[1..],
      0,
      0,
      output,
    );
    // 解析失败标记（int.MaxValue）→ 错误回复；否则以 result1 作整数回复
    // （C# SortedSetRemoveOrCountRangeByLex 仅回填 result1，RESP 层负责写整数）
    if result1 == i32::MAX as i64 {
      output.clear();
      output.extend_from_slice(b"-ERR min or max not valid string range item\r\n");
    } else if result1 != i32::MIN as i64 {
      output.clear();
      output.write_resp_int(result1);
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
    rmw(
      store,
      key,
      SortedSetOperation::Zincrby,
      &parse_state[1..],
      0,
      0,
      output,
    );
    Ok(true)
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
    let with_score = parse_state.len() > 2
      && parse_state[2].eq_ignore_ascii_case(b"WITHSCORE");

    let op = if ascending {
      SortedSetOperation::Zrank
    } else {
      SortedSetOperation::Zrevrank
    };

    let obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => {
        output.write_resp_null();
        return Ok(true);
      }
    };

    let mut obj = obj;
    let payload_start = output.len();
    operate(&mut obj, op, &parse_state[1..2], if with_score { 1 } else { 0 }, 0, output);
    let _ = payload_start;
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
    let (_, result1) = rmw(store, key, op, &parse_state[1..], 0, 0, output);
    // ZREMRANGEBYLEX 仅回填 result1（int.MaxValue=参数错误、int.MinValue=部分执行）
    if range_kind == RemoveRangeKind::Lex {
      if result1 == i32::MAX as i64 {
        output.truncate(payload_start);
        output.extend_from_slice(b"-ERR min or max not valid string range item\r\n");
      } else if output.len() == payload_start && result1 != i32::MIN as i64 {
        output.write_resp_int(result1);
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
    let obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => {
        if parse_state.len() > 1 {
          output.extend_from_slice(b"*0\r\n");
        } else {
          output.write_resp_null();
        }
        return Ok(true);
      }
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

    let mut obj = obj;
    operate(&mut obj, SortedSetOperation::Zrandmember, &[], arg1 as i32, fastrand::i32(..), output);
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
    let mut result: Option<SortedSetObject> = None;
    for (i, key) in keys.iter().enumerate() {
      let obj = match store.try_read_sync(key, |v| v.to_vec()) {
        Ok(Some(Some(raw))) => zset_from_blob(&raw),
        _ => SortedSetObject::new(),
      };

      if i == 0 {
        result = Some(obj);
      } else if let Some(current) = result.take() {
        result = Some(dict_to_zset(SortedSetObject::copy_diff(
          Some(&current),
          Some(&obj),
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

    let result = diff_sets(store, &keys);
    let count = result.count();
    let _ = store.try_upsert_sync(dst, &zset_to_blob(&result));
    output.write_resp_int(count as i64);
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
    let Some((keys, weights, aggregate, with_scores)) =
      parse_combine_args(parse_state, "ZINTER", output, false)
    else {
      return Ok(true);
    };

    let result = combine_sets(store, &keys, &weights, aggregate, CombineKind::Intersect);
    write_zset_entries(Some(&result), with_scores, output);
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
    let result = combine_sets(
      store,
      keys,
      &weights,
      crate::storage::session::objectstore::sorted_set_ops::ZSetAggregate::Sum,
      CombineKind::Intersect,
    );

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
    let Some((keys, weights, aggregate, with_scores)) =
      parse_combine_args(parse_state, "ZUNION", output, false)
    else {
      return Ok(true);
    };

    let result = combine_sets(store, &keys, &weights, aggregate, CombineKind::Union);
    write_zset_entries(Some(&result), with_scores, output);
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
      let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
        Ok(Some(Some(raw))) => zset_from_blob(&raw),
        _ => continue,
      };
      if let Some((score, member)) = obj.pop_min_or_max(!is_min) {
        let _ = store.try_upsert_sync(key, &zset_to_blob(&obj));
        output.write_resp_array_len(3);
        output.write_resp_bulk_string(key);
        output.write_resp_bulk_string(&member);
        let s = format_double_text(score);
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
      let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
        Ok(Some(Some(raw))) => zset_from_blob(&raw),
        _ => continue,
      };
      if obj.count() == 0 {
        continue;
      }

      let popped: Vec<(f64, Vec<u8>)> = (0..count)
        .filter_map(|_| obj.pop_min_or_max(!low_scores_first))
        .collect();
      let _ = store.try_upsert_sync(key, &zset_to_blob(&obj));

      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_array_len(popped.len());
      for (score, member) in &popped {
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(member);
        let s = format_double_text(*score);
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
      let Some(opt) =
        crate::objects::parse_utils::try_get_expire_option(parse_state[curr_idx])
      else {
        break;
      };
      expire_option |= opt.bits();
      curr_idx += 1;
    }

    // .NET Ticks 目标时刻
    const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
    let now_ticks = crate::objects::parse_utils::now_ticks();
    let now_ms = now_ticks / 10_000 - UNIX_EPOCH_TICKS / 10_000;
    let expiration_ticks = if is_timestamp {
      UNIX_EPOCH_TICKS
        + expiration_base
          * if is_milliseconds { 10_000 } else { 10_000_000 }
    } else {
      now_ticks + expiration_base * if is_milliseconds { 10_000 } else { 10_000_000 }
    };
    let _ = now_ms;

    let e = crate::objects::sortedset::sorted_set_object::ExpirationWithOption::new(
      expiration_ticks,
      crate::objects::sortedset::sorted_set_object::ExpireOption::from_bits_truncate(expire_option),
    );

    let args: Vec<&[u8]> = parse_state[curr_idx..].to_vec();
    rmw(
      store,
      key,
      SortedSetOperation::Zexpire,
      &args,
      (e.word() >> 32) as i32,
      e.word() as i32,
      output,
    );
    Ok(true)
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
    rmw(
      store,
      key,
      SortedSetOperation::Zttl,
      &parse_state[1..],
      if is_milliseconds { 1 } else { 0 },
      if is_timestamp { 1 } else { 0 },
      output,
    );
    Ok(true)
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
    rmw(
      store,
      key,
      SortedSetOperation::Zpersist,
      &parse_state[1..],
      0,
      0,
      output,
    );
    Ok(true)
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
  let Some((keys, weights, aggregate, _)) =
    parse_combine_args(&parse_state[1..], name, output, true)
  else {
    return Ok(true);
  };

  let result = combine_sets(store, &keys, &weights, aggregate, kind);
  let count = result.count();
  let _ = store.try_upsert_sync(dst, &zset_to_blob(&result));
  output.write_resp_int(count as i64);
  Ok(true)
}

use crate::resp::parser::resp_ext::RespVecExt;

/// 双精度 → Redis 文本（ZADD 回显等）
fn format_double_text(value: f64) -> String {
  if value.is_nan() {
    "nan".to_string()
  } else if value.is_infinite() {
    if value > 0.0 { "inf" } else { "-inf" }.to_string()
  } else {
    format!("{value}")
  }
}

/// 成对负载解析（ZRANGESTORE 回读：member score member score ...）
fn parse_pairs_payload(payload: &[u8]) -> Vec<(Vec<u8>, f64)> {
  let mut pairs = Vec::new();
  let mut pos = 0;

  // 跳过外层数组头 *<n>

  if payload.first() == Some(&b'*') {
    if let Some(line_end) = find_crlf(payload, 0) {
      pos = line_end + 2;
    }
  }

  while pos < payload.len() {
    // $<len>\r\n<bytes>\r\n
    if payload[pos] != b'$' {
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

    // 第二项：bulk string 形式的分值
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

/// ZINTER/ZUNION 参数解析：numkeys key... [WEIGHTS w...] [AGGREGATE agg] [WITHSCORES]
fn parse_combine_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  output: &mut Vec<u8>,
  _store_form: bool,
) -> Option<(Vec<&'p [u8]>, Vec<f64>, ZSetAggregate, bool)> {
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
        match str::from_utf8(parse_state[idx]).unwrap_or("").parse::<f64>() {
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

  Some((keys, weights, aggregate, with_scores))
}

use std::collections::HashMap;

use crate::storage::session::objectstore::sorted_set_ops::ZSetAggregate;

/// 集合运算种类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineKind {
  Intersect,
  Union,
}

/// 多键装载
fn load_many(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  keys: &[&[u8]],
) -> Vec<SortedSetObject> {
  keys
    .iter()
    .map(|key| match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => SortedSetObject::new(),
    })
    .collect()
}

/// 字典 → 有序集合（保持 (score, member) 双索引）
fn dict_to_zset(dict: HashMap<Vec<u8>, f64, gxhash::GxBuildHasher>) -> SortedSetObject {
  let entries: Vec<(Vec<u8>, f64)> = dict.iter().map(|(k, v)| (k.clone(), *v)).collect();
  SortedSetObject::from_entries(entries)
}

/// ZDIFF 语义计算
fn diff_sets(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  keys: &[&[u8]],
) -> SortedSetObject {
  let objs = load_many(store, keys);
  let mut result: Option<SortedSetObject> = None;
  for (i, obj) in objs.iter().enumerate() {
    if i == 0 {
      result = Some(obj.clone());
    } else if let Some(current) = result.take() {
      result = Some(dict_to_zset(SortedSetObject::copy_diff(Some(&current), Some(obj))));
    }
  }
  result.unwrap_or_default()
}

/// ZINTER/ZUNION 语义计算（权重 + 聚合）
fn combine_sets(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  keys: &[&[u8]],
  weights: &[f64],
  aggregate: ZSetAggregate,
  kind: CombineKind,
) -> SortedSetObject {
  let objs = load_many(store, keys);

  let mut combined: HashMap<Vec<u8>, f64, gxhash::GxBuildHasher> = HashMap::with_hasher(gxhash::GxBuildHasher::default());
  for (i, obj) in objs.iter().enumerate() {
    let weight = weights.get(i).copied().unwrap_or(1.0);
    for (member, score) in obj.to_entries() {
      let weighted = score * weight;
      let next = match aggregate {
        ZSetAggregate::Sum => weighted,
        ZSetAggregate::Min => weighted,
        ZSetAggregate::Max => weighted,
      };
      combined
        .entry(member)
        .and_modify(|existing| {
          *existing = match aggregate {
            ZSetAggregate::Sum => *existing + next,
            ZSetAggregate::Min => (*existing).min(next),
            ZSetAggregate::Max => (*existing).max(next),
          };
        })
        .or_insert(next);
    }
  }

  if kind == CombineKind::Intersect {
    // 只保留出现在全部集合中的成员
    let mut counts: HashMap<Vec<u8>, usize, gxhash::GxBuildHasher> =
      HashMap::with_hasher(gxhash::GxBuildHasher::default());
    for obj in &objs {
      let mut seen: Vec<Vec<u8>> = obj.to_entries().into_iter().map(|(m, _)| m).collect();
      seen.sort_unstable();
      seen.dedup();
      for m in seen {
        *counts.entry(m).or_insert(0) += 1;
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
      let s = format_double_text(score);
      output.write_resp_bulk_string(s.as_bytes());
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;
  use tempfile::{TempDir, tempdir};
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  type TestSession = wkv::StoreSession<SegmentedDevice>;

  fn fixture(tag: &str) -> (TempDir, Arc<WedbStore<SegmentedDevice>>, TestSession) {
    let dir = tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
    let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();
    (dir, store, session)
  }

  #[test]
  fn zadd_options_and_score_roundtrip() {
    let (_dir, _store, session) = fixture("zset.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    // ZADD 基础
    sess
      .sorted_set_add(&[b"z", b"1", b"a", b"2", b"b", b"3", b"c"], &batch, &mut out)
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
      .sorted_set_add(&[b"z", b"GT", b"CH", b"99", b"a", b"5", b"b"], &batch, &mut out)
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
    let mut sess = RespServerSession;
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
    assert_eq!(
      out,
      b"*4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );

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
      .sorted_set_add(&[b"lx", b"0", b"aa", b"0", b"bb", b"0", b"cc"], &batch, &mut out)
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
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    sess
      .sorted_set_add(&[b"z", b"10", b"x", b"20", b"y", b"30", b"z"], &batch, &mut out)
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
    assert_eq!(
      out,
      b"*4\r\n$1\r\nz\r\n$2\r\n30\r\n$1\r\ny\r\n$2\r\n20\r\n"
    );

    // ZRANDMEMBER
    out.clear();
    sess
      .sorted_set_add(&[b"r", b"1", b"m1", b"2", b"m2", b"3", b"m3"], &batch, &mut out)
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
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    sess
      .sorted_set_add(&[b"a", b"1", b"m1", b"2", b"m2", b"3", b"m3"], &batch, &mut out)
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
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    sess
      .sorted_set_add(&[b"z", b"1", b"a", b"2", b"b"], &batch, &mut out)
      .unwrap();
    out.clear();

    // ZEXPIRE 1h a（不存在的成员 zz）
    sess
      .sorted_set_expire(&[b"z", b"3600", b"a", b"zz"], &batch, &mut out, false, false)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:-2\r\n");

    // ZTTL 剩余秒（3590..3600）
    out.clear();
    sess
      .sorted_set_time_to_live(&[b"z", b"a"], &batch, &mut out, false, false)
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    let ttl: i64 = payload.lines().nth(1).unwrap().trim_start_matches(':').parse().unwrap();
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
    let mut sess = RespServerSession;
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
