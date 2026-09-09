//! 有序集合命令（ZADD/ZSCORE/ZREM/ZCARD/ZPOPMIN/ZPOPMAX）
//!
//! 同步快路径：经 [`super::object_store_utils`] 信封读写 wobject
//! [`SortedSetObject`]，磁盘候选等须异步裁决时返回 `Ok(false)` 交调用方降级。

use std::io::Cursor;

use wobject::sorted_set::sorted_set_object::{SortedSetObject, SortedSetOperation};

use super::object_store_utils::{
  OBJ_TAG_SORTED_SET, SyncObj, format_score, obj_load_sync, obj_save_or_gc_sync, obj_save_sync,
  read_object_or_reply,
};
use crate::resp::{
  cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments, write_error_raw},
  parser::resp_ext::{RespSliceExt, RespVecExt},
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd
  ///
  /// 返回新增成员数（已存在成员改分不计入）
  pub fn sorted_set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // key + 偶数个 score/member（NX/XX/GT/LT/CH/INCR 选项形态未实现）
    if parse_state.len() < 3 || parse_state.len().is_multiple_of(2) {
      abort_with_wrong_number_of_arguments(output, "ZADD");
      return Ok(true);
    }
    let key = parse_state[0];

    // 分值严格解析（对标 C# 存储层 TryParse 失败报非法浮点）
    let mut pairs = Vec::with_capacity((parse_state.len() - 1) / 2);
    for i in (1..parse_state.len()).step_by(2) {
      let Some(score) = parse_state[i].try_parse_f64() else {
        abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
        return Ok(true);
      };
      pairs.push((score, parse_state[i + 1]));
    }

    let zset = match obj_load_sync(store, key, OBJ_TAG_SORTED_SET) {
      Ok(None) => return Ok(false),
      Ok(Some(SyncObj::Missing)) => SortedSetObject::new(),
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        SortedSetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut added = 0i64;
    for (score, member) in pairs {
      // 先查后写：原先不存在才计入新增
      if zset
        .operate(SortedSetOperation::Zscore, member, 0.0)
        .is_none()
      {
        added += 1;
      }
      zset.operate(SortedSetOperation::Zadd, member, score);
    }

    let mut payload = Vec::new();
    if zset.serialize(&mut payload).is_err() {
      output.write_resp_error("generic error");
      return Ok(true);
    }
    match obj_save_sync(store, key, OBJ_TAG_SORTED_SET, &payload) {
      Ok(true) => output.write_resp_int(added),
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetScore
  pub fn sorted_set_score<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "ZSCORE");
      return Ok(true);
    }
    let key = parse_state[0];
    let member = parse_state[1];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_SORTED_SET,
      output,
      |o| o.write_resp_null(),
      |p, o| {
        let zset = SortedSetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        match zset.operate(SortedSetOperation::Zscore, member, 0.0) {
          Some(score) => {
            let text = format_score(score);
            o.write_resp_bulk_string(text.as_bytes());
          }
          None => o.write_resp_null(),
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRemove
  ///
  /// 删空后整键回收（对齐 storage 层 finalize_removal）
  pub fn sorted_set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      abort_with_wrong_number_of_arguments(output, "ZREM");
      return Ok(true);
    }
    let key = parse_state[0];
    let members = &parse_state[1..];

    let zset = match obj_load_sync(store, key, OBJ_TAG_SORTED_SET) {
      Ok(None) => return Ok(false),
      Ok(Some(SyncObj::Missing)) => {
        output.write_resp_int(0);
        return Ok(true);
      }
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        SortedSetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut removed = 0i64;
    for member in members {
      if zset
        .operate(SortedSetOperation::Zrem, member, 0.0)
        .is_some()
      {
        removed += 1;
      }
    }
    if removed > 0 {
      let mut payload = Vec::new();
      if zset.serialize(&mut payload).is_err() {
        output.write_resp_error("generic error");
        return Ok(true);
      }
      match obj_save_or_gc_sync(store, key, OBJ_TAG_SORTED_SET, &payload, zset.count() == 0) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      }
    }
    output.write_resp_int(removed);
    Ok(true)
  }

  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetLength
  pub fn sorted_set_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "ZCARD");
      return Ok(true);
    }
    let key = parse_state[0];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_SORTED_SET,
      output,
      |o| o.write_resp_int(0),
      |p, o| {
        let zset = SortedSetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        o.write_resp_int(zset.count() as i64);
      },
    ))
  }

  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetPop（ZPOPMIN/ZPOPMAX 共体）
  ///
  /// 弹空后整键回收；应答为 member/score 交错的扁平数组
  pub fn sorted_set_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_min: bool,
  ) -> wresp::Result<bool> {
    let cmd_name = if is_min { "ZPOPMIN" } else { "ZPOPMAX" };
    if parse_state.is_empty() || parse_state.len() > 2 {
      abort_with_wrong_number_of_arguments(output, cmd_name);
      return Ok(true);
    }
    let key = parse_state[0];
    // C# popCount 缺省 -1 即弹 1 个
    let mut count = 1usize;
    if parse_state.len() == 2 {
      match parse_state[1].try_parse_i64() {
        Some(c) if c >= 0 => count = c as usize,
        // C#：解析失败或负数同为 RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE
        _ => {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
          return Ok(true);
        }
      }
    }

    let zset = match obj_load_sync(store, key, OBJ_TAG_SORTED_SET) {
      Ok(None) => return Ok(false),
      // C# NOTFOUND → 空数组
      Ok(Some(SyncObj::Missing)) => {
        output.write_resp_array_len(0);
        return Ok(true);
      }
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        SortedSetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut popped = Vec::new();
    for _ in 0..count {
      let item = if is_min {
        zset.pop_min()
      } else {
        zset.pop_max()
      };
      match item {
        Some((m, s)) => popped.push((m, s)),
        None => break,
      }
    }

    if !popped.is_empty() {
      let mut payload = Vec::new();
      if zset.serialize(&mut payload).is_err() {
        output.write_resp_error("generic error");
        return Ok(true);
      }
      match obj_save_or_gc_sync(store, key, OBJ_TAG_SORTED_SET, &payload, zset.count() == 0) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      }
    }

    output.write_resp_array_len(popped.len() * 2);
    for (m, s) in popped {
      output.write_resp_bulk_string(&m);
      let text = format_score(s);
      output.write_resp_bulk_string(text.as_bytes());
    }
    Ok(true)
  }

  // Stubs for the rest
  pub fn sorted_set_range() {
    unimplemented!()
  }
  pub fn sorted_set_range_store() {
    unimplemented!()
  }
  pub fn sorted_set_scores() {
    unimplemented!()
  }
  pub fn sorted_set_m_pop() {
    unimplemented!()
  }
  pub fn sorted_set_count() {
    unimplemented!()
  }
  pub fn sorted_set_length_by_value() {
    unimplemented!()
  }
  pub fn sorted_set_increment() {
    unimplemented!()
  }
  pub fn sorted_set_rank() {
    unimplemented!()
  }
  pub fn sorted_set_remove_range() {
    unimplemented!()
  }
  pub fn sorted_set_random_member() {
    unimplemented!()
  }
  pub fn sorted_set_difference() {
    unimplemented!()
  }
  pub fn sorted_set_difference_store() {
    unimplemented!()
  }
  pub fn sorted_set_intersect() {
    unimplemented!()
  }
  pub fn sorted_set_intersect_length() {
    unimplemented!()
  }
  pub fn sorted_set_intersect_store() {
    unimplemented!()
  }
  pub fn sorted_set_union() {
    unimplemented!()
  }
  pub fn sorted_set_union_store() {
    unimplemented!()
  }
  pub fn sorted_set_blocking_pop() {
    unimplemented!()
  }
  pub fn sorted_set_blocking_m_pop() {
    unimplemented!()
  }
  pub fn sorted_set_expire() {
    unimplemented!()
  }
  pub fn sorted_set_time_to_live() {
    unimplemented!()
  }
  pub fn sorted_set_persist() {
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use super::super::super::batch_harness::with_batch;

  /// WRONGTYPE 错误应答帧
  const WRONGTYPE: &[u8] =
    b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

  #[test]
  fn zadd_zscore_zcard_semantics() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .sorted_set_add(&[b"z", b"1.5", b"a", b"2", b"b"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":2\r\n");

      // 已存在成员改分不计新增
      let mut out = Vec::new();
      let _ = s
        .sorted_set_add(&[b"z", b"3", b"a"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = s.sorted_set_score(&[b"z", b"a"], batch, &mut out).unwrap();
      assert_eq!(out, b"$1\r\n3\r\n");
      let mut out = Vec::new();
      let _ = s.sorted_set_score(&[b"z", b"nx"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");
      let mut out = Vec::new();
      let _ = s.sorted_set_score(&[b"nk", b"a"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");

      let mut out = Vec::new();
      let _ = s.sorted_set_length(&[b"z"], batch, &mut out).unwrap();
      assert_eq!(out, b":2\r\n");
      let mut out = Vec::new();
      let _ = s.sorted_set_length(&[b"nk"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");
    });
  }

  #[test]
  fn zadd_rejects_invalid_score_and_wrongtype() {
    with_batch(|s, batch| {
      // 非法分值 → 报错（原实现 if true 死码吞掉了校验）
      let mut out = Vec::new();
      let _ = s
        .sorted_set_add(&[b"z", b"abc", b"m"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not a valid float\r\n");

      // 字符串键 → WRONGTYPE 不覆写
      let _ = s
        .network_set(&[b"str", b"v"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s
        .sorted_set_add(&[b"str", b"1", b"m"], batch, &mut out)
        .unwrap();
      assert_eq!(out, WRONGTYPE);
    });
  }

  #[test]
  fn zrem_recycles_empty_and_counts() {
    with_batch(|s, batch| {
      let _ = s
        .sorted_set_add(&[b"z", b"1", b"m"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s
        .sorted_set_remove(&[b"z", b"m", b"nx"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
      // 删空 → 整键回收
      assert_eq!(batch.try_read_sync(b"z", |_| ()).unwrap(), Some(None::<()>));
      // 键缺失 → :0
      let mut out = Vec::new();
      let _ = s.sorted_set_remove(&[b"z", b"m"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");
    });
  }

  #[test]
  fn zpopmin_zpopmax_and_validation() {
    with_batch(|s, batch| {
      let _ = s
        .sorted_set_add(&[b"z", b"1", b"a", b"2.5", b"b"], batch, &mut Vec::new())
        .unwrap();

      // 无 count：扁平 [member, score]
      let mut out = Vec::new();
      let _ = s.sorted_set_pop(&[b"z"], batch, &mut out, true).unwrap();
      assert_eq!(out, b"*2\r\n$1\r\na\r\n$1\r\n1\r\n");

      // 带 count：多对扁平输出，弹空整键回收（分值走 format_score 17 位口径）
      let mut out = Vec::new();
      let _ = s
        .sorted_set_pop(&[b"z", b"10"], batch, &mut out, false)
        .unwrap();
      assert_eq!(out, b"*2\r\n$1\r\nb\r\n$19\r\n2.50000000000000000\r\n");
      assert_eq!(batch.try_read_sync(b"z", |_| ()).unwrap(), Some(None::<()>));

      // 键缺失 → 空数组；负 count → out of range（对标 C#）
      let mut out = Vec::new();
      let _ = s.sorted_set_pop(&[b"nk"], batch, &mut out, true).unwrap();
      assert_eq!(out, b"*0\r\n");
      let mut out = Vec::new();
      let _ = s
        .sorted_set_pop(&[b"z", b"-1"], batch, &mut out, true)
        .unwrap();
      assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");
      let mut out = Vec::new();
      let _ = s
        .sorted_set_pop(&[b"z", b"x"], batch, &mut out, true)
        .unwrap();
      assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");

      // arity 错误带命令名
      let mut out = Vec::new();
      let _ = s.sorted_set_pop(&[], batch, &mut out, false).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'ZPOPMAX' command\r\n"
      );
    });
  }
}
